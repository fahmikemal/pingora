// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Idle upstream streams kept for reuse.

use super::{test_reusable_stream, PoolCallback};
use crate::protocols::Stream;
use log::debug;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// How often idle streams are checked for being closed and for their idle timeout.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Idle streams to upstream servers, kept for reuse.
///
/// An idle stream has no task of its own. A stream is checked when it is taken out of the pool,
/// and by one periodic sweep per pool that closes streams the server closed and streams past
/// their idle timeout. Both checks read the stream without waiting: an idle stream stays
/// registered with the runtime's I/O driver, which keeps recording its readiness, so a FIN, an
/// RST or unexpected data that arrived while it sat here is seen by the next check.
///
/// A task per idle stream, as [pingora_pool::ConnectionPool] callers use, costs a task spawn, a
/// timer, a mutex around the stream, a oneshot channel, and a hand-off between that task and
/// the request that reuses the stream, for every request on a kept-alive upstream connection.
/// The price of not having it: a stream the server closed stays open on this side until the
/// next sweep, and an idle timeout closes the stream up to one sweep interval late. A stream
/// past its idle timeout is never handed out.
pub(crate) struct IdlePool {
    inner: Mutex<Inner>,
    capacity: usize,
    unexpected_data_conn_count: Arc<AtomicU64>,
    sweeper_running: AtomicBool,
}

#[derive(Default)]
struct Inner {
    /// Idle streams per reuse key, oldest first.
    by_key: HashMap<u64, VecDeque<Idle>>,
    len: usize,
}

struct Idle {
    stream: Stream,
    since: Instant,
    /// `None`: no idle timeout.
    expires: Option<Instant>,
}

impl Idle {
    fn expired(&self, now: Instant) -> bool {
        self.expires.is_some_and(|t| t <= now)
    }
}

impl Inner {
    /// Removes the least recently released stream across all keys.
    ///
    /// Linear in the number of keys, and only called when the pool is over capacity.
    fn pop_oldest(&mut self) -> Option<Idle> {
        let (_, key) = self
            .by_key
            .iter()
            .filter_map(|(key, list)| list.front().map(|idle| (idle.since, *key)))
            .min()?;
        let idle = self.by_key.get_mut(&key)?.pop_front()?;
        self.len -= 1;
        Some(idle)
    }
}

impl IdlePool {
    /// A pool that keeps at most `capacity` idle streams, closing the least recently released
    /// one to make room.
    pub fn new(capacity: usize, unexpected_data_conn_count: Arc<AtomicU64>) -> Self {
        IdlePool {
            inner: Mutex::new(Inner::default()),
            capacity,
            unexpected_data_conn_count,
            sweeper_running: AtomicBool::new(false),
        }
    }

    /// Takes the most recently released stream for `key` that is within its idle timeout and
    /// passes `usable`. Streams that fail either are closed.
    pub fn take(&self, key: u64, mut usable: impl FnMut(&mut Stream) -> bool) -> Option<Stream> {
        loop {
            // Checked outside the lock: `usable` may read the stream.
            let mut idle = {
                let mut inner = self.inner.lock();
                let idle = inner.by_key.get_mut(&key)?.pop_back()?;
                inner.len -= 1;
                idle
            };
            if idle.expired(Instant::now()) {
                debug!("idle stream is past its idle timeout");
                continue;
            }
            if usable(&mut idle.stream) {
                return Some(idle.stream);
            }
        }
    }

    /// Keeps `stream` for reuse under `key`, for at most `idle_timeout`.
    ///
    /// The caller has checked that the stream is reusable. `on_evict` is called with how long a
    /// stream was idle when it is closed to make room for this one.
    pub fn put(
        self: &Arc<Self>,
        key: u64,
        stream: Stream,
        idle_timeout: Option<Duration>,
        on_evict: Option<&PoolCallback>,
    ) {
        if self.capacity == 0 || idle_timeout.is_some_and(|t| t.is_zero()) {
            return;
        }
        let now = Instant::now();
        let idle = Idle {
            stream,
            since: now,
            // A timeout too large for an Instant is no timeout.
            expires: idle_timeout.and_then(|t| now.checked_add(t)),
        };
        let evicted = {
            let mut inner = self.inner.lock();
            inner.by_key.entry(key).or_default().push_back(idle);
            inner.len += 1;
            if inner.len > self.capacity {
                inner.pop_oldest()
            } else {
                None
            }
        };
        if let Some(evicted) = evicted {
            debug!("idle stream evicted to stay within the pool size");
            if let Some(callback) = on_evict {
                callback(evicted.since.elapsed());
            }
        }
        self.ensure_sweeper();
    }

    /// Closes the streams that are past their idle timeout or no longer reusable.
    fn sweep(&self) {
        let now = Instant::now();
        let mut closed = Vec::new();
        {
            let mut inner = self.inner.lock();
            for list in inner.by_key.values_mut() {
                let mut i = 0;
                while i < list.len() {
                    let idle = &mut list[i];
                    if idle.expired(now)
                        || !test_reusable_stream(&mut idle.stream, &self.unexpected_data_conn_count)
                    {
                        closed.extend(list.remove(i));
                    } else {
                        i += 1;
                    }
                }
            }
            inner.by_key.retain(|_, list| !list.is_empty());
            inner.len -= closed.len();
        }
        if !closed.is_empty() {
            debug!("closed {} idle streams", closed.len());
        }
        // `closed` is dropped here, outside the lock.
    }

    /// Starts the sweeping task unless one is running.
    ///
    /// The task holds only a weak reference, so it ends once the pool is dropped. If its
    /// runtime shuts down first, the next `put` starts it again on the current runtime.
    fn ensure_sweeper(self: &Arc<Self>) {
        if self.sweeper_running.load(Ordering::Relaxed)
            || self.sweeper_running.swap(true, Ordering::AcqRel)
        {
            return;
        }

        struct Running(Weak<IdlePool>);
        impl Drop for Running {
            fn drop(&mut self) {
                if let Some(pool) = self.0.upgrade() {
                    pool.sweeper_running.store(false, Ordering::Release);
                }
            }
        }

        let running = Running(Arc::downgrade(self));
        pingora_runtime::current_handle().spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // the first tick completes immediately
            loop {
                interval.tick().await;
                let Some(pool) = running.0.upgrade() else {
                    return;
                };
                pool.sweep();
            }
        });
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    fn pool(capacity: usize) -> Arc<IdlePool> {
        Arc::new(IdlePool::new(capacity, Arc::new(AtomicU64::new(0))))
    }

    /// A stream and the server's end of it.
    fn pair() -> (Stream, DuplexStream) {
        let (server, client) = tokio::io::duplex(64);
        (Box::new(client), server)
    }

    fn reusable(pool: &IdlePool) -> impl FnMut(&mut Stream) -> bool + '_ {
        |s| test_reusable_stream(s, &pool.unexpected_data_conn_count)
    }

    /// Whether `stream` is connected to `server`: test streams all share one ID.
    async fn connects(mut stream: Stream, server: &mut DuplexStream) -> bool {
        stream.write_all(b"x").await.unwrap();
        let mut buf = [0; 1];
        matches!(server.read(&mut buf).now_or_never(), Some(Ok(1)))
    }

    #[tokio::test]
    async fn takes_the_most_recently_released_stream_of_the_key() {
        let pool = pool(8);
        let (a, mut sa) = pair();
        let (b, mut sb) = pair();
        let (c, _sc) = pair();
        pool.put(1, a, None, None);
        pool.put(1, b, None, None);
        pool.put(2, c, None, None);

        assert!(connects(pool.take(1, reusable(&pool)).unwrap(), &mut sb).await);
        assert!(connects(pool.take(1, reusable(&pool)).unwrap(), &mut sa).await);
        assert!(pool.take(1, reusable(&pool)).is_none());
        assert!(pool.take(3, reusable(&pool)).is_none());
        assert_eq!(pool.len(), 1);
    }

    #[tokio::test]
    async fn closed_and_unusable_streams_are_skipped() {
        let pool = pool(8);
        let (a, mut sa) = pair();
        let (b, sb) = pair();
        pool.put(1, a, None, None);
        pool.put(1, b, None, None);
        drop(sb); // the server closed `b`

        assert!(connects(pool.take(1, reusable(&pool)).unwrap(), &mut sa).await);
        assert_eq!(pool.len(), 0);

        // a stream the caller rejects (e.g. a peer mismatch) is closed, not put back
        let (c, _sc) = pair();
        pool.put(1, c, None, None);
        assert!(pool.take(1, |_| false).is_none());
        assert_eq!(pool.len(), 0);
    }

    #[tokio::test]
    async fn expired_streams_are_never_handed_out() {
        let pool = pool(8);
        let (a, _sa) = pair();
        pool.put(1, a, Some(Duration::from_millis(20)), None);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(pool.take(1, reusable(&pool)).is_none());

        // zero means no keep-alive
        let (b, _sb) = pair();
        pool.put(1, b, Some(Duration::ZERO), None);
        assert_eq!(pool.len(), 0);

        // a timeout too large for an Instant means none
        let (c, _sc) = pair();
        pool.put(1, c, Some(Duration::MAX), None);
        assert!(pool.take(1, reusable(&pool)).is_some());
    }

    #[tokio::test]
    async fn over_capacity_evicts_the_oldest_stream_of_any_key() {
        let pool = pool(2);
        let evicted = Arc::new(AtomicU64::new(0));
        let counter = evicted.clone();
        let callback: PoolCallback = Arc::new(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        let (a, _sa) = pair();
        let (b, mut sb) = pair();
        let (c, mut sc) = pair();
        pool.put(1, a, None, Some(&callback));
        pool.put(2, b, None, Some(&callback));
        pool.put(2, c, None, Some(&callback));

        assert_eq!(pool.len(), 2);
        assert_eq!(evicted.load(Ordering::Relaxed), 1);
        assert!(
            pool.take(1, reusable(&pool)).is_none(),
            "key 1 held the oldest"
        );
        assert!(connects(pool.take(2, reusable(&pool)).unwrap(), &mut sc).await);
        assert!(connects(pool.take(2, reusable(&pool)).unwrap(), &mut sb).await);

        let zero = self::pool(0);
        let (d, _sd) = pair();
        zero.put(1, d, None, None);
        assert_eq!(zero.len(), 0);
    }

    #[tokio::test]
    async fn sweep_closes_closed_expired_and_chatty_streams() {
        let pool = pool(8);
        let (closed, server_closed) = pair();
        let (expired, _s1) = pair();
        let (chatty, mut server_chatty) = pair();
        let (fine, mut server_fine) = pair();
        pool.put(1, closed, None, None);
        pool.put(1, expired, Some(Duration::from_millis(10)), None);
        pool.put(2, chatty, None, None);
        pool.put(3, fine, None, None);
        drop(server_closed);
        server_chatty.write_all(b"surprise").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        pool.sweep();
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.unexpected_data_conn_count.load(Ordering::Relaxed), 1);
        assert_eq!(pool.inner.lock().by_key.len(), 1, "empty keys are dropped");
        assert!(connects(pool.take(3, reusable(&pool)).unwrap(), &mut server_fine).await);
    }

    #[tokio::test]
    async fn the_sweeper_runs_on_its_own_and_ends_with_the_pool() {
        tokio::time::pause();
        let pool = pool(8);
        let (a, server) = pair();
        pool.put(1, a, None, None);
        assert!(pool.sweeper_running.load(Ordering::Relaxed));
        drop(server);
        tokio::time::sleep(SWEEP_INTERVAL * 2).await;
        assert_eq!(pool.len(), 0, "the sweeper closed it");

        let weak = Arc::downgrade(&pool);
        drop(pool);
        tokio::time::sleep(SWEEP_INTERVAL * 2).await;
        assert!(weak.upgrade().is_none());
    }
}
