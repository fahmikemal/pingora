// Copyright 2026 Cloudflare, Inc.
//
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

//! The bounded channel between the two halves of an HTTP/1 proxy session.
//!
//! `proxy_1to1` creates two of these for every request: one carries the request body to the
//! upstream half, the other carries the response back. Both halves are futures joined in one
//! task, so nothing ever contends for the channel. `tokio::sync::mpsc` serves many producers
//! across threads and its setup, permits and teardown cost more than the few tasks a request
//! passes. This channel is one allocation: a fixed ring behind a lock that is never contended.
//!
//! The API is the part of `tokio::sync::mpsc` the proxy uses, with the same semantics: a
//! [`Permit`] holds one slot until it sends or drops, [`Receiver::recv`] returns `None` once
//! the [`Sender`] is gone and the queue is empty, and sending fails once the receiver is gone.
//! There is no cooperative budgeting.
//!
//! Each end keeps one waker: whoever waits on an end must be one task at a time, as in the
//! proxy, where both halves are polled by the task that joins them. A second task waiting on
//! the same end would take the first one's place and leave it unwoken.

use std::cell::UnsafeCell;
use std::fmt;
use std::future::poll_fn;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// A lock for state that one task uses at a time. Taking it is one compare-and-swap and
/// releasing it a plain store, where `std::sync::Mutex` releases with another atomic swap to
/// look for sleepers; in this channel those two instructions were half its cost. Nobody sleeps
/// here: a second thread, if there ever were one, would spin through the few instructions the
/// holder runs.
struct SpinLock<T> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

// SAFETY: the value is reached only through a `SpinGuard`, and the `locked` flag lets one guard
// exist at a time, so this gives the same guarantees as `Mutex<T>`.
unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    fn new(value: T) -> Self {
        SpinLock {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    fn lock(&self) -> SpinGuard<'_, T> {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            while self.locked.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }
        }
        SpinGuard { lock: self }
    }
}

struct SpinGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Deref for SpinGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: this guard holds the lock, so no other reference to the value exists.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for SpinGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, and `&mut self` makes this the only use of the guard.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for SpinGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

/// The waker of whoever waits on one end. It is kept after a wake-up, so waiting again with the
/// same task clones nothing; `waiting` says whether it should be woken.
#[derive(Default)]
struct WakeSlot {
    waker: Option<Waker>,
    waiting: bool,
}

impl WakeSlot {
    fn register(&mut self, cx: &Context<'_>) {
        match &self.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => self.waker = Some(cx.waker().clone()),
        }
        self.waiting = true;
    }

    /// Wake the waiter, if any. Waking a task only schedules it, so this is safe under the lock.
    fn wake(&mut self) {
        if std::mem::take(&mut self.waiting) {
            if let Some(w) = &self.waker {
                w.wake_by_ref();
            }
        }
    }
}

struct State<T, const N: usize> {
    ring: [Option<T>; N],
    head: usize,
    len: usize,
    /// Slots held by outstanding permits.
    reserved: usize,
    rx_wake: WakeSlot,
    tx_wake: WakeSlot,
    rx_closed: bool,
    tx_closed: bool,
}

impl<T, const N: usize> State<T, N> {
    fn has_room(&self) -> bool {
        self.len + self.reserved < N
    }

    fn push(&mut self, value: T) {
        debug_assert!(self.len < N);
        self.ring[(self.head + self.len) % N] = Some(value);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let value = self.ring[self.head].take();
        self.head = (self.head + 1) % N;
        self.len -= 1;
        value
    }
}

type Shared<T, const N: usize> = Arc<SpinLock<State<T, N>>>;

/// A channel holding at most `N` values.
pub(crate) fn channel<T, const N: usize>() -> (Sender<T, N>, Receiver<T, N>) {
    let shared = Arc::new(SpinLock::new(State {
        ring: [const { None }; N],
        head: 0,
        len: 0,
        reserved: 0,
        rx_wake: WakeSlot::default(),
        tx_wake: WakeSlot::default(),
        rx_closed: false,
        tx_closed: false,
    }));
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

/// The sending half. There is only one: it is not `Clone`.
pub(crate) struct Sender<T, const N: usize> {
    shared: Shared<T, N>,
}

impl<T, const N: usize> Sender<T, N> {
    /// Whether the receiver is gone.
    pub fn is_closed(&self) -> bool {
        self.shared.lock().rx_closed
    }

    /// Take a slot now if one is free.
    pub fn try_reserve(&self) -> Result<Permit<'_, T, N>, TrySendError<()>> {
        let mut s = self.shared.lock();
        if s.rx_closed {
            return Err(TrySendError::Closed(()));
        }
        if !s.has_room() {
            return Err(TrySendError::Full(()));
        }
        s.reserved += 1;
        Ok(Permit { sender: self })
    }

    fn poll_reserve(&self, cx: &Context<'_>) -> Poll<Result<(), SendError<()>>> {
        let mut s = self.shared.lock();
        if s.rx_closed {
            return Poll::Ready(Err(SendError(())));
        }
        if s.has_room() {
            s.reserved += 1;
            return Poll::Ready(Ok(()));
        }
        s.tx_wake.register(cx);
        Poll::Pending
    }

    /// Wait for a free slot. Cancel safe: the slot is taken only by the poll that completes.
    pub async fn reserve(&self) -> Result<Permit<'_, T, N>, SendError<()>> {
        poll_fn(|cx| self.poll_reserve(cx)).await?;
        Ok(Permit { sender: self })
    }

    /// Wait for a free slot and send `value` in it.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.reserve().await {
            Ok(permit) => {
                permit.send(value);
                Ok(())
            }
            Err(_) => Err(SendError(value)),
        }
    }
}

impl<T, const N: usize> Drop for Sender<T, N> {
    fn drop(&mut self) {
        let mut s = self.shared.lock();
        s.tx_closed = true;
        s.rx_wake.wake();
    }
}

/// One reserved slot.
pub(crate) struct Permit<'a, T, const N: usize> {
    sender: &'a Sender<T, N>,
}

impl<T, const N: usize> Permit<'_, T, N> {
    /// Send `value` in the reserved slot. If the receiver is gone the value is dropped.
    pub fn send(self, value: T) {
        let sender = self.sender;
        // The slot passes to the value instead of going back.
        std::mem::forget(self);
        let mut s = sender.shared.lock();
        s.reserved -= 1;
        if s.rx_closed {
            drop(s);
            drop(value);
            return;
        }
        s.push(value);
        s.rx_wake.wake();
    }
}

impl<T, const N: usize> Drop for Permit<'_, T, N> {
    fn drop(&mut self) {
        let mut s = self.sender.shared.lock();
        s.reserved -= 1;
        s.tx_wake.wake();
    }
}

impl<T, const N: usize> fmt::Debug for Permit<'_, T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Permit")
    }
}

/// The receiving half.
pub(crate) struct Receiver<T, const N: usize> {
    shared: Shared<T, N>,
}

impl<T, const N: usize> Receiver<T, N> {
    fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Option<T>> {
        let mut s = self.shared.lock();
        if let Some(value) = s.pop() {
            s.tx_wake.wake();
            return Poll::Ready(Some(value));
        }
        if s.tx_closed {
            return Poll::Ready(None);
        }
        s.rx_wake.register(cx);
        Poll::Pending
    }

    /// The next value, or `None` once the sender is gone and nothing is left. Cancel safe.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next value if one is waiting; `Err(true)` when none will come any more.
    #[cfg(test)]
    pub fn try_recv(&mut self) -> Result<T, bool> {
        let mut s = self.shared.lock();
        match s.pop() {
            Some(value) => {
                s.tx_wake.wake();
                Ok(value)
            }
            None => Err(s.tx_closed),
        }
    }
}

impl<T, const N: usize> Drop for Receiver<T, N> {
    fn drop(&mut self) {
        let queued = {
            let mut s = self.shared.lock();
            s.rx_closed = true;
            s.len = 0;
            s.head = 0;
            s.tx_wake.wake();
            std::mem::replace(&mut s.ring, [const { None }; N])
        };
        drop(queued);
    }
}

/// The receiver is gone; the value that could not be sent is returned.
pub(crate) struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError { .. }")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel closed")
    }
}

impl<T> std::error::Error for SendError<T> {}

pub(crate) enum TrySendError<T> {
    /// No slot is free.
    Full(T),
    /// The receiver is gone.
    Closed(T),
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("Full(..)"),
            TrySendError::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("no available capacity"),
            TrySendError::Closed(_) => f.write_str("channel closed"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::{assert_pending, assert_ready, task};

    #[tokio::test]
    async fn values_arrive_in_order_within_capacity() {
        let (tx, mut rx) = channel::<u32, 4>();
        for round in 0..3 {
            let permits: Vec<_> = (0..4).map(|_| tx.try_reserve().unwrap()).collect();
            assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
            for (i, p) in permits.into_iter().enumerate() {
                p.send(round * 10 + i as u32);
            }
            assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
            for i in 0..4 {
                assert_eq!(rx.recv().await, Some(round * 10 + i));
            }
        }
        // A dropped permit gives its slot back.
        drop(
            (0..4)
                .map(|_| tx.try_reserve().unwrap())
                .collect::<Vec<_>>(),
        );
        tx.try_reserve().unwrap().send(7);
        assert_eq!(rx.try_recv(), Ok(7));
        assert_eq!(rx.try_recv(), Err(false));
    }

    #[test]
    fn reserve_waits_for_a_slot() {
        let (tx, mut rx) = channel::<u32, 2>();
        for i in 0..2 {
            tx.try_reserve().unwrap().send(i);
        }
        let mut reserve = task::spawn(tx.reserve());
        assert_pending!(reserve.poll());
        let mut recv = task::spawn(rx.recv());
        assert_eq!(assert_ready!(recv.poll()), Some(0));
        drop(recv);
        assert!(reserve.is_woken());
        assert_ready!(reserve.poll()).unwrap().send(2);
        drop(reserve);

        // A held permit takes a slot too, and dropping it wakes a waiting sender.
        let permit = tx.try_reserve();
        assert!(matches!(permit, Err(TrySendError::Full(()))));
        let mut recv = task::spawn(rx.recv());
        assert_eq!(assert_ready!(recv.poll()), Some(1));
        drop(recv);
        let held = tx.try_reserve().unwrap();
        let mut send = task::spawn(tx.send(3));
        assert_pending!(send.poll());
        drop(held);
        assert!(send.is_woken());
        assert_ready!(send.poll()).unwrap();
    }

    #[test]
    fn recv_waits_and_ends_after_the_sender() {
        let (tx, mut rx) = channel::<u32, 2>();
        let mut recv = task::spawn(rx.recv());
        assert_pending!(recv.poll());
        tx.try_reserve().unwrap().send(1);
        assert!(recv.is_woken());
        assert_eq!(assert_ready!(recv.poll()), Some(1));
        drop(recv);

        tx.try_reserve().unwrap().send(2);
        let mut recv = task::spawn(rx.recv());
        assert_eq!(assert_ready!(recv.poll()), Some(2));
        drop(recv);
        let mut recv = task::spawn(rx.recv());
        assert_pending!(recv.poll());
        drop(tx);
        assert!(recv.is_woken());
        assert_eq!(assert_ready!(recv.poll()), None);
    }

    #[test]
    fn queued_values_are_received_before_the_end() {
        let (tx, mut rx) = channel::<u32, 2>();
        tx.try_reserve().unwrap().send(1);
        tx.try_reserve().unwrap().send(2);
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(true));
    }

    #[test]
    fn sending_fails_once_the_receiver_is_gone() {
        let value = Arc::new(());
        let (tx, rx) = channel::<Arc<()>, 2>();
        tx.try_reserve().unwrap().send(value.clone());
        let permit = tx.try_reserve().unwrap();
        let mut send = task::spawn(tx.send(value.clone()));
        assert_pending!(send.poll());
        assert!(!tx.is_closed());
        drop(rx);
        // The queued value went with the receiver; the waiting sender gets its value back.
        assert!(send.is_woken());
        assert!(assert_ready!(send.poll()).is_err());
        drop(send);
        assert!(tx.is_closed());
        permit.send(value.clone());
        assert_eq!(Arc::strong_count(&value), 1);
        assert!(matches!(tx.try_reserve(), Err(TrySendError::Closed(()))));
        let mut reserve = task::spawn(tx.reserve());
        assert!(assert_ready!(reserve.poll()).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streams_between_tasks() {
        let (tx, mut rx) = channel::<u32, 4>();
        let producer = tokio::spawn(async move {
            for i in 0..10_000 {
                if i % 3 == 0 {
                    tx.reserve().await.unwrap().send(i);
                } else {
                    tx.send(i).await.unwrap();
                }
            }
        });
        let mut want = 0;
        while let Some(v) = rx.recv().await {
            assert_eq!(v, want);
            want += 1;
        }
        assert_eq!(want, 10_000);
        producer.await.unwrap();
    }
}
