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

//! How the header names of a [crate::RequestHeader] or [crate::ResponseHeader] are written.
//!
//! Preserving the case of every name used to mean a second header map, parallel to the map of
//! values, holding the name as received for each value. Building, cloning and dropping that map
//! is a good part of the cost of handling a header. Yet the names of one message are nearly
//! always written in one style: all lower case (HTTP/2-era clients and servers, which a proxy
//! may face over HTTP/1) or capitalized after each `-` (browsers, curl, most servers).
//!
//! So a header now remembers a [Style] and the few names that do not follow it. A name the
//! style reproduces costs nothing to keep. Only when a message mixes too many styles, or one
//! name comes in two cases, does it fall back to the full map, built from the style and the
//! exceptions in the order of the value map. The bytes written are the same either way.

use crate::case_header_name::{titled_header_name_str, CaseHeaderName};
use crate::{CaseMap, HMap};
use bytes::BufMut;
use http::header::HeaderName;
use pingora_error::{ErrorType::InvalidHTTPHeader, OrErr, Result};
use std::sync::OnceLock;

/// Names kept apart from the style before the full map is cheaper to keep.
const MAX_EXCEPTIONS: usize = 8;

/// The case of the header names in a message.
#[derive(Debug, Clone)]
pub(crate) enum NameCase {
    /// Case is not preserved: well-known names are written capitalized, others in lower case.
    Off,
    /// Case is preserved: names follow `style`, except those in `exceptions`.
    Styled(Styled),
    /// Case is preserved name by name, in a map parallel to the value map.
    Map(CaseMap),
}

#[derive(Debug, Clone)]
pub(crate) struct Styled {
    style: Style,
    /// Names that do not follow the style, with the case of all their values.
    exceptions: Vec<(HeaderName, CaseHeaderName)>,
    /// The full map, built on demand for [NameCase::view()] and dropped on any change.
    view: OnceLock<CaseMap>,
}

/// How a name is written when it has no case of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
    /// Not decided yet: every name so far reads the same in both styles.
    Any,
    /// As [HeaderName::as_str()], all lower case.
    Lower,
    /// Upper case at the start and after every `-`, e.g. `X-Forwarded-For`.
    Title,
}

impl NameCase {
    /// Case preserved, no names yet.
    pub(crate) fn preserved() -> Self {
        NameCase::Styled(Styled {
            style: Style::Any,
            exceptions: Vec::new(),
            view: OnceLock::new(),
        })
    }

    pub(crate) fn is_preserved(&self) -> bool {
        !matches!(self, NameCase::Off)
    }

    /// Records the case of a value about to be appended under `name` to `values`.
    pub(crate) fn append<T>(
        &mut self,
        values: &HMap<T>,
        name: &HeaderName,
        case: CaseHeaderName,
    ) -> Result<()> {
        let styled = match self {
            NameCase::Off => return Ok(()),
            NameCase::Map(map) => return try_append(map, name, case),
            NameCase::Styled(styled) => styled,
        };
        styled.view.take();
        if let Some(i) = styled.exception(name) {
            if styled.exceptions[i].1.as_slice() == case.as_slice() {
                return Ok(());
            }
            // Values of one name in two cases: only the map can say which is which.
        } else if styled.follows(name, case.as_slice()) {
            return Ok(());
        } else if !values.contains_key(name) && styled.exceptions.len() < MAX_EXCEPTIONS {
            styled.exceptions.push((name.clone(), case));
            return Ok(());
        }
        let mut map = styled.to_map(values);
        let appended = try_append(&mut map, name, case);
        *self = NameCase::Map(map);
        appended
    }

    /// Records the case of a value about to replace every value under `name` in `values`.
    pub(crate) fn insert<T>(&mut self, values: &HMap<T>, name: &HeaderName, case: CaseHeaderName) {
        let styled = match self {
            NameCase::Off => return,
            NameCase::Map(map) => {
                map.insert(name.clone(), case);
                return;
            }
            NameCase::Styled(styled) => styled,
        };
        styled.view.take();
        if let Some(i) = styled.exception(name) {
            styled.exceptions.swap_remove(i);
        }
        if styled.follows(name, case.as_slice()) {
            return;
        }
        if styled.exceptions.len() < MAX_EXCEPTIONS {
            styled.exceptions.push((name.clone(), case));
            return;
        }
        let mut map = styled.to_map(values);
        map.insert(name.clone(), case);
        *self = NameCase::Map(map);
    }

    /// Forgets the names `values` no longer has, after a removal from it.
    pub(crate) fn removed<T>(
        &mut self,
        values: &HMap<T>,
        remove_from_map: impl FnOnce(&mut CaseMap),
    ) {
        match self {
            NameCase::Off => {}
            NameCase::Map(map) => remove_from_map(map),
            NameCase::Styled(styled) => {
                styled.view.take();
                styled
                    .exceptions
                    .retain(|(name, _)| values.contains_key(name));
            }
        }
    }

    /// Writes `values` with their names in HTTP/1.1 wire format.
    pub(crate) fn write_h1(&self, values: &HMap, buf: &mut impl BufMut) {
        const CRLF: &[u8; 2] = b"\r\n";
        const HEADER_KV_DELIMITER: &[u8; 2] = b": ";

        match self {
            NameCase::Map(map) => {
                for ((name, case), (name2, value)) in map.iter().zip(values.iter()) {
                    // in case the header iteration order changes in future versions of HMap
                    assert_eq!(name, name2, "header iter mismatch {}, {}", name, name2);
                    buf.put_slice(case.as_slice());
                    buf.put_slice(HEADER_KV_DELIMITER);
                    buf.put_slice(value.as_ref());
                    buf.put_slice(CRLF);
                }
            }
            NameCase::Styled(styled) => {
                for (name, value) in values {
                    styled.put_name(name, buf);
                    buf.put_slice(HEADER_KV_DELIMITER);
                    buf.put_slice(value.as_ref());
                    buf.put_slice(CRLF);
                }
            }
            NameCase::Off => {
                for (name, value) in values {
                    let titled = titled_header_name_str(name).unwrap_or(name.as_str());
                    buf.put_slice(titled.as_bytes());
                    buf.put_slice(HEADER_KV_DELIMITER);
                    buf.put_slice(value.as_ref());
                    buf.put_slice(CRLF);
                }
            }
        }
    }

    /// The case of every value in `values`, as a map parallel to it; `None` if case is not
    /// preserved.
    pub(crate) fn view<'a>(&'a self, values: &HMap) -> Option<&'a CaseMap> {
        match self {
            NameCase::Off => None,
            NameCase::Map(map) => Some(map),
            NameCase::Styled(styled) => Some(styled.view.get_or_init(|| styled.to_map(values))),
        }
    }
}

impl Styled {
    fn exception(&self, name: &HeaderName) -> Option<usize> {
        self.exceptions.iter().position(|(n, _)| n == name)
    }

    /// Whether `case` is `name` written in the style, which it may settle.
    fn follows(&mut self, name: &HeaderName, case: &[u8]) -> bool {
        let lower = name.as_str().as_bytes();
        match self.style {
            Style::Lower => lower == case,
            Style::Title => is_title_case(lower, case),
            Style::Any => match (lower == case, is_title_case(lower, case)) {
                (true, true) => true,
                (true, false) => {
                    self.style = Style::Lower;
                    true
                }
                (false, true) => {
                    self.style = Style::Title;
                    true
                }
                (false, false) => false,
            },
        }
    }

    fn put_name(&self, name: &HeaderName, buf: &mut impl BufMut) {
        if let Some((_, case)) = self.exceptions.iter().find(|(n, _)| n == name) {
            buf.put_slice(case.as_slice());
            return;
        }
        let lower = name.as_str().as_bytes();
        if self.style != Style::Title {
            buf.put_slice(lower);
            return;
        }
        // Converted in a stack buffer: one put_slice per piece rather than one put_u8 per byte.
        const PIECE: usize = 64;
        let mut piece = [0u8; PIECE];
        let mut prev = b'-';
        for part in lower.chunks(PIECE) {
            let out = &mut piece[..part.len()];
            to_title_case(part, prev, out);
            buf.put_slice(out);
            prev = part[part.len() - 1];
        }
    }

    fn case_of(&self, name: &HeaderName) -> CaseHeaderName {
        if let Some((_, case)) = self.exceptions.iter().find(|(n, _)| n == name) {
            return case.clone();
        }
        let lower = name.as_str().as_bytes();
        if self.style != Style::Title {
            return CaseHeaderName::from_slice(lower);
        }
        let mut titled = vec![0; lower.len()];
        to_title_case(lower, b'-', &mut titled);
        CaseHeaderName::from_slice(&titled)
    }

    /// The full map: one case per value, in the iteration order of `values`.
    ///
    /// [HMap] iterates its names in the order they were first added and the values of a name
    /// in the order they were added, so appending in that order rebuilds the same layout:
    /// later changes made to both maps keep them in step, as they do for a map kept from the
    /// start.
    fn to_map<T>(&self, values: &HMap<T>) -> CaseMap {
        let mut map = CaseMap::with_capacity(values.keys_len());
        for (name, _) in values {
            map.append(name.clone(), self.case_of(name));
        }
        map
    }
}

/// `b` of a lower case name in title case, given the byte before it (`-` at the start).
///
/// Branch free, so that the loops over a name below can be vectorized.
#[inline(always)]
fn title_byte(prev: u8, b: u8) -> u8 {
    let upper = (prev == b'-') & b.is_ascii_lowercase();
    b ^ ((upper as u8) << 5)
}

/// Writes `lower`, a piece of a lower case name after the byte `prev`, to `out` in title case.
#[inline]
fn to_title_case(lower: &[u8], prev: u8, out: &mut [u8]) {
    let (Some(&first), Some(head)) = (lower.first(), out.first_mut()) else {
        return;
    };
    *head = title_byte(prev, first);
    for ((o, &b), &p) in out[1..].iter_mut().zip(&lower[1..]).zip(lower) {
        *o = title_byte(p, b);
    }
}

/// Whether `case` is `lower`, a lower case name, in title case.
fn is_title_case(lower: &[u8], case: &[u8]) -> bool {
    let (Some(&first), Some(&case_first)) = (lower.first(), case.first()) else {
        return lower.len() == case.len();
    };
    if lower.len() != case.len() || title_byte(b'-', first) != case_first {
        return false;
    }
    let diff = lower[1..]
        .iter()
        .zip(lower)
        .zip(&case[1..])
        .fold(0u8, |acc, ((&b, &p), &c)| acc | (title_byte(p, b) ^ c));
    diff == 0
}

fn try_append(map: &mut CaseMap, name: &HeaderName, case: CaseHeaderName) -> Result<()> {
    // Use the non-panicking `try_append`: the infallible `append` calls
    // `.expect("size overflows MAX_SIZE")` internally, which would abort the
    // process if the case map ever exceeded `http`'s `MAX_SIZE` (1 << 15).
    map.try_append(name.clone(), case)
        .or_err(InvalidHTTPHeader, "header name map size overflows MAX_SIZE")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{CaseHeaderName, HMap, RequestHeader, ResponseHeader};
    use http::header::{HeaderName, HeaderValue};

    /// The former representation: a case map kept in step with the value map.
    #[derive(Default)]
    struct Model {
        names: HMap<CaseHeaderName>,
        values: HMap<HeaderValue>,
    }

    impl Model {
        fn append(&mut self, case: &str, value: &str) {
            let name = HeaderName::from_bytes(case.as_bytes()).unwrap();
            self.names
                .append(name.clone(), CaseHeaderName::from_slice(case.as_bytes()));
            self.values
                .append(name, HeaderValue::from_str(value).unwrap());
        }
        fn insert(&mut self, case: &str, value: &str) {
            let name = HeaderName::from_bytes(case.as_bytes()).unwrap();
            self.names
                .insert(name.clone(), CaseHeaderName::from_slice(case.as_bytes()));
            self.values
                .insert(name, HeaderValue::from_str(value).unwrap());
        }
        fn remove(&mut self, case: &str) {
            if self.values.remove(case).is_some() {
                self.names.remove(case);
            }
        }
        fn wire(&self) -> Vec<u8> {
            let mut buf = vec![];
            for ((_, case), (_, value)) in self.names.iter().zip(self.values.iter()) {
                buf.extend_from_slice(case.as_slice());
                buf.extend_from_slice(b": ");
                buf.extend_from_slice(value.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            buf
        }
    }

    fn wire(req: &RequestHeader) -> Vec<u8> {
        let mut buf = vec![];
        req.header_to_h1_wire(&mut buf);
        buf
    }

    fn iter_wire(req: &RequestHeader) -> Vec<u8> {
        let mut buf = vec![];
        for (case, value) in req.case_header_iter() {
            buf.extend_from_slice(case.as_slice());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(value.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    const NAMES: &[&str] = &[
        "host",
        "Host",
        "HOST",
        "x-foo",
        "X-Foo",
        "X-FOO",
        "x-Foo",
        "content-type",
        "Content-Type",
        "etag",
        "ETag",
        "Etag",
        "a",
        "A",
        "x-1",
        "X-1",
        "1-x",
        "1-X",
        "www-authenticate",
        "WWW-Authenticate",
        "Www-Authenticate",
        "x-a0",
        "X-A0",
        "x-a1",
        "X-A1",
        "x-a2",
        "X-A2",
        "x-a3",
        "X-A3",
        "x-a4",
        "X-A4",
        "x-a5",
        "X-A5",
        "x-a6",
        "X-A6",
        "x-a7",
        "X-A7",
        "x-a8",
        "X-A8",
        "x-a9",
        "X-A9",
        "X-Very-Long-Header-Name-That-Goes-On-And-On-For-More-Than-Sixty-Four-Bytes",
        "x-very-long-header-name-that-goes-on-and-on-for-more-than-sixty-four-bytes",
    ];

    /// Random sequences of appends, inserts and removes write the same bytes as the former
    /// case map, after every step and through a clone.
    #[test]
    fn matches_the_case_map_it_replaces() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        let mut seen = std::collections::HashSet::new();
        for round in 0..3000 {
            let mut model = Model::default();
            let mut req = RequestHeader::build("GET", b"/", None).unwrap();
            for step in 0..next(40) {
                let case = NAMES[next(NAMES.len())];
                let value = format!("v{round}-{step}");
                match next(20) {
                    0..=11 => {
                        model.append(case, &value);
                        req.append_header(case, value.as_str()).unwrap();
                    }
                    12..=16 => {
                        model.insert(case, &value);
                        req.insert_header(case, value.as_str()).unwrap();
                    }
                    _ => {
                        model.remove(case);
                        req.remove_header(case);
                    }
                }
                assert_eq!(wire(&req), model.wire(), "round {round} step {step}");
                seen.insert(kind(&req));
                if next(4) == 0 {
                    assert_eq!(iter_wire(&req), model.wire(), "round {round} step {step}");
                }
            }
            let copy = req.clone();
            assert_eq!(wire(&copy), model.wire(), "clone, round {round}");
        }
        // Every representation was exercised.
        assert_eq!(seen.len(), 3, "{seen:?}");
    }

    fn names_after<'a>(cases: impl IntoIterator<Item = &'a str>) -> RequestHeader {
        let mut req = RequestHeader::build("GET", b"/", None).unwrap();
        for case in cases {
            req.append_header(case.to_string(), "v").unwrap();
        }
        req
    }

    fn kind(req: &RequestHeader) -> &'static str {
        match &req.name_case {
            super::NameCase::Off => "off",
            super::NameCase::Styled(s) if s.exceptions.is_empty() => "style",
            super::NameCase::Styled(_) => "exceptions",
            super::NameCase::Map(_) => "map",
        }
    }

    #[test]
    fn common_messages_need_no_map() {
        // A client writing lower case, then a proxy adding capitalized names.
        let mut req = names_after(["host", "user-agent", "accept"]);
        req.insert_header("X-Forwarded-For", "192.0.2.1").unwrap();
        req.insert_header("X-Real-IP", "192.0.2.1").unwrap();
        assert_eq!(kind(&req), "exceptions");
        assert_eq!(
            wire(&req),
            b"host: v\r\nuser-agent: v\r\naccept: v\r\nX-Forwarded-For: 192.0.2.1\r\nX-Real-IP: 192.0.2.1\r\n"
        );
        // A server writing capitalized names.
        let resp_names = [
            "Server",
            "Date",
            "Content-Type",
            "Content-Length",
            "Connection",
        ];
        assert_eq!(kind(&names_after(resp_names)), "style");
        assert_eq!(kind(&names_after(["Server", "ETag"])), "exceptions");
    }

    #[test]
    fn falls_back_to_the_map() {
        // One name in two cases.
        let req = names_after(["Set-Cookie", "set-cookie"]);
        assert_eq!(kind(&req), "map");
        assert_eq!(wire(&req), b"Set-Cookie: v\r\nset-cookie: v\r\n");
        // Too many names off the style.
        let many: Vec<String> = (0..super::MAX_EXCEPTIONS + 1)
            .map(|i| format!("X-a{i}"))
            .collect();
        let req = names_after(["host"].into_iter().chain(many.iter().map(|s| s.as_str())));
        assert_eq!(kind(&req), "map");
        let mut want = b"host: v\r\n".to_vec();
        for name in &many {
            want.extend_from_slice(format!("{name}: v\r\n").as_bytes());
        }
        assert_eq!(wire(&req), want);
    }

    #[test]
    fn removing_a_name_forgets_its_case() {
        let mut req = names_after(["host", "X-Foo"]);
        req.remove_header("x-foo");
        assert_eq!(kind(&req), "style");
        req.append_header("x-foo", "w").unwrap();
        assert_eq!(wire(&req), b"host: v\r\nx-foo: w\r\n");
    }

    #[test]
    fn no_case_is_unchanged() {
        let mut resp = ResponseHeader::build_no_case(200, None).unwrap();
        resp.append_header("SERVER", "s").unwrap();
        resp.append_header("X-Foo", "f").unwrap();
        let mut buf = vec![];
        resp.header_to_h1_wire(&mut buf);
        assert_eq!(buf, b"Server: s\r\nx-foo: f\r\n");
        assert!(!resp.has_case());
        assert_eq!(resp.case_header_iter().count(), 0);
    }

    #[test]
    fn title_case_matches_a_plain_loop() {
        fn reference(lower: &[u8]) -> Vec<u8> {
            let mut out = lower.to_vec();
            for i in 0..out.len() {
                if i == 0 || lower[i - 1] == b'-' {
                    out[i] = out[i].to_ascii_uppercase();
                }
            }
            out
        }
        let mut names = vec![
            "a".to_string(),
            "-".to_string(),
            "x-forwarded-for".to_string(),
            "a--b".to_string(),
            "1x-2y".to_string(),
            "x-".to_string(),
        ];
        // Pieces of 64 bytes, with a '-' at the end of one and the start of the next.
        for len in [63, 64, 65, 127, 128, 129, 200] {
            for dash in [0, 62, 63, 64, 65, 127, 128] {
                let mut name = vec![b'k'; len];
                if dash < len {
                    name[dash] = b'-';
                }
                names.push(String::from_utf8(name).unwrap());
            }
        }
        for lower in &names {
            let expected = reference(lower.as_bytes());

            let mut req = RequestHeader::build("GET", b"/", None).unwrap();
            req.append_header(String::from_utf8(expected.clone()).unwrap(), "v")
                .unwrap();
            assert_eq!(kind(&req), "style", "{lower}");
            assert_eq!(wire(&req), [&expected[..], b": v\r\n"].concat(), "{lower}");

            assert!(super::is_title_case(lower.as_bytes(), &expected), "{lower}");
            let mut wrong = expected.clone();
            let last = wrong.len() - 1;
            wrong[last] ^= 0x20;
            if wrong[last].is_ascii_alphabetic() {
                assert!(!super::is_title_case(lower.as_bytes(), &wrong), "{lower}");
            }
            assert!(!super::is_title_case(lower.as_bytes(), &expected[1..]));
        }
    }
}
