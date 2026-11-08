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

use crate::*;
use bytes::Bytes;
use http::header;
use http::header::InvalidHeaderName;

#[derive(Debug, Clone)]
pub struct CaseHeaderName(Bytes);

impl CaseHeaderName {
    pub fn new(name: String) -> Self {
        CaseHeaderName(name.into())
    }
}

impl CaseHeaderName {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn from_slice(buf: &[u8]) -> Self {
        CaseHeaderName(Bytes::copy_from_slice(buf))
    }
}

/// A trait that converts into case-sensitive header names.
pub trait IntoCaseHeaderName {
    fn into_case_header_name(self) -> CaseHeaderName;

    /// The header name together with the case to write it in.
    ///
    /// By default the name is parsed out of [Self::into_case_header_name()]. Types that already
    /// hold a parsed [HeaderName] return it instead, so inserting a header by such a name skips
    /// parsing (and, for a name with upper case letters, a copy) every time.
    fn into_header_name_and_case(self) -> Result<(HeaderName, CaseHeaderName), InvalidHeaderName>
    where
        Self: Sized,
    {
        let case = self.into_case_header_name();
        let name = HeaderName::from_bytes(case.as_slice())?;
        Ok((name, case))
    }
}

/// A header name parsed once, together with the case it is written in on the wire.
///
/// Meant for names a proxy adds to every request or response, kept in a `static`:
///
/// ```
/// use pingora_http::{CasedHeaderName, RequestHeader};
///
/// static X_FORWARDED_FOR: CasedHeaderName =
///     CasedHeaderName::from_static("x-forwarded-for", "X-Forwarded-For");
///
/// let mut req = RequestHeader::build("GET", b"/", None).unwrap();
/// req.insert_header(&X_FORWARDED_FOR, "192.0.2.1").unwrap();
/// assert_eq!(req.headers["x-forwarded-for"], "192.0.2.1");
/// ```
#[derive(Debug, Clone)]
pub struct CasedHeaderName {
    name: HeaderName,
    case: CaseHeaderName,
}

impl CasedHeaderName {
    /// `name` is the header name in lower case, as [HeaderName::from_static()] takes it, and
    /// `case` the same name as written on the wire.
    ///
    /// # Panics
    ///
    /// If `name` is not a valid lower case header name, or `case` is not the same name. In a
    /// `static` or `const` that is a compile error.
    pub const fn from_static(name: &'static str, case: &'static str) -> Self {
        let (a, b) = (name.as_bytes(), case.as_bytes());
        assert!(a.len() == b.len(), "header name and its case differ");
        let mut i = 0;
        while i < a.len() {
            assert!(
                a[i] == b[i].to_ascii_lowercase(),
                "header name and its case differ"
            );
            i += 1;
        }
        CasedHeaderName {
            name: HeaderName::from_static(name),
            case: CaseHeaderName(Bytes::from_static(b)),
        }
    }

    /// The parsed header name.
    pub fn name(&self) -> &HeaderName {
        &self.name
    }
}

impl IntoCaseHeaderName for &CasedHeaderName {
    fn into_case_header_name(self) -> CaseHeaderName {
        self.case.clone()
    }

    fn into_header_name_and_case(self) -> Result<(HeaderName, CaseHeaderName), InvalidHeaderName> {
        Ok((self.name.clone(), self.case.clone()))
    }
}

impl IntoCaseHeaderName for CaseHeaderName {
    fn into_case_header_name(self) -> CaseHeaderName {
        self
    }
}

impl IntoCaseHeaderName for String {
    fn into_case_header_name(self) -> CaseHeaderName {
        CaseHeaderName(self.into())
    }
}

impl IntoCaseHeaderName for &'static str {
    fn into_case_header_name(self) -> CaseHeaderName {
        CaseHeaderName(self.into())
    }
}

impl IntoCaseHeaderName for HeaderName {
    fn into_case_header_name(self) -> CaseHeaderName {
        CaseHeaderName(titled_header_name(&self))
    }

    fn into_header_name_and_case(self) -> Result<(HeaderName, CaseHeaderName), InvalidHeaderName> {
        let case = CaseHeaderName(titled_header_name(&self));
        Ok((self, case))
    }
}

impl IntoCaseHeaderName for &HeaderName {
    fn into_case_header_name(self) -> CaseHeaderName {
        CaseHeaderName(titled_header_name(self))
    }

    fn into_header_name_and_case(self) -> Result<(HeaderName, CaseHeaderName), InvalidHeaderName> {
        Ok((self.clone(), CaseHeaderName(titled_header_name(self))))
    }
}

impl IntoCaseHeaderName for Bytes {
    fn into_case_header_name(self) -> CaseHeaderName {
        CaseHeaderName(self)
    }
}

fn titled_header_name(header_name: &HeaderName) -> Bytes {
    titled_header_name_str(header_name).map_or_else(
        || Bytes::copy_from_slice(header_name.as_str().as_bytes()),
        |s| Bytes::from_static(s.as_bytes()),
    )
}

pub(crate) fn titled_header_name_str(header_name: &HeaderName) -> Option<&'static str> {
    Some(match *header_name {
        header::ACCEPT_RANGES => "Accept-Ranges",
        header::AGE => "Age",
        header::CACHE_CONTROL => "Cache-Control",
        header::CONNECTION => "Connection",
        header::CONTENT_TYPE => "Content-Type",
        header::CONTENT_ENCODING => "Content-Encoding",
        header::CONTENT_LENGTH => "Content-Length",
        header::DATE => "Date",
        header::TRANSFER_ENCODING => "Transfer-Encoding",
        header::HOST => "Host",
        header::SERVER => "Server",
        header::SET_COOKIE => "Set-Cookie",
        // TODO: add more const header here to map to their titled case
        // TODO: automatically upper case the first letter?
        _ => {
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_name_and_case_agree_for_every_kind_of_name() {
        static FORWARDED: CasedHeaderName =
            CasedHeaderName::from_static("x-forwarded-for", "X-Forwarded-For");
        let cases: [(Result<(HeaderName, CaseHeaderName), _>, &str, &[u8]); 7] = [
            ("X-Foo".into_header_name_and_case(), "x-foo", b"X-Foo"),
            (
                "X-Foo".to_string().into_header_name_and_case(),
                "x-foo",
                b"X-Foo",
            ),
            (
                Bytes::from_static(b"x-FOO").into_header_name_and_case(),
                "x-foo",
                b"x-FOO",
            ),
            (
                header::SERVER.into_header_name_and_case(),
                "server",
                b"Server",
            ),
            (
                (&header::ACCEPT).into_header_name_and_case(),
                "accept",
                b"accept",
            ),
            (
                HeaderName::from_static("x-bar").into_header_name_and_case(),
                "x-bar",
                b"x-bar",
            ),
            (
                (&FORWARDED).into_header_name_and_case(),
                "x-forwarded-for",
                b"X-Forwarded-For",
            ),
        ];
        for (got, name, case) in cases {
            let (got_name, got_case) = got.unwrap();
            assert_eq!(got_name, name);
            assert_eq!(got_case.as_slice(), case);
        }
        assert!("bad name".into_header_name_and_case().is_err());
        assert_eq!(FORWARDED.name(), "x-forwarded-for");
    }

    #[test]
    #[should_panic(expected = "header name and its case differ")]
    fn cased_header_name_rejects_a_different_name() {
        CasedHeaderName::from_static("x-forwarded-for", "X-Forwarded-Host");
    }

    #[test]
    fn test_case_header_name() {
        assert_eq!("FoO".into_case_header_name().as_slice(), b"FoO");
        assert_eq!("FoO".to_string().into_case_header_name().as_slice(), b"FoO");
        assert_eq!(header::SERVER.into_case_header_name().as_slice(), b"Server");
    }
}
