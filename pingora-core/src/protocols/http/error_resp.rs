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

//! Error response generating utilities.

use http::header;
use once_cell::sync::Lazy;
use pingora_http::ResponseHeader;

use super::server_header;

/// Generate an error response with the given status code.
///
/// This error response has a zero `Content-Length` and `Cache-Control: private, no-store`,
/// and the `Server` header of [super::set_server_header].
pub fn gen_error_response(code: u16) -> ResponseHeader {
    let mut resp = ResponseHeader::build(code, Some(4)).unwrap();
    if let Some(server) = server_header() {
        resp.insert_header(header::SERVER, server).unwrap();
    }
    resp.insert_header(header::DATE, "Sun, 06 Nov 1994 08:49:37 GMT")
        .unwrap(); // placeholder
    resp.insert_header(header::CONTENT_LENGTH, "0").unwrap();
    resp.insert_header(header::CACHE_CONTROL, "private, no-store")
        .unwrap();
    resp
}

/// Pre-generated 502 response, with the `Server` header set when it was first used.
pub static HTTP_502_RESPONSE: Lazy<ResponseHeader> = Lazy::new(|| gen_error_response(502));
/// Pre-generated 400 response, with the `Server` header set when it was first used.
pub static HTTP_400_RESPONSE: Lazy<ResponseHeader> = Lazy::new(|| gen_error_response(400));

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn server_header_follows_the_setting() {
        assert_eq!(gen_error_response(400).headers[header::SERVER], "Pingora");
        crate::protocols::http::set_server_header(Some(HeaderValue::from_static("edge")));
        assert_eq!(gen_error_response(405).headers[header::SERVER], "edge");
        crate::protocols::http::set_server_header(None);
        let resp = gen_error_response(502);
        assert!(resp.headers.get(header::SERVER).is_none());
        assert_eq!(resp.headers[header::CONTENT_LENGTH], "0");
        crate::protocols::http::set_server_header(Some(HeaderValue::from_static("Pingora")));
    }
}
