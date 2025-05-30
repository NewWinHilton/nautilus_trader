// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! A high-performance HTTP client implementation.

use std::{collections::HashMap, hash::Hash, str::FromStr, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{HeaderValue, StatusCode, status::InvalidStatusCode};
use reqwest::{
    Method, Response, Url,
    header::{HeaderMap, HeaderName},
};

#[cfg(feature = "python")]
use crate::ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota};

/// Represents a HTTP status code.
///
/// Wraps [`http::StatusCode`] to expose a Python-compatible type and reuse
/// its validation and convenience methods.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.network")
)]
pub struct HttpStatus {
    inner: StatusCode,
}

impl HttpStatus {
    /// Create a new [`HttpStatus`] instance from a given [`StatusCode`].
    #[must_use]
    pub const fn new(code: StatusCode) -> Self {
        Self { inner: code }
    }

    /// Attempts to construct a [`HttpStatus`] from a `u16`.
    ///
    /// # Errors
    ///
    /// Returns an error if the code is not in the valid `100..999` range.
    pub fn from(code: u16) -> Result<Self, InvalidStatusCode> {
        Ok(Self {
            inner: StatusCode::from_u16(code)?,
        })
    }

    /// Returns the status code as a `u16` (e.g., `200` for OK).
    #[inline]
    #[must_use]
    pub const fn as_u16(&self) -> u16 {
        self.inner.as_u16()
    }

    /// Returns the three-digit ASCII representation of this status (e.g., `"200"`).
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }

    /// Checks if this status is in the 1xx (informational) range.
    #[inline]
    #[must_use]
    pub fn is_informational(&self) -> bool {
        self.inner.is_informational()
    }

    /// Checks if this status is in the 2xx (success) range.
    #[inline]
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.inner.is_success()
    }

    /// Checks if this status is in the 3xx (redirection) range.
    #[inline]
    #[must_use]
    pub fn is_redirection(&self) -> bool {
        self.inner.is_redirection()
    }

    /// Checks if this status is in the 4xx (client error) range.
    #[inline]
    #[must_use]
    pub fn is_client_error(&self) -> bool {
        self.inner.is_client_error()
    }

    /// Checks if this status is in the 5xx (server error) range.
    #[inline]
    #[must_use]
    pub fn is_server_error(&self) -> bool {
        self.inner.is_server_error()
    }
}

/// Represents the HTTP methods supported by the `HttpClient`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(eq, eq_int, module = "nautilus_trader.core.nautilus_pyo3.network")
)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
}

#[allow(clippy::from_over_into)]
impl Into<Method> for HttpMethod {
    fn into(self) -> Method {
        match self {
            Self::GET => Method::GET,
            Self::POST => Method::POST,
            Self::PUT => Method::PUT,
            Self::DELETE => Method::DELETE,
            Self::PATCH => Method::PATCH,
        }
    }
}

/// Represents the response from an HTTP request.
///
/// This struct encapsulates the status, headers, and body of an HTTP response,
/// providing easy access to the key components of the response.
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.network")
)]
pub struct HttpResponse {
    /// The HTTP status code.
    pub status: HttpStatus,
    /// The response headers as a map of key-value pairs.
    pub headers: HashMap<String, String>,
    /// The raw response body.
    pub body: Bytes,
}

/// Errors returned by the HTTP client.
///
/// Includes generic transport errors and timeouts.
#[derive(thiserror::Error, Debug)]
pub enum HttpClientError {
    #[error("HTTP error occurred: {0}")]
    Error(String),

    #[error("HTTP request timed out: {0}")]
    TimeoutError(String),
}

impl From<reqwest::Error> for HttpClientError {
    fn from(source: reqwest::Error) -> Self {
        if source.is_timeout() {
            Self::TimeoutError(source.to_string())
        } else {
            Self::Error(source.to_string())
        }
    }
}

impl From<String> for HttpClientError {
    fn from(value: String) -> Self {
        Self::Error(value)
    }
}

/// An HTTP client that supports rate limiting and timeouts.
///
/// Built on `reqwest` for async I/O. Allows per-endpoint and default quotas
/// through a rate limiter.
///
/// This struct is designed to handle HTTP requests efficiently, providing
/// support for rate limiting, timeouts, and custom headers. The client is
/// built on top of `reqwest` and can be used for both synchronous and
/// asynchronous HTTP requests.
#[derive(Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.network")
)]
pub struct HttpClient {
    /// The underlying HTTP client used to make requests.
    pub(crate) client: InnerHttpClient,
    /// The rate limiter to control the request rate.
    #[cfg(feature = "python")]
    pub(crate) rate_limiter: Arc<RateLimiter<String, MonotonicClock>>,
}

impl HttpClient {
    /// Creates a new [`HttpClient`] instance.
    #[must_use]
    pub fn new(
        headers: HashMap<String, String>,
        header_keys: Vec<String>,
        #[cfg(feature = "python")] keyed_quotas: Vec<(String, Quota)>,
        #[cfg(feature = "python")] default_quota: Option<Quota>,
        timeout_secs: Option<u64>,
    ) -> Self {
        // Build default headers
        let mut header_map = HeaderMap::new();
        for (key, value) in headers {
            let header_name = HeaderName::from_str(&key).expect("Invalid header name");
            let header_value = HeaderValue::from_str(&value).expect("Invalid header value");
            header_map.insert(header_name, header_value);
        }

        let mut client_builder = reqwest::Client::builder().default_headers(header_map);
        if let Some(timeout_secs) = timeout_secs {
            client_builder = client_builder.timeout(Duration::from_secs(timeout_secs));
        }

        let client = client_builder
            .build()
            .expect("Failed to build reqwest client");

        let client = InnerHttpClient {
            client,
            header_keys: Arc::new(header_keys),
        };

        #[cfg(feature = "python")]
        let rate_limiter = Arc::new(RateLimiter::new_with_quota(default_quota, keyed_quotas));

        Self {
            client,
            #[cfg(feature = "python")]
            rate_limiter,
        }
    }

    /// Sends an HTTP request.
    ///
    /// - `method`: The [`Method`] to use (GET, POST, etc.).
    /// - `url`: The target URL.
    /// - `headers`: Additional headers for this request.
    /// - `body`: Optional request body.
    /// - `keys`: Rate-limit keys to control request frequency.
    /// - `timeout_secs`: Optional request timeout in seconds.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to send request or times out.
    ///
    /// # Examples
    ///
    /// If requesting `/foo/bar`, pass rate-limit keys `["foo/bar", "foo"]`.
    #[allow(clippy::too_many_arguments)]
    pub async fn request(
        &self,
        method: Method,
        url: String,
        headers: Option<HashMap<String, String>>,
        body: Option<Vec<u8>>,
        timeout_secs: Option<u64>,
        #[cfg(feature = "python")] keys: Option<Vec<String>>,
    ) -> Result<HttpResponse, HttpClientError> {
        #[cfg(feature = "python")]
        {
            let rate_limiter = self.rate_limiter.clone();
            rate_limiter.await_keys_ready(keys).await;
        }

        self.client
            .send_request(method, url, headers, body, timeout_secs)
            .await
    }
}

/// Internal implementation backing [`HttpClient`].
///
/// The client is backed by a [`reqwest::Client`] which keeps connections alive and
/// can be cloned cheaply. The client also has a list of header fields to
/// extract from the response.
///
/// The client returns an [`HttpResponse`]. The client filters only the key value
/// for the give `header_keys`.
#[derive(Clone, Debug)]
pub struct InnerHttpClient {
    pub(crate) client: reqwest::Client,
    pub(crate) header_keys: Arc<Vec<String>>,
}

impl InnerHttpClient {
    /// Sends an HTTP request and returns an [`HttpResponse`].
    ///
    /// - `method`: The HTTP method (e.g. GET, POST).
    /// - `url`: The target URL.
    /// - `headers`: Extra headers to send.
    /// - `body`: Optional request body.
    /// - `timeout_secs`: Optional request timeout in seconds.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to send request or times out.
    pub async fn send_request(
        &self,
        method: Method,
        url: String,
        headers: Option<HashMap<String, String>>,
        body: Option<Vec<u8>>,
        timeout_secs: Option<u64>,
    ) -> Result<HttpResponse, HttpClientError> {
        let headers = headers.unwrap_or_default();
        let reqwest_url = Url::parse(url.as_str())
            .map_err(|e| HttpClientError::from(format!("URL parse error: {e}")))?;

        let mut header_map = HeaderMap::new();
        for (header_key, header_value) in &headers {
            let key = HeaderName::from_bytes(header_key.as_bytes())
                .map_err(|e| HttpClientError::from(format!("Invalid header name: {e}")))?;
            let _ = header_map.insert(
                key,
                header_value
                    .parse()
                    .map_err(|e| HttpClientError::from(format!("Invalid header value: {e}")))?,
            );
        }

        let mut request_builder = self.client.request(method, reqwest_url).headers(header_map);

        if let Some(timeout_secs) = timeout_secs {
            request_builder = request_builder.timeout(Duration::new(timeout_secs, 0));
        }

        let request = match body {
            Some(b) => request_builder
                .body(b)
                .build()
                .map_err(HttpClientError::from)?,
            None => request_builder.build().map_err(HttpClientError::from)?,
        };

        tracing::trace!("{request:?}");

        let response = self
            .client
            .execute(request)
            .await
            .map_err(HttpClientError::from)?;

        self.to_response(response).await
    }

    /// Converts a `reqwest::Response` into an `HttpResponse`.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to send request or times out.
    pub async fn to_response(&self, response: Response) -> Result<HttpResponse, HttpClientError> {
        tracing::trace!("{response:?}");

        let headers: HashMap<String, String> = self
            .header_keys
            .iter()
            .filter_map(|key| response.headers().get(key).map(|val| (key, val)))
            .filter_map(|(key, val)| val.to_str().map(|v| (key, v)).ok())
            .map(|(k, v)| (k.clone(), v.to_owned()))
            .collect();
        let status = HttpStatus::new(response.status());
        let body = response.bytes().await.map_err(HttpClientError::from)?;

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

impl Default for InnerHttpClient {
    /// Creates a new default [`InnerHttpClient`] instance.
    ///
    /// The default client is initialized with an empty list of header keys and a new `reqwest::Client`.
    fn default() -> Self {
        let client = reqwest::Client::new();
        Self {
            client,
            header_keys: Default::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
#[cfg(target_os = "linux")] // Only run network tests on Linux (CI stability)
mod tests {
    use std::net::{SocketAddr, TcpListener};

    use axum::{
        Router,
        routing::{delete, get, patch, post},
        serve,
    };
    use http::status::StatusCode;

    use super::*;

    fn get_unique_port() -> u16 {
        // Create a temporary TcpListener to get an available port
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("Failed to bind temporary TcpListener");
        let port = listener.local_addr().unwrap().port();

        // Close the listener to free up the port
        drop(listener);

        port
    }

    fn create_router() -> Router {
        Router::new()
            .route("/get", get(|| async { "hello-world!" }))
            .route("/post", post(|| async { StatusCode::OK }))
            .route("/patch", patch(|| async { StatusCode::OK }))
            .route("/delete", delete(|| async { StatusCode::OK }))
            .route("/notfound", get(|| async { StatusCode::NOT_FOUND }))
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    "Eventually responded"
                }),
            )
    }

    async fn start_test_server() -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
        let port = get_unique_port();
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            serve(listener, create_router()).await.unwrap();
        });

        Ok(addr)
    }

    #[tokio::test]
    async fn test_get() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}");

        let client = InnerHttpClient::default();
        let response = client
            .send_request(reqwest::Method::GET, format!("{url}/get"), None, None, None)
            .await
            .unwrap();

        assert!(response.status.is_success());
        assert_eq!(String::from_utf8_lossy(&response.body), "hello-world!");
    }

    #[tokio::test]
    async fn test_post() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}");

        let client = InnerHttpClient::default();
        let response = client
            .send_request(
                reqwest::Method::POST,
                format!("{url}/post"),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(response.status.is_success());
    }

    #[tokio::test]
    async fn test_post_with_body() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}");

        let client = InnerHttpClient::default();

        let mut body = HashMap::new();
        body.insert(
            "key1".to_string(),
            serde_json::Value::String("value1".to_string()),
        );
        body.insert(
            "key2".to_string(),
            serde_json::Value::String("value2".to_string()),
        );

        let body_string = serde_json::to_string(&body).unwrap();
        let body_bytes = body_string.into_bytes();

        let response = client
            .send_request(
                reqwest::Method::POST,
                format!("{url}/post"),
                None,
                Some(body_bytes),
                None,
            )
            .await
            .unwrap();

        assert!(response.status.is_success());
    }

    #[tokio::test]
    async fn test_patch() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}");

        let client = InnerHttpClient::default();
        let response = client
            .send_request(
                reqwest::Method::PATCH,
                format!("{url}/patch"),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(response.status.is_success());
    }

    #[tokio::test]
    async fn test_delete() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}");

        let client = InnerHttpClient::default();
        let response = client
            .send_request(
                reqwest::Method::DELETE,
                format!("{url}/delete"),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        assert!(response.status.is_success());
    }

    #[tokio::test]
    async fn test_not_found() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}/notfound");
        let client = InnerHttpClient::default();

        let response = client
            .send_request(reqwest::Method::GET, url, None, None, None)
            .await
            .unwrap();

        assert!(response.status.is_client_error());
        assert_eq!(response.status.as_u16(), 404);
    }

    #[tokio::test]
    async fn test_timeout() {
        let addr = start_test_server().await.unwrap();
        let url = format!("http://{addr}/slow");
        let client = InnerHttpClient::default();

        // We'll set a 1-second timeout for a route that sleeps 2 seconds
        let result = client
            .send_request(reqwest::Method::GET, url, None, None, Some(1))
            .await;

        match result {
            Err(HttpClientError::TimeoutError(msg)) => {
                println!("Got expected timeout error: {msg}");
            }
            Err(other) => panic!("Expected a timeout error, got: {other:?}"),
            Ok(resp) => panic!("Expected a timeout error, but got a successful response: {resp:?}"),
        }
    }
}
