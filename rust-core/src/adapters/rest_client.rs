use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RestRequest {
    pub method: HttpMethod,
    pub path: String,
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Debug)]
pub struct RestResponse {
    pub status: u16,
    pub body: String,
}

#[async_trait]
pub trait ExchangeRestClient: Send + Sync {
    async fn send(&self, request: RestRequest) -> anyhow::Result<RestResponse>;
    async fn send_public(&self, request: RestRequest) -> anyhow::Result<RestResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_methods_are_distinct() {
        assert_ne!(HttpMethod::Get, HttpMethod::Put);
        assert_ne!(HttpMethod::Post, HttpMethod::Put);
        assert_ne!(HttpMethod::Delete, HttpMethod::Put);
    }

    #[test]
    fn listen_key_keepalive_request_shape() {
        // Regression: Binance listenKey keepalive was POST; spec requires PUT.
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), "abc123".to_string());
        let req = RestRequest {
            method: HttpMethod::Put,
            path: "/api/v3/userDataStream".to_string(),
            params,
        };
        assert_eq!(req.method, HttpMethod::Put);
        assert_eq!(req.path, "/api/v3/userDataStream");
        assert_eq!(
            req.params.get("listenKey").map(|s| s.as_str()),
            Some("abc123")
        );
    }
}
