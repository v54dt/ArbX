use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RestRequest {
    pub method: HttpMethod,
    pub path: String,
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
pub enum HttpMethod {
    Get,
    Post,
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
