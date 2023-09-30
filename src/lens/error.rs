use thiserror::Error;
use reqwest::header::ToStrError;

#[derive(Error, Debug)]
pub enum LensError {
    #[error("request error")]
    Request(#[from] reqwest::Error),
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("rate limit missing")]
    RateLimitMissing,
    #[error("request to str error")]
    RequestToStr(#[from] ToStrError),
    #[error("serde_json error")]
    SerdeJson(#[from] serde_json::Error),
    #[error("value as_str error")]
    ValueAsStr,
}