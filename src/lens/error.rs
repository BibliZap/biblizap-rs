use thiserror::Error;
use reqwest::header::ToStrError;

#[derive(Error, Debug)]
pub enum LensError {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("rate limit missing")]
    RateLimitMissing,
    #[error(transparent)]
    RequestToStr(#[from] ToStrError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("value as_str error")]
    ValueAsStr,
}