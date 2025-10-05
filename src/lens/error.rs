use reqwest::header::ToStrError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LensError {
    #[error("no valid ids in input list")]
    NoValidIdsInInputList,
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("unexpected response status: {0}")]
    LensApi(LensApiErrorInfo),
    #[error(transparent)]
    RequestToStr(#[from] ToStrError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("value as_str error")]
    ValueAsStr,
}

pub struct LensApiErrorInfo {
    pub status_code: u16,
    pub message: String,
}

impl std::fmt::Display for LensApiErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Status {}: {}", self.status_code, self.message)
    }
}

impl std::fmt::Debug for LensApiErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Status {}: {}", self.status_code, self.message)
    }
}
