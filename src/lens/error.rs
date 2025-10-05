use reqwest::header::ToStrError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LensError {
    #[error("All provided ids are invalid")]
    NoValidIdsInInputList,
    #[error("Lens API is unresponsive")]
    Request(#[from] reqwest::Error),
    #[error("Failed to extract rate limit information")]
    RateLimitExtraction(#[from] RateLimitExtractionError),
    #[error("{0}")]
    LensApi(LensApiErrorInfo),
    #[error("Failed to parse JSON response from Lens API : {0}")]
    SerdeJson(#[from] serde_json::Error),
}

#[derive(Error, Debug)]
pub enum RateLimitExtractionError {
    #[error(transparent)]
    RequestToStr(#[from] ToStrError),
    #[error(transparent)]
    ParseError(#[from] std::num::ParseIntError),
}

pub struct LensApiErrorInfo {
    pub status_code: u16,
    pub message: String,
}

// Shown to users
impl std::fmt::Display for LensApiErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lens API replied with status {}, please ask your administrator if his API key is valid", self.status_code)
    }
}

// Shown in server logs
impl std::fmt::Debug for LensApiErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Lens Api replied with status {}: {}",
            self.status_code, self.message
        )
    }
}
