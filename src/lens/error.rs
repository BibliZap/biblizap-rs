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
    #[error("{0}")]
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

// Shown to users
impl std::fmt::Display for LensApiErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lens Api replied with status {}, please ask your administrator if his api key is valid", self.status_code)
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
