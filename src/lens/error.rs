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
    Not200(String),
    #[error(transparent)]
    RequestToStr(#[from] ToStrError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("value as_str error")]
    ValueAsStr,
}
