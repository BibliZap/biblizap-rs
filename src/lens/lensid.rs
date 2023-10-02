use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LensId(pub String);

impl AsRef<str> for LensId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for LensId {
    fn from(s: &str) -> Self {
        LensId(s.to_string())
    }
}