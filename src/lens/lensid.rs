use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct LensId(pub String);

impl AsRef<str> for LensId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
