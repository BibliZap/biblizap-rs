use arrayvec::ArrayString;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LensId(pub ArrayString<19>);

impl AsRef<str> for LensId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for LensId {
    fn from(s: &str) -> Self {
        LensId(ArrayString::from(s).expect("lens id string must be 19 characters"))
    }
}