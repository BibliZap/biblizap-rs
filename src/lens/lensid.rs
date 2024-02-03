use std::marker::PhantomData;

use arrayvec::ArrayString;
use serde::{Serialize, Deserialize, Deserializer};
use serde::de::{self, Visitor};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LensIdError {
    #[error("LensID candidate string is not 19 characters long")]
    Not19Characters,
    #[error("LensID is zero")]
    ZeroLensID
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LensId {
    int: u64,
    string: ArrayString<19>,
}



#[derive(Debug, Default)]
pub struct References(pub Vec<LensId>);

impl<'de> Deserialize<'de> for LensId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        let visitor = LensIdVisitor(PhantomData);
        deserializer.deserialize_str(visitor)
    }
}

struct LensIdVisitor(PhantomData<fn() -> LensId>); 
impl<'de> Visitor<'de> for LensIdVisitor {
    type Value = LensId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a vec of keys and values")
    }

    fn visit_str<E>(self, lensid_str: &str) -> Result<LensId, E>
    where
        E: de::Error,
    {   
        let lensid = LensId::try_from(lensid_str)
            .ok().ok_or_else(||de::Error::missing_field("type"))?;
        
        Ok(lensid)
    }
}

impl TryFrom<&str> for LensId {
    type Error = LensIdError;
    fn try_from(lensid_str: &str) -> Result<Self, Self::Error> {
        if lensid_str.len() != 19 {
            return Err(LensIdError::Not19Characters);
        }

        let lensid = Self::from_str_unchecked(lensid_str);

        match lensid.int {
            0 => Err(LensIdError::ZeroLensID),
            _ => Ok(lensid)
        }
    }
}

impl AsRef<str> for LensId {
    fn as_ref(&self) -> &str {
        self.string.as_str()
    }
}

impl std::hash::Hash for LensId {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        hasher.write_u64(self.int)
    }
}
impl nohash_hasher::IsEnabled for LensId {}

impl LensId {
    pub fn from_str_unchecked(lensid_str: &str) -> Self {
        let lensid_int: u64 = lensid_str
            .chars()
            .take(18)
            .filter_map(|c| c.to_digit(10))
            .fold(0, |acc, digit| acc * 10 + (digit as u64));

        LensId {
            string: ArrayString::from(lensid_str).unwrap(),
            int: lensid_int
        }
    }
}
