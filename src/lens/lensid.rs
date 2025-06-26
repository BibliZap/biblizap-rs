//! Defines a custom type for Lens.org specific IDs and associated parsing/validation logic.

use std::marker::PhantomData;

use arrayvec::ArrayString;
use serde::{Serialize, Deserialize, Deserializer};
use serde::de::{self, Visitor};
use thiserror::Error;

/// Represents errors that can occur when parsing or validating a Lens.org ID string.
#[derive(Error, Debug)]
pub enum LensIdError {
    /// The candidate string for a LensID is not exactly 19 characters long.
    #[error("LensID candidate string is not 19 characters long")]
    Not19Characters,
    /// The parsed integer value of the LensID is zero, which is considered invalid.
    #[error("LensID is zero")]
    ZeroLensID
}

/// A custom type representing a validated Lens.org specific ID.
///
/// Stores both the original string representation and a parsed integer value
/// for efficient hashing and comparison.
#[derive(Serialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LensId {
    /// The integer representation of the first 18 digits of the Lens ID string.
    int: u64,
    /// The original 19-character string representation of the Lens ID.
    string: ArrayString<19>,
}


// Note: The `References` struct here seems misplaced and might belong in `citations.rs`.
// Keeping it for documentation purposes based on the provided code, but it might need refactoring.
/// Represents a list of Lens IDs, potentially used for references or citations.
#[derive(Debug, Default)]
pub struct References(pub Vec<LensId>);

impl<'de> Deserialize<'de> for LensId {
    /// Custom deserialization logic for `LensId`.
    ///
    /// Deserializes a string value into a validated `LensId` struct.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        let visitor = LensIdVisitor(PhantomData);
        deserializer.deserialize_str(visitor)
    }
}

/// A visitor for deserializing a string into a `LensId`.
struct LensIdVisitor(PhantomData<fn() -> LensId>);
impl<'de> Visitor<'de> for LensIdVisitor {
    type Value = LensId;

    /// Indicates the expected format for deserialization.
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a 19-character Lens.org ID string")
    }

    /// Visits a string value and attempts to convert it into a `LensId`.
    fn visit_str<E>(self, lensid_str: &str) -> Result<LensId, E>
    where
        E: de::Error,
    {
        // Use the TryFrom implementation to validate and create the LensId
        LensId::try_from(lensid_str)
            .map_err(|e| de::Error::custom(format!("invalid lensid: {}", e)))
    }
}

impl TryFrom<&str> for LensId {
    type Error = LensIdError;
    /// Attempts to create a `LensId` from a string slice.
    ///
    /// Validates that the string is 19 characters long and that the parsed
    /// integer value is not zero.
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
    /// Returns the string slice representation of the Lens ID.
    fn as_ref(&self) -> &str {
        self.string.as_str()
    }
}

impl std::hash::Hash for LensId {
    /// Implements hashing for `LensId` based on its integer representation.
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        hasher.write_u64(self.int)
    }
}

/// Enables the use of `LensId` with `nohash_hasher::NoHashHasher`.
impl nohash_hasher::IsEnabled for LensId {}

impl LensId {
    /// Creates a `LensId` from a string slice without performing length or zero validation.
    ///
    /// This is an internal helper function and should be used with caution.
    /// The input string is expected to be exactly 19 characters long.
    fn from_str_unchecked(lensid_str: &str) -> Self {
        let lensid_int: u64 = lensid_str
            .chars()
            .take(18) // Take the first 18 characters
            .filter_map(|c| c.to_digit(10)) // Convert digits to u32
            .fold(0, |acc, digit| acc * 10 + (digit as u64)); // Build the u64

        LensId {
            string: ArrayString::from(lensid_str).unwrap(), // Safe because length is checked by caller
            int: lensid_int
        }
    }
}
