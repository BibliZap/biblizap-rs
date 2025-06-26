use serde::{Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqAccess};
use std::marker::PhantomData;

use super::lensid::LensId;

/// Represents a list of references returned by the Lens.org API.
///
/// This struct is used for deserializing the `references` field, which is
/// expected to be a sequence of objects containing a `lens_id`.
#[derive(Debug, Default)]
pub struct References(pub Vec<LensId>);

impl<'de> Deserialize<'de> for References {
    /// Custom deserialization logic for the `References` struct.
    ///
    /// This is needed because the API returns a list of objects like
    /// `{"lens_id": "..."}` instead of just a list of Lens IDs.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        let visitor = ReferencesVisitor(PhantomData);
        deserializer.deserialize_seq(visitor)
    }
}

/// A visitor for deserializing the `references` field from the Lens.org API.
///
/// This field is returned as a sequence of objects, each expected to contain a "lens_id" field.
struct ReferencesVisitor(PhantomData<fn() -> References>);
impl<'de> Visitor<'de> for ReferencesVisitor {
    type Value = References;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a vec of objects with a 'lens_id' field")
    }

    /// Visits a sequence of reference objects and extracts the Lens IDs.
    fn visit_seq<V>(self, mut seq: V) -> Result<References, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut out = References::default();
        while let Some(value) = seq.next_element()? {
            let map: serde_json::Map<String, serde_json::Value> = value;
            let lensid_value = map.get("lens_id");
            if let Some(lensid_value) = lensid_value {
                let lensid_str = lensid_value.as_str()
                    .ok_or_else(|| de::Error::custom("error converting lensid value to string"))?;
                let lensid = LensId::try_from(lensid_str)
                    .ok().ok_or_else(|| de::Error::custom("invalid lensid"))?;
                out.0.push(lensid)
            }
        }

        Ok(out)
    }
}

/// Represents a list of scholarly citations returned by the Lens.org API.
///
/// This struct is used for deserializing the `scholarly_citations` field, which is
/// expected to be a list of Lens IDs.
#[derive(Debug, Default, Deserialize)]
pub struct ScholarlyCitations(pub Vec<LensId>);


/// Combines references and scholarly citations lists from the Lens.org API response.
#[derive(Debug, Default, Deserialize)]
pub struct ReferencesAndCitations {
    /// The list of references cited by the article.
    #[serde(default)]
    pub references: References,
    /// The list of scholarly citations that cite the article.
    #[serde(default)]
    pub scholarly_citations: ScholarlyCitations
}

impl ReferencesAndCitations {
    /// Gets a combined list of Lens IDs from both references and scholarly citations.
    pub fn get_both(&self) -> Vec<LensId> {
        let references = self.references.0.iter().map(|n| n.to_owned());
        let scholarly_citations = self.scholarly_citations.0.iter().map(|n| n.to_owned());

        references.chain(scholarly_citations).collect()
    }
}