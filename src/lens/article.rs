use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Map;
use std::marker::PhantomData;

use super::lensid::LensId;

/// Represents an article as returned by the Lens.org API.
///
/// This struct mirrors the structure of the article objects in the Lens.org API response.
#[derive(Deserialize, Debug)]
pub struct Article {
    /// The Lens.org specific ID for the article.
    pub lens_id: LensId,
    /// The title of the article.
    pub title: Option<String>,
    /// The abstract or summary of the article.
    #[serde(rename = "abstract")]
    pub summary: Option<String>,
    /// The number of scholarly citations this article has received.
    pub scholarly_citations_count: Option<i32>,

    /// External identifiers for the article (e.g., DOI, PMID).
    pub external_ids: Option<ExternalIds>,
    /// The list of authors.
    pub authors: Option<Vec<Author>>,
    /// Information about the source (e.g., journal, conference).
    pub source: Option<Source>,
    /// The year of publication.
    pub year_published: Option<i32>,
}

/// Represents external identifiers for an article from the Lens.org API.
///
/// This struct is used to parse the list of external IDs provided by the API,
/// which is represented as a list of key-value pairs.
#[derive(Debug, Default, Clone)]
pub struct ExternalIds {
    /// List of PubMed IDs (PMID).
    pub pmid: Vec<String>,
    /// List of DOIs (Digital Object Identifier).
    pub doi: Vec<String>,
    /// List of CORE IDs.
    pub coreid: Vec<String>,
    /// List of PubMed Central IDs (PMCID).
    pub pmcid: Vec<String>,
    /// List of Microsoft Academic Graph IDs (MAGID).
    pub magid: Vec<String>,
}

/// Represents an author in the Lens.org API response.
#[derive(Deserialize, Debug, Default, Clone)]
pub struct Author {
    /// The first name of the author.
    pub first_name: Option<String>,
    /// The initials of the author.
    pub initials: Option<String>,
    /// The last name of the author.
    pub last_name: Option<String>,
}

/// Represents the source (e.g., journal) in the Lens.org API response.
#[derive(Deserialize, Debug, Clone)]
pub struct Source {
    /// The publisher of the source.
    pub publisher: Option<String>,
    /// The title of the source (e.g., journal title).
    pub title: Option<String>,
    /// The type of source (e.g., "journal").
    #[serde(rename = "type")]
    pub kind: Option<String>,
}

impl<'de> Deserialize<'de> for ExternalIds {
    /// Custom deserialization logic for `ExternalIds`.
    ///
    /// The Lens.org API returns external IDs as a list of objects like
    /// `{"type": "doi", "value": "..."}`, so this visitor is needed
    /// to parse them into the structured `ExternalIds` struct.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = ExternalIdsVisitor(PhantomData);
        deserializer.deserialize_seq(visitor)
    }
}

/// A visitor for deserializing the `external_ids` field from the Lens.org API.
///
/// This field is returned as a sequence of objects, each with a "type" and "value" field.
struct ExternalIdsVisitor(PhantomData<fn() -> ExternalIds>);
impl<'de> Visitor<'de> for ExternalIdsVisitor {
    type Value = ExternalIds;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a vec of objects with 'type' and 'value' fields")
    }

    /// Visits a sequence of external ID objects and populates the `ExternalIds` struct.
    fn visit_seq<V>(self, mut seq: V) -> Result<ExternalIds, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut out = ExternalIds::default();
        while let Some(value) = seq.next_element()? {
            let map: Map<String, serde_json::Value> = value;
            let value_type = map
                .get("type")
                .ok_or_else(|| de::Error::missing_field("type"))?
                .as_str()
                .ok_or_else(|| de::Error::custom("failed to get type string"))?;
            match value_type {
                "pmid" => out
                    .pmid
                    .push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "doi" => out.doi.push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "pmcid" => out
                    .pmcid
                    .push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "magid" => out
                    .magid
                    .push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "coreid" => out
                    .coreid
                    .push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                _ => {} // Ignore unknown types
            }
        }

        Ok(out)
    }
}

impl<'de> ExternalIdsVisitor {
    /// Helper function to extract the "value" field from an external ID object map.
    fn get_value_field<V>(map: Map<String, serde_json::Value>) -> Result<String, V::Error>
    where
        V: SeqAccess<'de>,
    {
        Ok(map
            .get("value")
            .ok_or_else(|| de::Error::missing_field("value"))?
            .as_str()
            .ok_or_else(|| de::Error::custom("failed to get value string"))?
            .to_owned())
    }
}

impl Article {
    /// Gets the full name of the first author, if available.
    ///
    /// Returns `None` if there are no authors or the first author's name is incomplete.
    pub fn first_author_name(&self) -> Option<String> {
        let authors = self.authors.clone()?;
        let author = authors.first()?;

        Some(format!(
            "{} {}",
            author.first_name.clone().unwrap_or_default(),
            author.last_name.clone().unwrap_or_default()
        ))
    }

    /// Gets the first PMID (PubMed ID) from the external identifiers, if available.
    ///
    /// Note: This currently incorrectly returns the first DOI. It should return the first PMID.
    pub fn pmid(&self) -> Option<String> {
        let external_ids = self.external_ids.clone()?;
        // TODO: This should return the first PMID, not DOI
        let id = external_ids.doi.first()?.to_owned();
        Some(id)
    }

    /// Gets the first DOI (Digital Object Identifier) from the external identifiers, if available.
    pub fn doi(&self) -> Option<String> {
        let external_ids = self.external_ids.clone()?;
        let id = external_ids.doi.first()?.to_owned();
        Some(id)
    }

    /// Gets the title of the source (e.g., journal title), if available.
    pub fn journal(&self) -> Option<String> {
        let source = self.source.clone()?;

        source.title
    }
}
