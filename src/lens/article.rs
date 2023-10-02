use serde::{Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqAccess};
use std::marker::PhantomData;
use serde_json::Map;

use super::lensid::LensId;

#[derive(Deserialize, Debug)]
pub struct Article {
    pub lens_id: LensId,
    pub title: Option<String>,
    #[serde(rename = "abstract")] 
    pub summary: Option<String>,
    pub scholarly_citations_count: Option<i32>,

    pub external_ids: Option<ExternalIds>,
    pub authors: Option<Vec<Author>>,
    pub source: Option<Source>,
    pub year_published: Option<i32>
}

#[derive(Debug, Default, Clone)]
pub struct ExternalIds {
    pub pmid: Vec<String>,
    pub doi: Vec<String>,
    pub coreid: Vec<String>,
    pub pmcid: Vec<String>,
    pub magid: Vec<String>
}

#[derive(Deserialize, Debug, Default, Clone)]
pub struct Author {
    pub first_name: Option<String>,
    pub initials: Option<String>,
    pub last_name: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Source {
    pub publisher: Option<String>,
    pub title: Option<String>,
    #[serde(rename = "type")] 
    pub kind: Option<String>,
}

impl<'de> Deserialize<'de> for ExternalIds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        let visitor = ExternalIdsVisitor(PhantomData);
        deserializer.deserialize_seq(visitor)
    }
}

struct ExternalIdsVisitor(PhantomData<fn() -> ExternalIds>); 
impl<'de> Visitor<'de> for ExternalIdsVisitor {
    type Value = ExternalIds;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a vec of keys and values")
    }

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
                "pmid" => out.pmid.push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "doi" => out.doi.push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "pmcid" => out.pmcid.push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "magid" => out.magid.push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                "coreid" => out.coreid.push(ExternalIdsVisitor::get_value_field::<V>(map)?),
                _ => {}
            }
        }
        
        Ok(out)
    }
}

impl<'de> ExternalIdsVisitor {
    fn get_value_field<V>(map: Map<String, serde_json::Value>) -> Result<String, V::Error>
    where
        V: SeqAccess<'de>,
    {
        Ok(map.get("value")
            .ok_or_else(|| de::Error::missing_field("value"))?
            .as_str()
            .ok_or_else(|| de::Error::custom("failed to get value string"))?
            .to_owned())
    }
}


impl Article {
    pub fn first_author_name(&self) -> Option<String> {
        let authors = self.authors.clone()?;
        let author = authors.get(0)?;
    
        Some(format!("{} {}", author.first_name.clone().unwrap_or_default(), author.last_name.clone().unwrap_or_default()))
    }
    
    pub fn pmid(&self) -> Option<String> {
        let external_ids = self.external_ids.clone()?;
        let id = external_ids.doi.get(0)?.to_owned();
        Some(id)
    }

    pub fn doi(&self) -> Option<String> {
        let external_ids = self.external_ids.clone()?;
        let id = external_ids.doi.get(0)?.to_owned();
        Some(id)
    }
}
