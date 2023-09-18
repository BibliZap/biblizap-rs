use serde::{Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqAccess};
use serde_json::Map;
use std::marker::PhantomData;

#[derive(Deserialize, Debug)]
pub struct Article {
    pub lens_id: String,
    pub title: Option<String>,
    #[serde(rename = "abstract")] 
    pub summary: Option<String>,

    pub external_ids: ExternalIds,
}

#[derive(Debug, Default)]
pub struct ExternalIds {
    pub pmid: Vec<String>,
    pub doi: Vec<String>,
    pub coreid: Vec<String>,
    pub pmcid: Vec<String>,
    pub magid: Vec<String>
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