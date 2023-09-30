
use serde::{Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqAccess};
use std::marker::PhantomData;

use super::lensid::LensId;

#[derive(Debug, Default)]
pub struct References(pub Vec<LensId>);

impl<'de> Deserialize<'de> for References {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        let visitor = ReferencesVisitor(PhantomData);
        deserializer.deserialize_seq(visitor)
    }
}

struct ReferencesVisitor(PhantomData<fn() -> References>); 
impl<'de> Visitor<'de> for ReferencesVisitor {
    type Value = References;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a vec of keys and values")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<References, V::Error>
    where
        V: SeqAccess<'de>,
    {   
        let mut out = References::default();
        while let Some(value) = seq.next_element()? {
            let map: serde_json::Map<String, serde_json::Value> = value;
            let lens_id = map.get("lens_id");
            if let Some(lens_id_value) = lens_id {
                let lens_id_str = lens_id_value.as_str().ok_or_else(|| de::Error::missing_field("type"))?;
                out.0.push(LensId(lens_id_str.to_owned()))
            }
        }
        
        Ok(out)
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ScholarlyCitations(pub Vec<LensId>);



#[derive(Debug, Default, Deserialize)]
pub struct ReferencesAndCitations {
    #[serde(default)]
    pub references: References,
    #[serde(default)]
    pub scholarly_citations: ScholarlyCitations
}

impl ReferencesAndCitations {
    pub fn flatten(&self) -> Vec<LensId> {
        let references = self.references.0.iter().map(|n| n.to_owned());
        let scholarly_citations = self.scholarly_citations.0.iter().map(|n| n.to_owned());

        references.chain(scholarly_citations).collect()
    }
}