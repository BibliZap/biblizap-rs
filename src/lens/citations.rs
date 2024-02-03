
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
    pub fn get_both(&self) -> Vec<LensId> {
        let references = self.references.0.iter().map(|n| n.to_owned());
        let scholarly_citations = self.scholarly_citations.0.iter().map(|n| n.to_owned());

        references.chain(scholarly_citations).collect()
    }
}