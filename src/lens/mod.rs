use reqwest::header::ToStrError;
use serde::{Deserialize, Deserializer};
use serde::de::{self, Visitor, SeqAccess};
use serde_json::Map;
use std::marker::PhantomData;

use thiserror::Error;
#[derive(Error, Debug)]
pub enum LensError {
    #[error("request error")]
    Request(#[from] reqwest::Error),
    #[error("parse int error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("rate limit missing")]
    RateLimitMissing,
    #[error("request to str error")]
    RequestToStr(#[from] ToStrError),
    #[error("serde_json error")]
    SerdeJson(#[from] serde_json::Error),
    #[error("value as_str error")]
    ValueAsStr,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LensId(pub String);

#[derive(Deserialize, Debug)]
pub struct Article {
    pub lens_id: LensId,
    pub title: Option<String>,
    #[serde(rename = "abstract")] 
    pub summary: Option<String>,
    pub scholarly_citations_count: Option<i32>,

    pub external_ids: Option<ExternalIds>,
    pub authors: Vec<Author>,
    pub source: Option<Source>,
    pub year_published: Option<i32>
}

#[derive(Debug, Default)]
pub struct ExternalIds {
    pub pmid: Vec<String>,
    pub doi: Vec<String>,
    pub coreid: Vec<String>,
    pub pmcid: Vec<String>,
    pub magid: Vec<String>
}

#[derive(Deserialize, Debug)]
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



pub async fn request_response(client: &reqwest::Client, api_key: &str, body: &str) -> Result<reqwest::Response, LensError> {
    let base_url: &str = "https://api.lens.org/scholarly/search";

    loop {
        let response = client
            .post(base_url)
            .header("Authorization", api_key)
            .header("Content-Type", "application/json")
            .body(body.to_owned())
            .send()
            .await?;

        if response.status() == 200 {
            return Ok(response);
        } else {
            println!("{:?}", response.headers());
            let seconds_to_wait = response.headers()
                .get("x-rate-limit-retry-after-seconds")
                .ok_or(LensError::RateLimitMissing)?
                .to_str()?
                .parse::<u64>()?;
            
            async_std::task::sleep(std::time::Duration::from_secs(seconds_to_wait)).await;
        }
    }
}

pub fn request_body(id_list: &[&str], include: &[&str]) -> String {
    let body = serde_json::json!(
    {
        "query": {
            "terms": {
                "lens_id": id_list
            }
        },
        "include": include,
        "size": id_list.len()
    }).to_string();

    body
}

pub async fn request_articles(id_list: &[&str], include:&[&str], api_key: &str, client: Option<reqwest::Client>) -> Result<Vec<Article>, LensError>{
    let client = match client {
        Some(t) => t,
        None => reqwest::Client::new()
    };
    
    let body = request_body(&id_list, &include);
    let response = request_response(&client, api_key, &body)
            .await?;

    let json_str = response
        .text()
        .await?;
    
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    let ret: Vec<Article> = serde_json::from_value::<Vec<Article>>(json_value["data"].clone())?;

    Ok(ret)
}

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
            match lens_id {
                Some(lens_id_value) => {
                    let lens_id_str = lens_id_value.as_str().ok_or_else(|| de::Error::missing_field("type"))?;
                    out.0.push(LensId(lens_id_str.to_owned()))
                },
                None => (),
            }
        }
        
        Ok(out)
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ScholarlyCitations(pub Vec<LensId>);