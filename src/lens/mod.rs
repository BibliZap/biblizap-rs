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

impl AsRef<str> for LensId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

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


pub async fn request_response(client: &reqwest::Client,
                            api_key: &str,
                            id_list: impl IntoIterator<Item = impl serde::Serialize> + serde::Serialize,
                            id_type: &str,
                            include: &[&str]) -> Result<reqwest::Response, LensError> {
    request_response_with_body(client, api_key, &make_request_body(id_list, id_type, include)).await
}

async fn request_response_with_body(client: &reqwest::Client, api_key: &str, body: &str) -> Result<reqwest::Response, LensError> {
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

fn make_request_body(id_list: impl IntoIterator<Item = impl serde::Serialize> + serde::Serialize,
                    id_type: &str,
                    include: &[&str]) -> String {
    let body = serde_json::json!(
    {
        "query": {
            "terms": {
                id_type: id_list
            }
        },
        "include": include,
        "size": id_list.into_iter().count()
    }).to_string();

    body
}

pub struct TypedIdList<'a> {
    pub pmid: Vec<&'a str>,
    pub lens_id: Vec<&'a str>,
    pub doi: Vec<&'a str>
}

impl <'a> TypedIdList<'a> {
    pub fn from_raw_id_list<I>(id_list: I) -> TypedIdList<'a>
    where
        I: IntoIterator<Item = &'a str>  + Clone
    {
        use regex::Regex;
        let pmid_regex = Regex::new("^[0-9]+$").unwrap();
        let lens_id_regex = Regex::new("^...-...-...-...-...$").unwrap();
        let doi_regex = Regex::new("^10\\.").unwrap();

        TypedIdList {
            pmid: id_list.clone().into_iter().filter(|n| pmid_regex.is_match(*n)).collect::<Vec<_>>(),
            lens_id: id_list.clone().into_iter().filter(|n| lens_id_regex.is_match(*n)).collect::<Vec<_>>(),
            doi : id_list.into_iter().filter(|n| doi_regex.is_match(*n)).collect::<Vec<&str>>()
        }
    }
}

pub async fn complete_articles_chunk<T>(id_list: &[T],
        api_key: &str,
        client: Option<&reqwest::Client>) -> Result<Vec<Article>, LensError>
where
    T: AsRef<str>
{
    let client = match client {
        Some(t) => t.to_owned(),
        None => reqwest::Client::new()
    };
    let iter = id_list.iter().map(|item| item.as_ref());

    let typed_id_list = TypedIdList::from_raw_id_list(iter.clone());

    let mut complete_articles = Vec::<Article>::with_capacity(iter.len());

    complete_articles.append(&mut complete_articles_typed(&typed_id_list.pmid, "pmid", api_key, &client).await?);
    complete_articles.append(&mut complete_articles_typed(&typed_id_list.lens_id, "lens_id", api_key, &client).await?);
    complete_articles.append(&mut complete_articles_typed(&typed_id_list.doi, "doi", api_key, &client).await?);

    Ok(complete_articles)
}

async fn complete_articles_typed(id_list: &[&str], id_type: &str, api_key: &str, client: &reqwest::Client) -> Result<Vec<Article>, LensError> {
    let include = ["lens_id","title", "authors", "abstract", "external_ids", "scholarly_citations_count", "source", "year_published"];

    let response = request_response(&client, api_key, id_list, id_type, &include).await?;

    let json_str = response.text().await?;
    
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    let ret: Vec<Article> = serde_json::from_value::<Vec<Article>>(json_value["data"].clone())?;

    Ok(ret)
}

pub async fn request_references_and_citations<T>(id_list: &[T],
    api_key: &str,
    client: Option<&reqwest::Client>) -> Result<Vec<LensId>, LensError> 
where
    T: AsRef<str>
{
    let output_id = futures::future::join_all(id_list
        .chunks(1000)
        .map(|x| request_references_and_citations_chunk(x, api_key, client)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, LensError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(output_id)
}

pub async fn request_references_and_citations_chunk<T>(id_list: &[T],
        api_key: &str,
        client: Option<&reqwest::Client>) -> Result<Vec<LensId>, LensError> 
where
    T: AsRef<str>
{
    let iter = id_list.iter().map(|item| item.as_ref());

    let typed_id_list = TypedIdList::from_raw_id_list(iter.clone());
    let mut references_and_citations = Vec::<LensId>::with_capacity(iter.into_iter().count());

    let client = match client {
        Some(t) => t.to_owned(),
        None => reqwest::Client::new()
    };

    references_and_citations.append(&mut request_references_and_citations_typed_chunk(&typed_id_list.pmid, "pmid", api_key, &client).await?);
    references_and_citations.append(&mut request_references_and_citations_typed_chunk(&typed_id_list.lens_id, "lens_id", api_key, &client).await?);
    references_and_citations.append(&mut request_references_and_citations_typed_chunk(&typed_id_list.doi, "doi", api_key, &client).await?);

    Ok(references_and_citations)
}

async fn request_references_and_citations_typed_chunk(id_list: &[&str],
        id_type: &str,
        api_key: &str,
        client: &reqwest::Client) -> Result<Vec<LensId>, LensError>
{
    let include = ["lens_id", "references", "scholarly_citations"];
    let response = request_response(&client, api_key, id_list, id_type, &include).await?;

    let json_str = response.text().await?;
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    let ret = serde_json::from_value::<Vec<ReferencesAndCitations>>(json_value["data"].clone())?
        .into_iter()
        .flat_map(|n| n.flatten())
        .collect::<Vec<_>>();
    
    Ok(ret)
}

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

pub async fn snowball<T>(src_pmid: &[T], max_depth: u8,
    api_key: &str,
    client: Option<&reqwest::Client>) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str> {
    let mut all_pmid : Vec<LensId> = Vec::new();

    let mut current_pmid = src_pmid
        .iter()
        .map(|x| LensId(x.as_ref().to_owned()))
        .collect::<Vec<LensId>>();

    for _ in 0..max_depth {
        current_pmid = request_references_and_citations(&current_pmid, api_key, client).await?;

        all_pmid.append(&mut current_pmid.clone());
    }

    Ok(all_pmid)
}