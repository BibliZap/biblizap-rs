pub mod lensid;
pub mod error;
pub mod article;
pub mod request;
pub mod citations;

use super::common::SearchFor;

use lensid::LensId;
use error::LensError;
use article::Article;
use request::request_response;
use citations::ReferencesAndCitations;

struct TypedIdList<'a> {
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
            pmid: id_list.clone().into_iter().filter(|n| pmid_regex.is_match(n)).collect::<Vec<_>>(),
            lens_id: id_list.clone().into_iter().filter(|n| lens_id_regex.is_match(n)).collect::<Vec<_>>(),
            doi : id_list.into_iter().filter(|n| doi_regex.is_match(n)).collect::<Vec<&str>>()
        }
    }
}

pub async fn complete_articles<T>(id_list: &[T],
    api_key: &str,
    client: Option<&reqwest::Client>) -> Result<Vec<Article>, LensError>
where
    T: AsRef<str>
{
    let output_id = futures::future::join_all(id_list
        .chunks(1000)
        .map(|x| complete_articles_chunk(x, api_key, client)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, LensError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(output_id)
}

async fn complete_articles_chunk<T>(id_list: &[T],
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

    let response = request_response(client, api_key, id_list, id_type, &include).await?;

    let json_str = response.text().await?;
    
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    let ret: Vec<Article> = serde_json::from_value::<Vec<Article>>(json_value["data"].clone())?;

    Ok(ret)
}

async fn request_references_and_citations<T>(id_list: &[T],
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>) -> Result<Vec<LensId>, LensError> 
where
    T: AsRef<str>
{
    let output_id = futures::future::join_all(id_list
        .chunks(1000)
        .map(|x| request_references_and_citations_chunk(x, search_for, api_key, client)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, LensError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    Ok(output_id)
}

async fn request_references_and_citations_chunk<T>(id_list: &[T],
        search_for: &SearchFor,
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

    references_and_citations.append(&mut request_references_and_citations_typed_chunk(&typed_id_list.pmid, "pmid", search_for, api_key, &client).await?);
    references_and_citations.append(&mut request_references_and_citations_typed_chunk(&typed_id_list.lens_id, "lens_id", search_for, api_key, &client).await?);
    references_and_citations.append(&mut request_references_and_citations_typed_chunk(&typed_id_list.doi, "doi", search_for, api_key, &client).await?);

    Ok(references_and_citations)
}

async fn request_references_and_citations_typed_chunk(id_list: &[&str],
        id_type: &str,
        search_for: &SearchFor,
        api_key: &str,
        client: &reqwest::Client) -> Result<Vec<LensId>, LensError>
{
    let include = match search_for {
        SearchFor::Both => vec!["lens_id", "references", "scholarly_citations"],
        SearchFor::Citations => vec!["lens_id", "scholarly_citations"],
        SearchFor::References => vec!["lens_id", "references"]
    };
    let response = request_response(client, api_key, id_list, id_type, &include).await?;

    let json_str = response.text().await?;
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    let ret = serde_json::from_value::<Vec<ReferencesAndCitations>>(json_value["data"].clone())?
        .into_iter()
        .flat_map(|n| n.get_both())
        .collect::<Vec<_>>();
    
    Ok(ret)
}

pub async fn snowball<T>(src_lensid: &[T],
    max_depth: u8,
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str> {
    let mut all_lensid : Vec<LensId> = Vec::new();

    let mut current_lensid = src_lensid
        .iter()
        .map(|x| LensId::from(x.as_ref()))
        .collect::<Vec<LensId>>();

    for _ in 0..max_depth {
        current_lensid = request_references_and_citations(&current_lensid, search_for, api_key, client).await?;

        all_lensid.append(&mut current_lensid.clone());
    }

    Ok(all_lensid)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn complete_articles_test() {
        let src_id = ["020-200-401-307-33X", "050-708-976-791-252", "30507730", "10.1016/j.nephro.2007.05.005"];

        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";

        let articles = complete_articles(&src_id, api_key, None).await.unwrap();

        assert_eq!(articles.len(), src_id.len());

        for article in articles.into_iter() {
            println!("{:#?}", article);
        }
    }
    
    #[tokio::test]
    async fn snowball_test() {
        let id_list = ["020-200-401-307-33X", "050-708-976-791-252", "30507730", "10.1016/j.nephro.2007.05.005"];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let client = reqwest::Client::new();
        let new_id = snowball(&id_list, 2, &SearchFor::Both, api_key, Some(&client)).await.unwrap();
        
        assert_eq!(new_id.len(), 84570);
        
        let score_hashmap = new_id
            .into_iter()
            .fold(std::collections::HashMap::<LensId, usize>::new(), |mut m, x| {
                *m.entry(x).or_default() += 1;
                m
            });
        assert_eq!(score_hashmap.len(), 74657);

        let max_score_lens_id = score_hashmap.iter().max_by_key(|entry | entry.1).unwrap();
        assert_eq!(max_score_lens_id.0.as_ref(), "050-708-976-791-252");
        assert_eq!(*max_score_lens_id.1, 67usize);
                
        let new_id_dedup= score_hashmap
            .into_iter()
            .enumerate()
            .filter(|&(index, _)| index < 500 )
            .map(|x| x.1.0)
            .collect::<Vec<_>>();

        let articles = complete_articles(&new_id_dedup, api_key, Some(&client)).await.unwrap();
        assert_eq!(articles.len(), 500);
    }
}
