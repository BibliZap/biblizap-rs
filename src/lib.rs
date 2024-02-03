use lens::lensid;

pub mod lens;
pub mod pubmed;
pub mod common;

use thiserror::Error;
use serde::{Serialize, Deserialize};
pub use common::SearchFor;

#[derive(Error, Debug)]
pub enum Error {
    #[error("lens error")]
    LensError(#[from] lens::error::LensError),
    #[error("empty snowball")]
    EmptySnowball
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Article {
    pub first_author: Option<String>,
    pub year_published: Option<i32>,
    pub journal: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub doi: Option<String>,
    pub citations: Option<i32>,
    pub score: Option<i32>
}


impl From<lens::article::Article> for Article {
    fn from(article: lens::article::Article) -> Self {
        Article {
            first_author: article.first_author_name(),
            year_published: article.year_published,
            journal: article.journal(),
            title: article.title.to_owned(),
            summary: article.summary.to_owned(),
            doi: article.doi(),
            citations: article.scholarly_citations_count,
            score: None
        }
    }
}

pub async fn snowball<T>(id_list: &[T], max_depth: u8, output_max_size: usize, search_for: &SearchFor, api_key: &str) -> Result<Vec<Article>, Error>
where
    T: AsRef<str>
{
    if id_list.is_empty() || id_list.first().unwrap().as_ref() == "" {
        return Err(Error::EmptySnowball)
    }
    let client = reqwest::Client::new();
    let snowball_id = lens::snowball(id_list, max_depth, search_for, api_key, Some(&client)).await?;

    let map_capacity = snowball_id.len();
    let score_hashmap = snowball_id
        .iter()
        .fold(nohash_hasher::IntMap::<lens::lensid::LensId, i32>::with_capacity_and_hasher(map_capacity, std::hash::BuildHasherDefault::default()),
            |mut m, x| {
            *m.entry(x.to_owned()).or_default() += 1;
            m
        });
    
    let mut s = score_hashmap.iter().collect::<Vec<_>>();
    s.sort_by_key(|x| std::cmp::Reverse(x.1));
    s.truncate(output_max_size);

    let selected_id = s
        .into_iter()
        .map(|(id, _)| id)
        .to_owned().collect::<Vec<_>>();
    
    let lens_articles = lens::complete_articles(&selected_id, api_key, Some(&client)).await?;

    let mut articles_kv = lens_articles
            .into_iter()
            .map(|lens_article| (lens_article.lens_id.to_owned(), lens_article.into()))
            .collect::<Vec<(lensid::LensId, Article)>>();
    
    for (k, v) in articles_kv.iter_mut() {
        v.score = score_hashmap.get(k).copied();
    }

    let mut articles = articles_kv
        .into_iter()
        .map(|(_, article)| article)
        .filter(|article| article.score.is_some())
        .collect::<Vec<_>>();

    articles.sort_by_key(|v| v.score.unwrap_or_default());

    Ok(articles)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn snowball_test() {
        let id_list = ["020-200-401-307-33X"];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";

        let articles = snowball(&id_list, 2, 10, &SearchFor::Both, api_key).await.unwrap();

        assert_eq!(articles.len(), 10);

        println!("{:#?}", articles);
    }

    #[tokio::test]
    #[should_panic]
    async fn empty_snowball() {
        let id_list = [""];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";

        let _ = snowball(&id_list, 2, 10, &SearchFor::Both, api_key).await.unwrap();
    }
}