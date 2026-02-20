//! BibliZap is a library for building citation networks starting from seed articles.
//!
//! It interacts with APIs like Lens.org and PubMed to retrieve article data
//! and expand the network by finding references and citations.
use lens::lensid;

pub mod common;
pub mod lens;
pub mod pubmed;

pub use common::SearchFor;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::lens::{cache::CacheBackend, lensid::LensId};

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    LensError(#[from] lens::error::LensError),
}

/// Represents an article with core bibliographic information.
///
/// This struct is used throughout the library to represent articles
/// retrieved from various sources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Article {
    pub first_author: Option<String>,
    pub year_published: Option<i32>,
    pub journal: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub doi: Option<String>,
    pub citations: Option<i32>,
    pub score: Option<i32>,
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
            score: None,
        }
    }
}

impl From<lens::article::ArticleWithData> for Article {
    fn from(article_with_data: lens::article::ArticleWithData) -> Self {
        let article_data = article_with_data.article_data;
        let external_ids = article_data.external_ids;

        let doi = external_ids
            .as_ref()
            .and_then(|ids| ids.doi.first().cloned());

        let first_author = article_data
            .authors
            .as_ref()
            .and_then(|authors| authors.first())
            .map(|author| {
                format!(
                    "{} {}",
                    author.first_name.clone().unwrap_or_default(),
                    author.last_name.clone().unwrap_or_default()
                )
            });

        let journal = article_data
            .source
            .as_ref()
            .and_then(|source| source.title.clone());

        Article {
            first_author,
            year_published: article_data.year_published,
            journal,
            title: article_data.title,
            summary: article_data.summary,
            doi,
            citations: article_data.scholarly_citations_count,
            score: None,
        }
    }
}

/// Expands a citation network starting from a set of seed articles.
///
/// This function performs a "snowballing" process, iteratively finding
/// references and/or citations for the current set of articles and adding
/// new articles to the network until the desired depth is reached or
/// no new articles are found.
///
/// # Arguments
///
/// * `seed_articles`: A vector of initial `Article` structs to start the network from.
/// * `depth`: The maximum depth of the snowballing process. A depth of 0 means only
///   the seed articles are returned. A depth of 1 means seed articles and their
///   direct references/citations are included, and so on.
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `lens_api_key`: The API key for Lens.org. Required for using the Lens.org API.
/// * `pubmed_api_key`: The API key for PubMed. Required for using the PubMed API.
///
/// # Returns
///
/// A `Result` containing a `HashSet` of unique `Article` structs found
/// during the snowballing process, or a `Box<dyn Error>` if an error occurs.
pub async fn snowball<S>(
    id_list: &[S],
    max_depth: u8,
    output_max_size: usize,
    search_for: &SearchFor,
    api_key: &str,
    cache: Option<&dyn CacheBackend>,
) -> Result<Vec<Article>, Error>
where
    S: AsRef<str>,
{
    let client = reqwest::Client::new();
    let snowball_id = lens::snowball(
        id_list,
        max_depth,
        search_for,
        api_key,
        Some(&client),
        cache,
    )
    .await?;

    let score_hashmap = snowball_id.into_inner();

    let mut s = score_hashmap.iter().collect::<Vec<_>>();
    s.sort_by_key(|x| std::cmp::Reverse(x.1));
    s.truncate(output_max_size);

    let selected_id: Vec<LensId> = score_hashmap.keys().cloned().collect();

    let lens_articles =
        lens::complete_articles(&selected_id, api_key, Some(&client), cache).await?;

    let mut articles_kv = lens_articles
        .into_iter()
        .map(|lens_article| (lens_article.lens_id.to_owned(), lens_article.into()))
        .collect::<Vec<(lensid::LensId, Article)>>();

    for (k, v) in articles_kv.iter_mut() {
        v.score = score_hashmap.get(k).map(|x| *x as i32);
    }

    let mut articles = articles_kv
        .into_iter()
        .map(|(_, article)| article)
        .filter(|article| article.score.is_some())
        .collect::<Vec<_>>();

    articles.sort_by_key(|v| v.score.unwrap_or_default());

    Ok(articles)
}
