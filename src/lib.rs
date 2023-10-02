use lens::lensid;

pub mod lens;
pub mod pubmed;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("lens error")]
    LensError(#[from] lens::error::LensError)
}

#[derive(Debug)]
pub struct Article {
    pub first_author: Option<String>,
    pub year_published: Option<i32>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub doi: Option<String>,
    pub score: Option<i32>
}


impl From<lens::article::Article> for Article {
    fn from(article: lens::article::Article) -> Self {
        Article {
            first_author: article.first_author_name(),
            year_published: article.year_published,
            title: article.title.to_owned(),
            summary: article.summary.to_owned(),
            doi: article.doi(),
            score: None
        }
    }
}

pub async fn snowball<T>(id_list: &[T], max_depth: u8, output_max_size: usize, api_key: &str) -> Result<Vec<Article>, Error>
where
    T: AsRef<str>
{
    let client = reqwest::Client::new();
    let snowball_id = lens::snowball(id_list, max_depth, api_key, Some(&client)).await?;

    let score_hashmap = snowball_id
        .iter()
        .fold(std::collections::HashMap::<lens::lensid::LensId, i32>::new(), |mut m, x| {
            *m.entry(x.to_owned()).or_default() += 1;
            m
        });

    let mut s = score_hashmap.iter().collect::<Vec<_>>();
    s.sort_by_key(|x| std::cmp::Reverse(x.1));
    let _ = s.split_off(output_max_size);

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

        let a = snowball(&id_list, 2, 10, api_key).await.unwrap();

        println!("{:#?}", a);
    }
}