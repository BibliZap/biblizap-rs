use crate::lens::{
    article::Article, cache::CacheBackend, error::LensError, id_types::TypedIdList,
    request::request_and_parse,
};

/// Completes the information for a list of articles using the Lens.org API.
///
/// This function takes a list of article IDs (as strings) and fetches detailed
/// data for them from Lens.org, returning a vector of `Article` structs.
/// It handles chunking the requests to avoid hitting API limits.
///
/// # Arguments
///
/// * `id_list`: A slice of items that can be referenced as strings (e.g., `&str`, `String`, `LensId`).
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests. If `None`, a new client is created.
///
/// # Returns
///
/// A `Result` containing a vector of `Article` structs, or a `LensError` if an error occurs.
pub async fn complete_articles<T>(
    id_list: &[T],
    api_key: &str,
    client: Option<&reqwest::Client>,
    cache: Option<&dyn CacheBackend>,
) -> Result<Vec<Article>, LensError>
where
    T: AsRef<str>,
{
    let output_id = futures::future::join_all(
        id_list
            .chunks(1000) // Chunk requests to manage load
            .map(|x| request_batch(x, api_key, client)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, LensError>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    Ok(output_id)
}

/// Completes the information for a chunk of article IDs using the Lens.org API.
///
/// This is an internal helper function used by `complete_articles`. It categorizes
/// the IDs and makes typed requests to the API.
///
/// # Arguments
///
/// * `id_list`: A slice of items that can be referenced as strings for this chunk.
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests. If `None`, a new client is created.
///
/// # Returns
///
/// A `Result` containing a vector of `Article` structs for this chunk, or a `LensError`.
async fn request_batch<T>(
    id_list: &[T],
    api_key: &str,
    client: Option<&reqwest::Client>,
) -> Result<Vec<Article>, LensError>
where
    T: AsRef<str>,
{
    let client = match client {
        Some(t) => t.to_owned(),
        None => reqwest::Client::new(),
    };
    let iter = id_list.iter().map(|item| item.as_ref());

    let typed_id_list = TypedIdList::from_raw_id_list(iter.clone())?;

    let mut complete_articles = Vec::<Article>::with_capacity(iter.len());

    // Fetch articles by each ID type
    complete_articles.append(
        &mut request_batch_one_id_type(&typed_id_list.pmid, "pmid", api_key, &client).await?,
    );
    complete_articles.append(
        &mut request_batch_one_id_type(&typed_id_list.lens_id, "lens_id", api_key, &client).await?,
    );
    complete_articles
        .append(&mut request_batch_one_id_type(&typed_id_list.doi, "doi", api_key, &client).await?);

    Ok(complete_articles)
}

/// Fetches detailed article information from Lens.org for a list of IDs of a specific type.
///
/// This is an internal helper function used by `complete_articles_chunk`.
///
/// # Arguments
///
/// * `id_list`: A slice of string slices representing IDs of a single type (e.g., all PMIDs).
/// * `id_type`: The type of IDs in `id_list` (e.g., "pmid", "lens_id", "doi").
/// * `api_key`: The API key for Lens.org.
/// * `client`: The `reqwest::Client` to use for the request.
///
/// # Returns
///
/// A `Result` containing a vector of `Article` structs, or a `LensError`.
async fn request_batch_one_id_type(
    id_list: &[&str],
    id_type: &str,
    api_key: &str,
    client: &reqwest::Client,
) -> Result<Vec<Article>, LensError> {
    // Fields to include in the API response
    let include = [
        "lens_id",
        "title",
        "authors",
        "abstract",
        "external_ids",
        "scholarly_citations_count",
        "source",
        "year_published",
    ];

    // Use optimized direct deserialization
    request_and_parse(client, api_key, id_list, id_type, &include).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the `complete_articles` function by fetching details for known IDs.
    #[tokio::test]
    async fn complete_articles_test() {
        let src_id = [
            "020-200-401-307-33X",
            "050-708-976-791-252",
            "30507730",
            "10.1016/j.nephro.2007.05.005",
        ];

        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");

        let articles = complete_articles(&src_id, &api_key, None, None)
            .await
            .unwrap();

        assert_eq!(articles.len(), src_id.len());

        for article in articles.into_iter() {
            println!("{article:#?}");
        }
    }
}
