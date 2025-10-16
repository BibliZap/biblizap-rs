pub mod article;
pub mod citations;
pub mod error;
pub mod lensid;
pub mod request;

use super::common::SearchFor;

use article::Article;
use citations::ReferencesAndCitations;
use error::LensError;
use lensid::LensId;
use request::request_response;

/// A helper struct to categorize raw string IDs into known types (PMID, Lens ID, DOI).
struct TypedIdList<'a> {
    /// List of potential PubMed IDs.
    pub pmid: Vec<&'a str>,
    /// List of potential Lens.org IDs.
    pub lens_id: Vec<&'a str>,
    /// List of potential DOIs.
    pub doi: Vec<&'a str>,
}

impl<'a> TypedIdList<'a> {
    /// Categorizes a list of raw string IDs into known types using regular expressions.
    ///
    /// # Arguments
    ///
    /// * `id_list`: An iterator over string slices representing potential IDs.
    ///
    /// # Returns
    ///
    /// A `TypedIdList` containing the categorized IDs.
    pub fn from_raw_id_list<I>(id_list: I) -> Result<Self, LensError>
    where
        I: IntoIterator<Item = &'a str> + Clone,
    {
        use regex::Regex;
        // Regex for matching PMIDs (digits only)
        let pmid_regex = Regex::new("^[0-9]+$").expect("Failed to create PMID regex");
        // Regex for matching Lens IDs (format like XXX-XXX-...)
        let lens_id_regex =
            Regex::new("^...-...-...-...-...$").expect("Failed to create Lens ID regex");
        // Regex for matching DOIs (starts with 10.)
        let doi_regex = Regex::new("^10\\.").expect("Failed to create DOI regex");

        let pmid = id_list
            .clone()
            .into_iter()
            .filter(|n| pmid_regex.is_match(n))
            .collect::<Vec<_>>();

        let lens_id = id_list
            .clone()
            .into_iter()
            .filter(|n| lens_id_regex.is_match(n))
            .collect::<Vec<_>>();

        let doi = id_list
            .into_iter()
            .filter(|n| doi_regex.is_match(n))
            .collect::<Vec<&str>>();

        if pmid.is_empty() && lens_id.is_empty() && doi.is_empty() {
            return Err(LensError::NoValidIdsInInputList);
        }

        Ok(Self { pmid, lens_id, doi })
    }
}

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
) -> Result<Vec<Article>, LensError>
where
    T: AsRef<str>,
{
    let output_id = futures::future::join_all(
        id_list
            .chunks(1000) // Chunk requests to manage load
            .map(|x| complete_articles_chunk(x, api_key, client)),
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
async fn complete_articles_chunk<T>(
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
    complete_articles
        .append(&mut complete_articles_typed(&typed_id_list.pmid, "pmid", api_key, &client).await?);
    complete_articles.append(
        &mut complete_articles_typed(&typed_id_list.lens_id, "lens_id", api_key, &client).await?,
    );
    complete_articles
        .append(&mut complete_articles_typed(&typed_id_list.doi, "doi", api_key, &client).await?);

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
async fn complete_articles_typed(
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

    let response = request_response(client, api_key, id_list, id_type, &include).await?;

    let json_str = response.text().await?;

    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    // Deserialize the 'data' field of the response into a vector of Articles
    let ret: Vec<Article> = serde_json::from_value::<Vec<Article>>(json_value["data"].clone())?;

    Ok(ret)
}

/// Requests references and/or citations for a list of article IDs from the Lens.org API.
///
/// This function takes a list of article IDs and fetches the IDs of articles
/// that they reference or that cite them, based on the `search_for` parameter.
/// It handles chunking the requests.
///
/// # Arguments
///
/// * `id_list`: A slice of items that can be referenced as strings (e.g., `&str`, `String`, `LensId`).
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests. If `None`, a new client is created.
///
/// # Returns
///
/// A `Result` containing a vector of `LensId`s of the related articles, or a `LensError`.
async fn request_references_and_citations<T>(
    id_list: &[T],
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>,
) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str>,
{
    let output_id = futures::future::join_all(
        id_list
            .chunks(1000) // Chunk requests
            .map(|x| request_references_and_citations_chunk(x, search_for, api_key, client)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, LensError>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    if output_id.is_empty() {
        return Err(LensError::NoArticlesFound);
    }

    Ok(output_id)
}

/// Requests references and/or citations for a chunk of article IDs from the Lens.org API.
///
/// This is an internal helper function used by `request_references_and_citations`.
/// It categorizes the IDs and makes typed requests.
///
/// # Arguments
///
/// * `id_list`: A slice of items that can be referenced as strings for this chunk.
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests. If `None`, a new client is created.
///
/// # Returns
///
/// A `Result` containing a vector of `LensId`s for this chunk, or a `LensError`.
async fn request_references_and_citations_chunk<T>(
    id_list: &[T],
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>,
) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str>,
{
    let iter = id_list.iter().map(|item| item.as_ref());

    let typed_id_list = TypedIdList::from_raw_id_list(iter.clone())?;
    let mut references_and_citations = Vec::<LensId>::with_capacity(iter.into_iter().count());

    let client = match client {
        Some(t) => t.to_owned(),
        None => reqwest::Client::new(),
    };

    // Fetch references/citations by each ID type
    references_and_citations.append(
        &mut request_references_and_citations_typed_chunk(
            &typed_id_list.pmid,
            "pmid",
            search_for,
            api_key,
            &client,
        )
        .await?,
    );
    references_and_citations.append(
        &mut request_references_and_citations_typed_chunk(
            &typed_id_list.lens_id,
            "lens_id",
            search_for,
            api_key,
            &client,
        )
        .await?,
    );
    references_and_citations.append(
        &mut request_references_and_citations_typed_chunk(
            &typed_id_list.doi,
            "doi",
            search_for,
            api_key,
            &client,
        )
        .await?,
    );

    Ok(references_and_citations)
}

/// Fetches references and/or citations from Lens.org for a list of IDs of a specific type.
///
/// This is an internal helper function used by `request_references_and_citations_chunk`.
///
/// # Arguments
///
/// * `id_list`: A slice of string slices representing IDs of a single type.
/// * `id_type`: The type of IDs in `id_list`.
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `api_key`: The API key for Lens.org.
/// * `client`: The `reqwest::Client` to use for the request.
///
/// # Returns
///
/// A `Result` containing a vector of `LensId`s, or a `LensError`.
async fn request_references_and_citations_typed_chunk(
    id_list: &[&str],
    id_type: &str,
    search_for: &SearchFor,
    api_key: &str,
    client: &reqwest::Client,
) -> Result<Vec<LensId>, LensError> {
    // Determine which fields to include based on the search direction
    let include = match search_for {
        SearchFor::Both => vec!["lens_id", "references", "scholarly_citations"],
        SearchFor::Citations => vec!["lens_id", "scholarly_citations"],
        SearchFor::References => vec!["lens_id", "references"],
    };
    let response = request_response(client, api_key, id_list, id_type, &include).await?;

    let json_str = response.text().await?;
    let json_value: serde_json::Value = serde_json::from_str(&json_str)?;

    // Deserialize the 'data' field and extract all related Lens IDs
    let ret = serde_json::from_value::<Vec<ReferencesAndCitations>>(json_value["data"].clone())?
        .into_iter()
        .flat_map(|n| n.get_both())
        .collect::<Vec<_>>();

    Ok(ret)
}

/// Performs a snowballing expansion of a citation network starting from Lens.org IDs.
///
/// This function iteratively finds references and/or citations for the current set
/// of articles using the Lens.org API up to a specified maximum depth.
///
/// # Arguments
///
/// * `src_lensid`: A slice of items that can be referenced as strings, representing the starting Lens IDs.
/// * `max_depth`: The maximum depth of the snowballing process. Depth 0 returns only the initial IDs.
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests. If `None`, a new client is created.
///
/// # Returns
///
/// A `Result` containing a vector of unique `LensId`s found during the snowballing, or a `LensError`.
pub async fn snowball<T>(
    src_lensid: &[T],
    max_depth: u8,
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>,
) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str>,
{
    // This is done to avoid excessive memory allocation for deep snowballing
    let mut all_lensid: Vec<LensId> = Vec::with_capacity(probable_output_size(max_depth));

    // Start with the direct references/citations of the source IDs
    let mut current_lensid =
        request_references_and_citations(src_lensid, search_for, api_key, client).await?;
    all_lensid.append(&mut current_lensid.clone());

    // Iterate for the remaining depth
    for _ in 1..max_depth {
        // Find references/citations for the current set of IDs
        current_lensid =
            request_references_and_citations(&current_lensid, search_for, api_key, client).await?;

        // Add new IDs to the total list
        all_lensid.append(&mut current_lensid);
    }

    Ok(all_lensid)
}

/// Estimates a probable output size for the snowballing process based on depth.
///
/// This is a heuristic function to provide an initial capacity hint for vectors.
///
/// # Arguments
///
/// * `max_depth`: The maximum depth of the snowballing.
///
/// # Returns
///
/// An estimated number of IDs.
fn probable_output_size(max_depth: u8) -> usize {
    let max_depth = max_depth as usize;
    // Simple heuristic: grows exponentially with depth
    100 << (7 * (max_depth - 1)) // around 100^(max_depth-1) but fast
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the `probable_output_size` function.
    #[test]
    fn probable_output_size_test() {
        assert_eq!(probable_output_size(1), 100);
        assert_eq!(probable_output_size(2), 12800);
        assert_eq!(probable_output_size(3), 1638400);
    }

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

        let articles = complete_articles(&src_id, &api_key, None).await.unwrap();

        assert_eq!(articles.len(), src_id.len());

        for article in articles.into_iter() {
            println!("{:#?}", article);
        }
    }

    /// Tests the `snowball` function with invalid IDs to ensure proper error handling.
    #[tokio::test]
    async fn snowball_fail_invalid_ids() {
        let id_list = ["I AM AN INVALID ID", "I AM AN INVALID ID TOO"];
        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();
        let error = snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client))
            .await
            .unwrap_err();

        match error {
            LensError::NoValidIdsInInputList => (),
            _ => panic!("Expected NoValidIdsInInputList error"),
        }
    }

    /// Tests the `snowball` function with invalid IDs to ensure proper error handling.
    #[tokio::test]
    async fn snowball_fail_valid_but_nonexistent() {
        let id_list = ["10.9999/invalid.doi"];
        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();
        let error = snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client))
            .await
            .unwrap_err();

        match error {
            LensError::NoArticlesFound => (),
            _ => panic!("Expected NoArticlesFound error"),
        }
    }

    /// Tests the `snowball` function by expanding a network from seed IDs.
    #[tokio::test]
    async fn snowball_test() {
        let id_list = [
            "020-200-401-307-33X",
            "050-708-976-791-252",
            "30507730",
            "10.1016/j.nephro.2007.05.005",
        ];
        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();
        let new_id = snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client))
            .await
            .unwrap();

        // Assertions based on expected results from the API for these specific IDs and depth
        assert!(new_id.len() > 85551);

        let map_capacity = new_id.len();
        // Use nohash_hasher for potentially faster hashing with LensId
        let score_hashmap = new_id.into_iter().fold(
            nohash_hasher::IntMap::<LensId, usize>::with_capacity_and_hasher(
                map_capacity,
                std::hash::BuildHasherDefault::default(),
            ),
            |mut m, x| {
                *m.entry(x).or_default() += 1;
                m
            },
        );
        assert!(score_hashmap.len() > 75565);

        let max_score_lens_id = score_hashmap.iter().max_by_key(|entry| entry.1).unwrap();
        assert_eq!(max_score_lens_id.0.as_ref(), "050-708-976-791-252");
        assert!(*max_score_lens_id.1 > 67usize);

        // Take a subset of unique IDs for further testing (e.g., completing articles)
        let new_id_dedup = score_hashmap
            .into_iter()
            .enumerate()
            .filter(|&(index, _)| index < 500) // Limit to 500 for the next step
            .map(|x| x.1 .0)
            .collect::<Vec<_>>();

        let articles = complete_articles(&new_id_dedup, &api_key, Some(&client))
            .await
            .unwrap();
        assert_eq!(articles.len(), 500);
    }

    #[tokio::test]
    async fn depth1_snowball() {
        let id_list = ["10.1111/j.1468-0262.2006.00668.x"];

        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();
        let direct_citations =
            snowball(&id_list, 1, &SearchFor::Citations, &api_key, Some(&client))
                .await
                .unwrap();

        assert!(direct_citations.len() >= 991);

        let direct_references =
            snowball(&id_list, 1, &SearchFor::References, &api_key, Some(&client))
                .await
                .unwrap();

        assert!(direct_references.len() >= 76);
    }
}
