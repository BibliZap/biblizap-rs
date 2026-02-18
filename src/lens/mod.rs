pub mod article;
pub mod cache;
pub mod citations;
pub mod counter;
pub mod error;
pub mod lensid;
pub mod request;

use super::common::SearchFor;

use article::Article;
use cache::CacheBackend;
use citations::ReferencesAndCitations;
use counter::LensIdCounter;
use error::LensError;
use lensid::LensId;
use request::request_and_parse;
use std::collections::{HashMap, HashSet};

/// Helper struct that includes both the parent ID and its references/citations.
/// This can be deserialized directly from the Lens API response.
#[derive(Debug, serde::Deserialize)]
struct ArticleWithReferencesAndCitations {
    lens_id: LensId,
    #[serde(flatten)]
    refs_and_cites: ReferencesAndCitations,
}

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

    // Use optimized direct deserialization
    request_and_parse(client, api_key, id_list, id_type, &include).await
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
    cache: Option<&dyn CacheBackend>,
) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str>,
{
    // If cache is enabled, use the _with_parents variant to preserve relationships
    if cache.is_some() {
        let parents_with_children = request_references_and_citations_with_parents(
            id_list, search_for, api_key, client, cache,
        )
        .await?;

        // Flatten to just the children IDs
        let results: Vec<LensId> = parents_with_children
            .into_iter()
            .flat_map(|pwc| pwc.children)
            .collect();

        return Ok(results);
    }

    // No cache - use the original fast path
    let output_id = futures::future::join_all(
        id_list
            .chunks(1000)
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

    // Use optimized direct deserialization
    let refs_and_cites: Vec<ReferencesAndCitations> =
        request_and_parse(client, api_key, id_list, id_type, &include).await?;

    // Extract all related Lens IDs
    let ret = refs_and_cites
        .into_iter()
        .flat_map(|n| n.get_both())
        .collect::<Vec<_>>();

    Ok(ret)
}

/// Helper struct to preserve parent-child relationship when querying citations.
#[derive(Debug)]
struct ParentWithChildren {
    parent_id: LensId,
    children: Vec<LensId>,
}

/// Requests references and/or citations while preserving parent-child relationships.
///
/// Unlike `request_references_and_citations`, this function returns a mapping
/// of each parent ID to its children, which is necessary for proper count multiplication
/// in the optimized snowball algorithm.
///
/// **Important**: This function respects the Lens API limit of 1000 articles per request
/// by chunking the input into batches.
///
/// # Arguments
///
/// * `id_list`: A slice of items that can be referenced as LensIds.
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests.
///
/// # Returns
///
/// A `Result` containing a vector of `ParentWithChildren` structs, or a `LensError`.
async fn request_references_and_citations_with_parents<T>(
    id_list: &[T],
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>,
    cache: Option<&dyn CacheBackend>,
) -> Result<Vec<ParentWithChildren>, LensError>
where
    T: AsRef<str>,
{
    // Convert input to LensIds for cache lookup
    let lens_ids: Vec<LensId> = id_list
        .iter()
        .filter_map(|id| LensId::try_from(id.as_ref()).ok())
        .collect();

    // If no cache, fall back to direct HTTP
    let Some(cache_backend) = cache else {
        let all_results = futures::future::join_all(id_list.chunks(1000).map(|chunk| {
            request_references_and_citations_with_parents_chunk(chunk, search_for, api_key, client)
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, LensError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

        if all_results.is_empty() {
            return Err(LensError::NoArticlesFound);
        }

        return Ok(all_results);
    };

    // Query cache based on search_for
    // For SearchFor::Both, we query both tables separately
    let (cached_refs, cached_cites) = match search_for {
        SearchFor::References => {
            let refs = cache_backend.get_references(&lens_ids).await?;
            (refs, HashMap::new())
        }
        SearchFor::Citations => {
            let cites = cache_backend.get_citations(&lens_ids).await?;
            (HashMap::new(), cites)
        }
        SearchFor::Both => {
            let refs = cache_backend.get_references(&lens_ids).await?;
            let cites = cache_backend.get_citations(&lens_ids).await?;
            (refs, cites)
        }
    };

    // Determine which IDs have complete cache hits
    let fully_cached_ids: HashSet<LensId> = lens_ids
        .iter()
        .filter(|id| match search_for {
            SearchFor::References => cached_refs.contains_key(id),
            SearchFor::Citations => cached_cites.contains_key(id),
            SearchFor::Both => cached_refs.contains_key(id) && cached_cites.contains_key(id),
        })
        .cloned()
        .collect();

    // Build results from cache for fully cached IDs
    let mut results: Vec<ParentWithChildren> = fully_cached_ids
        .iter()
        .map(|id| {
            let children = match search_for {
                SearchFor::References => cached_refs.get(id).cloned().unwrap_or_default(),
                SearchFor::Citations => cached_cites.get(id).cloned().unwrap_or_default(),
                SearchFor::Both => {
                    let mut refs = cached_refs.get(id).cloned().unwrap_or_default();
                    let mut cites = cached_cites.get(id).cloned().unwrap_or_default();
                    refs.append(&mut cites);
                    refs
                }
            };
            ParentWithChildren {
                parent_id: id.clone(),
                children,
            }
        })
        .collect();

    // Determine which IDs need to be fetched via HTTP (misses)
    let misses: Vec<LensId> = lens_ids
        .iter()
        .filter(|id| !fully_cached_ids.contains(id))
        .cloned()
        .collect();

    // Fetch misses from API if any
    if !misses.is_empty() {
        let miss_strs: Vec<String> = misses.iter().map(|id| id.as_ref().to_string()).collect();
        let miss_refs: Vec<&str> = miss_strs.iter().map(|s| s.as_str()).collect();

        // For SearchFor::Both, we need to fetch refs and cites separately
        // so we can store them in the correct cache tables
        let fetched_results = if matches!(search_for, SearchFor::Both) {
            // Fetch references
            let refs_results = futures::future::join_all(miss_refs.chunks(1000).map(|chunk| {
                request_references_and_citations_with_parents_chunk(
                    chunk,
                    &SearchFor::References,
                    api_key,
                    client,
                )
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, LensError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

            // Fetch citations
            let cites_results = futures::future::join_all(miss_refs.chunks(1000).map(|chunk| {
                request_references_and_citations_with_parents_chunk(
                    chunk,
                    &SearchFor::Citations,
                    api_key,
                    client,
                )
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, LensError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

            // Store refs and cites in their respective tables
            if !refs_results.is_empty() {
                let refs_batch: Vec<(LensId, Vec<LensId>)> = refs_results
                    .iter()
                    .map(|pwc| (pwc.parent_id.clone(), pwc.children.clone()))
                    .collect();
                cache_backend.store_references(&refs_batch).await?;
            }

            if !cites_results.is_empty() {
                let cites_batch: Vec<(LensId, Vec<LensId>)> = cites_results
                    .iter()
                    .map(|pwc| (pwc.parent_id.clone(), pwc.children.clone()))
                    .collect();
                cache_backend.store_citations(&cites_batch).await?;
            }

            // Combine results - merge refs and cites for the same parent
            let mut combined_map: HashMap<LensId, Vec<LensId>> = HashMap::new();
            for pwc in refs_results.into_iter().chain(cites_results.into_iter()) {
                combined_map
                    .entry(pwc.parent_id.clone())
                    .or_default()
                    .extend(pwc.children);
            }
            combined_map
                .into_iter()
                .map(|(parent_id, children)| ParentWithChildren {
                    parent_id,
                    children,
                })
                .collect::<Vec<_>>()
        } else {
            futures::future::join_all(miss_refs.chunks(1000).map(|chunk| {
                request_references_and_citations_with_parents_chunk(
                    chunk, search_for, api_key, client,
                )
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, LensError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
        };

        // Store fetched results in cache (only for References or Citations, not Both)
        if !fetched_results.is_empty() && !matches!(search_for, SearchFor::Both) {
            let batch: Vec<(LensId, Vec<LensId>)> = fetched_results
                .iter()
                .map(|pwc| (pwc.parent_id.clone(), pwc.children.clone()))
                .collect();

            match search_for {
                SearchFor::References => {
                    cache_backend.store_references(&batch).await?;
                }
                SearchFor::Citations => {
                    cache_backend.store_citations(&batch).await?;
                }
                SearchFor::Both => {
                    // Already handled in the fetch logic above
                }
            }
        }

        results.extend(fetched_results);
    }

    if results.is_empty() {
        return Err(LensError::NoArticlesFound);
    }

    Ok(results)
}

/// Helper function that processes a single chunk (≤1000 articles) for the parent-child mapping.
async fn request_references_and_citations_with_parents_chunk<T>(
    id_list: &[T],
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>,
) -> Result<Vec<ParentWithChildren>, LensError>
where
    T: AsRef<str>,
{
    let client = match client {
        Some(t) => t,
        None => &reqwest::Client::new(),
    };

    // Determine which fields to include based on the search direction
    let include = match search_for {
        SearchFor::Both => vec!["lens_id", "references", "scholarly_citations"],
        SearchFor::Citations => vec!["lens_id", "scholarly_citations"],
        SearchFor::References => vec!["lens_id", "references"],
    };

    let id_strs: Vec<&str> = id_list.iter().map(|id| id.as_ref()).collect();

    // Use optimized direct deserialization
    let articles: Vec<ArticleWithReferencesAndCitations> =
        request_and_parse(client, api_key, &id_strs, "lens_id", &include).await?;

    // Convert to ParentWithChildren
    let results = articles
        .into_iter()
        .map(|article| ParentWithChildren {
            parent_id: article.lens_id,
            children: article.refs_and_cites.get_both(),
        })
        .collect();

    Ok(results)
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
/// * `cache`: An optional cache backend to speed up queries by caching article relationships.
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
    cache: Option<&dyn CacheBackend>,
) -> Result<Vec<LensId>, LensError>
where
    T: AsRef<str>,
{
    // This is done to avoid excessive memory allocation for deep snowballing
    let mut all_lensid: Vec<LensId> = Vec::with_capacity(probable_output_size(max_depth));

    // Start with the direct references/citations of the source IDs
    let mut current_lensid =
        request_references_and_citations(src_lensid, search_for, api_key, client, cache).await?;
    all_lensid.append(&mut current_lensid.clone());

    // Iterate for the remaining depth
    for _ in 1..max_depth {
        // Find references/citations for the current set of IDs
        current_lensid =
            request_references_and_citations(&current_lensid, search_for, api_key, client, cache)
                .await?;

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

/// Optimized snowball function that deduplicates API requests.
///
/// This function performs the same citation expansion as `snowball`, but with
/// an important optimization: each unique article ID is queried only once per depth level.
/// The occurrence counts are tracked and multiplied appropriately to maintain
/// identical scoring behavior.
///
/// # Algorithm
///
/// - **Within iteration**: Multiply child counts by parent count
///   (if parent A appears 5 times and cites child D, then D gets count 5)
/// - **Across iterations**: Add counts together
///   (if D appears in depth 1 and depth 2, sum the counts)
///
/// # Arguments
///
/// * `src_lensid`: A slice of seed article IDs to start the snowball from.
/// * `max_depth`: The maximum depth of the snowballing process.
/// * `search_for`: Specifies whether to search for references, citations, or both.
/// * `api_key`: The API key for Lens.org.
/// * `client`: An optional `reqwest::Client` to use for requests.
///
/// # Returns
///
/// A `Result` containing a `LensIdCounter` with occurrence counts, or a `LensError`.
pub async fn snowball2<T>(
    src_lensid: &[T],
    max_depth: u8,
    search_for: &SearchFor,
    api_key: &str,
    client: Option<&reqwest::Client>,
    cache: Option<&dyn CacheBackend>,
) -> Result<LensIdCounter, LensError>
where
    T: AsRef<str>,
{
    // Counter to accumulate results across all depths (ADDITION across iterations)
    let mut all_counts = LensIdCounter::with_capacity(probable_output_size(max_depth));

    // Start with depth 1: direct references/citations of the source IDs
    let depth1_results =
        request_references_and_citations(src_lensid, search_for, api_key, client, cache).await?;
    let mut current_counts = LensIdCounter::from(depth1_results);

    // Add depth 1 counts to total
    all_counts.add_from(&current_counts);

    // Iterate for the remaining depths
    for _ in 1..max_depth {
        let mut next_counts = LensIdCounter::new();

        // Collect unique IDs from current depth (DEDUPLICATION!)
        let unique_ids: Vec<&LensId> = current_counts.keys().collect();

        if unique_ids.is_empty() {
            break;
        }

        // Query all unique parent IDs in a batch, preserving parent-child relationships
        let parents_with_children = request_references_and_citations_with_parents(
            &unique_ids,
            search_for,
            api_key,
            client,
            cache,
        )
        .await?;

        // MULTIPLICATION: each child inherits the parent's count
        // If parent appears 5 times and cites child D, then D gets +5 to its count
        for parent_with_children in parents_with_children {
            let parent_count = current_counts.get(&parent_with_children.parent_id);

            for child_id in parent_with_children.children {
                next_counts.add_single_with_count(child_id, parent_count);
            }
        }

        // ADDITION: add this depth's counts to the total
        all_counts.add(next_counts.clone());
        current_counts = next_counts;
    }

    Ok(all_counts)
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
            println!("{article:#?}");
        }
    }

    /// Tests the `snowball` function with invalid IDs to ensure proper error handling.
    #[tokio::test]
    async fn snowball_fail_invalid_ids() {
        let id_list = ["I AM AN INVALID ID", "I AM AN INVALID ID TOO"];
        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();
        let error = snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client), None)
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
        let error = snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client), None)
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
        let new_id = snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client), None)
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
        let direct_citations = snowball(
            &id_list,
            1,
            &SearchFor::Citations,
            &api_key,
            Some(&client),
            None,
        )
        .await
        .unwrap();

        assert!(direct_citations.len() >= 991);

        let direct_references = snowball(
            &id_list,
            1,
            &SearchFor::References,
            &api_key,
            Some(&client),
            None,
        )
        .await
        .unwrap();

        assert!(direct_references.len() >= 76);
    }

    /// Tests that snowball2 (optimized) produces identical results to snowball (original).
    #[tokio::test]
    async fn snowball2_matches_snowball() {
        let id_list = ["020-200-401-307-33X", "050-708-976-791-252"];
        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();

        // Run original snowball
        let original_results =
            snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client), None)
                .await
                .unwrap();

        // Convert to counter for comparison
        let original_counter = LensIdCounter::from(original_results);

        // Run optimized snowball2
        let optimized_counter =
            snowball2(&id_list, 2, &SearchFor::Both, &api_key, Some(&client), None)
                .await
                .unwrap();

        // Verify same number of unique IDs
        assert_eq!(
            original_counter.len(),
            optimized_counter.len(),
            "Different number of unique IDs"
        );

        // Verify each ID has the same count
        for (id, count) in original_counter.iter() {
            let optimized_count = optimized_counter.get(id);
            assert_eq!(
                *count,
                optimized_count,
                "Count mismatch for ID {}: original={}, optimized={}",
                id.as_ref(),
                count,
                optimized_count
            );
        }

        println!("✓ snowball2 produces identical results to snowball");
        println!("  Total unique IDs: {}", original_counter.len());
        println!("  All counts match!");
    }

    /// Tests snowball with Postgres cache integration.
    ///
    /// This test validates:
    /// 1. First call populates cache (cache miss)
    /// 2. Second call uses cache (cache hit)
    /// 3. Results are identical with and without cache
    #[tokio::test]
    #[cfg(feature = "cache-postgres")]
    async fn snowball_with_postgres() {
        use crate::lens::cache::postgres::PostgresBackend;
        use std::time::Instant;

        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();
        let id_list = ["020-200-401-307-33X", "050-708-976-791-252"];

        // Create test backend with unique schema
        let db_url = std::env::var("TEST_POSTGRES_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres@localhost/lens_test".to_string());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let random = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let schema_name = format!("test_snowball_{timestamp}_{random}");

        // Create schema and backend
        let pool = sqlx::PgPool::connect(&db_url).await.unwrap();
        sqlx::query(&format!("CREATE SCHEMA {schema_name}"))
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let url_with_schema = format!("{db_url}?options=-c%20search_path%3D{schema_name}");
        let cache = PostgresBackend::from_url(&url_with_schema)
            .await
            .expect("Failed to create cache backend");

        // First call - should hit API and populate cache
        println!("First snowball call (populating cache)...");
        let start = Instant::now();
        let result1 = snowball(
            &id_list,
            2,
            &SearchFor::Both,
            &api_key,
            Some(&client),
            Some(&cache),
        )
        .await
        .unwrap();
        let first_duration = start.elapsed();
        println!("  Took: {:?}, Found {} IDs", first_duration, result1.len());

        // Second call - should use cache (much faster)
        println!("Second snowball call (using cache)...");
        let start = Instant::now();
        let result2 = snowball(
            &id_list,
            2,
            &SearchFor::Both,
            &api_key,
            Some(&client),
            Some(&cache),
        )
        .await
        .unwrap();
        let cached_duration = start.elapsed();
        println!("  Took: {:?}, Found {} IDs", cached_duration, result2.len());

        // Results should have same number of IDs
        assert_eq!(
            result1.len(),
            result2.len(),
            "Cached and uncached results should have same number of IDs"
        );

        // Cached call should be significantly faster
        println!(
            "✓ Cache speedup: {:.2}x faster",
            first_duration.as_secs_f64() / cached_duration.as_secs_f64()
        );
        assert!(
            cached_duration < first_duration,
            "Cached call should be faster than first call"
        );

        // Call without cache for comparison
        println!("Third call (no cache, for validation)...");
        let result_no_cache =
            snowball(&id_list, 2, &SearchFor::Both, &api_key, Some(&client), None)
                .await
                .unwrap();

        // Convert to sets for comparison (order doesn't matter)
        let set1: std::collections::HashSet<_> = result1.into_iter().collect();
        let set2: std::collections::HashSet<_> = result2.into_iter().collect();
        let set_no_cache: std::collections::HashSet<_> = result_no_cache.into_iter().collect();

        assert_eq!(set1, set2, "Cached results should match first call");
        assert_eq!(
            set1, set_no_cache,
            "Cached results should match no-cache call"
        );

        // Cleanup: drop schema
        let pool = sqlx::PgPool::connect(&db_url).await.unwrap();
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            .execute(&pool)
            .await
            .ok();
        pool.close().await;

        println!("✓ snowball with Postgres cache works correctly!");
    }
}
