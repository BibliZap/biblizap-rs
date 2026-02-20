pub mod article;
pub mod cache;
pub mod citations;
mod completion;
pub mod counter;
pub mod error;
mod id_types;
pub mod lensid;
pub mod request;

pub use completion::complete_articles;

use super::common::SearchFor;
use article::Article;
use cache::CacheBackend;
use citations::ReferencesAndCitations;
use counter::LensIdCounter;
use error::LensError;
use id_types::*;
use lensid::LensId;
use request::request_and_parse;
use std::collections::{HashMap, HashSet};

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

/// Helper struct that includes both the parent ID and its references/citations.
/// This can be deserialized directly from the Lens API response.
#[derive(Debug, serde::Deserialize)]
struct ArticleWithReferencesAndCitations {
    lens_id: LensId,
    #[serde(flatten)]
    refs_and_cites: ReferencesAndCitations,
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
    // Separate LensIds from non-LensIds (PMID, DOI, etc.)
    let mut lens_ids: Vec<LensId> = Vec::new();
    let mut non_lens_ids: Vec<String> = Vec::new();

    for id in id_list {
        let id_str = id.as_ref();
        if let Ok(lens_id) = LensId::try_from(id_str) {
            lens_ids.push(lens_id);
        } else {
            non_lens_ids.push(id_str.to_string());
        }
    }

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

    // Query cache for LensIds
    let (cached_refs_lens, cached_cites_lens) = match search_for {
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

    // Query cache for non-LensIds (PMID, DOI, etc.)
    let (cached_refs_non_lens, cached_cites_non_lens) = match search_for {
        SearchFor::References => {
            let refs = cache_backend
                .get_references_not_lens_id(&non_lens_ids)
                .await?;
            (refs, HashMap::new())
        }
        SearchFor::Citations => {
            let cites = cache_backend
                .get_citations_not_lens_id(&non_lens_ids)
                .await?;
            (HashMap::new(), cites)
        }
        SearchFor::Both => {
            let refs = cache_backend
                .get_references_not_lens_id(&non_lens_ids)
                .await?;
            let cites = cache_backend
                .get_citations_not_lens_id(&non_lens_ids)
                .await?;
            (refs, cites)
        }
    };

    // Determine which LensIds have complete cache hits
    let fully_cached_lens_ids: HashSet<LensId> = lens_ids
        .iter()
        .filter(|id| match search_for {
            SearchFor::References => cached_refs_lens.contains_key(id),
            SearchFor::Citations => cached_cites_lens.contains_key(id),
            SearchFor::Both => {
                cached_refs_lens.contains_key(id) && cached_cites_lens.contains_key(id)
            }
        })
        .cloned()
        .collect();

    // Determine which non-LensIds have complete cache hits
    let fully_cached_non_lens_ids: HashSet<String> = non_lens_ids
        .iter()
        .filter(|id| match search_for {
            SearchFor::References => cached_refs_non_lens.contains_key(*id),
            SearchFor::Citations => cached_cites_non_lens.contains_key(*id),
            SearchFor::Both => {
                cached_refs_non_lens.contains_key(*id) && cached_cites_non_lens.contains_key(*id)
            }
        })
        .cloned()
        .collect();

    // Build results from cache for LensIds
    let mut results: Vec<ParentWithChildren> = fully_cached_lens_ids
        .iter()
        .map(|id| {
            let children = match search_for {
                SearchFor::References => cached_refs_lens.get(id).cloned().unwrap_or_default(),
                SearchFor::Citations => cached_cites_lens.get(id).cloned().unwrap_or_default(),
                SearchFor::Both => {
                    let mut refs = cached_refs_lens.get(id).cloned().unwrap_or_default();
                    let mut cites = cached_cites_lens.get(id).cloned().unwrap_or_default();
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

    // Add results from cache for non-LensIds
    for id_str in &fully_cached_non_lens_ids {
        // Convert string ID to LensId - we need to fetch from API to get the LensId
        // For now, we'll just use the children directly
        let children = match search_for {
            SearchFor::References => cached_refs_non_lens
                .get(id_str)
                .cloned()
                .unwrap_or_default(),
            SearchFor::Citations => cached_cites_non_lens
                .get(id_str)
                .cloned()
                .unwrap_or_default(),
            SearchFor::Both => {
                let mut refs = cached_refs_non_lens
                    .get(id_str)
                    .cloned()
                    .unwrap_or_default();
                let mut cites = cached_cites_non_lens
                    .get(id_str)
                    .cloned()
                    .unwrap_or_default();
                refs.append(&mut cites);
                refs
            }
        };

        // We need to convert the string ID to a LensId for ParentWithChildren
        // If we can't parse it as LensId, we need to skip it (it will be fetched from API)
        if let Ok(parent_id) = LensId::try_from(id_str.as_str()) {
            results.push(ParentWithChildren {
                parent_id,
                children,
            });
        }
    }

    // Determine which IDs need to be fetched via HTTP (misses)
    let lens_id_misses: Vec<LensId> = lens_ids
        .iter()
        .filter(|id| !fully_cached_lens_ids.contains(id))
        .cloned()
        .collect();

    let non_lens_id_misses: Vec<String> = non_lens_ids
        .iter()
        .filter(|id| !fully_cached_non_lens_ids.contains(id.as_str()))
        .cloned()
        .collect();

    // Combine misses for API fetch
    let mut all_misses: Vec<String> = lens_id_misses
        .iter()
        .map(|id| id.as_ref().to_string())
        .collect();
    all_misses.extend(non_lens_id_misses);
    let misses = all_misses;

    // Fetch misses from API if any
    if !misses.is_empty() {
        let miss_refs: Vec<&str> = misses.iter().map(|s| s.as_str()).collect();

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
            // Separate LensIds from non-LensIds for storage
            if !refs_results.is_empty() {
                let (refs_batch_lens, refs_batch_non_lens): (Vec<_>, Vec<_>) =
                    refs_results.iter().partition(|pwc| {
                        // Check if parent_id is in original lens_ids list
                        lens_ids.contains(&pwc.parent_id)
                    });

                let refs_lens: Vec<(LensId, Vec<LensId>)> = refs_batch_lens
                    .into_iter()
                    .map(|pwc| (pwc.parent_id.clone(), pwc.children.clone()))
                    .collect();

                let refs_non_lens: Vec<(String, Vec<LensId>)> = refs_batch_non_lens
                    .into_iter()
                    .map(|pwc| (pwc.parent_id.as_ref().to_string(), pwc.children.clone()))
                    .collect();

                if !refs_lens.is_empty() {
                    cache_backend.store_references(&refs_lens).await?;
                }
                if !refs_non_lens.is_empty() {
                    cache_backend
                        .store_references_not_lens_id(&refs_non_lens)
                        .await?;
                }
            }

            if !cites_results.is_empty() {
                let (cites_batch_lens, cites_batch_non_lens): (Vec<_>, Vec<_>) = cites_results
                    .iter()
                    .partition(|pwc| lens_ids.contains(&pwc.parent_id));

                let cites_lens: Vec<(LensId, Vec<LensId>)> = cites_batch_lens
                    .into_iter()
                    .map(|pwc| (pwc.parent_id.clone(), pwc.children.clone()))
                    .collect();

                let cites_non_lens: Vec<(String, Vec<LensId>)> = cites_batch_non_lens
                    .into_iter()
                    .map(|pwc| (pwc.parent_id.as_ref().to_string(), pwc.children.clone()))
                    .collect();

                if !cites_lens.is_empty() {
                    cache_backend.store_citations(&cites_lens).await?;
                }
                if !cites_non_lens.is_empty() {
                    cache_backend
                        .store_citations_not_lens_id(&cites_non_lens)
                        .await?;
                }
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
            let (batch_lens, batch_non_lens): (Vec<_>, Vec<_>) = fetched_results
                .iter()
                .partition(|pwc| lens_ids.contains(&pwc.parent_id));

            let lens_batch: Vec<(LensId, Vec<LensId>)> = batch_lens
                .into_iter()
                .map(|pwc| (pwc.parent_id.clone(), pwc.children.clone()))
                .collect();

            let non_lens_batch: Vec<(String, Vec<LensId>)> = batch_non_lens
                .into_iter()
                .map(|pwc| (pwc.parent_id.as_ref().to_string(), pwc.children.clone()))
                .collect();

            match search_for {
                SearchFor::References => {
                    if !lens_batch.is_empty() {
                        cache_backend.store_references(&lens_batch).await?;
                    }
                    if !non_lens_batch.is_empty() {
                        cache_backend
                            .store_references_not_lens_id(&non_lens_batch)
                            .await?;
                    }
                }
                SearchFor::Citations => {
                    if !lens_batch.is_empty() {
                        cache_backend.store_citations(&lens_batch).await?;
                    }
                    if !non_lens_batch.is_empty() {
                        cache_backend
                            .store_citations_not_lens_id(&non_lens_batch)
                            .await?;
                    }
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
pub async fn snowball<T>(
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

        let articles = completion::complete_articles(&src_id, &api_key, None, None)
            .await
            .unwrap();

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
        assert!(new_id.len() > 75565);

        let score_hashmap = new_id.into_inner();

        let max_score_lens_id = score_hashmap.iter().max_by_key(|entry| entry.1).unwrap();
        assert_eq!(max_score_lens_id.0.as_ref(), "020-200-401-307-33X");
        assert!(*max_score_lens_id.1 > 67usize);

        // Take a subset of unique IDs for further testing (e.g., completing articles)
        let new_id_dedup = score_hashmap
            .into_iter()
            .enumerate()
            .filter(|&(index, _)| index < 500) // Limit to 500 for the next step
            .map(|x| x.1.0)
            .collect::<Vec<_>>();

        let articles = completion::complete_articles(&new_id_dedup, &api_key, Some(&client), None)
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

        assert_eq!(result1, result2, "Cached results should match first call");
        assert_eq!(
            result1, result_no_cache,
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

    /// Test that requests citations and references for a PMID twice with cache.
    /// This should expose a bug where PMIDs are not properly converted to LensIds
    /// before cache lookup, causing the second request to fail or return incorrect results.
    #[tokio::test]
    async fn test_pmid_twice_with_cache() {
        use crate::lens::cache::sqlite::SqliteBackend;

        let api_key = dotenvy::var("LENS_API_KEY").expect("LENS_API_KEY must be set in .env file");
        let client = reqwest::Client::new();

        // Use PMID as input (not a LensId)
        let pmid = "30507730";
        let id_list = [pmid];

        // Create in-memory SQLite cache
        let cache = SqliteBackend::from_url("sqlite::memory:")
            .await
            .expect("Failed to create cache backend");

        // First call - should hit API and populate cache
        println!("First snowball call with PMID (populating cache)...");
        let result1 = snowball(
            &id_list,
            1,
            &SearchFor::Both,
            &api_key,
            Some(&client),
            Some(&cache),
        )
        .await
        .expect("First snowball call failed");

        println!("  Found {} IDs", result1.len());
        assert!(result1.len() > 0, "Should find some results on first call");

        // Second call - should use cache
        println!("Second snowball call with PMID (using cache)...");
        let result2 = snowball(
            &id_list,
            1,
            &SearchFor::Both,
            &api_key,
            Some(&client),
            Some(&cache),
        )
        .await
        .expect("Second snowball call failed");

        println!("  Found {} IDs", result2.len());

        // Results should be identical
        assert_eq!(
            result1.len(),
            result2.len(),
            "Cached and uncached results should have same number of IDs"
        );
        assert_eq!(result1, result2, "Cached results should match first call");

        println!("✓ PMID can be requested twice with cache!");
    }
}
