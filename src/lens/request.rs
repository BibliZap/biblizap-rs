use super::error::LensError;

/// Makes a POST request to the Lens.org API's scholarly search endpoint.
///
/// This function constructs the request body based on the provided IDs, ID type,
/// and fields to include, and then sends the request using `request_response_with_body`.
///
/// # Arguments
///
/// * `client`: The `reqwest::Client` to use for the request.
/// * `api_key`: The API key for Lens.org.
/// * `id_list`: An iterator of IDs to search for. Must be serializable.
/// * `id_type`: The type of IDs in `id_list` (e.g., "pmid", "lens_id", "doi").
/// * `include`: A slice of strings specifying which fields to include in the response.
///
/// # Returns
///
/// A `Result` containing the `reqwest::Response` if successful, or a `LensError` if an error occurs.
pub async fn request_response(
    client: &reqwest::Client,
    api_key: &str,
    id_list: impl IntoIterator<Item = impl serde::Serialize> + serde::Serialize,
    id_type: &str,
    include: &[&str],
) -> Result<reqwest::Response, LensError> {
    request_response_with_body(
        client,
        api_key,
        &make_request_body(id_list, id_type, include),
    )
    .await
}

/// Sends a POST request with a pre-built JSON body to the Lens.org API.
///
/// This function handles sending the HTTP request, adding necessary headers
/// (Authorization, Content-Type), and retrying the request if a rate limit
/// error is encountered based on the `x-rate-limit-retry-after-seconds` header.
///
/// # Arguments
///
/// * `client`: The `reqwest::Client` to use for the request.
/// * `api_key`: The API key for Lens.org.
/// * `body`: The JSON request body as a string.
///
/// # Returns
///
/// A `Result` containing the `reqwest::Response` if successful (status 200),
/// or a `LensError` if an error occurs (e.g., HTTP error, missing rate limit header, parse error).
async fn request_response_with_body(
    client: &reqwest::Client,
    api_key: &str,
    body: &str,
) -> Result<reqwest::Response, LensError> {
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
            log::debug!("{:?}", response.headers());
            let seconds_to_wait = response
                .headers()
                .get("x-rate-limit-retry-after-seconds")
                .ok_or(LensError::RateLimitMissing)?
                .to_str()?
                .parse::<u64>()?;

            async_std::task::sleep(std::time::Duration::from_secs(seconds_to_wait)).await;
        }
    }
}

/// Constructs the JSON request body for a Lens.org scholarly search API request.
///
/// The body includes the search query based on the provided IDs and type,
/// the list of fields to include in the response, and the requested size
/// (number of results).
///
/// # Arguments
///
/// * `id_list`: An iterator of IDs to include in the query. Must be serializable.
/// * `id_type`: The type of IDs in `id_list` (e.g., "pmid", "lens_id", "doi").
/// * `include`: A slice of strings specifying which fields to include in the response.
///
/// # Returns
///
/// A `String` containing the JSON request body.
fn make_request_body(
    id_list: impl IntoIterator<Item = impl serde::Serialize> + serde::Serialize,
    id_type: &str,
    include: &[&str],
) -> String {
    serde_json::json!(
    {
        "query": {
        "terms": {
        id_type: id_list
    }
    },
        "include": include,
        "size": id_list.into_iter().count()
    })
    .to_string()
}
