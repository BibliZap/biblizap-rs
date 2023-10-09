use super::error::LensError;

pub async fn request_response(client: &reqwest::Client,
    api_key: &str,
    id_list: impl IntoIterator<Item = impl serde::Serialize> + serde::Serialize,
    id_type: &str,
    include: &[&str]) -> Result<reqwest::Response, LensError>
{
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
    serde_json::json!(
    {
        "query": {
        "terms": {
        id_type: id_list
    }
    },
        "include": include,
        "size": id_list.into_iter().count()
    }).to_string()
}