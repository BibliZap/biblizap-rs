pub mod lens;
pub mod pubmed;
use anyhow::{Context, Result};

async fn request(client: &reqwest::Client, api_key: &str, body: &str) -> Result<reqwest::Response> {
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
                .context("x-rate-limit-retry-after-seconds missing")?
                .to_str()?
                .parse::<u64>()?;
            
            async_std::task::sleep(std::time::Duration::from_secs(seconds_to_wait)).await;
        }
    }
}

fn request_body(id_list: &[&str], include_list: &[&str]) -> String {
    let body = serde_json::json!(
    {
        "query": {
            "terms": {
                "lens_id": id_list
            }
        },
        "include": include_list,
        "size": id_list.len()
    }).to_string();

    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn proto() {
        use serde_json::Value;
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let src_lensid = ["020-200-401-307-33X", "050-708-976-791-252"];

        let include_list = ["lens_id","title", "authors", "abstract", "external_ids", "scholarly_citations_count", "source", "year_published"];
        let body = request_body(&src_lensid, &include_list);
        println!("{}", body);
        let client = reqwest::Client::new();
        let response = request(&client, api_key, &body)
                .await.unwrap();
        
        let text = response
            .text()
            .await
            .unwrap();

        //println!("{text}");

        //let text = std::fs::read_to_string("sample.txt").unwrap();

        let v: Value = serde_json::from_str(&text).unwrap();

        let articles = &v["data"].as_array().unwrap();

        for article_value in articles.into_iter() {
            let article = serde_json::from_value::<lens::Article>(article_value.clone()).unwrap();
            println!("{:?}", article);
        }        
    }
}
