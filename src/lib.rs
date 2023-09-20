pub mod lens;
pub mod pubmed;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_articles() {
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let src_lensid = ["020-200-401-307-33X", "050-708-976-791-252"];

        let include = ["lens_id","title", "authors", "abstract", "external_ids", "scholarly_citations_count", "source", "year_published"];
        let articles = lens::request_articles(&src_lensid, &include, api_key, None).await.unwrap();

        for article in articles.into_iter() {
            println!("{:#?}", article);
        }
    }
    
    #[tokio::test]
    async fn proto() {
        let src_lensid = ["020-200-401-307-33X", "050-708-976-791-252"];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let include = ["lens_id", "references", "scholarly_citations"];
        let client = reqwest::Client::new();

        let body = lens::request_body(&src_lensid, &include);

        let json_str = lens::request_response(&client, api_key, &body).await.unwrap().text().await.unwrap();

        let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        println!("{}", serde_json::to_string_pretty(&json_value).unwrap())
        
    }

    
}
