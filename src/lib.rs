pub mod lens;
pub mod pubmed;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn proto() {
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let src_lensid = ["020-200-401-307-33X", "050-708-976-791-252"];

        let include = ["lens_id","title", "authors", "abstract", "external_ids", "scholarly_citations_count", "source", "year_published"];
        let articles = lens::request_articles(&src_lensid, &include, api_key, None).await.unwrap();

        for article in articles.into_iter() {
            println!("{:#?}", article);
        }
    }
}
