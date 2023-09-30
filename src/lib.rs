pub mod lens;
pub mod pubmed;


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_articles() {
        let src_id = ["020-200-401-307-33X", "050-708-976-791-252", "30507730", "10.1016/j.nephro.2007.05.005"];

        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";

        let articles = lens::complete_articles_chunk(&src_id, api_key, None).await.unwrap();

        assert_eq!(articles.len(), src_id.len());

        for article in articles.into_iter() {
            println!("{:#?}", article);
        }
    }
    
    #[tokio::test]
    async fn proto() {
        let id_list = ["020-200-401-307-33X", "050-708-976-791-252", "30507730", "10.1016/j.nephro.2007.05.005"];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let client = reqwest::Client::new();
        let new_id = lens::snowball(&id_list, 2, api_key, Some(&client)).await.unwrap();
        println!("{:?} {}", new_id, new_id.len());

        return;
    }
}
