use lens::LensId;

pub mod lens;
pub mod pubmed;

fn snowball_onestep_chunk<'a>(lens_ids: impl Iterator<Item = &'a lens::LensId>) -> anyhow::Result<Vec<lens::LensId>> {
    let out = vec![];
    let a = lens_ids.into_iter().collect::<Vec<&LensId>>();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use crate::lens::{TypedIdList, request_response, References};

    use super::*;

    #[tokio::test]
    async fn request_articles() {
        let src_id = ["020-200-401-307-33X", "050-708-976-791-252", "30507730", "10.1016/j.nephro.2007.05.005"];

        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";

        let articles = lens::complete_articles(&src_id, api_key, None).await.unwrap();

        assert_eq!(articles.len(), src_id.len());

        for article in articles.into_iter() {
            println!("{:#?}", article);
        }
    }
    
    #[tokio::test]
    async fn proto() {
        /*let id_list = ["020-200-401-307-33X", "050-708-976-791-252", "30507730", "10.1016/j.nephro.2007.05.005"];
        let include = ["lens_id", "references", "scholarly_citations"];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";

        let typed_id_list = TypedIdList::from_raw_id_list(&id_list);
        let client = reqwest::Client::new();
        let response = request_response(&client, api_key, &typed_id_list.lens_id, "lens_id", &include).await.unwrap();

        let json_str = response
            .text()
            .await.unwrap();*/

        let json_str = std::fs::read_to_string("snowball.json").unwrap();

        println!("{json_str}");
    
        let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let ret: Vec<lens::LensId> = serde_json::from_value::<Vec<lens::ReferencesAndCitations>>(json_value["data"].clone()).unwrap()
            .into_iter()
            .flat_map(|n| n.flatten())
            .collect::<Vec<_>>();
        

        println!("{:#?}", ret);
        return;
        /*let src_lens_id = ["020-200-401-307-33X", "050-708-976-791-252", "30507730"];
        let api_key = "TdUUUOLUWn9HpA7zkZnu01NDYO1gVdVz71cDjFRQPeVDCrYGKWoY";
        let include = ["lens_id", "references", "scholarly_citations"];
        let client = reqwest::Client::new();

        let body = lens::request_body(&src_lens_id, &include);

        let json_str = lens::request_response(&client, api_key, &body).await.unwrap().text().await.unwrap();

        let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        //println!("{}", serde_json::to_string_pretty(&json_value).unwrap());

        let references_value = json_value["data"].get(0).unwrap().get("references");
        
        let references = serde_json::from_value::<lens::References>(references_value.unwrap().clone()).unwrap();

        let scholarly_citations_value = json_value["data"].get(0).unwrap().get("scholarly_citations");
        
        let scholarly_citations = serde_json::from_value::<lens::ScholarlyCitations>(scholarly_citations_value.unwrap().clone()).unwrap();
        
        println!("{:?}", scholarly_citations);


        let c = references.0.iter().chain(scholarly_citations.0.iter());

        snowball_onestep_chunk(c);*/

        //println!("{:?}", c);
    }

    
}
