use regex::Regex;
use anyhow::{Context, Result};

macro_rules! pattern {
    ($feature_identifer:expr) => {{
        concat!(
            "(?s)",
            $feature_identifer,
            "[[:space:]]*-[[:space:]]*(.*?)[A-Z]+[[:space:]]*-"
        )
    }};
}

static PMID_PATTERN: &str = pattern!("PMID");
static TITLE_PATTERN: &str = pattern!("TI");
static ABSTRACT_PATTERN: &str = pattern!("AB");
static AUTHOR_PATTERN: &str = pattern!("AU");

static ASC_URL_BASE: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pubmed_citedin&id=";
static DESC_URL_BASE: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pubmed_refs&id=";

lazy_static::lazy_static! {
    static ref PMID_REGEX: Regex = Regex::new(PMID_PATTERN).expect("PMID_REGEX failed to compile");
    static ref TITLE_REGEX: Regex = Regex::new(TITLE_PATTERN).expect("TITLE_REGEX failed to compile");
    static ref ABSTRACT_REGEX: Regex = Regex::new(ABSTRACT_PATTERN).expect("ABSTRACT_REGEX failed to compile");
    static ref AUTHOR_REGEX: Regex = Regex::new(AUTHOR_PATTERN).expect("AUTHOR_REGEX failed to compile");
    static ref ID_REGEX: Regex = Regex::new("(?s)<Id>(.*?)</Id>").expect("AUTHOR_REGEX failed to compile");
}


#[derive(Debug, Clone, PartialEq)]
pub struct Article {
    pmid: String,
    title: Option<String>,
    summary: Option<String>,
    authors: Option<Vec<String>>
}

impl Article {
    pub fn from_raw_article(raw_article: &str) -> Result<Article> {
        fn clean_feature(input: &str) -> String {
            pub fn trim_whitespace(s: &str) -> String {
                let mut new_str = s.trim().to_owned();
                let mut prev = ' '; // The initial value doesn't really matter
                new_str.retain(|ch| {
                    let result = ch != ' ' || prev != ' ';
                    prev = ch;
                    result
                });
                new_str
            }
        
            let ret = input.replace("\r\n", "");
        
            trim_whitespace(&ret)
        }

        fn extract_single(article: &str, regex: &Regex, group: usize) -> Option<String> {
            let string = regex.captures(article)?.get(group)?.as_str();
            Some(clean_feature(string))
        }

        fn extract_all(article: &str, regex: &Regex, group: usize) -> Option<Vec<String>> {
            let vec: Option<Vec<String>> = regex.captures_iter(article).map(|x| Some(clean_feature(x.get(group)?.as_str()))).collect();
            vec
        }

        Ok(Article {
            pmid: extract_single(raw_article, &PMID_REGEX, 1)
                .with_context(|| format!("No PMID found for this article : \n{raw_article}"))?,
            title: extract_single(raw_article, &TITLE_REGEX, 1),
            summary: extract_single(raw_article, &ABSTRACT_REGEX, 1),
            authors: extract_all(raw_article, &AUTHOR_REGEX, 1)
        })
    }

    pub fn from_raw_articles(raw_articles: &str) -> Result<impl Iterator<Item = Article> + '_> { 
        Ok(raw_articles
            .split("\r\n\r\n")
            .map(|x| Article::from_raw_article(x).unwrap()))
    }


    async fn request_raw_articles(src_pmid: &[&str]) -> Result<String> {
        let url: String = format!(
            "https://pubmed.ncbi.nlm.nih.gov/?term={}&show_snippets=off&format=pubmed&size=200",
            src_pmid.join(",")
        );

        let body = reqwest::get(url).await?.text().await?;

        let body_to_raw_articles: Regex = Regex::new(r"(?s)<pre.*?(PMID.*)</pre>")?;

        Ok(body_to_raw_articles
            .captures(&body).context("Capture failed")?
            .get(1).context("Get group 1 failed")?
            .as_str()
            .to_owned())
    }

    pub async fn complete_articles(src_pmid: &[&str]) -> Result<Vec<Article>> {
        let raw_articles = Article::request_raw_articles(src_pmid).await?;
        let ret = Ok(Article::from_raw_articles(&raw_articles)?.collect());
        ret
    }
}

async fn snowball_onestep_unsafe(src_pmid: &[&str]) -> Result<Vec<String>> {
    let src_pmid_comma =  src_pmid.join(",");
    let asc_url: String = format!("{ASC_URL_BASE}{src_pmid_comma}");
    let desc_url: String = format!("{DESC_URL_BASE}{src_pmid_comma}");

    let body_asc: String = reqwest::get(asc_url).await?.text().await?;
    let body_desc: String = reqwest::get(desc_url).await?.text().await?;

    let body: String = [body_desc, body_asc].join("\n");

    let dest_pmid : Result<Vec<_>> = ID_REGEX.captures_iter(&body)
        .map(|x| anyhow::Ok(x
            .get(1)
            .context("Couldn't get ID")?
            .as_str()
            .to_owned()))
        .skip(src_pmid.len()) // Skip n first as pubmed returns input as output before giving citations
        .collect();

    dest_pmid
}

pub async fn snowball_onestep(src_pmid: &[&str]) -> Result<Vec<String>> {
    let dest_pmid = futures::future::join_all(src_pmid
            .chunks(325)
            .map(snowball_onestep_unsafe))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<String>>();

    Ok(dest_pmid)
}

pub async fn snowball(src_pmid: &[&str], max_depth: u8) -> Result<Vec<String>> {
    let mut all_pmid : Vec<String> = Vec::new();
    
    let mut current_pmid = src_pmid
        .iter()
        .cloned()
        .map(|x| x.to_owned())
        .collect::<Vec<String>>();

    all_pmid.append(&mut current_pmid.clone());

    for _ in 0..max_depth {
        let current_pmid_refs = current_pmid
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<&str>>();
        
        current_pmid = snowball_onestep(&current_pmid_refs).await?;

        all_pmid.append(&mut current_pmid.clone());
    }

    Ok(all_pmid)
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn regex_pattern_construction() {
        assert_eq!(ABSTRACT_PATTERN, "(?s)AB[[:space:]]*-[[:space:]]*(.*?)[A-Z]+[[:space:]]*-");
    }

    #[tokio::test]
    async fn test_snowball_multiple() {
        let src_pmid = ["30507730"];

        let dest_pmid = snowball(&src_pmid, 2).await.unwrap();

        let expected_dest_pmid = 
            ["30507730", "30507730", "36311211", "35509534", "34997040",
            "34886363", "34067730", "34628279", "34125618", "34065984",
            "33872735", "33334688", "33312483", "33021583", "32955837",
            "32629826", "32487981", "32472025", "32442789", "32338038",
            "32162500", "31991706", "31987537", "31855914", "31837838",
            "31837386", "31817936", "31814877", "31775952", "31694665",
            "31663312", "31530900", "31524089", "31352255", "30899545",
            "30659818", "30507730", "30290832", "30224304", "30193830",
            "30139418", "29879146", "29629183", "29373506", "29189580",
            "29113569", "29097009", "29084230", "28955193", "28870141",
            "28813586", "28783444", "28596798", "28593089", "28548972",
            "28448210", "28422589", "27901037", "27475271", "27421291",
            "27385549", "27350847", "27243798", "27080110", "27030897",
            "26896559", "26842868", "26349502", "26221161", "26205763",
            "25991989", "25141359", "24949644", "24113704", "23299872",
            "23038786", "23012634", "22303996", "22011559", "21773020",
            "21694556", "19910802", "19565683", "19286913", "19197213",
            "19127177", "19002201", "18053143", "17603144", "17452684",
            "16616614", "15380917", "12569225", "12540391", "12183207",
            "7961281", "7124671", "2278545", "1509229", "30507730",
            "36311211", "35509534", "34997040", "34886363", "34067730",
            "36553386", "36311211", "35509534", "34997040", "34886363",
            "34067730"];            

        assert_eq!(dest_pmid, expected_dest_pmid);
    }

    #[tokio::test]
    async fn complete_multiple_articles() {
        let src_pmid = 
            ["30507730", "30507730", "36311211", "35509534", "34997040",
            "34886363", "34067730", "34628279", "34125618", "34065984",
            "33872735", "33334688", "33312483", "33021583", "32955837",
            "32629826", "32487981", "32472025", "32442789", "32338038",
            "32162500", "31991706", "31987537", "31855914", "31837838",
            "31837386", "31817936", "31814877", "31775952", "31694665",
            "31663312", "31530900", "31524089", "31352255", "30899545",
            "30659818", "30507730", "30290832", "30224304", "30193830",
            "30139418", "29879146", "29629183", "29373506", "29189580",
            "29113569", "29097009", "29084230", "28955193", "28870141",
            "28813586", "28783444", "28596798", "28593089", "28548972",
            "28448210", "28422589", "27901037", "27475271", "27421291",
            "27385549", "27350847", "27243798", "27080110", "27030897",
            "26896559", "26842868", "26349502", "26221161", "26205763",
            "25991989", "25141359", "24949644", "24113704", "23299872",
            "23038786", "23012634", "22303996", "22011559", "21773020",
            "21694556", "19910802", "19565683", "19286913", "19197213",
            "19127177", "19002201", "18053143", "17603144", "17452684",
            "16616614", "15380917", "12569225", "12540391", "12183207",
            "7961281", "7124671", "2278545", "1509229", "30507730",
            "36311211", "35509534", "34997040", "34886363", "34067730",
            "36553386", "36311211", "35509534", "34997040", "34886363",
            "34067730"];

        let articles = Article::complete_articles(&src_pmid)
            .await
            .expect("Article request failed");

        assert_eq!(articles.len(), 98);
    }
    
    #[tokio::test]
    async fn complete_single_article() {
        
        let src_pmid: [&str; 1] = ["30507730"];

        let articles = Article::complete_articles(&src_pmid)
            .await
            .expect("Article request failed");
        
        assert_eq!(articles.len(), 1);

        let article = articles
            .get(0)
            .expect("One article must be returned");
        
        let expected_article = Article {
            pmid: "30507730".to_owned(),
            title: Some("Pole Dancing for Fitness: The Physiological and Metabolic Demand of a 60-Minute Class.".to_owned()),
            summary: Some("Nicholas, JC, McDonald, KA, Peeling, P, Jackson, B, Dimmock, JA, Alderson, JA, and Donnelly, CJ. Pole dancing for fitness: The physiological and metabolic demand of a 60-minute class. J Strength Cond Res 33(10): 2704-2710, 2019-Little is understood about the acute physiological or metabolic demand of pole dancing classes. As such, the aims of this study were to quantify the demands of a standardized recreational pole dancing class, classifying outcomes according to American College of Sports Medicine (ACSM) exercise-intensity guidelines, and to explore differences in physiological and metabolic measures between skill- and routine-based class components. Fourteen advanced-level amateur female pole dancers completed three 60-minute standardized pole dancing classes. In one class, participants were fitted with a portable metabolic analysis unit. Overall, classes were performed at a mean VO2 of 16.0 ml·kg·min, total energy cost (EC) of 281.6 kcal (4.7 kcal·min), metabolic equivalent (METs) of 4.6, heart rate of 131 b·min, rate of perceived exertion (RPE) of 6.3/10, and blood lactate of 3.1 mM. When comparing skill- and routine-based components of the class, EC per minute (4.4 vs. 5.3 kcal·min), peak VO2 (21.5 vs. 29.6 ml·kg·min), METs (4.3 vs. 5.2), and RPE (7.2 vs. 8.4) were all greater in the routine-based component (p &lt; 0.01), indicating that classes with an increased focus on routine-based training, as compared to skill-based training, may benefit those seeking to exercise at a higher intensity level, resulting in greater caloric expenditure. In accordance with ASCM guidelines, an advanced-level 60-minute pole dancing class can be classified as a moderate-intensity cardiorespiratory exercise; when completed for ≥30 minutes, ≥5 days per week (total ≥150 minutes) satisfies the recommended level of exercise for improved health and cardiorespiratory fitness.".to_owned()),
            authors: Some(vec!["Nicholas, Joanna C".to_owned(), "McDonald, Kirsty A".to_owned(), "Peeling, Peter".to_owned(), "Jackson, Ben".to_owned(), "Dimmock, James A".to_owned(), "Alderson, Jacqueline A".to_owned(), "Donnelly, Cyril J".to_owned()])
        };
        assert_eq!(*article, expected_article);
    }
}
