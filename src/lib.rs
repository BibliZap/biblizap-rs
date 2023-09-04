pub fn add(left: usize, right: usize) -> usize {
    left + right
}

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

lazy_static::lazy_static! {
    static ref PMID_REGEX: Regex = Regex::new(PMID_PATTERN).unwrap();
    static ref TITLE_REGEX: Regex = Regex::new(TITLE_PATTERN).unwrap();
    static ref ABSTRACT_REGEX: Regex = Regex::new(ABSTRACT_PATTERN).unwrap();
}


#[derive(Debug, Clone)]
struct Article {
    pmid: String,
    title: Option<String>,
    summary: Option<String>,
}

impl Article {
    fn from_raw_article(raw_article: &str) -> Result<Article> {
        fn extract_pattern<'a>(article: &'a str, regex: &Regex, group: usize) -> Option<&'a str> {
            fn str_from_capture(capture: Option<regex::Captures<'_>>, group: usize) -> Option<&str> {
                Some(capture?.get(group)?.as_str())
            }
            let capture = regex.captures(article);
            str_from_capture(capture, group)
        }
        
        fn clean_feature(input: Option<&str>) -> Option<String> {
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
        
            let ret = input?.replace("\r\n", "");
        
            Some(trim_whitespace(&ret))
        }

        Ok(Article {
            pmid: clean_feature(extract_pattern(raw_article, &PMID_REGEX, 1))
                .with_context(|| format!("No PMID found for this article : \n{raw_article}"))?,
            title: clean_feature(extract_pattern(raw_article, &TITLE_REGEX, 1)),
            summary: clean_feature(extract_pattern(raw_article, &ABSTRACT_REGEX, 1)),
        })
    }

    pub fn from_raw_articles<'buf>(raw_articles: &'buf str) -> Result<impl Iterator<Item = Article> + 'buf> { 
        Ok(raw_articles
            .split("\r\n\r\n")
            .map(|x| Article::from_raw_article(x).unwrap()))
    }


    fn request_raw_articles(src_pmid: &[&str]) -> String {
        let url = format!(
            "https://pubmed.ncbi.nlm.nih.gov/?term={}&show_snippets=off&format=pubmed&size=200",
            src_pmid.join(",")
        );

        let body = reqwest::blocking::get(url)
            .expect("Pubmed request failed")
            .text()
            .expect("Couldn't convert request result to text");

        let body_to_raw_articles: Regex = Regex::new(r"(?s)<pre.*?(PMID.*)</pre>").unwrap();

        body_to_raw_articles
            .captures(&body)
            .unwrap()
            .get(1)
            .unwrap()
            .as_str()
            .to_owned()
    }

    fn request_articles(src_pmid: &[&str]) -> Vec<Article> {
        let raw_articles = Article::request_raw_articles(src_pmid);
        Article::from_raw_articles(&raw_articles).expect("a").collect()
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        println!("{ABSTRACT_PATTERN:?}");
        assert_eq!(result, 4);
    }

    #[test]
    fn regex_pattern_construction() {
        assert_eq!(ABSTRACT_PATTERN, "(?s)AB[[:space:]]*-[[:space:]]*(.*?)[A-Z]+[[:space:]]*-");
    }

    #[test]
    fn request_articles() {
        let src_pmid = vec![
            "30507730", "27385549", "32162500", "33312483", "34067730", "12183207", "12540391",
            "12569225", "1509229", "15380917", "16616614", "17452684", "17603144", "18053143",
            "19002201", "19127177", "19197213", "19286913", "19565683", "19910802", "21694556",
            "21773020", "22011559", "22303996", "2278545", "23012634", "23038786", "23299872",
            "24113704", "24949644", "25141359", "25991989", "26205763", "26221161", "26349502",
            "26842868", "26896559", "27030897", "27080110", "27243798", "27350847", "27421291",
            "27475271", "27901037", "28422589", "28448210", "28548972", "28593089", "28596798",
            "28783444", "28813586", "28870141", "28955193", "29084230", "29097009", "29113569",
            "29189580", "29373506", "29629183", "29879146", "30139418", "30193830", "30224304",
            "30290832", "30659818", "30899545", "31352255", "31524089", "31530900", "31663312",
            "31694665", "31775952", "31814877", "31817936", "31837386", "31837838", "31855914",
            "31987537", "31991706", "32442789", "32472025", "32487981", "32629826", "33334688",
            "33872735", "34065984", "34628279", "34886363", "34997040", "35509534", "7124671",
            "7961281",
        ];

        let raw_articles_str = Article::request_raw_articles(&src_pmid);
        //println!("{raw_articles_str}");

        

        let raw_articles: Vec<&str> = raw_articles_str
            .split("\r\n\r\n").collect();

        let first_raw_article = raw_articles.get(0).unwrap();

        let author_regex = Regex::new("(?s)AU[[:space:]]*-[[:space:]]*(.*?)[A-Z]+[[:space:]]*-").unwrap();
        let capture_iter = author_regex.captures_iter(first_raw_article);

        for au_field in capture_iter {
            let a = au_field.get(1);
        }


        println!("{}", first_raw_article);

        let articles = Article::request_articles(&src_pmid);

        assert_eq!(articles.len(), 92);
    }
}
