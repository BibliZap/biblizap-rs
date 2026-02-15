//! Example demonstrating cache usage with biblizap-rs
//!
//! This example shows how to use the caching layer to dramatically reduce
//! API calls and improve performance for repeated queries.
//!
//! Run with:
//! ```bash
//! # SQLite (development)
//! cargo run --example cache_usage --features cache-sqlite
//!
//! # PostgreSQL (production)
//! cargo run --example cache_usage --features cache-postgres
//! ```

#[cfg(feature = "cache")]
use biblizap_rs::lens::cache::{CacheResult, LensCache};
use biblizap_rs::lens::lensid::LensId;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(feature = "cache"))]
    {
        println!("❌ Cache feature not enabled!");
        println!("Run with: cargo run --example cache_usage --features cache-sqlite");
    }

    #[cfg(feature = "cache")]
    {
        println!("🚀 BibliZap Cache Example\n");

        // Initialize cache
        #[cfg(feature = "cache-sqlite")]
        let cache = LensCache::new_sqlite("example_cache.db").await?;

        #[cfg(feature = "cache-postgres")]
        let cache = {
            let db_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://localhost/biblizap".to_string());
            LensCache::new_postgres(&db_url).await?
        };

        println!("✅ Cache initialized\n");

        // Example article IDs
        let article_ids = vec!["020-200-401-307-33X", "050-708-976-791-252"];

        let lens_ids: Vec<LensId> = article_ids
            .iter()
            .filter_map(|id| LensId::try_from(*id).ok())
            .collect();

        println!("📊 Querying {} articles...", lens_ids.len());

        // First query - will be cache misses
        println!("\n--- First Query (Cache Miss Expected) ---");
        let result = cache.get_relationships_batch(&lens_ids).await?;
        print_cache_result(&result);

        if !result.misses.is_empty() {
            println!(
                "\n🌐 Fetching {} articles from Lens API...",
                result.misses.len()
            );

            // Simulate storing data (in real usage, fetch from API)
            for id in &result.misses {
                let citations = vec![]; // Would come from API
                let references = vec![]; // Would come from API
                cache
                    .store_relationships(id, &citations, &references)
                    .await?;
            }

            println!("✅ Stored in cache");
        }

        // Second query - should be cache hits!
        println!("\n--- Second Query (Cache Hit Expected) ---");
        let result = cache.get_relationships_batch(&lens_ids).await?;
        print_cache_result(&result);

        // Cache statistics
        println!("\n--- Cache Statistics ---");
        let stats = cache.get_stats().await?;
        println!("Total cached articles: {}", stats.api_calls_saved);

        println!("\n🎉 Example completed successfully!");
        println!("\n💡 Tips:");
        println!("  - Cache hits are 100-1000x faster than API calls");
        println!("  - First query builds cache, subsequent queries are instant");
        println!("  - Perfect for 500+ concurrent users with similar queries");
        println!("\n📁 Cache file: example_cache.db (can be deleted safely)");
    }

    Ok(())
}

#[cfg(feature = "cache")]
fn print_cache_result(result: &CacheResult<biblizap_rs::lens::cache::CachedArticleData>) {
    println!("  ✅ Cache hits:   {}", result.hits.len());
    println!("  ❌ Cache misses: {}", result.misses.len());

    if result.hits.len() > 0 {
        let hit_rate =
            (result.hits.len() as f64 / (result.hits.len() + result.misses.len()) as f64) * 100.0;
        println!("  📈 Hit rate:     {:.1}%", hit_rate);
    }
}
