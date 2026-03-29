use crate::crawler::extractor::DownloadTarget;

/// Discover download links using the spider crate (requires `spider` feature).
/// Falls back gracefully to empty vec if spider is not available.
#[cfg(feature = "spider")]
pub async fn discover_download_links(url: &str) -> anyhow::Result<Vec<DownloadTarget>> {
    use spider::website::Website;

    let mut website = Website::new(url);
    website.with_limit(1).with_depth(1);
    website.scrape().await;

    let pages = website.get_pages();
    let mut targets = Vec::new();
    if let Some(pages) = pages {
        for page in pages.iter() {
            let html = page.get_html();
            let base = page.get_url().as_str();
            let meta = crate::crawler::extractor::extract_metadata(html, base);
            targets.extend(meta.downloads);
        }
    }
    Ok(targets)
}

#[cfg(not(feature = "spider"))]
pub async fn discover_download_links(_url: &str) -> anyhow::Result<Vec<DownloadTarget>> {
    Ok(vec![])
}
