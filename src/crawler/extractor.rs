/// Metadata parsed from a book page.
#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub title: String,
    pub author: String,
    pub downloads: Vec<DownloadTarget>,
}

#[derive(Debug, Clone)]
pub struct DownloadTarget {
    pub url: String,
    pub format: String, // "pdf" | "epub" | "html"
}

/// Extract metadata and download links from rendered HTML.
pub fn extract_metadata(html: &str, base_url: &str) -> PageMetadata {
    let title = extract_meta_content(html, "og:title")
        .or_else(|| extract_tag_content(html, "title"))
        .unwrap_or_else(|| "Unknown Title".to_string());

    let author = extract_meta_content(html, "author")
        .or_else(|| extract_meta_content(html, "dc.creator"))
        .unwrap_or_else(|| "Unknown Author".to_string());

    let mut downloads = Vec::new();
    for (suffix, fmt) in [(".epub", "epub"), (".pdf", "pdf")] {
        for href in find_hrefs_with_suffix(html, suffix) {
            let url = resolve_url(base_url, &href);
            downloads.push(DownloadTarget {
                url,
                format: fmt.to_string(),
            });
        }
    }

    PageMetadata {
        title,
        author,
        downloads,
    }
}

fn extract_meta_content(html: &str, name: &str) -> Option<String> {
    // Handles both name="..." content="..." and property="..." content="..."
    let patterns = [format!("name=\"{name}\""), format!("property=\"{name}\"")];
    for needle in &patterns {
        if let Some(pos) = html.find(needle.as_str()) {
            let after = &html[pos..];
            if let Some(c_pos) = after.find("content=\"") {
                let start = c_pos + 9;
                if let Some(end) = after[start..].find('"') {
                    return Some(after[start..start + end].to_string());
                }
            }
        }
    }
    None
}

fn extract_tag_content(html: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let pos = html.find(&open)?;
    let inner_start = html[pos..].find('>')? + pos + 1;
    let end = html[inner_start..].find(&close)? + inner_start;
    Some(html[inner_start..end].trim().to_string())
}

fn find_hrefs_with_suffix(html: &str, suffix: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut search = html;
    while let Some(pos) = search.find("href=\"") {
        search = &search[pos + 6..];
        if let Some(end) = search.find('"') {
            let href = &search[..end];
            if href.to_lowercase().ends_with(suffix) {
                results.push(href.to_string());
            }
            search = &search[end + 1..];
        }
    }
    results
}

pub fn resolve_url(base: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if href.starts_with('/') {
        if let Some(after_scheme) = base.find("://") {
            let host_end = base[after_scheme + 3..]
                .find('/')
                .map(|p| after_scheme + 3 + p)
                .unwrap_or(base.len());
            return format!("{}{}", &base[..host_end], href);
        }
    }
    format!("{}/{}", base.trim_end_matches('/'), href)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_epub_and_pdf_links() {
        let html = r#"<html>
            <title>Moby Dick</title>
            <a href="/books/1/moby-dick.epub">EPUB</a>
            <a href="/books/1/moby-dick.pdf">PDF</a>
        </html>"#;
        let meta = extract_metadata(html, "https://example.com");
        assert_eq!(meta.title, "Moby Dick");
        assert_eq!(meta.downloads.len(), 2);
        assert!(meta.downloads.iter().any(|d| d.format == "epub"));
        assert!(meta.downloads.iter().any(|d| d.format == "pdf"));
    }

    #[test]
    fn test_og_title_preferred_over_tag() {
        let html = r#"<html>
            <meta property="og:title" content="OG Title" />
            <title>Tag Title</title>
        </html>"#;
        let meta = extract_metadata(html, "https://example.com");
        assert_eq!(meta.title, "OG Title");
    }

    #[test]
    fn test_resolve_absolute_url_unchanged() {
        assert_eq!(
            resolve_url("https://example.com/books/1", "https://other.com/file.pdf"),
            "https://other.com/file.pdf"
        );
    }

    #[test]
    fn test_resolve_root_relative() {
        assert_eq!(
            resolve_url("https://example.com/books/1", "/files/book.epub"),
            "https://example.com/files/book.epub"
        );
    }

    #[test]
    fn test_no_downloads_returns_empty() {
        let html = "<html><title>Empty</title></html>";
        let meta = extract_metadata(html, "https://example.com");
        assert!(meta.downloads.is_empty());
    }
}
