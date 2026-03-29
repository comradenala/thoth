/// Headless browser wrapper using chromiumoxide.
/// Only compiled when the `spider` feature is enabled.
#[cfg(feature = "spider")]
pub mod inner {
    use chromiumoxide::browser::{Browser, BrowserConfig};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub struct HeadlessBrowser {
        browser: Arc<Mutex<Browser>>,
    }

    impl HeadlessBrowser {
        pub async fn launch() -> anyhow::Result<Self> {
            let (browser, mut handler) = Browser::launch(
                BrowserConfig::builder()
                    .no_sandbox()
                    .build()
                    .map_err(|e| anyhow::anyhow!("browser config error: {e}"))?,
            )
            .await?;

            tokio::spawn(async move {
                loop {
                    if handler.next().await.is_none() {
                        break;
                    }
                }
            });

            Ok(Self {
                browser: Arc::new(Mutex::new(browser)),
            })
        }

        pub async fn fetch_html(&self, url: &str) -> anyhow::Result<String> {
            let page = {
                let browser = self.browser.lock().await;
                browser.new_page(url).await?
            };
            page.wait_for_navigation().await?;
            let html = page.content().await?;
            page.close().await?;
            Ok(html)
        }
    }
}

/// Fallback for non-spider builds: simple reqwest fetch.
#[cfg(not(feature = "spider"))]
pub mod inner {
    pub struct HeadlessBrowser {
        client: reqwest::Client,
    }

    impl HeadlessBrowser {
        pub async fn launch() -> anyhow::Result<Self> {
            let client = reqwest::Client::builder()
                .user_agent("thoth-archiver/0.1")
                .timeout(std::time::Duration::from_secs(30))
                .build()?;
            Ok(Self { client })
        }

        pub async fn fetch_html(&self, url: &str) -> anyhow::Result<String> {
            let html = self.client.get(url).send().await?.text().await?;
            Ok(html)
        }
    }
}

pub use inner::HeadlessBrowser;
