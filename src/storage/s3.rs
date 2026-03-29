use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::Builder as S3ConfigBuilder,
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;

pub struct S3Store {
    client: Client,
    bucket: String,
    multipart_threshold: u64,
    part_size: u64,
}

impl S3Store {
    pub async fn new(
        bucket: impl Into<String>,
        endpoint_url: Option<&str>,
        multipart_threshold: u64,
        part_size: u64,
    ) -> anyhow::Result<Self> {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let mut builder = S3ConfigBuilder::from(&sdk_config);
        if let Some(url) = endpoint_url {
            builder = builder.endpoint_url(url).force_path_style(true);
        }
        let client = Client::from_conf(builder.build());
        Ok(Self { client, bucket: bucket.into(), multipart_threshold, part_size })
    }

    /// Upload bytes — streaming for small, multipart for large.
    pub async fn put_object(&self, key: &str, data: Bytes, content_type: &str) -> anyhow::Result<()> {
        if data.len() as u64 >= self.multipart_threshold {
            self.multipart_upload(key, data, content_type).await
        } else {
            self.streaming_upload(key, data, content_type).await
        }
    }

    async fn streaming_upload(&self, key: &str, data: Bytes, content_type: &str) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("streaming upload failed for {key}: {e}"))?;
        Ok(())
    }

    async fn multipart_upload(&self, key: &str, data: Bytes, content_type: &str) -> anyhow::Result<()> {
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

        let create = self.client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("create multipart failed: {e}"))?;

        let upload_id = create.upload_id()
            .ok_or_else(|| anyhow::anyhow!("no upload_id returned"))?
            .to_string();

        let mut parts: Vec<CompletedPart> = Vec::new();
        for (i, chunk) in data.chunks(self.part_size as usize).enumerate() {
            let part_number = (i + 1) as i32;
            let resp = self.client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(Bytes::copy_from_slice(chunk)))
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("upload part {part_number} failed: {e}"))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(resp.e_tag().unwrap_or_default())
                    .build(),
            );
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("complete multipart failed: {e}"))?;
        Ok(())
    }

    /// Returns None if the key does not exist.
    pub async fn get_object(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        match self.client.get_object().bucket(&self.bucket).key(key).send().await {
            Ok(resp) => {
                let data = resp.body.collect().await
                    .map_err(|e| anyhow::anyhow!("body collect failed: {e}"))?
                    .into_bytes();
                Ok(Some(data))
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("get_object failed for {key}: {svc}"))
                }
            }
        }
    }

    /// Write only if key does not exist. Returns true if written.
    /// Best-effort (not atomic on R2) — acceptable for shard claim registration.
    pub async fn put_if_absent(&self, key: &str, data: Bytes) -> anyhow::Result<bool> {
        if self.get_object(key).await?.is_some() {
            return Ok(false);
        }
        self.streaming_upload(key, data, "application/json").await?;
        Ok(true)
    }

    pub async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("delete_object failed for {key}: {e}"))?;
        Ok(())
    }

    pub async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }
            let resp = req.send().await
                .map_err(|e| anyhow::anyhow!("list_objects failed: {e}"))?;
            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(keys)
    }
}
