use bytes::Bytes;

fn bucket() -> Option<String> {
    std::env::var("TEST_S3_BUCKET").ok()
}

async fn store(bucket: &str) -> thoth::storage::S3Store {
    thoth::storage::S3Store::new(bucket, None, 100 * 1024 * 1024, 10 * 1024 * 1024)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_put_and_get_small() {
    let Some(b) = bucket() else { return };
    let s = store(&b).await;
    let key = "test/small_object";
    let data = Bytes::from_static(b"hello thoth");
    s.put_object(key, data.clone(), "text/plain").await.unwrap();
    let fetched = s.get_object(key).await.unwrap().expect("should exist");
    assert_eq!(fetched, data);
    s.delete_object(key).await.unwrap();
}

#[tokio::test]
async fn test_get_missing_returns_none() {
    let Some(b) = bucket() else { return };
    let s = store(&b).await;
    let result = s.get_object("test/definitely_does_not_exist_xyz").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_put_if_absent() {
    let Some(b) = bucket() else { return };
    let s = store(&b).await;
    let key = "test/put_if_absent";
    let _ = s.delete_object(key).await;
    let first = s.put_if_absent(key, Bytes::from_static(b"first")).await.unwrap();
    let second = s.put_if_absent(key, Bytes::from_static(b"second")).await.unwrap();
    assert!(first);
    assert!(!second);
    let val = s.get_object(key).await.unwrap().unwrap();
    assert_eq!(val, Bytes::from_static(b"first"));
    s.delete_object(key).await.unwrap();
}
