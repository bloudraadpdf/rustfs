use crate::common::{RustFSTestClusterEnvironment, admin_request};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use http::{Method, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BUCKET: &str = "tokoloshe-closed-prefix-cluster";
const CLOSE: &str = "/rustfs/admin/v3/tokoloshe/closed-prefix";
const DELETE: &str = "/rustfs/admin/v3/tokoloshe/closed-prefix/delete";

async fn assert_put_fenced(client: &aws_sdk_s3::Client, key: &str) {
    let result = client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from_static(b"late"))
        .send()
        .await;
    assert!(
        matches!(&result, Err(SdkError::ServiceError(error)) if error.err().meta().code() == Some("PreconditionFailed")),
        "closed prefix accepted a PUT: {result:?}"
    );
}

#[tokio::test]
async fn closed_prefix_survives_peer_restart_and_fences_every_node() -> TestResult {
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;
    let clients = cluster.create_all_clients()?;
    let database = Uuid::new_v4();
    let prefix = format!(
        "root/v1/databases/{:02x}{:02x}/{:02x}{:02x}/{database}/epochs/1-{}/objects/",
        database.as_bytes()[0],
        database.as_bytes()[1],
        database.as_bytes()[2],
        database.as_bytes()[3],
        Uuid::new_v4(),
    );
    let key = format!("{prefix}{}", "a".repeat(64));
    clients[0]
        .put_object()
        .bucket(BUCKET)
        .key(&key)
        .body(ByteStream::from_static(b"before"))
        .send()
        .await?;

    let closed = json!({
        "bucket": BUCKET,
        "prefix": prefix,
        "operation": Uuid::new_v4(),
        "context_sha256": vec![7_u8; 32],
    });
    let body = serde_json::to_string(&closed)?;
    let (status, reply) = admin_request(
        &cluster.nodes[1].url,
        Method::POST,
        CLOSE,
        Some(body.clone()),
        &cluster.access_key,
        &cluster.secret_key,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let proof: Value = serde_json::from_str(&reply)?;
    assert_eq!(proof["closed"], closed);

    for client in &clients {
        assert_put_fenced(client, &key).await;
    }

    cluster.stop_node(2)?;
    cluster.start_node(2).await?;
    let (status, replay) = admin_request(
        &cluster.nodes[2].url,
        Method::POST,
        CLOSE,
        Some(body),
        &cluster.access_key,
        &cluster.secret_key,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_str::<Value>(&replay)?, proof);
    assert_put_fenced(&clients[2], &key).await;

    let deletion = json!({"proof": proof, "keys": [key]});
    let (status, reply) = admin_request(
        &cluster.nodes[3].url,
        Method::POST,
        DELETE,
        Some(serde_json::to_string(&deletion)?),
        &cluster.access_key,
        &cluster.secret_key,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let accepted: Value = serde_json::from_str(&reply)?;
    assert_eq!(accepted, deletion);

    for client in &clients {
        let list = client.list_objects_v2().bucket(BUCKET).prefix(&prefix).send().await?;
        assert!(list.contents().is_empty(), "closed epoch still contains objects");
    }
    Ok(())
}
