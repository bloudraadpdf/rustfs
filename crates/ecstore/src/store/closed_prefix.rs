use std::sync::Arc;

use rustfs_lock::NamespaceLockGuard;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

use super::ECStore;
use crate::config::com::save_config_with_opts;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::object_api::ObjectOptions;
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;
use crate::storage_api_contracts::object::HTTPPreconditions;

const MARKER_MAX_BYTES: usize = 2_048;
const MARKER_ROOT: &str = "tokoloshe/closed-prefix/v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedPrefixV1 {
    pub bucket: String,
    pub prefix: String,
    pub operation: Uuid,
    pub context_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedPrefixProofV1 {
    pub closed: ClosedPrefixV1,
    pub provider_deployment: Uuid,
    pub bucket_incarnation: Uuid,
}

pub(crate) struct PrefixMutationGuard {
    guards: Vec<NamespaceLockGuard>,
}

impl PrefixMutationGuard {
    pub(crate) fn options<'a>(&self, options: &'a crate::object_api::ObjectOptions) -> std::borrow::Cow<'a, ObjectOptions> {
        if self.guards.is_empty() {
            return std::borrow::Cow::Borrowed(options);
        }
        let mut owned = options.clone();
        self.add_to_options(&mut owned);
        std::borrow::Cow::Owned(owned)
    }

    pub(crate) fn add_to_options(&self, options: &mut crate::object_api::ObjectOptions) {
        for guard in &self.guards {
            options.add_namespace_lock_guard(guard);
        }
    }
}

impl ECStore {
    pub(crate) async fn admit_prefix_mutation(&self, bucket: &str, object: &str) -> Result<PrefixMutationGuard> {
        self.admit_prefix_mutations(bucket, &[object]).await
    }

    pub(crate) async fn admit_prefix_mutations(&self, bucket: &str, objects: &[&str]) -> Result<PrefixMutationGuard> {
        let mut prefixes: Vec<&str> = objects.iter().flat_map(|object| object_scopes(object)).collect();
        prefixes.sort_unstable();
        prefixes.dedup();
        let mut guards = Vec::new();
        for prefix in prefixes {
            let guard = self
                .prefix_lock(bucket, prefix)
                .await?
                .get_read_lock(get_lock_acquire_timeout())
                .await?;
            match self.read_prefix_marker(bucket, prefix).await {
                Ok(Some(_)) => return Err(Error::PreconditionFailed),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
            if guard.is_lock_lost() {
                return Err(Error::PreconditionFailed);
            }
            guards.push(guard);
        }
        Ok(PrefixMutationGuard { guards })
    }

    pub async fn close_prefix(self: &Arc<Self>, requested: ClosedPrefixV1) -> Result<ClosedPrefixProofV1> {
        if !object_scopes(&format!("{}x", requested.prefix)).contains(&requested.prefix.as_str())
            || requested.operation.is_nil()
            || requested.context_sha256 == [0; 32]
        {
            return Err(Error::PreconditionFailed);
        }
        let lock = self.prefix_lock(&requested.bucket, &requested.prefix).await?;
        let guard = lock.get_write_lock(get_lock_acquire_timeout()).await?;
        // Ordinary mutations acquire prefix before bucket lifecycle.
        let bucket_guard = self.acquire_bucket_lifecycle_write_lock(&requested.bucket).await?;
        let proof = ClosedPrefixProofV1 {
            provider_deployment: self.id,
            bucket_incarnation: self.bucket_incarnation_id_from_disk(&requested.bucket).await?,
            closed: requested.clone(),
        };
        match self.read_prefix_marker(&requested.bucket, &requested.prefix).await? {
            Some(existing) if existing != proof => return Err(Error::PreconditionFailed),
            Some(_) => {}
            None => {
                let bytes = serde_json::to_vec(&proof).map_err(|_| Error::CorruptedFormat)?;
                if bytes.len() > MARKER_MAX_BYTES {
                    return Err(Error::PreconditionFailed);
                }
                save_config_with_opts(
                    Arc::clone(self),
                    &marker_key(&requested.bucket, &requested.prefix),
                    bytes,
                    &ObjectOptions {
                        max_parity: true,
                        http_preconditions: Some(HTTPPreconditions {
                            if_none_match: Some("*".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            }
        }
        if guard.is_lock_lost()
            || bucket_guard.is_lock_lost()
            || self.read_prefix_marker(&requested.bucket, &requested.prefix).await? != Some(proof.clone())
        {
            return Err(Error::PreconditionFailed);
        }
        Ok(proof)
    }

    pub async fn delete_closed_prefix_objects(&self, proof: &ClosedPrefixProofV1, keys: &[String]) -> Result<()> {
        let closed = &proof.closed;
        if keys.is_empty() || keys.len() > 1_000 || keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::PreconditionFailed);
        }
        for key in keys {
            let Some(digest) = key.strip_prefix(&closed.prefix) else {
                return Err(Error::PreconditionFailed);
            };
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(Error::PreconditionFailed);
            }
        }
        let guard = self
            .prefix_lock(&closed.bucket, &closed.prefix)
            .await?
            .get_read_lock(get_lock_acquire_timeout())
            .await?;
        let bucket_guard = self.acquire_bucket_lifecycle_read_lock(&closed.bucket).await?;
        if self.read_prefix_marker(&closed.bucket, &closed.prefix).await? != Some(proof.clone())
            || proof.provider_deployment != self.id
            || proof.bucket_incarnation != self.bucket_incarnation_id_from_disk(&closed.bucket).await?
            || guard.is_lock_lost()
            || bucket_guard.is_lock_lost()
        {
            return Err(Error::PreconditionFailed);
        }
        let mut options = ObjectOptions::default();
        options.add_namespace_lock_guard(&guard);
        options.add_bucket_lifecycle_lock_guard(&bucket_guard);
        for key in keys {
            if guard.is_lock_lost() || bucket_guard.is_lock_lost() {
                return Err(Error::PreconditionFailed);
            }
            match self.handle_delete_object(&closed.bucket, key, options.clone()).await {
                Ok(_) | Err(Error::ObjectNotFound(_, _) | Error::FileNotFound) => {}
                Err(error) => return Err(error),
            }
            super::list_objects::observe_list_objects_mutation(self, &closed.bucket).await;
        }
        if guard.is_lock_lost() || bucket_guard.is_lock_lost() {
            return Err(Error::PreconditionFailed);
        }
        Ok(())
    }

    async fn prefix_lock(&self, bucket: &str, prefix: &str) -> Result<rustfs_lock::NamespaceLockWrapper> {
        self.new_ns_lock(bucket, &format!("{MARKER_ROOT}/{}", scope_hash(bucket, prefix)))
            .await
    }

    async fn read_prefix_marker(&self, bucket: &str, prefix: &str) -> Result<Option<ClosedPrefixProofV1>> {
        let mut reader = match self
            .handle_get_object_reader(
                RUSTFS_META_BUCKET,
                &marker_key(bucket, prefix),
                None,
                http::HeaderMap::new(),
                &ObjectOptions::default(),
            )
            .await
        {
            Ok(reader) => reader,
            Err(Error::FileNotFound | Error::ObjectNotFound(_, _)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let size = usize::try_from(reader.object_info.size).map_err(|_| Error::CorruptedFormat)?;
        if size == 0 || size > MARKER_MAX_BYTES {
            return Err(Error::CorruptedFormat);
        }
        let mut bytes = Vec::with_capacity(size);
        (&mut reader)
            .take(u64::try_from(MARKER_MAX_BYTES + 1).map_err(|_| Error::CorruptedFormat)?)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() != size {
            return Err(Error::CorruptedFormat);
        }
        let marker: ClosedPrefixProofV1 = serde_json::from_slice(&bytes).map_err(|_| Error::CorruptedFormat)?;
        if marker.closed.bucket != bucket
            || marker.closed.prefix != prefix
            || marker.provider_deployment.is_nil()
            || marker.bucket_incarnation.is_nil()
        {
            return Err(Error::CorruptedFormat);
        }
        Ok(Some(marker))
    }
}

fn marker_key(bucket: &str, prefix: &str) -> String {
    format!("{MARKER_ROOT}/{}", scope_hash(bucket, prefix))
}

fn scope_hash(bucket: &str, prefix: &str) -> String {
    let mut scope = Vec::with_capacity(bucket.len() + prefix.len() + 1);
    scope.extend_from_slice(bucket.as_bytes());
    scope.push(0);
    scope.extend_from_slice(prefix.as_bytes());
    rustfs_utils::crypto::hex_sha256(&scope, str::to_owned)
}

fn object_scopes(object: &str) -> Vec<&str> {
    if !object.contains("/v1/databases/") {
        return Vec::new();
    }
    let mut result = Vec::new();
    let segments: Vec<(usize, &str)> = object
        .split_inclusive('/')
        .scan(0, |start, segment| {
            let current = *start;
            *start += segment.len();
            Some((current, segment.trim_end_matches('/')))
        })
        .collect();
    for index in 1..segments.len().saturating_sub(7) {
        let parts = &segments[index..index + 8];
        if parts[0].1 != "v1" || parts[1].1 != "databases" || parts[5].1 != "epochs" || parts[7].1 != "objects" {
            continue;
        }
        let Ok(database) = Uuid::parse_str(parts[4].1) else { continue };
        if database.is_nil()
            || parts[2].1 != format!("{:02x}{:02x}", database.as_bytes()[0], database.as_bytes()[1])
            || parts[3].1 != format!("{:02x}{:02x}", database.as_bytes()[2], database.as_bytes()[3])
        {
            continue;
        }
        let Some((ordinal, label)) = parts[6].1.split_once('-') else { continue };
        if ordinal.parse::<u64>().ok().filter(|value| *value > 0).is_none()
            || Uuid::parse_str(label).ok().filter(|value| !value.is_nil()).is_none()
        {
            continue;
        }
        let end = parts[7].0 + parts[7].1.len() + 1;
        if object.as_bytes().get(end - 1) == Some(&b'/') {
            result.push(&object[..end]);
        }
    }
    result.sort_unstable();
    result.dedup();
    result
}

#[cfg(test)]
mod tests {
    use super::object_scopes;

    const SCOPE: &str =
        "root/v1/databases/1234/5678/12345678-1234-1234-1234-123456789abc/epochs/1-12345678-1234-1234-1234-123456789abc/objects/";

    #[test]
    fn finds_canonical_scope_and_rejects_misspelt_shard() {
        let key = format!("{SCOPE}digest");
        assert_eq!(object_scopes(&key), vec![SCOPE]);
        assert!(object_scopes(&key.replace("/1234/5678/", "/1234/5679/")).is_empty());
    }

    #[test]
    fn finds_closed_scope_even_when_suffix_contains_another_scope() {
        let key = format!("{SCOPE}nested/{SCOPE}digest");
        assert!(object_scopes(&key).contains(&SCOPE));
    }
}
