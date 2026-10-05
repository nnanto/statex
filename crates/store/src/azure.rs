//! Azure Blob Storage / ADLS Gen2 store (Blob endpoint).
//!
//! Configuration (environment):
//! - `AZURE_STORAGE_ACCOUNT_NAME` (required)
//! - `AZURE_STORAGE_ENDPOINT` optional override, e.g. Azurite
//!   `http://127.0.0.1:10000/devstoreaccount1`
//! - exactly one credential family:
//!   - `AZURE_STORAGE_KEY` (shared key)
//!   - workload identity: `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_CLIENT_ID`,
//!     `AZURE_TENANT_ID`, optional `AZURE_AUTHORITY_HOST`
//!   - otherwise managed identity via IMDS (optional `AZURE_CLIENT_ID`)
//!
//! Conditional writes use `If-None-Match: *` / `If-Match: <etag>`; HTTP 412 and
//! `BlobAlreadyExists` (409) are clean rejections, everything else is ambiguous.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use reqwest::{header::HeaderMap, Method, StatusCode};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::{ETag, Object, ObjectStore, Result, StoreError};

const API_VERSION: &str = "2023-11-03";
/// Block size for streamed uploads and range size for streamed downloads.
const CHUNK: u64 = 8 << 20;

enum Credential {
    SharedKey(Vec<u8>),
    Workload { tenant: String, client: String, token_file: String, authority: String },
    Managed { client: Option<String> },
}

pub struct AzureBlobStore {
    account: String,
    container: String,
    base: String,
    cred: Credential,
    http: reqwest::Client,
    token: Mutex<Option<(String, Instant)>>,
}

impl AzureBlobStore {
    pub fn from_env(container: &str) -> anyhow::Result<Self> {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME")
            .context("AZURE_STORAGE_ACCOUNT_NAME must be set for az:// buckets")?;
        let base = std::env::var("AZURE_STORAGE_ENDPOINT")
            .unwrap_or_else(|_| format!("https://{account}.blob.core.windows.net"));
        let cred = if let Ok(k) = std::env::var("AZURE_STORAGE_KEY") {
            Credential::SharedKey(base64::engine::general_purpose::STANDARD.decode(k.trim())?)
        } else if let Ok(token_file) = std::env::var("AZURE_FEDERATED_TOKEN_FILE") {
            Credential::Workload {
                tenant: std::env::var("AZURE_TENANT_ID").context("AZURE_TENANT_ID")?,
                client: std::env::var("AZURE_CLIENT_ID").context("AZURE_CLIENT_ID")?,
                token_file,
                authority: std::env::var("AZURE_AUTHORITY_HOST")
                    .unwrap_or_else(|_| "https://login.microsoftonline.com/".into()),
            }
        } else {
            Credential::Managed { client: std::env::var("AZURE_CLIENT_ID").ok() }
        };
        Ok(Self {
            account,
            container: container.to_string(),
            base: base.trim_end_matches('/').to_string(),
            cred,
            http: reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?,
            token: Mutex::new(None),
        })
    }

    fn url(&self, key: Option<&str>) -> String {
        match key {
            Some(k) => {
                let enc: Vec<String> =
                    k.split('/').map(|s| urlencoding::encode(s).into_owned()).collect();
                format!("{}/{}/{}", self.base, self.container, enc.join("/"))
            }
            None => format!("{}/{}", self.base, self.container),
        }
    }

    async fn bearer(&self) -> anyhow::Result<String> {
        let mut g = self.token.lock().await;
        if let Some((t, exp)) = g.as_ref() {
            if *exp > Instant::now() + Duration::from_secs(120) {
                return Ok(t.clone());
            }
        }
        #[derive(serde::Deserialize)]
        struct Tok {
            access_token: String,
            #[serde(default)]
            expires_in: serde_json::Value,
        }
        let tok: Tok = match &self.cred {
            Credential::Workload { tenant, client, token_file, authority } => {
                let assertion = tokio::fs::read_to_string(token_file).await?;
                let url = format!(
                    "{}/{}/oauth2/v2.0/token",
                    authority.trim_end_matches('/'),
                    tenant
                );
                let form = [
                    ("client_id", client.as_str()),
                    ("scope", "https://storage.azure.com/.default"),
                    ("grant_type", "client_credentials"),
                    (
                        "client_assertion_type",
                        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                    ),
                    ("client_assertion", assertion.trim()),
                ];
                let body = form
                    .iter()
                    .map(|(k, v)| format!("{k}={}", urlencoding::encode(v)))
                    .collect::<Vec<_>>()
                    .join("&");
                self.http
                    .post(url)
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(body)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?
            }
            Credential::Managed { client } => {
                let mut url = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F".to_string();
                if let Some(c) = client {
                    url.push_str(&format!("&client_id={}", urlencoding::encode(c)));
                }
                self.http
                    .get(url)
                    .header("Metadata", "true")
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?
            }
            Credential::SharedKey(_) => unreachable!(),
        };
        let secs = match &tok.expires_in {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(3600),
            serde_json::Value::String(s) => s.parse().unwrap_or(3600),
            _ => 3600,
        };
        *g = Some((tok.access_token.clone(), Instant::now() + Duration::from_secs(secs)));
        Ok(tok.access_token)
    }

    fn sign(
        &self,
        key: &[u8],
        method: &Method,
        url: &reqwest::Url,
        headers: &HeaderMap,
        content_len: usize,
    ) -> anyhow::Result<String> {
        let h = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        let len = if content_len == 0 { String::new() } else { content_len.to_string() };
        let mut ms: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| k.as_str().starts_with("x-ms-"))
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").trim().to_string()))
            .collect();
        ms.sort();
        let canon_headers: String = ms.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
        let mut resource = format!("/{}{}", self.account, url.path());
        let mut q: Vec<(String, String)> =
            url.query_pairs().map(|(k, v)| (k.to_lowercase(), v.into_owned())).collect();
        q.sort();
        for (k, v) in q {
            resource.push_str(&format!("\n{k}:{v}"));
        }
        let to_sign = format!(
            "{}\n\n\n{}\n\n{}\n\n\n{}\n{}\n\n{}\n{}{}",
            method.as_str(),
            len,
            h("content-type"),
            h("if-match"),
            h("if-none-match"),
            h("range"),
            canon_headers,
            resource
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(key)?;
        mac.update(to_sign.as_bytes());
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        Ok(format!("SharedKey {}:{}", self.account, sig))
    }

    async fn request(
        &self,
        method: Method,
        url: String,
        extra: &[(&str, String)],
        body: Option<Bytes>,
    ) -> anyhow::Result<reqwest::Response> {
        let url = reqwest::Url::parse(&url)?;
        let mut headers = HeaderMap::new();
        headers.insert("x-ms-version", API_VERSION.parse()?);
        headers.insert("x-ms-date", httpdate::fmt_http_date(std::time::SystemTime::now()).parse()?);
        for (k, v) in extra {
            headers.insert(reqwest::header::HeaderName::from_bytes(k.as_bytes())?, v.parse()?);
        }
        let len = body.as_ref().map(|b| b.len()).unwrap_or(0);
        if method == Method::PUT {
            headers.insert("content-length", len.to_string().parse()?);
        }
        let auth = match &self.cred {
            Credential::SharedKey(k) => self.sign(k, &method, &url, &headers, len)?,
            _ => format!("Bearer {}", self.bearer().await?),
        };
        headers.insert("authorization", auth.parse()?);
        let mut req = self.http.request(method, url).headers(headers);
        if let Some(b) = body {
            req = req.body(b);
        }
        Ok(req.send().await?)
    }

    async fn put_with(&self, key: &str, data: Bytes, cond: Option<(&str, String)>) -> Result<ETag> {
        let mut extra = vec![("x-ms-blob-type", "BlockBlob".to_string())];
        if let Some(c) = cond {
            extra.push(c);
        }
        let resp = self.request(Method::PUT, self.url(Some(key)), &extra, Some(data)).await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(etag_of(&resp));
        }
        let code = error_code(&resp);
        if status == StatusCode::PRECONDITION_FAILED
            || (status == StatusCode::CONFLICT && code == "BlobAlreadyExists")
        {
            return Err(StoreError::Precondition);
        }
        let text = resp.text().await.unwrap_or_default();
        Err(StoreError::Other(anyhow!("PUT {key}: {status} {code} {text}")))
    }
}

fn etag_of(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn error_code(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("x-ms-error-code")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Full object size from `Content-Range: bytes a-b/total`, or from
/// `Content-Length` when the server returned the whole object.
fn content_total(resp: &reqwest::Response) -> Option<u64> {
    let h = |n: &str| resp.headers().get(n).and_then(|v| v.to_str().ok());
    match h("content-range") {
        Some(r) => parse_content_range_total(r),
        None => h("content-length")?.parse().ok(),
    }
}

fn parse_content_range_total(r: &str) -> Option<u64> {
    r.rsplit_once('/')?.1.trim().parse().ok()
}

fn parse_list(xml: &str) -> anyhow::Result<(Vec<String>, Option<String>)> {
    use quick_xml::events::Event;
    let mut r = quick_xml::Reader::from_str(xml);
    let mut path: Vec<String> = Vec::new();
    let mut names = Vec::new();
    let mut marker = None;
    loop {
        match r.read_event()? {
            Event::Start(e) => path.push(String::from_utf8_lossy(e.name().as_ref()).into_owned()),
            Event::End(_) => {
                path.pop();
            }
            Event::Text(t) => {
                let txt = t.unescape()?.into_owned();
                match path.last().map(|s| s.as_str()) {
                    Some("Name") if path.iter().any(|p| p == "Blob") => names.push(txt),
                    Some("NextMarker") if !txt.is_empty() => marker = Some(txt),
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok((names, marker))
}

#[async_trait]
impl ObjectStore for AzureBlobStore {
    async fn get(&self, key: &str) -> Result<Option<Object>> {
        let resp = self.request(Method::GET, self.url(Some(key)), &[], None).await?;
        match resp.status() {
            StatusCode::NOT_FOUND => Ok(None),
            s if s.is_success() => {
                let etag = etag_of(&resp);
                let data = resp.bytes().await.map_err(anyhow::Error::from)?;
                Ok(Some(Object { data, etag }))
            }
            s => Err(StoreError::Other(anyhow!("GET {key}: {s}"))),
        }
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Bytes>> {
        if len == 0 {
            return Ok(Some(Bytes::new()));
        }
        let range = format!("bytes={}-{}", start, start + len - 1);
        let resp =
            self.request(Method::GET, self.url(Some(key)), &[("x-ms-range", range)], None).await?;
        match resp.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Some(Bytes::new())),
            s if s.is_success() => {
                Ok(Some(resp.bytes().await.map_err(anyhow::Error::from)?))
            }
            s => Err(StoreError::Other(anyhow!("GET range {key}: {s}"))),
        }
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<ETag> {
        self.put_with(key, data, None).await
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<ETag> {
        self.put_with(key, data, Some(("if-none-match", "*".into()))).await
    }

    async fn put_if_match(&self, key: &str, data: Bytes, etag: &str) -> Result<ETag> {
        self.put_with(key, data, Some(("if-match", etag.to_string()))).await
    }

    async fn put_file(&self, key: &str, path: &Path) -> Result<ETag> {
        let mut f = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        let len = f.metadata().await.map_err(anyhow::Error::from)?.len();
        if len <= CHUNK {
            let mut buf = Vec::with_capacity(len as usize);
            f.read_to_end(&mut buf).await.map_err(anyhow::Error::from)?;
            return self.put(key, Bytes::from(buf)).await;
        }
        // Stage fixed-size blocks, then commit them with one Put Block List.
        // Block ids must all have the same length. The per-upload nonce keeps
        // two concurrent uploads to the same key from overwriting each
        // other's staged blocks.
        let url = self.url(Some(key));
        let nonce: u64 = rand::random();
        let mut ids = Vec::new();
        let mut remaining = len;
        while remaining > 0 {
            let n = remaining.min(CHUNK) as usize;
            let mut buf = vec![0u8; n];
            f.read_exact(&mut buf)
                .await
                .with_context(|| format!("read {} (file changed during upload?)", path.display()))?;
            let id = base64::engine::general_purpose::STANDARD
                .encode(format!("{nonce:016x}{:08}", ids.len()));
            let block_url = format!("{url}?comp=block&blockid={}", urlencoding::encode(&id));
            let resp = self.request(Method::PUT, block_url, &[], Some(Bytes::from(buf))).await?;
            if !resp.status().is_success() {
                let s = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(StoreError::Other(anyhow!("PUT block {key}: {s} {text}")));
            }
            ids.push(id);
            remaining -= n as u64;
        }
        let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?><BlockList>"#);
        for id in &ids {
            xml.push_str(&format!("<Latest>{id}</Latest>"));
        }
        xml.push_str("</BlockList>");
        let resp = self
            .request(Method::PUT, format!("{url}?comp=blocklist"), &[], Some(Bytes::from(xml)))
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(etag_of(&resp));
        }
        let text = resp.text().await.unwrap_or_default();
        Err(StoreError::Other(anyhow!("PUT block list {key}: {status} {text}")))
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> Result<bool> {
        // Ranged reads keep each request within the client timeout; If-Match
        // pins every chunk to the version the first chunk came from.
        let url = self.url(Some(key));
        let mut out: Option<tokio::fs::File> = None;
        let mut etag: Option<String> = None;
        let mut pos = 0u64;
        loop {
            let mut extra = vec![("x-ms-range", format!("bytes={}-{}", pos, pos + CHUNK - 1))];
            if let Some(e) = &etag {
                extra.push(("if-match", e.clone()));
            }
            let resp = self.request(Method::GET, url.clone(), &extra, None).await?;
            let status = resp.status();
            let total = match status {
                StatusCode::NOT_FOUND if etag.is_none() => return Ok(false),
                // Only an empty blob rejects a range starting at 0.
                StatusCode::RANGE_NOT_SATISFIABLE if pos == 0 => 0,
                s if s.is_success() => content_total(&resp)
                    .ok_or_else(|| anyhow!("GET {key}: missing content length"))?,
                s => return Err(StoreError::Other(anyhow!("GET {key}: {s}"))),
            };
            if etag.is_none() {
                etag = Some(etag_of(&resp));
            }
            let body = if total == 0 {
                Bytes::new()
            } else {
                resp.bytes().await.map_err(anyhow::Error::from)?
            };
            if out.is_none() {
                out = Some(
                    tokio::fs::File::create(path)
                        .await
                        .with_context(|| format!("create {}", path.display()))?,
                );
            }
            let f = out.as_mut().unwrap();
            f.write_all(&body).await.map_err(anyhow::Error::from)?;
            pos += body.len() as u64;
            if pos >= total {
                break;
            }
            if body.is_empty() {
                return Err(StoreError::Other(anyhow!("GET {key}: empty chunk at {pos}/{total}")));
            }
        }
        let f = out.as_mut().unwrap();
        f.flush().await.map_err(anyhow::Error::from)?;
        f.sync_all().await.map_err(anyhow::Error::from)?;
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut url = format!(
                "{}?restype=container&comp=list&prefix={}",
                self.url(None),
                urlencoding::encode(prefix)
            );
            if let Some(m) = &marker {
                url.push_str(&format!("&marker={}", urlencoding::encode(m)));
            }
            let resp = self.request(Method::GET, url, &[], None).await?;
            if !resp.status().is_success() {
                let s = resp.status();
                let body = resp.text().await.unwrap_or_default();
                bail_store(format!("LIST {prefix}: {s} {body}"))?;
                unreachable!();
            }
            let text = resp.text().await.map_err(anyhow::Error::from)?;
            let (names, next) = parse_list(&text)?;
            out.extend(names);
            match next {
                Some(m) => marker = Some(m),
                None => break,
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let resp = self.request(Method::DELETE, self.url(Some(key)), &[], None).await?;
        match resp.status() {
            StatusCode::NOT_FOUND => Ok(()),
            s if s.is_success() => Ok(()),
            s => Err(StoreError::Other(anyhow!("DELETE {key}: {s}"))),
        }
    }

    fn describe(&self) -> String {
        format!("az://{} (account {})", self.container, self.account)
    }
}

fn bail_store(msg: String) -> anyhow::Result<()> {
    bail!(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_list_xml() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs><Blob><Name>a/b&amp;c</Name><Properties/></Blob><Blob><Name>a/d</Name></Blob></Blobs><NextMarker>xyz</NextMarker></EnumerationResults>"#;
        let (n, m) = parse_list(xml).unwrap();
        assert_eq!(n, vec!["a/b&c", "a/d"]);
        assert_eq!(m.as_deref(), Some("xyz"));
    }

    #[test]
    fn parses_content_range_total() {
        assert_eq!(parse_content_range_total("bytes 0-8388607/20000000"), Some(20_000_000));
        assert_eq!(parse_content_range_total("bytes */0"), Some(0));
        assert_eq!(parse_content_range_total("garbage"), None);
    }

    /// Runs against Azurite when `STATEX_AZURITE_TEST=1` and the usual
    /// `AZURE_STORAGE_*` variables point at it.
    #[tokio::test]
    async fn azurite_file_roundtrip() {
        if std::env::var("STATEX_AZURITE_TEST").as_deref() != Ok("1") {
            return;
        }
        let container = format!("t{:016x}", rand::random::<u64>());
        let s = AzureBlobStore::from_env(&container).unwrap();
        let r = s
            .request(Method::PUT, format!("{}?restype=container", s.url(None)), &[], Some(Bytes::new()))
            .await
            .unwrap();
        assert!(r.status().is_success(), "create container: {}", r.status());
        let dir = tempfile::tempdir().unwrap();
        // Larger than two chunks and not a multiple of the chunk size.
        let n = (2 * CHUNK + 12_345) as usize;
        let data: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let src = dir.path().join("src.bin");
        std::fs::write(&src, &data).unwrap();
        s.put_file("a/big.db", &src).await.unwrap();
        let dst = dir.path().join("dst.bin");
        assert!(s.get_to_file("a/big.db", &dst).await.unwrap());
        assert!(std::fs::read(&dst).unwrap() == data);
        // Small and empty files go through a single PUT.
        std::fs::write(&src, b"tiny").unwrap();
        s.put_file("a/small.db", &src).await.unwrap();
        assert!(s.get_to_file("a/small.db", &dst).await.unwrap());
        assert_eq!(std::fs::read(&dst).unwrap(), b"tiny");
        std::fs::write(&src, b"").unwrap();
        s.put_file("a/empty.db", &src).await.unwrap();
        assert!(s.get_to_file("a/empty.db", &dst).await.unwrap());
        assert!(std::fs::read(&dst).unwrap().is_empty());
        assert!(!s.get_to_file("a/missing.db", &dst).await.unwrap());
        let _ = s.request(Method::DELETE, format!("{}?restype=container", s.url(None)), &[], None).await;
    }
}
