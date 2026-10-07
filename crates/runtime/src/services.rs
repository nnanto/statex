//! Replaceable outbound I/O and log adapters. The host enforces HTTP admission
//! before dispatch; neither adapter participates in actor transactions.

use std::time::Duration;

use crate::host::{
    statex::host::{http_client, log},
    ActorIdentity,
};

pub use http_client::{Request as HttpRequest, Response as HttpResponse};
pub use log::Level as LogLevel;

pub trait HttpTransport: Send + Sync {
    /// Runs on the actor's blocking execution thread. Honor `timeout` and
    /// bound response size; external side effects are not rolled back.
    fn send(&self, request: HttpRequest, timeout: Duration) -> Result<HttpResponse, String>;
}

pub trait LogSink: Send + Sync {
    fn log(&self, actor: &ActorIdentity, level: LogLevel, message: &str);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricLabel {
    pub name: &'static str,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricValue {
    Counter(u64),
    Duration(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metric {
    pub name: &'static str,
    pub value: MetricValue,
    pub labels: Vec<MetricLabel>,
}

impl Metric {
    pub fn counter(name: &'static str, value: u64, labels: Vec<MetricLabel>) -> Self {
        Self {
            name,
            value: MetricValue::Counter(value),
            labels,
        }
    }

    pub fn duration(name: &'static str, value: Duration, labels: Vec<MetricLabel>) -> Self {
        Self {
            name,
            value: MetricValue::Duration(value),
            labels,
        }
    }
}

/// Receives framework measurements. Implementations should avoid unbounded
/// label values and must not block indefinitely.
pub trait MetricsSink: Send + Sync {
    fn record(&self, metric: &Metric);
}

pub struct TracingMetricsSink;

impl MetricsSink for TracingMetricsSink {
    fn record(&self, metric: &Metric) {
        tracing::info!(
            target: "statex::metrics",
            metric = metric.name,
            value = ?metric.value,
            labels = ?metric.labels,
            "framework metric"
        );
    }
}

/// Reports a measurement without allowing an observer panic to affect work.
pub fn emit_metric(sink: &dyn MetricsSink, metric: Metric) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.record(&metric))).is_err() {
        tracing::warn!(metric = metric.name, "metrics sink panicked");
    }
}

/// Default synchronous HTTP adapter, with redirects disabled so an admitted
/// host cannot redirect a request to a host outside the allowlist.
pub struct DefaultHttpTransport;

impl HttpTransport for DefaultHttpTransport {
    fn send(&self, req: HttpRequest, timeout: Duration) -> Result<HttpResponse, String> {
        const MAX_BODY: u64 = 10 * 1024 * 1024;
        if timeout.is_zero() {
            return Err("HTTP request deadline expired".into());
        }
        let agent = ureq::AgentBuilder::new()
            .timeout(timeout)
            .redirects(0)
            .build();
        let mut r = agent.request(&req.method, &req.url);
        for (k, v) in &req.headers {
            r = r.set(k, v);
        }
        let resp = match req.body {
            Some(b) => r.send_bytes(&b),
            None => r.call(),
        };
        let resp = match resp {
            Ok(r) | Err(ureq::Error::Status(_, r)) => r,
            Err(e) => return Err(e.to_string()),
        };
        let status = resp.status();
        let headers = resp
            .headers_names()
            .into_iter()
            .filter_map(|n| resp.header(&n).map(|v| (n.clone(), v.to_string())))
            .collect();
        let mut body = Vec::new();
        std::io::Read::read_to_end(
            &mut std::io::Read::take(resp.into_reader(), MAX_BODY + 1),
            &mut body,
        )
        .map_err(|e| e.to_string())?;
        if body.len() as u64 > MAX_BODY {
            return Err("HTTP response exceeds 10 MiB limit".into());
        }
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

pub struct TracingLogSink;

impl LogSink for TracingLogSink {
    fn log(&self, id: &ActorIdentity, level: LogLevel, message: &str) {
        let actor = format!("{}/{}/{}", id.app, id.actor_type, id.key);
        match level {
            LogLevel::Trace => tracing::trace!(target: "actor", %actor, "{message}"),
            LogLevel::Debug => tracing::debug!(target: "actor", %actor, "{message}"),
            LogLevel::Info => tracing::info!(target: "actor", %actor, "{message}"),
            LogLevel::Warn => tracing::warn!(target: "actor", %actor, "{message}"),
            LogLevel::Error => tracing::error!(target: "actor", %actor, "{message}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn request(url: String) -> HttpRequest {
        HttpRequest {
            method: "GET".into(),
            url,
            headers: vec![],
            body: None,
        }
    }

    fn serve(response: Vec<u8>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = [0; 4096];
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let received = stream.read(&mut buf).unwrap();
                assert!(received > 0, "request ended before its headers");
                request.extend_from_slice(&buf[..received]);
                assert!(request.len() <= 16 * 1024, "test request headers too large");
            }
            // Oversized responses can make the client close the connection.
            if let Err(error) = stream.write_all(&response) {
                assert!(matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ));
            }
        });
        (format!("http://{addr}/"), handle)
    }

    #[test]
    fn default_transport_does_not_follow_redirects() {
        let (url, server) = serve(
            b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
        );
        let response = DefaultHttpTransport
            .send(request(url), Duration::from_secs(2))
            .unwrap();
        assert_eq!(response.status, 302);
        server.join().unwrap();
    }

    #[test]
    fn default_transport_rejects_oversized_response_and_expired_budget() {
        let size = 10 * 1024 * 1024 + 1;
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n")
                .into_bytes();
        response.resize(response.len() + size, b'x');
        let (url, server) = serve(response);
        let error = DefaultHttpTransport
            .send(request(url), Duration::from_secs(5))
            .unwrap_err();
        assert!(error.contains("10 MiB"));
        server.join().unwrap();
        let error = DefaultHttpTransport
            .send(request("http://127.0.0.1:1/".into()), Duration::ZERO)
            .unwrap_err();
        assert!(error.contains("deadline"));
    }
}
