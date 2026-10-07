//! statex node: leases, actor ownership, replication, routing and the HTTP API.

pub mod actor;
mod alarms;
pub mod api;
pub mod deploy;
pub mod extensions;
pub mod layout;
pub mod lease;
pub mod node;
mod outbox;
pub mod owner;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use layout::ActorId;
pub use node::{InvOp, Invocation, Node, NodeConfig, Outcome};
pub use statex_runtime::database;
pub use statex_runtime::{Metric, MetricLabel, MetricsSink, MetricValue, TracingMetricsSink};

/// A running node.
pub struct NodeHandle {
    pub node: Arc<Node>,
    pub addr: SocketAddr,
    pub internal_addr: SocketAddr,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl NodeHandle {
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Graceful shutdown: release actors and the lease so peers take over immediately.
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        self.node.release_all().await;
        self.node.lease.release().await;
        for t in self.tasks.drain(..) {
            t.abort();
        }
    }

    /// Simulates a crash: stops everything without releasing anything.
    pub fn kill(mut self) {
        let _ = self.stop.send(true);
        self.node.lease.fence("killed");
        for t in self.tasks.drain(..) {
            t.abort();
        }
        self.node.drop_all();
    }

    /// Waits until the node stops (fenced or shut down).
    pub async fn wait(&mut self) {
        let mut rx = self.stop.subscribe();
        let mut fenced = self.node.lease.subscribe();
        tokio::select! {
            _ = rx.wait_for(|s| *s) => {}
            _ = fenced.wait_for(|f| f.is_some()) => {}
        }
    }
}

fn advertise_for(addr: SocketAddr) -> String {
    if addr.ip().is_unspecified() {
        tracing::warn!(
            "listening on {addr}; advertising 127.0.0.1 — set --advertise for multi-host fleets"
        );
        format!("http://127.0.0.1:{}", addr.port())
    } else {
        format!("http://{addr}")
    }
}

/// Starts a node: binds listeners, acquires the lease, loads deployments and
/// spawns the background loops.
pub async fn start(cfg: NodeConfig) -> Result<NodeHandle> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let public = tokio::net::TcpListener::bind(cfg.listen).await?;
    let addr = public.local_addr()?;
    let internal = match cfg.internal_listen {
        Some(a) => Some(tokio::net::TcpListener::bind(a).await?),
        None => None,
    };
    let internal_addr = internal
        .as_ref()
        .map(|l| l.local_addr())
        .transpose()?
        .unwrap_or(addr);
    let advertise = cfg
        .advertise
        .clone()
        .unwrap_or_else(|| advertise_for(internal_addr));
    let (node, renew) = Node::new(cfg.clone(), advertise).await?;
    let (stop, _) = watch::channel(false);
    let mut tasks = vec![renew];
    {
        let node = node.clone();
        let mut fenced = node.lease.subscribe();
        let exit = cfg.exit_on_fence;
        tasks.push(tokio::spawn(async move {
            if fenced.wait_for(|f| f.is_some()).await.is_ok() {
                node.drop_all();
                if exit {
                    std::process::exit(3);
                }
            }
        }));
    }
    {
        let node = node.clone();
        let every = cfg.deploy_poll;
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(e) = node.refresh_apps().await {
                    tracing::warn!("deploy poll failed: {e:#}");
                }
            }
        }));
    }
    {
        let node = node.clone();
        let idle = cfg.idle_timeout;
        tasks.push(tokio::spawn(async move {
            let every = (idle / 4).clamp(
                std::time::Duration::from_millis(100),
                std::time::Duration::from_secs(30),
            );
            loop {
                tokio::time::sleep(every).await;
                node.evict_idle(idle).await;
            }
        }));
    }
    tasks.push(tokio::spawn(node.clone().run_timers()));
    tasks.push(tokio::spawn(node.clone().run_waker()));
    {
        let node = node.clone();
        let mut rx = stop.subscribe();
        tasks.push(tokio::spawn(async move {
            tokio::select! {
                _ = node.run_outbox() => {}
                _ = rx.wait_for(|stopped| *stopped) => {}
            }
        }));
    }
    let serve = |listener: tokio::net::TcpListener, router: axum::Router| {
        let mut rx = stop.subscribe();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = rx.wait_for(|s| *s).await;
                })
                .await;
        })
    };
    match internal {
        Some(l) => {
            tasks.push(serve(public, api::public_router(node.clone())));
            tasks.push(serve(l, api::internal_router(node.clone())));
        }
        None => tasks.push(serve(
            public,
            api::public_router(node.clone()).merge(api::internal_router(node.clone())),
        )),
    }
    tracing::info!(node = cfg.node_id, %addr, store = node.store.describe(), "statex node listening");
    Ok(NodeHandle {
        node,
        addr,
        internal_addr,
        stop,
        tasks,
    })
}
