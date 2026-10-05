//! HTTP API. Actors are addressed generically by app, type, key and method,
//! so apps never register routes:
//!
//! ```text
//! GET    /v1/apps/<app>/schema
//! GET    /v1/apps/<app>/actors[?type=&limit=]
//! POST   /v1/apps/<app>/actors/<type>/<key>/<method>
//! DELETE /v1/apps/<app>/actors/<type>/<key>
//! ```
//!
//! `<app>` is one or two path segments (`shop` or `payments/shop`). App name
//! segments can never be `actors` or `schema`, which makes parsing unambiguous.
//! Keys are percent-encoded single segments.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde_json::{json, Value as J};

use crate::layout::dec;
use crate::node::{norm, InvOp, Invocation, Node, Outcome, SIGNATURE_HEADER};

impl IntoResponse for Outcome {
    fn into_response(self) -> Response {
        (StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), Json(self.body)).into_response()
    }
}

type S = State<Arc<Node>>;

pub fn public_router(node: Arc<Node>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/apps", get(list_apps))
        .route("/v1/apps/{*rest}", any(dispatch))
        .with_state(node)
}

pub fn internal_router(node: Arc<Node>) -> Router {
    Router::new().route("/internal/v1/invoke", post(internal_invoke)).with_state(node)
}

async fn health(State(node): S) -> Json<J> {
    let me = node.me();
    Json(json!({
        "ok": node.lease.valid(),
        "node": me.node_id,
        "session": me.session,
        "advertise": me.advertise,
        "fenced": node.lease.fenced(),
        "apps": node.apps().iter().map(|a| json!({"app": a.manifest.app, "sha256": a.manifest.sha256})).collect::<Vec<_>>(),
        "resident_actors": node.resident_actors().iter().map(|c| c.to_string()).collect::<Vec<_>>(),
    }))
}

async fn list_apps(State(node): S) -> Json<J> {
    Json(json!({
        "apps": node.apps().iter().map(|a| json!({
            "app": a.manifest.app,
            "sha256": a.manifest.sha256,
            "types": a.manifest.types.iter().map(|t| &t.name).collect::<Vec<_>>(),
        })).collect::<Vec<_>>()
    }))
}

/// A parsed `/v1/apps/...` route.
#[derive(Debug, PartialEq)]
enum Route {
    Schema { app: String },
    Actors { app: String },
    Actor { app: String, ty: String, key: String },
    Call { app: String, ty: String, key: String, method: String },
}

/// Parses the raw (still percent-encoded) path after `/v1/apps/`. Returns
/// `None` when no route matches.
fn parse_route(rest: &str) -> Option<Route> {
    let segs: Vec<&str> = rest.split('/').collect();
    let i = (1..=2.min(segs.len().saturating_sub(1))).find(|&i| matches!(segs[i], "actors" | "schema"))?;
    let app = segs[..i].iter().map(|s| dec(s)).collect::<Option<Vec<_>>>()?.join("/");
    let tail: Vec<String> = segs[i + 1..].iter().map(|s| dec(s)).collect::<Option<_>>()?;
    Some(match (segs[i], tail.as_slice()) {
        ("schema", []) => Route::Schema { app },
        ("actors", []) => Route::Actors { app },
        ("actors", [ty, key]) => Route::Actor { app, ty: ty.clone(), key: key.clone() },
        ("actors", [ty, key, m]) => Route::Call { app, ty: ty.clone(), key: key.clone(), method: m.clone() },
        _ => return None,
    })
}

async fn dispatch(
    State(node): S,
    method: Method,
    OriginalUri(uri): OriginalUri,
    q: Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let rest = uri.path().strip_prefix("/v1/apps/").unwrap_or_default();
    let Some(route) = parse_route(rest) else {
        return Outcome::err(404, "not_found", format!("no route for {}", uri.path())).into_response();
    };
    match (method, route) {
        (Method::GET, Route::Schema { app }) => schema(node, app),
        (Method::GET, Route::Actors { app }) => list_actors(node, app, q).await,
        (Method::DELETE, Route::Actor { app, ty, key }) => delete_actor(node, app, ty, key).await.into_response(),
        (Method::POST, Route::Call { app, ty, key, method }) => call(node, app, ty, key, method, body).await.into_response(),
        (m, _) => Outcome::err(405, "method_not_allowed", format!("{m} is not allowed on {}", uri.path())).into_response(),
    }
}

fn schema(node: Arc<Node>, app: String) -> Response {
    match node.app(&app) {
        Some(a) => Json(serde_json::to_value(&a.manifest).unwrap()).into_response(),
        None => Outcome::err(404, "not_found", format!("app {app:?} is not deployed")).into_response(),
    }
}

async fn list_actors(node: Arc<Node>, app: String, Query(q): Query<HashMap<String, String>>) -> Response {
    let limit = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(100);
    match node.list_actors(&app, q.get("type").map(|s| s.as_str()), limit).await {
        Ok(actors) => Json(json!({ "actors": actors })).into_response(),
        Err(e) => Outcome::err(500, "internal", format!("{e:#}")).into_response(),
    }
}

async fn call(node: Arc<Node>, app: String, ty: String, key: String, method: String, body: Bytes) -> Outcome {
    let args: J = if body.iter().all(|b| b.is_ascii_whitespace()) {
        J::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => return Outcome::err(400, "bad_request", format!("request body is not JSON: {e}")),
        }
    };
    let op = match norm(&method).as_str() {
        "_create" => InvOp::Create,
        m if m.starts_with('_') => return Outcome::err(404, "not_found", format!("unknown operation {method:?}")),
        m => InvOp::Call { method: m.to_string(), args },
    };
    node.invoke(Invocation { app, ty, key, op, chain: vec![] }, 0).await
}

async fn delete_actor(node: Arc<Node>, app: String, ty: String, key: String) -> Outcome {
    node.invoke(Invocation { app, ty, key, op: InvOp::Delete, chain: vec![] }, 0).await
}

async fn internal_invoke(State(node): S, headers: HeaderMap, body: Bytes) -> Outcome {
    let sig = headers.get(SIGNATURE_HEADER).and_then(|v| v.to_str().ok());
    node.invoke_forwarded(&body, sig).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes() {
        let r = |s: &str| parse_route(s);
        assert_eq!(r("shop/schema"), Some(Route::Schema { app: "shop".into() }));
        assert_eq!(r("payments/shop/actors"), Some(Route::Actors { app: "payments/shop".into() }));
        assert_eq!(
            r("payments/shop/actors/cart/a%2Fb/add"),
            Some(Route::Call { app: "payments/shop".into(), ty: "cart".into(), key: "a/b".into(), method: "add".into() })
        );
        assert_eq!(r("shop/actors/cart/alice"), Some(Route::Actor { app: "shop".into(), ty: "cart".into(), key: "alice".into() }));
        // a key may itself be "actors"
        assert_eq!(
            r("shop/actors/cart/actors/get"),
            Some(Route::Call { app: "shop".into(), ty: "cart".into(), key: "actors".into(), method: "get".into() })
        );
        assert_eq!(r("a/b/c/actors"), None);
        assert_eq!(r("shop"), None);
        assert_eq!(r("shop/actors/cart/k/m/extra"), None);
        assert_eq!(r("actors"), None);
    }
}
