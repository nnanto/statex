//! Actor-to-actor calls.
//!
//! A component calls another actor type through a *client interface* it
//! imports (package `team:app`, see [`crate::manifest::client_package`]). Each
//! function takes the callee's actor key first and returns
//! `result<T, call-error>`. The runtime implements these imports dynamically:
//! arguments are mapped to JSON with the caller's view of the types, handed to
//! the embedder's [`ActorCaller`] (the node routes them exactly like an HTTP
//! call), and the reply is mapped back. A callee that no longer matches the
//! caller's types yields `call-error::incompatible` rather than a trap.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as J};
use wasmtime::component::{Linker, Val};

use crate::host::HostState;
use crate::json;
use crate::manifest::{CallImport, Method, Ty};

/// An actor address.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorRef {
    pub app: String,
    #[serde(rename = "type")]
    pub actor_type: String,
    pub key: String,
}

impl fmt::Display for ActorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.app, self.actor_type, self.key)
    }
}

/// A call from one actor to another.
#[derive(Debug, Clone)]
pub struct CallRequest {
    pub target: ActorRef,
    pub method: String,
    /// Positional JSON arguments.
    pub args: J,
    /// Actors currently executing up the call chain, outermost first; the
    /// last entry is the direct caller. A target already on the chain is a cycle.
    pub chain: Vec<ActorRef>,
    /// How long the caller is willing to wait.
    pub timeout: Duration,
}

/// Why a call did not produce a callee result (`statex:host/actors.call-error`).
#[derive(Debug, Clone, PartialEq)]
pub enum CallFailure {
    NotFound(String),
    Incompatible(String),
    Trap(String),
    Unavailable(String),
    Cycle(String),
    Timeout,
}

impl fmt::Display for CallFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallFailure::NotFound(m) => write!(f, "not found: {m}"),
            CallFailure::Incompatible(m) => write!(f, "incompatible: {m}"),
            CallFailure::Trap(m) => write!(f, "trap: {m}"),
            CallFailure::Unavailable(m) => write!(f, "unavailable: {m}"),
            CallFailure::Cycle(m) => write!(f, "cycle: {m}"),
            CallFailure::Timeout => write!(f, "timed out"),
        }
    }
}

/// Outcome of a call, in the same shape as the HTTP API's.
#[derive(Debug, Clone, PartialEq)]
pub enum CallReply {
    /// The method returned (for `result<T, E>` methods: returned `ok(T)`).
    Ok(J),
    /// A `result<T, E>` method returned `err(E)`; holds E.
    MethodErr(J),
    Failed(CallFailure),
}

/// Routes calls made by actors. Called on the thread executing the caller,
/// which blocks until the reply arrives.
pub trait ActorCaller: Send + Sync {
    fn call(&self, req: CallRequest) -> CallReply;
}

/// Time kept back from the callee so the caller can still handle a timeout
/// before its own deadline traps it.
fn budget(deadline: Option<Instant>) -> Duration {
    let Some(d) = deadline else { return Duration::from_secs(60) };
    let left = d.saturating_duration_since(Instant::now());
    left.saturating_sub((left / 10).min(Duration::from_millis(100)))
}

/// Defines every client interface of `calls` in `linker`.
pub(crate) fn link(linker: &mut Linker<HostState>, calls: &[CallImport]) -> Result<()> {
    for c in calls {
        let c = Arc::new(c.clone());
        let mut inst = linker.instance(&c.import)?;
        for (i, m) in c.methods.iter().enumerate() {
            let c = c.clone();
            inst.func_new(&m.name, move |store, _ty, params, results| {
                results[0] = dispatch(store.data(), &c, &c.methods[i], params);
                Ok(())
            })?;
        }
    }
    Ok(())
}

fn failed(f: CallFailure) -> Val {
    let (case, msg) = match f {
        CallFailure::NotFound(m) => ("not-found", Some(m)),
        CallFailure::Incompatible(m) => ("incompatible", Some(m)),
        CallFailure::Trap(m) => ("trap", Some(m)),
        CallFailure::Unavailable(m) => ("unavailable", Some(m)),
        CallFailure::Cycle(m) => ("cycle", Some(m)),
        CallFailure::Timeout => ("timeout", None),
    };
    let err = Val::Variant(case.into(), msg.map(|m| Box::new(Val::String(m))));
    Val::Result(Err(Some(Box::new(err))))
}

fn dispatch(state: &HostState, c: &CallImport, m: &Method, params: &[Val]) -> Val {
    let what = format!("{}.{}", c.import, m.name);
    let Some(Val::String(key)) = params.first() else {
        return failed(CallFailure::Incompatible(format!("{what}: the actor key must be a string")));
    };
    let args: Vec<J> = m.params.iter().zip(&params[1..]).map(|(p, v)| json::val_to_json(&p.ty, v)).collect();
    let Some(caller) = &state.caller else {
        return failed(CallFailure::Unavailable("actor calls are not available in this host".into()));
    };
    let timeout = budget(state.deadline);
    if timeout.is_zero() {
        return failed(CallFailure::Timeout);
    }
    let req = CallRequest {
        target: ActorRef { app: c.app.clone(), actor_type: c.actor_type.clone(), key: key.clone() },
        method: m.name.clone(),
        args: J::Array(args),
        chain: state.chain.clone(),
        timeout,
    };
    let target = req.target.clone();
    let value = match (caller.call(req), &m.result) {
        (CallReply::Failed(f), _) => return failed(f),
        (CallReply::Ok(_), None) => return Val::Result(Ok(None)),
        (CallReply::Ok(v), Some(t @ Ty::Result { .. })) => json::json_to_val(t, &json!({ "ok": v }), "result"),
        (CallReply::Ok(v), Some(t)) => json::json_to_val(t, &v, "result"),
        (CallReply::MethodErr(e), Some(t @ Ty::Result { .. })) => json::json_to_val(t, &json!({ "err": e }), "result"),
        (CallReply::MethodErr(e), _) => Err(format!("returned an error the client interface does not expect: {e}")),
    };
    match value {
        Ok(v) => Val::Result(Ok(Some(Box::new(v)))),
        Err(e) => failed(CallFailure::Incompatible(format!(
            "{target}.{}: {e} (the callee's signature differs from this client interface; run `statex calls sync`)",
            m.name
        ))),
    }
}
