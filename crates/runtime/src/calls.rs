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
    /// Trusted invocation lineage and identity, forwarded under peer authentication.
    pub context: crate::invocation::InvocationMetadata,
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
    /// A host extension vetoed execution or commit; the callee did not commit.
    Rejected(String),
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
            CallFailure::Rejected(m) => write!(f, "rejected: {m}"),
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
        CallFailure::Rejected(m) => ("rejected", Some(m)),
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
    let Some(context) = state.invocation_context() else {
        return failed(CallFailure::Rejected("missing invocation context".into()));
    };
    let timeout = timeout.min(context.remaining());
    if timeout.is_zero() || context.check_deadline().is_err() {
        return failed(CallFailure::Timeout);
    }
    let metadata = match context.request.child(ActorRef {
        app: state.identity.app.clone(), actor_type: state.identity.actor_type.clone(), key: state.identity.key.clone(),
    }, timeout) {
        Ok(metadata) => metadata,
        Err(error) => return failed(CallFailure::Rejected(error.to_string())),
    };
    let req = CallRequest {
        context: metadata,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invocation::{Caller, InvocationContext, InvocationOperation, Principal};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    struct Capture(Mutex<Option<CallRequest>>);

    impl ActorCaller for Capture {
        fn call(&self, request: CallRequest) -> CallReply {
            *self.0.lock().unwrap() = Some(request);
            CallReply::Failed(CallFailure::Rejected("callee denied".into()))
        }
    }

    #[test]
    fn outgoing_calls_propagate_context_budget_chain_and_explicit_rejection() {
        let actor = ActorRef { app: "source".into(), actor_type: "counter".into(), key: "a".into() };
        let mut context = InvocationContext::new(actor.clone(), InvocationOperation::Create, Caller::External,
            Duration::from_secs(30)).unwrap();
        context.request.principal = Some(Principal { subject: "alice".into(), claims: BTreeMap::from([("role".into(), json!("admin"))]) });
        context.request.attributes.insert("trace".into(), json!("test"));
        let capture = Arc::new(Capture(Mutex::new(None)));
        let state = HostState {
            wasi: wasmtime_wasi::WasiCtxBuilder::new().build(),
            table: Default::default(),
            limits: Default::default(),
            identity: crate::ActorIdentity { app: actor.app.clone(), actor_type: actor.actor_type.clone(), key: actor.key.clone(), epoch: 1 },
            db: crate::database::sqlite_handle(Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()))),
            http: Default::default(),
            http_timeout: Duration::from_secs(30),
            caller: Some(capture.clone()),
            chain: vec![actor.clone()],
            deadline: Some(Instant::now() + Duration::from_secs(2)),
            has_alarm: false,
            extensions: Default::default(),
            http_transport: Arc::new(crate::DefaultHttpTransport),
            log_sink: Arc::new(crate::TracingLogSink),
            capability_error: None,
            invocation_context: Some(context.clone()),
        };
        let method = Method { name: "get".into(), params: vec![], result: None, docs: None };
        let import = CallImport { import: "target:app/counter".into(), app: "target".into(), actor_type: "counter".into(), methods: vec![method.clone()] };
        let result = dispatch(&state, &import, &method, &[Val::String("b".into())]);
        let Val::Result(Err(Some(error))) = result else { panic!("expected rejected"); };
        let Val::Variant(name, _) = *error else { panic!("expected call-error"); };
        assert_eq!(name, "rejected");
        let request = capture.0.lock().unwrap().take().unwrap();
        assert_eq!(request.context.caller, Caller::Actor { actor: actor.clone() });
        assert_eq!(request.context.parent_request_id.as_ref(), Some(&context.request.request_id));
        assert_ne!(request.context.request_id, context.request.request_id);
        assert_eq!(request.context.principal, context.request.principal);
        assert_eq!(request.context.attributes, context.request.attributes);
        assert!(request.context.deadline_unix_ms < context.request.deadline_unix_ms);
        assert!(request.timeout < Duration::from_secs(2));
        assert_eq!(request.chain, vec![actor]);
    }
}
