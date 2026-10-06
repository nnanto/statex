//! WebAssembly runtime for statex actors: compiles app components, links the
//! `statex:host` capabilities and invokes exported methods dynamically from
//! JSON using the app [`manifest`].

pub mod alarm;
pub mod calls;
pub mod compat;
pub mod database;
pub mod host;
pub mod invocation;
pub mod json;
pub mod limits;
pub mod manifest;
pub mod services;
pub mod sqlite;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use serde_json::Value as J;
use wasmtime::component::{Component, ComponentExportIndex, HasSelf, InstancePre, Linker, Val};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::WasiCtxBuilder;

pub use calls::{ActorCaller, ActorRef, CallFailure, CallReply, CallRequest};
pub use host::{forbidden, ActorIdentity, HostState};
pub use compat::{breaking_changes, call_mismatches, call_mismatches_in, fmt_ty, Surface};
pub use manifest::{
    app_of_package, client_package, inspect, inspect_wit, inspect_wit_calls, validate_app_name, validate_name, ActorType, CallImport,
    Case, Field, HttpPolicy, Limits, Manifest, Method, Migration, Param, Ty,
};
pub use services::{DefaultHttpTransport, HttpTransport, LogSink, TracingLogSink};
pub use limits::HostLimits;
use invocation::{Caller, ExecutionExtension, HookError, InvocationContext, InvocationOperation};

/// Actor-local data for additional host capabilities. It is discarded with
/// the component instance; durable state belongs in the actor database.
#[derive(Default)]
pub struct Extensions {
    values: HashMap<TypeId, Box<dyn Any + Send>>,
}

impl Extensions {
    pub fn insert<T: Any + Send>(&mut self, value: T) -> Option<T> {
        self.values.insert(TypeId::of::<T>(), Box::new(value))
            .and_then(|old| old.downcast::<T>().ok().map(|v| *v))
    }

    pub fn get<T: Any + Send>(&self) -> Option<&T> {
        self.values.get(&TypeId::of::<T>())?.downcast_ref()
    }

    pub fn get_mut<T: Any + Send>(&mut self) -> Option<&mut T> {
        self.values.get_mut(&TypeId::of::<T>())?.downcast_mut()
    }
}

type HostInitializer = dyn Fn(&mut HostState) -> Result<()> + Send + Sync;
type HostLinker = Box<dyn FnOnce(&mut Linker<HostState>) -> Result<()>>;

/// Builds a runtime with explicitly registered host capabilities.
pub struct RuntimeBuilder {
    config: Config,
    interfaces: Vec<(String, HostLinker)>,
    initializers: Vec<Arc<HostInitializer>>,
    http_transport: Arc<dyn HttpTransport>,
    log_sink: Arc<dyn LogSink>,
    execution_extensions: Vec<Arc<dyn ExecutionExtension>>,
    host_limits: HostLimits,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self {
            config: Config::new(),
            interfaces: Vec::new(),
            initializers: Vec::new(),
            http_transport: Arc::new(DefaultHttpTransport),
            log_sink: Arc::new(TracingLogSink),
            execution_extensions: Vec::new(),
            host_limits: HostLimits::default(),
        }
    }
}

impl RuntimeBuilder {
    pub fn host_limits(mut self, limits: HostLimits) -> Self {
        self.host_limits = limits;
        self
    }
    /// Appends a synchronous execution hook. The default chain is empty;
    /// hooks run in registration order and cannot be forcibly interrupted.
    pub fn execution_extension(mut self, extension: Arc<dyn ExecutionExtension>) -> Self {
        self.execution_extensions.push(extension);
        self
    }
    /// Engine tuning. Epoch interruption and fuel consumption are always
    /// enabled by `build`, regardless of this callback.
    pub fn configure_engine(mut self, configure: impl FnOnce(&mut Config)) -> Self {
        configure(&mut self.config);
        self
    }

    /// Registers one exact WIT interface import and its linker definitions.
    /// Built-in interfaces cannot be replaced through this hook.
    pub fn host_interface(
        mut self,
        import: impl Into<String>,
        link: impl FnOnce(&mut Linker<HostState>) -> Result<()> + 'static,
    ) -> Result<Self> {
        let import = import.into();
        anyhow::ensure!(!import.is_empty(), "host interface name must not be empty");
        anyhow::ensure!(
            !manifest::import_allowed(&import),
            "cannot replace built-in host interface {import}"
        );
        anyhow::ensure!(
            !self.interfaces.iter().any(|(name, _)| name == &import),
            "host interface {import} is already registered"
        );
        self.interfaces.push((import, Box::new(link)));
        Ok(self)
    }

    /// Initializes actor-local extension data before component instantiation.
    pub fn initialize_host(
        mut self,
        initialize: impl Fn(&mut HostState) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.initializers.push(Arc::new(initialize));
        self
    }

    pub fn http_transport(mut self, transport: Arc<dyn HttpTransport>) -> Self {
        self.http_transport = transport;
        self
    }

    pub fn log_sink(mut self, sink: Arc<dyn LogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    pub fn build(mut self) -> Result<Runtime> {
        self.host_limits.validate()?;
        self.config.epoch_interruption(true);
        self.config.consume_fuel(true);
        // Wasmtime does not report shared-memory growth to ResourceLimiter.
        // Do not allow engine tuning to bypass the aggregate memory envelope.
        self.config.shared_memory(false);
        let engine = Engine::new(&self.config)?;
        let mut linker = Linker::<HostState>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        host::Imports::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |s| s)?;
        let mut imports = Vec::new();
        for (name, link) in self.interfaces {
            link(&mut linker).with_context(|| format!("register host interface {name}"))?;
            imports.push(name);
        }
        let weak = engine.weak();
        std::thread::Builder::new().name("statex-epoch".into()).spawn(move || loop {
            std::thread::sleep(TICK);
            match weak.upgrade() {
                Some(e) => e.increment_epoch(),
                None => break,
            }
        })?;
        Ok(Runtime {
            engine,
            linker: Arc::new(linker),
            imports,
            initializers: self.initializers,
            http_transport: self.http_transport,
            log_sink: self.log_sink,
            execution_extensions: self.execution_extensions,
            host_limits: self.host_limits,
            quotas: Arc::new(limits::QuotaRegistry::default()),
        })
    }
}

/// Engine epoch tick used for call timeouts.
const TICK: Duration = Duration::from_millis(10);

fn epoch_ticks(timeout: Duration) -> u64 {
    timeout.as_nanos().div_ceil(TICK.as_nanos()).max(1) as u64
}

const LIMIT_TRAP: &str = "guest execution limit exceeded: ";

fn guest_error(error: &wasmtime::Error, timeout: Duration) -> String {
    if let Some(memory) = error.downcast_ref::<limits::MemoryLimit>() {
        return format!("{LIMIT_TRAP}{memory}");
    }
    match error.downcast_ref::<wasmtime::Trap>() {
        Some(wasmtime::Trap::OutOfFuel) => format!("{LIMIT_TRAP}guest fuel budget exhausted"),
        Some(wasmtime::Trap::Interrupt) => format!("{LIMIT_TRAP}timed out after {} ms", timeout.as_millis()),
        _ => format!("{error:?}"),
    }
}

/// Shared engine + linker. Cheap to clone.
#[derive(Clone)]
pub struct Runtime {
    engine: Engine,
    linker: Arc<Linker<HostState>>,
    imports: Vec<String>,
    initializers: Vec<Arc<HostInitializer>>,
    http_transport: Arc<dyn HttpTransport>,
    log_sink: Arc<dyn LogSink>,
    execution_extensions: Vec<Arc<dyn ExecutionExtension>>,
    host_limits: HostLimits,
    quotas: Arc<limits::QuotaRegistry>,
}

impl Runtime {
    /// Shares engine infrastructure, but creates independent per-app admission
    /// state for one node. Existing host policy can only tighten.
    pub fn for_node(&self, limits: HostLimits) -> Result<Self> {
        limits.validate()?;
        let mut runtime = self.clone();
        runtime.host_limits = self.host_limits.intersect(&limits);
        runtime.quotas = Arc::new(limits::QuotaRegistry::default());
        Ok(runtime)
    }
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    /// Creates a manifest admitting only built-ins, typed actor calls, and
    /// this runtime's explicitly registered additional interfaces.
    pub fn build_manifest(
        &self,
        wasm: &[u8],
        app: &str,
        migrations: std::collections::BTreeMap<String, Vec<Migration>>,
        http: HttpPolicy,
        limits: Limits,
    ) -> Result<Manifest> {
        Manifest::build_with_imports(wasm, app, migrations, http, limits, &self.imports)
    }

    /// Process-wide default runtime.
    pub fn shared() -> Result<Self> {
        static RT: OnceLock<Runtime> = OnceLock::new();
        if let Some(r) = RT.get() {
            return Ok(r.clone());
        }
        let r = Runtime::new()?;
        Ok(RT.get_or_init(|| r).clone())
    }

    /// Compiles and links an app. Fails if the component imports anything the
    /// host does not provide or does not export what the manifest describes.
    pub fn load(&self, wasm: &[u8], manifest: Manifest) -> Result<Arc<AppCode>> {
        manifest.limits.validate()?;
        let effective_limits = self.host_limits.effective(&manifest.limits)?;
        let component = Component::new(&self.engine, wasm).map_err(anyhow::Error::from).context("compile component")?;
        let linker = if manifest.calls.is_empty() {
            self.linker.clone()
        } else {
            let mut l = (*self.linker).clone();
            calls::link(&mut l, &manifest.calls).context("link client interfaces")?;
            Arc::new(l)
        };
        let pre = linker
            .instantiate_pre(&component)
            .map_err(anyhow::Error::from)
            .context("link component (does it import something the host does not provide?)")?;
        let mut exports = HashMap::new();
        let mut alarm_exports = HashMap::new();
        for t in &manifest.types {
            let iface = component
                .get_export_index(None, &t.export)
                .ok_or_else(|| anyhow!("component does not export {}", t.export))?;
            for m in &t.methods {
                let f = component
                    .get_export_index(Some(&iface), &m.name)
                    .ok_or_else(|| anyhow!("component does not export {}.{}", t.export, m.name))?;
                exports.insert((t.name.clone(), m.name.clone()), f);
            }
            if let Some(m) = &t.alarm {
                let f = component
                    .get_export_index(Some(&iface), &m.name)
                    .ok_or_else(|| anyhow!("component does not export {}.{}", t.export, m.name))?;
                alarm_exports.insert(t.name.clone(), f);
            }
        }
        let quota = self.quotas.register(&manifest.app, &effective_limits);
        Ok(Arc::new(AppCode {
            manifest, pre, exports, alarm_exports, engine: self.engine.clone(),
            initializers: self.initializers.clone(),
            http_transport: self.http_transport.clone(),
            log_sink: self.log_sink.clone(),
            execution_extensions: self.execution_extensions.clone(),
            effective_limits,
            quota,
        }))
    }
}

/// A compiled, linked app version.
pub struct AppCode {
    pub manifest: Manifest,
    pre: InstancePre<HostState>,
    exports: HashMap<(String, String), ComponentExportIndex>,
    /// Alarm handler per actor type.
    alarm_exports: HashMap<String, ComponentExportIndex>,
    engine: Engine,
    initializers: Vec<Arc<HostInitializer>>,
    http_transport: Arc<dyn HttpTransport>,
    log_sink: Arc<dyn LogSink>,
    execution_extensions: Vec<Arc<dyn ExecutionExtension>>,
    effective_limits: Limits,
    quota: Arc<limits::Quota>,
}

/// A single-use reservation owned by the executing task, not its invocation
/// context or completion observers. Dropping it always releases concurrency.
pub struct ExecutionAdmission {
    app: Arc<AppCode>,
    _permit: limits::Permit,
    actor_type: String,
    method: String,
    args: J,
    request_id: String,
    alarm: bool,
}

impl AppCode {
    pub fn effective_limits(&self) -> &Limits { &self.effective_limits }

    /// Current initialization/execution reservations for this app across all
    /// actor types, instances and versions in its runtime scope. Saturates at
    /// `u32::MAX` for an unbounded scope.
    pub fn active_executions(&self) -> u32 { self.quota.active_executions() }

    /// Validates routing/arguments and policy before charging one attempt.
    /// Reserve before creating a fresh instance so rejected calls cannot run
    /// component start functions.
    pub fn admit_call(
        self: &Arc<Self>, actor_type: &str, method: &str, args: &J, context: &InvocationContext,
    ) -> Result<ExecutionAdmission, CallError> {
        let m = resolve_method(&self.manifest, actor_type, method)?;
        json::args_to_vals(m, args).map_err(CallError::BadArgs)?;
        self.admit(actor_type, method, args, false, context)
    }

    pub fn admit_alarm(
        self: &Arc<Self>, actor_type: &str, context: &InvocationContext,
    ) -> Result<ExecutionAdmission, CallError> {
        let m = self.manifest.actor_type(actor_type).and_then(|t| t.alarm.as_ref())
            .ok_or_else(|| CallError::NotFound(format!("actor type {actor_type} has no alarm handler")))?;
        self.admit(actor_type, &m.name, &J::Null, true, context)
    }

    fn admit(
        self: &Arc<Self>, actor_type: &str, method: &str, args: &J, alarm: bool, context: &InvocationContext,
    ) -> Result<ExecutionAdmission, CallError> {
        context.check_deadline().map_err(CallError::Rejected)?;
        for extension in &self.execution_extensions {
            invocation::run_hook(context, || extension.before_guest(context)).map_err(CallError::Rejected)?;
        }
        let permit = self.quota.acquire().map_err(CallError::Rejected)?;
        Ok(ExecutionAdmission { app: self.clone(), _permit: permit, actor_type: actor_type.into(),
            method: method.into(), args: args.clone(), request_id: context.request.request_id.clone(), alarm })
    }

    fn validate_admission(&self, admission: &ExecutionAdmission, context: &InvocationContext) -> Result<(), CallError> {
        if !std::ptr::eq(self, &*admission.app) || admission.request_id != context.request.request_id {
            return Err(CallError::Rejected(HookError::Invalid("execution admission belongs to another app or invocation".into())));
        }
        context.check_deadline().map_err(CallError::Rejected)
    }

    /// Instantiates the component for one actor, without actor-to-actor calls
    /// (they fail with `call-error::unavailable`).
    pub fn instantiate(self: &Arc<Self>, identity: ActorIdentity, db: database::DatabaseHandle) -> Result<ActorInstance> {
        self.instantiate_with(identity, db, None)
    }

    /// Instantiates the component for one actor; `caller` routes the calls it
    /// makes to other actors.
    pub fn instantiate_with(
        self: &Arc<Self>,
        identity: ActorIdentity,
        db: database::DatabaseHandle,
        caller: Option<Arc<dyn ActorCaller>>,
    ) -> Result<ActorInstance> {
        // Standalone initialization reserves execution pressure, not an RPS
        // attempt: no method or arguments have been admitted yet.
        let _initialization = self.quota.acquire_initialization()?;
        self.instantiate_inner(identity, db, caller, None)
    }

    /// Initializes under an already reserved execution envelope without
    /// charging admission twice.
    pub fn instantiate_admitted_with(
        self: &Arc<Self>, identity: ActorIdentity, db: database::DatabaseHandle,
        caller: Option<Arc<dyn ActorCaller>>, admission: &ExecutionAdmission, context: &InvocationContext,
    ) -> Result<ActorInstance, CallError> {
        self.validate_admission(admission, context)?;
        self.instantiate_inner(identity, db, caller, Some(context)).map_err(|e| CallError::Trap(format!("{e:#}")))
    }

    fn instantiate_inner(
        self: &Arc<Self>, identity: ActorIdentity, db: database::DatabaseHandle,
        caller: Option<Arc<dyn ActorCaller>>, context: Option<&InvocationContext>,
    ) -> Result<ActorInstance> {
        let limits = &self.effective_limits;
        let timeout = context.map(|c| c.remaining()).unwrap_or(Duration::from_millis(limits.timeout_ms))
            .min(Duration::from_millis(limits.timeout_ms));
        anyhow::ensure!(!timeout.is_zero(), "{LIMIT_TRAP}component initialization deadline expired");
        let deadline = Instant::now() + timeout;
        let has_alarm = self.alarm_exports.contains_key(&identity.actor_type);
        let mut state = HostState {
            wasi: WasiCtxBuilder::new().inherit_stdout().inherit_stderr().build(),
            table: Default::default(),
            limits: limits::MemoryEnvelope::new(limits::memory_bytes(limits.memory_mb)?),
            identity,
            db,
            http: self.manifest.http.clone(),
            http_timeout: Duration::from_millis(limits.timeout_ms),
            caller,
            chain: Vec::new(),
            deadline: Some(deadline),
            has_alarm,
            extensions: Extensions::default(),
            http_transport: self.http_transport.clone(),
            log_sink: self.log_sink.clone(),
            capability_error: None,
            invocation_context: None,
        };
        for initialize in &self.initializers {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| initialize(&mut state)))
                .unwrap_or_else(|_| Err(anyhow!("actor host initializer panicked")))
                .context("initialize actor host extensions")?;
        }
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);
        anyhow::ensure!(Instant::now() < deadline, "{LIMIT_TRAP}component initialization timed out");
        store.data_mut().invocation_context = context.cloned();
        store.set_fuel(limits.fuel.unwrap_or(u64::MAX))?;
        store.set_epoch_deadline(epoch_ticks(deadline.saturating_duration_since(Instant::now())));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.pre.instantiate(&mut store)))
            .unwrap_or_else(|_| Err(wasmtime::Error::msg("component initialization host callback panicked")));
        store.set_epoch_deadline(u64::MAX / 2);
        store.set_fuel(0)?;
        let state = store.data_mut();
        state.deadline = None;
        state.invocation_context = None;
        state.chain.clear();
        anyhow::ensure!(Instant::now() < deadline, "{LIMIT_TRAP}component initialization timed out after {} ms", limits.timeout_ms);
        if let Some(error) = state.capability_error.take() { return Err(anyhow!(error)); }
        let instance = result.map_err(|error| anyhow!(guest_error(&error, timeout))).context("instantiate component")?;
        Ok(ActorInstance { store, instance, app: self.clone() })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallError {
    /// A host hook or invocation deadline vetoed execution. Roll back the
    /// transaction and discard the instance, as its memory may not be durable.
    Rejected(HookError),
    /// Unknown actor type or method.
    NotFound(String),
    /// Arguments do not match the method signature.
    BadArgs(String),
    /// The guest trapped (panic, timeout, out of memory). Discard the instance
    /// and roll back the transaction.
    Trap(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Rejected(error) => write!(f, "rejected: {error}"),
            CallError::NotFound(s) => write!(f, "not found: {s}"),
            CallError::BadArgs(s) => write!(f, "bad arguments: {s}"),
            CallError::Trap(s) => write!(f, "trap: {s}"),
        }
    }
}

impl std::error::Error for CallError {}

impl CallError {
    /// Resource exhaustion is not an alarm application failure: callers must
    /// not turn it into success while committing retry bookkeeping.
    pub fn is_execution_limit(&self) -> bool {
        matches!(self, Self::Trap(message) if message.contains(LIMIT_TRAP))
    }
}

/// Successful method return.
#[derive(Debug, Clone, PartialEq)]
pub struct CallOutput {
    /// JSON result. For `result<T, E>` methods: the unwrapped ok or err payload.
    pub value: J,
    /// True when a `result<T, E>` method returned `err`.
    pub is_err: bool,
}

/// A live component instance bound to one actor.
pub struct ActorInstance {
    store: Store<HostState>,
    instance: wasmtime::component::Instance,
    app: Arc<AppCode>,
}

impl ActorInstance {
    pub fn app(&self) -> &Arc<AppCode> {
        &self.app
    }

    pub fn identity(&self) -> &ActorIdentity {
        &self.store.data().identity
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.store.data_mut().identity.epoch = epoch;
    }

    /// Invokes `actor_type.method(args)`. The caller owns the transaction.
    pub fn call(&mut self, actor_type: &str, method: &str, args: &J) -> Result<CallOutput, CallError> {
        self.call_with(actor_type, method, args, &[])
    }

    /// Like [`call`](Self::call), for a call made by another actor: `chain`
    /// holds the actors executing further up the call chain (outermost first).
    pub fn call_with(&mut self, actor_type: &str, method: &str, args: &J, chain: &[ActorRef]) -> Result<CallOutput, CallError> {
        let context = self.embedded_context(actor_type, InvocationOperation::Call {
            method: method.into(), args: args.clone(),
        })?;
        self.call_with_context(actor_type, method, args, chain, &context)
    }

    fn embedded_context(&self, actor_type: &str, operation: InvocationOperation) -> Result<InvocationContext, CallError> {
        InvocationContext::new(ActorRef {
            app: self.identity().app.clone(), actor_type: actor_type.into(), key: self.identity().key.clone(),
        }, operation, Caller::Embedded, Duration::from_millis(self.app.effective_limits.timeout_ms))
            .map_err(CallError::Rejected)
    }

    /// Invokes with trusted metadata and a non-extendable parent deadline.
    pub fn call_with_context(&mut self, actor_type: &str, method: &str, args: &J, chain: &[ActorRef], context: &InvocationContext) -> Result<CallOutput, CallError> {
        let admission = self.app.admit_call(actor_type, method, args, context)?;
        self.call_admitted(actor_type, method, args, chain, context, admission)
    }

    pub fn call_admitted(
        &mut self, actor_type: &str, method: &str, args: &J, chain: &[ActorRef],
        context: &InvocationContext, admission: ExecutionAdmission,
    ) -> Result<CallOutput, CallError> {
        let app = self.app.clone();
        app.validate_admission(&admission, context)?;
        if admission.alarm || admission.actor_type != actor_type || admission.method != method || admission.args != *args {
            return Err(CallError::Rejected(HookError::Invalid("execution admission does not match call".into())));
        }
        let m = resolve_method(&app.manifest, actor_type, method)?;
        let params = json::args_to_vals(m, args).map_err(CallError::BadArgs)?;
        let idx = *app.exports.get(&(actor_type.to_string(), method.to_string())).expect("indexed");
        self.invoke(actor_type, m, idx, &params, chain, context, admission)
    }

    /// Whether `actor_type` exports an alarm handler.
    pub fn has_alarm_handler(&self, actor_type: &str) -> bool {
        self.app.alarm_exports.contains_key(actor_type)
    }

    /// Runs the alarm handler of `actor_type`; `retry` counts its earlier
    /// failed attempts. The caller owns the transaction.
    pub fn call_alarm(&mut self, actor_type: &str, retry: u32) -> Result<CallOutput, CallError> {
        let context = self.embedded_context(actor_type, InvocationOperation::Alarm)?;
        self.call_alarm_with_context(actor_type, retry, &context)
    }

    /// Invokes the private alarm handler with the owner's invocation context.
    pub fn call_alarm_with_context(&mut self, actor_type: &str, retry: u32, context: &InvocationContext) -> Result<CallOutput, CallError> {
        let admission = self.app.admit_alarm(actor_type, context)?;
        self.call_alarm_admitted(actor_type, retry, context, admission)
    }

    pub fn call_alarm_admitted(
        &mut self, actor_type: &str, retry: u32, context: &InvocationContext, admission: ExecutionAdmission,
    ) -> Result<CallOutput, CallError> {
        let app = self.app.clone();
        app.validate_admission(&admission, context)?;
        if !admission.alarm || admission.actor_type != actor_type {
            return Err(CallError::Rejected(HookError::Invalid("execution admission does not match alarm".into())));
        }
        let (Some(m), Some(idx)) = (
            app.manifest.actor_type(actor_type).and_then(|t| t.alarm.as_ref()),
            app.alarm_exports.get(actor_type).copied(),
        ) else {
            return Err(CallError::NotFound(format!("actor type {actor_type} has no alarm handler")));
        };
        let params: Vec<Val> = if m.params.is_empty() { vec![] } else { vec![Val::U32(retry)] };
        self.invoke(actor_type, m, idx, &params, &[], context, admission)
    }

    // The low-level adapter owns admission separately from borrowed WIT call
    // data so it can release execution capacity before completion observers.
    #[allow(clippy::too_many_arguments)]
    fn invoke(
        &mut self,
        actor_type: &str,
        m: &Method,
        idx: ComponentExportIndex,
        params: &[Val],
        chain: &[ActorRef],
        context: &InvocationContext,
        admission: ExecutionAdmission,
    ) -> Result<CallOutput, CallError> {
        let app = self.app.clone();
        context.check_deadline().map_err(CallError::Rejected)?;
        let outcome = self.invoke_guest(actor_type, m, idx, params, chain, context);
        drop(admission);
        for extension in &app.execution_extensions {
            if let Err(error) = invocation::run_hook(context, || extension.after_guest(context, &outcome)) {
                tracing::warn!(%error, "after_guest observer failed");
            }
        }
        outcome
    }

    fn invoke_guest(
        &mut self,
        actor_type: &str,
        m: &Method,
        idx: ComponentExportIndex,
        params: &[Val],
        chain: &[ActorRef],
        context: &InvocationContext,
    ) -> Result<CallOutput, CallError> {
        if let Some(error) = self.store.data_mut().capability_error.take() {
            return Err(CallError::Trap(error));
        }
        let app = self.app.clone();
        let method = &m.name;
        let func = self
            .instance
            .get_func(&mut self.store, idx)
            .ok_or_else(|| CallError::NotFound(format!("{actor_type}.{method}")))?;
        let mut results = vec![json::placeholder(); usize::from(m.result.is_some())];
        let timeout = context.remaining().min(Duration::from_millis(app.effective_limits.timeout_ms));
        if timeout.is_zero() { return Err(CallError::Rejected(HookError::Timeout)); }
        let deadline = Instant::now() + timeout;
        self.store.set_fuel(app.effective_limits.fuel.unwrap_or(u64::MAX))
            .map_err(|e| CallError::Trap(e.to_string()))?;
        {
            let s = self.store.data_mut();
            s.chain = chain.to_vec();
            s.chain.push(ActorRef {
                app: s.identity.app.clone(),
                actor_type: actor_type.to_string(),
                key: s.identity.key.clone(),
            });
            s.deadline = Some(deadline);
            s.invocation_context = Some(context.clone());
        }
        self.store.set_epoch_deadline(epoch_ticks(timeout));
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| func.call(&mut self.store, params, &mut results)))
            .unwrap_or_else(|_| Err(wasmtime::Error::msg("guest host callback panicked")));
        self.store.set_epoch_deadline(u64::MAX / 2);
        let _ = self.store.set_fuel(0);
        let capability_error = {
            let s = self.store.data_mut();
            s.chain.clear();
            s.deadline = None;
            s.invocation_context = None;
            s.capability_error.take()
        };
        if let Some(error) = capability_error {
            return Err(CallError::Trap(error));
        }
        // Epochs cannot preempt native callbacks, and a callback may return
        // without executing another WASM instruction. Never accept late success.
        if Instant::now() >= deadline {
            return Err(CallError::Trap(format!("{LIMIT_TRAP}timed out after {} ms", timeout.as_millis())));
        }
        if let Err(e) = r {
            let msg = guest_error(&e, timeout);
            return Err(CallError::Trap(msg));
        }
        context.check_deadline().map_err(CallError::Rejected)?;
        Ok(match (&m.result, results.first()) {
            (Some(Ty::Result { ok, err }), Some(Val::Result(r))) => {
                let (t, p, is_err) = match r {
                    Ok(p) => (ok, p, false),
                    Err(p) => (err, p, true),
                };
                let value = match (t, p) {
                    (Some(t), Some(p)) => json::val_to_json(t, p),
                    _ => J::Null,
                };
                CallOutput { value, is_err }
            }
            (Some(t), Some(v)) => CallOutput { value: json::val_to_json(t, v), is_err: false },
            _ => CallOutput { value: J::Null, is_err: false },
        })
    }
}

pub fn resolve_method<'a>(m: &'a Manifest, actor_type: &str, method: &str) -> Result<&'a Method, CallError> {
    let t = m.actor_type(actor_type).ok_or_else(|| {
        CallError::NotFound(format!(
            "app {} has no actor type {actor_type:?} (types: {})",
            m.app,
            m.types.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", ")
        ))
    })?;
    t.method(method).ok_or_else(|| {
        CallError::NotFound(format!(
            "actor type {actor_type} has no method {method:?} (methods: {})",
            t.methods.iter().map(|m| m.name.as_str()).collect::<Vec<_>>().join(", ")
        ))
    })
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(data))
}

#[cfg(test)]
mod extension_tests {
    use super::Extensions;

    #[test]
    fn typed_extension_data_is_replaced_and_isolated() {
        let mut first = Extensions::default();
        let second = Extensions::default();
        assert_eq!(first.insert(1u64), None);
        first.insert(String::from("actor-local"));
        assert_eq!(first.insert(2u64), Some(1));
        *first.get_mut::<u64>().unwrap() += 1;
        assert_eq!(first.get::<u64>(), Some(&3));
        assert_eq!(first.get::<String>().map(String::as_str), Some("actor-local"));
        assert!(first.get::<u32>().is_none());
        assert!(second.get::<u64>().is_none());
    }

    #[test]
    fn invocation_context_is_cleared_for_every_exit_and_reused_instance() {
        use super::*;
        use std::collections::BTreeMap;
        const COMPONENT: &str = r#"
        (component
          (core module $m
            (func (export "run") (result i32) i32.const 7)
            (func (export "trap") (result i32) unreachable))
          (core instance $i (instantiate $m))
          (func $run (result u32) (canon lift (core func $i "run")))
          (func $trap (result u32) (canon lift (core func $i "trap")))
          (instance $actor (export "run" (func $run)) (export "trap" (func $trap)))
          (export "test:context/counter@0.1.0" (instance $actor)))
        "#;
        let manifest = Manifest {
            format: 1, app: "context".into(), sha256: String::new(),
            types: vec![ActorType {
                name: "counter".into(), export: "test:context/counter@0.1.0".into(), docs: None,
                methods: ["run", "trap"].into_iter().map(|name| Method {
                    name: name.into(), params: vec![], result: Some(Ty::U32), docs: None,
                }).collect(), alarm: None,
            }],
            migrations: BTreeMap::new(), http: Default::default(), limits: Default::default(), calls: vec![],
        };
        let code = Runtime::new().unwrap().load(COMPONENT.as_bytes(), manifest).unwrap();
        let mut actor = code.instantiate(
            ActorIdentity { app: "context".into(), actor_type: "counter".into(), key: "k".into(), epoch: 1 },
            database::sqlite_handle(Arc::new(std::sync::Mutex::new(rusqlite::Connection::open_in_memory().unwrap()))),
        ).unwrap();
        assert_eq!(actor.store.get_fuel().unwrap(), 0);
        for (method, args) in [
            ("run", J::Null), ("missing", J::Null), ("run", serde_json::json!([1])),
        ] {
            let _ = actor.call("counter", method, &args);
            assert!(actor.store.data().invocation_context().is_none());
            assert!(actor.store.data().deadline.is_none());
            assert!(actor.store.data().chain.is_empty());
            assert_eq!(actor.store.get_fuel().unwrap(), 0);
        }
        let context = actor.embedded_context("counter", InvocationOperation::Create).unwrap();
        actor.store.data_mut().capability_error = Some("infallible host failure".into());
        assert!(matches!(actor.call_with_context("counter", "run", &J::Null, &[], &context), Err(CallError::Trap(_))));
        assert!(actor.store.data().invocation_context().is_none());
        let expired = InvocationContext::new(context.target.clone(), context.operation.clone(), Caller::Embedded,
            Duration::from_millis(1)).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert!(matches!(actor.call_with_context("counter", "run", &J::Null, &[], &expired), Err(CallError::Rejected(_))));
        assert!(actor.store.data().invocation_context().is_none());
        assert_eq!(actor.call("counter", "run", &J::Null).unwrap().value, serde_json::json!(7));
        assert!(actor.store.data().invocation_context().is_none());
        assert!(matches!(actor.call("counter", "trap", &J::Null), Err(CallError::Trap(_))));
        assert!(actor.store.data().invocation_context().is_none());
        assert!(actor.store.data().deadline.is_none());
        assert!(actor.store.data().chain.is_empty());
        assert_eq!(actor.store.get_fuel().unwrap(), 0);
    }
}
