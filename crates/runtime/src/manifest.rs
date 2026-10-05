//! App manifest: the actor types, methods and type signatures discovered from a
//! component's WIT exports, plus deployment metadata. The manifest drives
//! routing, JSON mapping and client SDK generation, so teams never register
//! routes by hand.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use wit_parser::{Resolve, Type, TypeDefKind, WorldItem};

/// A WIT type, fully inlined (WIT types are never recursive).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Ty {
    Bool,
    U8,
    U16,
    U32,
    U64,
    S8,
    S16,
    S32,
    S64,
    F32,
    F64,
    Char,
    String,
    List { element: Box<Ty> },
    Option { inner: Box<Ty> },
    Result {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ok: Option<Box<Ty>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        err: Option<Box<Ty>>,
    },
    Tuple { items: Vec<Ty> },
    Record { name: String, fields: Vec<Field> },
    Variant { name: String, cases: Vec<Case> },
    Enum { name: String, cases: Vec<String> },
    Flags { name: String, flags: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: Ty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Case {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<Ty>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Param {
    pub name: String,
    pub ty: Ty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Method {
    pub name: String,
    pub params: Vec<Param>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Ty>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorType {
    /// Short interface name, used in URLs: `counter`.
    pub name: String,
    /// Full export name: `example:counter/counter@0.1.0`.
    pub export: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
    pub methods: Vec<Method>,
    /// The alarm handler (`alarm: func(retry-count: u32)`), if the type has
    /// one. It runs when the actor's alarm fires and is not a public method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alarm: Option<Method>,
    /// Run each call in a fresh instance without durable actor state.
    #[serde(default)]
    pub stateless: bool,
    /// Batch durable acknowledgements for this actor type.
    #[serde(default)]
    pub group_commit: bool,
}

/// Name of the method an actor type exports to handle its alarm.
pub const ALARM_HANDLER: &str = "alarm";

impl ActorType {
    pub fn method(&self, name: &str) -> Option<&Method> {
        self.methods.iter().find(|m| m.name == name)
    }
}

/// A client interface the component imports to call another actor type
/// (`statex:host/actors`). `methods` describe the callee as the caller sees
/// it: without the leading `actor` key parameter and with the outer
/// `result<_, call-error>` removed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallImport {
    /// Import name as it appears in the component: `demo:db/kv`.
    pub import: String,
    /// Callee app: `demo/db`.
    pub app: String,
    /// Callee actor type: `kv`.
    pub actor_type: String,
    pub methods: Vec<Method>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Migration {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HttpPolicy {
    /// Allowed hosts: `api.example.com`, `*.example.com` or `*`.
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    pub timeout_ms: u64,
    pub memory_mb: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self { timeout_ms: 5_000, memory_mb: 64 }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub app: String,
    /// sha256 of component.wasm; also the deployment version id.
    pub sha256: String,
    pub types: Vec<ActorType>,
    /// Per actor type, applied in order on activation.
    #[serde(default)]
    pub migrations: BTreeMap<String, Vec<Migration>>,
    #[serde(default)]
    pub http: HttpPolicy,
    #[serde(default)]
    pub limits: Limits,
    /// Actor types of other apps (or this one) the component calls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calls: Vec<CallImport>,
}

impl Manifest {
    pub fn actor_type(&self, name: &str) -> Option<&ActorType> {
        self.types.iter().find(|t| t.name == name)
    }

    /// Checks options even for manifests received directly through the deploy API.
    pub fn validate_actor_options(&self) -> Result<()> {
        for t in &self.types {
            if t.stateless {
                anyhow::ensure!(t.alarm.is_none(), "stateless actor type {} cannot export an alarm handler", t.name);
                anyhow::ensure!(
                    self.migrations.get(&t.name).is_none_or(Vec::is_empty),
                    "stateless actor type {} cannot have migrations", t.name
                );
                anyhow::ensure!(!t.group_commit, "stateless actor type {} cannot enable group_commit", t.name);
            }
        }
        Ok(())
    }

    /// Builds and validates a manifest for a component binary: checks imports,
    /// extracts actor types and attaches migrations (`type -> [(file, sql)]`).
    pub fn build(
        wasm: &[u8],
        app: &str,
        migrations: BTreeMap<String, Vec<Migration>>,
        http: HttpPolicy,
        limits: Limits,
    ) -> Result<Manifest> {
        validate_app_name(app)?;
        let ins = inspect(wasm)?;
        let bad: Vec<_> = ins
            .imports
            .iter()
            .filter(|i| !import_allowed(i) && !ins.calls.iter().any(|c| &c.import == *i))
            .cloned()
            .collect();
        if !bad.is_empty() {
            bail!(
                "component imports interfaces the host does not provide: {}. Only statex:host/*, wasi:* and statex client interfaces (`statex calls sync`) are available",
                bad.join(", ")
            );
        }
        for t in migrations.keys() {
            if !ins.types.iter().any(|x| &x.name == t) {
                bail!("migrations/{t}/ does not match any exported actor type");
            }
        }
        for list in migrations.values() {
            let mut names: Vec<_> = list.iter().map(|m| &m.name).collect();
            let sorted = {
                let mut s = names.clone();
                s.sort();
                s
            };
            if names != sorted {
                bail!("migrations must be sorted by name");
            }
            names.dedup();
            if names.len() != list.len() {
                bail!("duplicate migration names");
            }
        }
        Ok(Manifest {
            format: 1,
            app: app.to_string(),
            sha256: crate::sha256_hex(wasm),
            types: ins.types,
            migrations,
            http,
            limits,
            calls: ins.calls,
        })
    }
}

/// App names: lowercase letters, digits and dashes, starting with a letter.
pub fn validate_name(what: &str, s: &str) -> Result<()> {
    let ok = !s.is_empty()
        && s.len() <= 63
        && s.starts_with(|c: char| c.is_ascii_lowercase())
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !ok {
        bail!("invalid {what} name {s:?}: use lowercase letters, digits and dashes, starting with a letter");
    }
    Ok(())
}

/// Path segments the HTTP API uses after an app name; no app name segment may
/// use them, so `/v1/apps/<app...>/actors/...` parses unambiguously.
pub const RESERVED_APP_SEGMENTS: &[&str] = &["actors", "schema"];

/// App names are one segment (`shop`) or namespaced as `team/app`
/// (`payments/shop`). Each segment follows [`validate_name`].
pub fn validate_app_name(s: &str) -> Result<()> {
    let segs: Vec<&str> = s.split('/').collect();
    if segs.len() > 2 {
        bail!("invalid app name {s:?}: use `app` or `team/app`");
    }
    for seg in segs {
        validate_name("app", seg).map_err(|_| {
            anyhow!("invalid app name {s:?}: use `app` or `team/app`, where each part is lowercase letters, digits and dashes, starting with a letter")
        })?;
        if RESERVED_APP_SEGMENTS.contains(&seg) {
            bail!("invalid app name {s:?}: `{seg}` is reserved");
        }
    }
    // Client interfaces of app `team/app` live in WIT package `team:app`, and
    // of a single-segment app `app` in `statex:app` (see [`client_package`]).
    match s.split_once('/') {
        Some((team, _)) if RESERVED_TEAMS.contains(&team) => bail!("invalid app name {s:?}: team `{team}` is reserved"),
        None if s == "host" => bail!("invalid app name {s:?}: `host` is reserved"),
        _ => {}
    }
    Ok(())
}

/// Teams that cannot own apps because their WIT namespaces belong to the host.
pub const RESERVED_TEAMS: &[&str] = &["statex", "wasi"];

/// WIT package (`namespace`, `name`) holding the client interfaces of `app`:
/// `team/app` -> `team:app`, `app` -> `statex:app`.
pub fn client_package(app: &str) -> (String, String) {
    match app.split_once('/') {
        Some((team, name)) => (team.to_string(), name.to_string()),
        None => ("statex".to_string(), app.to_string()),
    }
}

/// Inverse of [`client_package`]. `None` for host packages (`statex:host`, `wasi:*`).
pub fn app_of_package(namespace: &str, name: &str) -> Option<String> {
    match namespace {
        "wasi" => None,
        "statex" if name == "host" => None,
        "statex" => Some(name.to_string()),
        ns => Some(format!("{ns}/{name}")),
    }
}

/// What a component exports and imports.
#[derive(Debug, Clone)]
pub struct Inspection {
    pub types: Vec<ActorType>,
    pub imports: Vec<String>,
    /// Imported client interfaces of other actor types.
    pub calls: Vec<CallImport>,
}

/// Import namespaces an actor component may use.
pub fn import_allowed(name: &str) -> bool {
    name.starts_with("statex:host/") || name.starts_with("wasi:")
}

/// Decodes a component binary and extracts its actor types.
pub fn inspect(wasm: &[u8]) -> Result<Inspection> {
    let decoded = wit_component::decode(wasm).map_err(|e| anyhow!("not a valid component: {e}"))?;
    let (resolve, world) = match decoded {
        wit_component::DecodedWasm::Component(r, w) => (r, w),
        _ => bail!("expected a WebAssembly component, found a WIT package"),
    };
    inspect_world(&resolve, world)
}

/// Extracts actor types from WIT source (a directory such as `wit/`, with
/// dependencies under `wit/deps/`), without building the component. `world`
/// may be omitted when the package defines a single world.
pub fn inspect_wit(dir: &std::path::Path, world: Option<&str>) -> Result<Inspection> {
    let mut resolve = Resolve::default();
    let (pkg, _) = resolve.push_dir(dir).with_context(|| format!("parse WIT in {}", dir.display()))?;
    let world = resolve.select_world(&[pkg], world)?;
    inspect_world(&resolve, world)
}

/// Client interfaces imported by a WIT world, e.g. a generated `statex-calls`
/// world (which, unlike an app world, exports nothing).
pub fn inspect_wit_calls(dir: &std::path::Path, world: Option<&str>) -> Result<Vec<CallImport>> {
    let mut resolve = Resolve::default();
    let (pkg, _) = resolve.push_dir(dir).with_context(|| format!("parse WIT in {}", dir.display()))?;
    let world = resolve.select_world(&[pkg], world)?;
    world_calls(&resolve, world)
}

fn world_calls(resolve: &Resolve, world: wit_parser::WorldId) -> Result<Vec<CallImport>> {
    let mut calls = Vec::new();
    for (key, item) in &resolve.worlds[world].imports {
        if let WorldItem::Interface { id, .. } = item {
            if let Some(c) = call_import(resolve, *id, &resolve.name_world_key(key))? {
                calls.push(c);
            }
        }
    }
    Ok(calls)
}

fn inspect_world(resolve: &Resolve, world: wit_parser::WorldId) -> Result<Inspection> {
    let w = &resolve.worlds[world];
    let imports = w.imports.keys().map(|k| resolve.name_world_key(k)).collect();
    let calls = world_calls(resolve, world)?;
    let mut types: Vec<ActorType> = Vec::new();
    for (key, item) in &w.exports {
        let export = resolve.name_world_key(key);
        match item {
            WorldItem::Interface { id, .. } => {
                let iface = &resolve.interfaces[*id];
                // Unnamed (inline) interfaces are toolchain plumbing, e.g. the
                // pre-initialization hook componentize-py exports; they are
                // never actor types.
                let Some(name) = iface.name.clone() else { continue };
                if iface.functions.is_empty() {
                    continue;
                }
                let mut methods = Vec::new();
                let mut alarm = None;
                for f in iface.functions.values() {
                    if !matches!(f.kind, wit_parser::FunctionKind::Freestanding) {
                        bail!("{export}.{}: resources are not supported in actor interfaces", f.name);
                    }
                    let ctx = format!("{name}.{}", f.name);
                    let m = Method {
                        name: f.name.clone(),
                        params: f
                            .params
                            .iter()
                            .map(|p| Ok(Param { name: p.name.clone(), ty: ty_of(resolve, &p.ty, &ctx)? }))
                            .collect::<Result<_>>()?,
                        result: f.result.as_ref().map(|t| ty_of(resolve, t, &ctx)).transpose()?,
                        docs: f.docs.contents.clone(),
                    };
                    if m.name == ALARM_HANDLER {
                        check_alarm_handler(&m, &ctx)?;
                        alarm = Some(m);
                    } else {
                        methods.push(m);
                    }
                }
                if types.iter().any(|t| t.name == name) {
                    bail!("two exported interfaces are both named `{name}`; actor type names must be unique");
                }
                types.push(ActorType {
                    name, export, docs: iface.docs.contents.clone(), methods, alarm, stateless: false, group_commit: false,
                });
            }
            WorldItem::Function(f) => {
                bail!("world-level export function `{}` is not supported; export an interface instead", f.name)
            }
            WorldItem::Type { .. } => {}
        }
    }
    if types.is_empty() {
        bail!("component exports no interfaces with functions; nothing to serve");
    }
    Ok(Inspection { types, imports, calls })
}

/// The alarm handler takes nothing or the retry count (`u32`) and returns
/// nothing or a `result` (an `err` counts as a failure and is retried).
fn check_alarm_handler(m: &Method, ctx: &str) -> Result<()> {
    let params_ok = match m.params.as_slice() {
        [] => true,
        [p] => matches!(p.ty, Ty::U32),
        _ => false,
    };
    let result_ok = matches!(m.result, None | Some(Ty::Result { .. }));
    if !params_ok || !result_ok {
        bail!(
            "{ctx}: `{ALARM_HANDLER}` is reserved for the alarm handler; declare it as \
             `{ALARM_HANDLER}: func(retry-count: u32);` (the parameter and a `result<_, E>` return are optional)"
        );
    }
    Ok(())
}

/// Parses an imported interface as a statex client interface, or returns
/// `None` for host interfaces (`statex:host/*`, `wasi:*`).
fn call_import(resolve: &Resolve, id: wit_parser::InterfaceId, import: &str) -> Result<Option<CallImport>> {
    let iface = &resolve.interfaces[id];
    let Some(pkg) = iface.package else { return Ok(None) };
    let pn = &resolve.packages[pkg].name;
    let Some(app) = app_of_package(&pn.namespace, &pn.name) else { return Ok(None) };
    let hint = "client interfaces are generated by `statex calls sync`";
    let actor_type = iface.name.clone().ok_or_else(|| anyhow!("import {import}: unnamed interface; {hint}"))?;
    validate_app_name(&app).with_context(|| format!("import {import} does not name a statex app; {hint}"))?;
    let mut methods = Vec::new();
    for f in iface.functions.values() {
        let ctx = format!("{import}.{}", f.name);
        if !matches!(f.kind, wit_parser::FunctionKind::Freestanding) {
            bail!("{ctx}: resources are not supported in client interfaces; {hint}");
        }
        match f.params.first() {
            Some(p) if p.ty == Type::String => {}
            _ => bail!("{ctx}: the first parameter must be the callee's actor key (`actor: string`); {hint}"),
        }
        let result = match f.result.as_ref().map(|t| deref(resolve, t)) {
            Some(TypeDefKind::Result(r)) if r.err.as_ref().is_some_and(|e| is_call_error(resolve, e)) => {
                r.ok.as_ref().map(|t| ty_of(resolve, t, &ctx)).transpose()?
            }
            _ => bail!("{ctx}: must return `result<T, call-error>` (call-error from statex:host/actors); {hint}"),
        };
        methods.push(Method {
            name: f.name.clone(),
            params: f.params[1..]
                .iter()
                .map(|p| Ok(Param { name: p.name.clone(), ty: ty_of(resolve, &p.ty, &ctx)? }))
                .collect::<Result<_>>()?,
            result,
            docs: f.docs.contents.clone(),
        });
    }
    Ok(Some(CallImport { import: import.to_string(), app, actor_type, methods }))
}

/// Follows type aliases (`use`, `type x = y`) to the defining type.
fn deref<'a>(resolve: &'a Resolve, t: &Type) -> &'a TypeDefKind {
    static NONE: TypeDefKind = TypeDefKind::Unknown;
    let mut t = *t;
    loop {
        match t {
            Type::Id(id) => match &resolve.types[id].kind {
                TypeDefKind::Type(inner) => t = *inner,
                k => return k,
            },
            _ => return &NONE,
        }
    }
}

/// Whether `t` is `call-error` from `statex:host/actors`.
fn is_call_error(resolve: &Resolve, t: &Type) -> bool {
    let mut t = *t;
    loop {
        let Type::Id(id) = t else { return false };
        let td = &resolve.types[id];
        if let TypeDefKind::Type(inner) = &td.kind {
            t = *inner;
            continue;
        }
        let wit_parser::TypeOwner::Interface(i) = td.owner else { return false };
        let iface = &resolve.interfaces[i];
        let host = iface.package.is_some_and(|p| {
            let n = &resolve.packages[p].name;
            n.namespace == "statex" && n.name == "host"
        });
        return host && iface.name.as_deref() == Some("actors") && td.name.as_deref() == Some("call-error");
    }
}

fn ty_of(resolve: &Resolve, t: &Type, ctx: &str) -> Result<Ty> {
    Ok(match t {
        Type::Bool => Ty::Bool,
        Type::U8 => Ty::U8,
        Type::U16 => Ty::U16,
        Type::U32 => Ty::U32,
        Type::U64 => Ty::U64,
        Type::S8 => Ty::S8,
        Type::S16 => Ty::S16,
        Type::S32 => Ty::S32,
        Type::S64 => Ty::S64,
        Type::F32 => Ty::F32,
        Type::F64 => Ty::F64,
        Type::Char => Ty::Char,
        Type::String => Ty::String,
        Type::ErrorContext => bail!("{ctx}: error-context is not supported"),
        Type::Id(id) => {
            let td = &resolve.types[*id];
            let name = td.name.clone().unwrap_or_default();
            match &td.kind {
                TypeDefKind::Type(inner) => ty_of(resolve, inner, ctx)?,
                TypeDefKind::Record(r) => Ty::Record {
                    name,
                    fields: r
                        .fields
                        .iter()
                        .map(|f| Ok(Field { name: f.name.clone(), ty: ty_of(resolve, &f.ty, ctx)? }))
                        .collect::<Result<_>>()?,
                },
                TypeDefKind::Variant(v) => Ty::Variant {
                    name,
                    cases: v
                        .cases
                        .iter()
                        .map(|c| {
                            Ok(Case {
                                name: c.name.clone(),
                                ty: c.ty.as_ref().map(|t| ty_of(resolve, t, ctx)).transpose()?,
                            })
                        })
                        .collect::<Result<_>>()?,
                },
                TypeDefKind::Enum(e) => {
                    Ty::Enum { name, cases: e.cases.iter().map(|c| c.name.clone()).collect() }
                }
                TypeDefKind::Flags(f) => {
                    Ty::Flags { name, flags: f.flags.iter().map(|c| c.name.clone()).collect() }
                }
                TypeDefKind::Tuple(t) => Ty::Tuple {
                    items: t.types.iter().map(|t| ty_of(resolve, t, ctx)).collect::<Result<_>>()?,
                },
                TypeDefKind::Option(t) => Ty::Option { inner: Box::new(ty_of(resolve, t, ctx)?) },
                TypeDefKind::Result(r) => Ty::Result {
                    ok: r.ok.as_ref().map(|t| ty_of(resolve, t, ctx).map(Box::new)).transpose()?,
                    err: r.err.as_ref().map(|t| ty_of(resolve, t, ctx).map(Box::new)).transpose()?,
                },
                TypeDefKind::List(t) => Ty::List { element: Box::new(ty_of(resolve, t, ctx)?) },
                other => bail!("{ctx}: WIT type `{}` is not supported in actor interfaces", other.as_str()),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_options_default_for_old_manifests_and_validate_stateless() {
        let mut ty: ActorType = serde_json::from_value(serde_json::json!({
            "name": "worker", "export": "x:y/worker", "methods": []
        })).unwrap();
        assert!(!ty.stateless && !ty.group_commit);
        ty.stateless = true;
        let mut m = Manifest {
            format: 1, app: "test".into(), sha256: String::new(), types: vec![ty],
            migrations: BTreeMap::new(), http: Default::default(), limits: Default::default(), calls: vec![],
        };
        m.validate_actor_options().unwrap();
        m.types[0].group_commit = true;
        assert!(m.validate_actor_options().unwrap_err().to_string().contains("group_commit"));
        m.types[0].group_commit = false;
        m.types[0].alarm = Some(Method { name: "alarm".into(), params: vec![], result: None, docs: None });
        assert!(m.validate_actor_options().unwrap_err().to_string().contains("alarm"));
        m.types[0].alarm = None;
        m.migrations.insert("worker".into(), vec![Migration { name: "init.sql".into(), sql: "SELECT 1".into() }]);
        assert!(m.validate_actor_options().unwrap_err().to_string().contains("migrations"));
    }

    #[test]
    fn app_names() {
        for ok in ["shop", "payments/shop", "a1/b-2"] {
            validate_app_name(ok).unwrap();
        }
        for bad in ["", "/shop", "shop/", "a/b/c", "Pay/shop", "payments/actors", "schema", "a//b", "a.b"] {
            assert!(validate_app_name(bad).is_err(), "{bad}");
        }
    }
}
