//! App manifest: the actor types, methods and type signatures discovered from a
//! component's WIT exports, plus deployment metadata. The manifest drives
//! routing, JSON mapping and client SDK generation, so applications never register
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
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub timeout_ms: u64,
    pub memory_mb: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
}

impl Default for Limits {
    fn default() -> Self {
        Self { timeout_ms: 5_000, memory_mb: 64, fuel: None, rps: None, burst: None, max_concurrent: None }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<()> {
        crate::limits::validate_timeout(self.timeout_ms)?;
        crate::limits::memory_bytes(self.memory_mb)?;
        anyhow::ensure!(self.fuel != Some(0), "fuel must be positive");
        anyhow::ensure!(self.rps != Some(0), "rps must be positive");
        anyhow::ensure!(self.burst != Some(0), "burst must be positive");
        anyhow::ensure!(self.max_concurrent != Some(0), "max_concurrent must be positive");
        anyhow::ensure!(self.burst.is_none() || self.rps.is_some(), "burst requires rps");
        Ok(())
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

    /// Builds and validates a manifest for a component binary: checks imports,
    /// extracts actor types and attaches migrations (`type -> [(file, sql)]`).
    pub fn build(
        wasm: &[u8],
        app: &str,
        migrations: BTreeMap<String, Vec<Migration>>,
        http: HttpPolicy,
        limits: Limits,
    ) -> Result<Manifest> {
        Self::build_with_imports(wasm, app, migrations, http, limits, &[])
    }

    /// Builds a manifest with explicitly registered additional host interfaces.
    /// Entries are exact component import names, including any WIT version.
    /// The ordinary [`Self::build`] never admits these extension imports.
    pub fn build_with_imports(
        wasm: &[u8],
        app: &str,
        migrations: BTreeMap<String, Vec<Migration>>,
        http: HttpPolicy,
        limits: Limits,
        additional_imports: &[String],
    ) -> Result<Manifest> {
        limits.validate()?;
        validate_app_name(app)?;
        let ins = inspect_with_imports(wasm, additional_imports)?;
        let bad = unprovided_imports(&ins, additional_imports);
        if !bad.is_empty() {
            bail!(
                "component imports interfaces the host does not provide: {}. Only statex:host/*, wasi:*, typed actor clients (`statex calls sync`) and explicitly registered host interfaces are available",
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

/// General identifiers: lowercase letters, digits and dashes, starting with a letter.
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

/// App names are one segment (`shop`) or namespaced as `namespace/app`
/// (`payments/shop`). Each segment is a WIT-compatible kebab identifier,
/// limited to 63 characters.
pub fn validate_app_name(s: &str) -> Result<()> {
    let segs: Vec<&str> = s.split('/').collect();
    if segs.len() > 2 {
        bail!("invalid app name {s:?}: use `app` or `namespace/app`");
    }
    for seg in segs {
        let valid = seg.len() <= 63
            && seg.split('-').all(|word| {
                word.starts_with(|c: char| c.is_ascii_lowercase())
                    && word.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            });
        if !valid {
            bail!("invalid app name {s:?}: use `app` or `namespace/app`, with 1–63-character segments of lowercase alphanumeric words separated by single dashes, each word starting with a letter");
        }
        if RESERVED_APP_SEGMENTS.contains(&seg) {
            bail!("invalid app name {s:?}: `{seg}` is reserved");
        }
    }
    // Client interfaces of app `namespace/app` live in WIT package `namespace:app`, and
    // of a single-segment app `app` in `statex:app` (see [`client_package`]).
    match s.split_once('/') {
        Some((namespace, _)) if RESERVED_NAMESPACES.contains(&namespace) => bail!("invalid app name {s:?}: namespace `{namespace}` is reserved"),
        None if s == "host" => bail!("invalid app name {s:?}: `host` is reserved"),
        _ => {}
    }
    Ok(())
}

/// WIT namespaces reserved for host capabilities.
pub const RESERVED_NAMESPACES: &[&str] = &["statex", "wasi"];

/// WIT package (`namespace`, `name`) holding the client interfaces of `app`:
/// `namespace/app` -> `namespace:app`, `app` -> `statex:app`.
pub fn client_package(app: &str) -> (String, String) {
    match app.split_once('/') {
        Some((namespace, name)) => (namespace.to_string(), name.to_string()),
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

fn unprovided_imports(ins: &Inspection, additional_imports: &[String]) -> Vec<String> {
    ins.imports.iter()
        .filter(|i| !import_allowed(i) && !additional_imports.contains(i) && !ins.calls.iter().any(|c| &c.import == *i))
        .cloned()
        .collect()
}

/// Import namespaces an actor component may use.
pub fn import_allowed(name: &str) -> bool {
    name.starts_with("statex:host/") || name.starts_with("wasi:")
}

/// Decodes a component binary and extracts its actor types.
pub fn inspect(wasm: &[u8]) -> Result<Inspection> {
    inspect_with_imports(wasm, &[])
}

fn inspect_with_imports(wasm: &[u8], additional_imports: &[String]) -> Result<Inspection> {
    let decoded = wit_component::decode(wasm).map_err(|e| anyhow!("not a valid component: {e}"))?;
    let (resolve, world) = match decoded {
        wit_component::DecodedWasm::Component(r, w) => (r, w),
        _ => bail!("expected a WebAssembly component, found a WIT package"),
    };
    inspect_world(&resolve, world, additional_imports)
}

/// Extracts actor types from WIT source (a directory such as `wit/`, with
/// dependencies under `wit/deps/`), without building the component. `world`
/// may be omitted when the package defines a single world.
pub fn inspect_wit(dir: &std::path::Path, world: Option<&str>) -> Result<Inspection> {
    let mut resolve = Resolve::default();
    let (pkg, _) = resolve.push_dir(dir).with_context(|| format!("parse WIT in {}", dir.display()))?;
    let world = resolve.select_world(&[pkg], world)?;
    inspect_world(&resolve, world, &[])
}

/// Client interfaces imported by a WIT world, e.g. a generated `statex-calls`
/// world (which, unlike an app world, exports nothing).
pub fn inspect_wit_calls(dir: &std::path::Path, world: Option<&str>) -> Result<Vec<CallImport>> {
    let mut resolve = Resolve::default();
    let (pkg, _) = resolve.push_dir(dir).with_context(|| format!("parse WIT in {}", dir.display()))?;
    let world = resolve.select_world(&[pkg], world)?;
    world_calls(&resolve, world, &[])
}

fn world_calls(resolve: &Resolve, world: wit_parser::WorldId, additional_imports: &[String]) -> Result<Vec<CallImport>> {
    let mut calls = Vec::new();
    for (key, item) in &resolve.worlds[world].imports {
        if let WorldItem::Interface { id, .. } = item {
            let import = resolve.name_world_key(key);
            if additional_imports.contains(&import) {
                continue;
            }
            if let Some(c) = call_import(resolve, *id, &import)? {
                calls.push(c);
            }
        }
    }
    Ok(calls)
}

fn inspect_world(resolve: &Resolve, world: wit_parser::WorldId, additional_imports: &[String]) -> Result<Inspection> {
    let w = &resolve.worlds[world];
    let imports = w.imports.keys().map(|k| resolve.name_world_key(k)).collect();
    let calls = world_calls(resolve, world, additional_imports)?;
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
                types.push(ActorType { name, export, docs: iface.docs.contents.clone(), methods, alarm });
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

/// Recognizes typed actor clients by their use of the host's `call-error`.
/// Other interfaces remain imports that manifest capability validation must
/// admit explicitly; a package name alone never turns an import into a client.
fn call_import(resolve: &Resolve, id: wit_parser::InterfaceId, import: &str) -> Result<Option<CallImport>> {
    let iface = &resolve.interfaces[id];
    let Some(pkg) = iface.package else { return Ok(None) };
    let pn = &resolve.packages[pkg].name;
    let Some(app) = app_of_package(&pn.namespace, &pn.name) else { return Ok(None) };
    let uses_call_error = iface.types.values().any(|id| is_call_error(resolve, &Type::Id(*id)))
        || iface.functions.values().any(|f| {
            matches!(
                f.result.as_ref().map(|t| deref(resolve, t)),
                Some(TypeDefKind::Result(r)) if r.err.as_ref().is_some_and(|e| is_call_error(resolve, e))
            )
        });
    if !uses_call_error {
        return Ok(None);
    }
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
    fn app_names() {
        for ok in ["shop", "payments/shop", "a1/b-v2"] {
            validate_app_name(ok).unwrap();
        }
        for bad in ["", "/shop", "shop/", "a/b/c", "Pay/shop", "payments/actors", "schema", "a//b", "a.b", "a-2", "a--b", "shop-"] {
            assert!(validate_app_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn client_packages_round_trip_without_changing_existing_names() {
        for app in ["shop", "payments/shop", "type", "interface/type"] {
            validate_app_name(app).unwrap();
            let (namespace, name) = client_package(app);
            assert_eq!(app_of_package(&namespace, &name).as_deref(), Some(app));
        }
        assert_eq!(client_package("shop"), ("statex".into(), "shop".into()));
        assert_eq!(client_package("payments/shop"), ("payments".into(), "shop".into()));
        for host in [("statex", "host"), ("wasi", "io")] {
            assert!(app_of_package(host.0, host.1).is_none());
        }
        for reserved in ["host", "statex/shop", "wasi/shop", "actors", "schema", "payments/schema"] {
            assert!(validate_app_name(reserved).is_err(), "{reserved}");
        }
    }

    #[test]
    fn registered_interfaces_are_not_mistaken_for_actor_clients() {
        let mut resolve = Resolve::default();
        resolve.push_str("metrics.wit", "package vendor:metrics@1.0.0;\ninterface recorder { write: func(value: u64); }\n").unwrap();
        let pkg = resolve.push_str("app.wit", r#"
            package local:shop;
            interface counter { get: func() -> u64; }
            world app {
                import vendor:metrics/recorder@1.0.0;
                export counter;
            }
        "#).unwrap();
        let world = resolve.select_world(&[pkg], Some("app")).unwrap();
        let unknown = inspect_world(&resolve, world, &[]).unwrap();
        assert!(unknown.calls.is_empty());
        assert_eq!(unprovided_imports(&unknown, &[]), ["vendor:metrics/recorder@1.0.0"]);
        let ins = inspect_world(&resolve, world, &["vendor:metrics/recorder@1.0.0".into()]).unwrap();
        assert_eq!(ins.types[0].name, "counter");
        assert!(ins.calls.is_empty());
        assert_eq!(ins.imports, ["vendor:metrics/recorder@1.0.0"]);
        assert!(unprovided_imports(&ins, &["vendor:metrics/recorder@1.0.0".into()]).is_empty());
        assert_eq!(unprovided_imports(&ins, &["vendor:metrics/recorder".into()]), ["vendor:metrics/recorder@1.0.0"]);
        assert_eq!(unprovided_imports(&ins, &["vendor:metrics/*".into()]), ["vendor:metrics/recorder@1.0.0"]);
    }

    #[test]
    fn typed_actor_clients_remain_distinct_from_custom_capabilities() {
        for key in ["string", "u64"] {
            let mut resolve = Resolve::default();
            resolve.push_str("host.wit", include_str!("../../../wit/statex-host.wit")).unwrap();
            resolve.push_str("client.wit", &format!(r#"
                package demo:counter;
                interface counter {{
                    use statex:host/actors@0.1.0.{{call-error}};
                    increment: func(actor: {key}) -> result<u64, call-error>;
                }}
            "#)).unwrap();
            let pkg = resolve.push_str("app.wit", r#"
                package local:shop;
                interface shop { get: func() -> u64; }
                world app {
                    import demo:counter/counter;
                    export shop;
                }
            "#).unwrap();
            let world = resolve.select_world(&[pkg], Some("app")).unwrap();
            if key == "string" {
                let ins = inspect_world(&resolve, world, &[]).unwrap();
                assert_eq!(ins.calls[0].app, "demo/counter");
                assert!(unprovided_imports(&ins, &[]).is_empty());
            } else {
                let err = inspect_world(&resolve, world, &[]).unwrap_err();
                assert!(err.to_string().contains("the first parameter must be"));
                let registered = inspect_world(&resolve, world, &["demo:counter/counter".into()]).unwrap();
                assert!(registered.calls.is_empty());
                assert!(unprovided_imports(&registered, &["demo:counter/counter".into()]).is_empty());
            }
        }
    }
}
