//! Backward-compatibility checks between two versions of an app.
//!
//! A change is breaking when an existing client or an existing actor could
//! fail because of it:
//!
//! - an actor type or method is removed, or a method signature (parameter
//!   names, parameter types, result type, including the full shape of named
//!   types) changes;
//! - a migration that may already have been applied is edited, removed or
//!   renamed, or a new migration sorts before an existing one (it would never
//!   run on actors that already applied the later ones).
//!
//! Adding actor types, methods and migrations (sorted after existing ones) is
//! always compatible.

use std::collections::BTreeMap;

use crate::manifest::{ActorType, CallImport, Migration, Ty};

/// The parts of an app version that compatibility is judged on.
#[derive(Clone, Copy)]
pub struct Surface<'a> {
    pub types: &'a [ActorType],
    pub migrations: &'a BTreeMap<String, Vec<Migration>>,
}

impl<'a> From<&'a crate::Manifest> for Surface<'a> {
    fn from(m: &'a crate::Manifest) -> Self {
        Surface { types: &m.types, migrations: &m.migrations }
    }
}

/// Returns human-readable descriptions of every breaking change from `old` to
/// `new`; empty when `new` is backward compatible.
pub fn breaking_changes(old: Surface<'_>, new: Surface<'_>) -> Vec<String> {
    let mut out = Vec::new();
    for ot in old.types {
        let Some(nt) = new.types.iter().find(|t| t.name == ot.name) else {
            out.push(format!("actor type `{}` was removed", ot.name));
            continue;
        };
        for om in &ot.methods {
            let Some(nm) = nt.method(&om.name) else {
                out.push(format!("method `{}.{}` was removed", ot.name, om.name));
                continue;
            };
            let at = format!("{}.{}", ot.name, om.name);
            let (op, np) = (sig_params(&om.params), sig_params(&nm.params));
            if op != np {
                out.push(format!("method `{at}`: parameters changed from ({op}) to ({np})"));
            } else {
                for (a, b) in om.params.iter().zip(&nm.params) {
                    if let Some(d) = diff_ty(&a.ty, &b.ty) {
                        out.push(format!("method `{at}`, parameter `{}`: {d}", a.name));
                    }
                }
            }
            match (&om.result, &nm.result) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    if let Some(d) = diff_ty(a, b) {
                        out.push(format!("method `{at}`, result: {d}"));
                    }
                }
                (a, b) => out.push(format!("method `{at}`: result changed from {} to {}", opt(a), opt(b))),
            }
        }
    }
    let empty = Vec::new();
    for (ty, olds) in old.migrations {
        if !new.types.iter().any(|t| &t.name == ty) {
            continue; // already reported as a removed actor type
        }
        let news = new.migrations.get(ty).unwrap_or(&empty);
        for om in olds {
            match news.iter().find(|m| m.name == om.name) {
                None => out.push(format!("migration `{ty}/{}` was removed or renamed; applied migrations are append-only", om.name)),
                Some(nm) if norm_sql(&nm.sql) != norm_sql(&om.sql) => out.push(format!(
                    "migration `{ty}/{}` was edited; actors that already applied it will not re-run it. Add a new migration instead",
                    om.name
                )),
                Some(_) => {}
            }
        }
        if let Some(last) = olds.iter().map(|m| &m.name).max() {
            for nm in news {
                if !olds.iter().any(|m| m.name == nm.name) && &nm.name < last {
                    out.push(format!(
                        "new migration `{ty}/{}` sorts before existing `{ty}/{last}`; it would never run on existing actors",
                        nm.name
                    ));
                }
            }
        }
    }
    out
}

/// Checks a caller's client interface against the callee app's manifest.
/// Returns every method the caller expects that the callee does not provide
/// with the same signature; empty when the call import is satisfied.
/// Parameter names do not matter: calls pass arguments by position.
pub fn call_mismatches(call: &CallImport, callee: &crate::Manifest) -> Vec<String> {
    call_mismatches_in(call, &callee.app, &callee.types)
}

/// [`call_mismatches`] against the actor types of app `app` (e.g. read from source).
pub fn call_mismatches_in(call: &CallImport, app: &str, types: &[crate::ActorType]) -> Vec<String> {
    let Some(t) = types.iter().find(|t| t.name == call.actor_type) else {
        return vec![format!("app {app} has no actor type `{}`", call.actor_type)];
    };
    let mut out = Vec::new();
    for m in &call.methods {
        let at = format!("{}.{}", call.actor_type, m.name);
        let Some(cm) = t.method(&m.name) else {
            out.push(format!("app {app} has no method `{at}`"));
            continue;
        };
        let types = |ps: &[crate::manifest::Param]| ps.iter().map(|p| fmt_ty(&p.ty)).collect::<Vec<_>>().join(", ");
        if m.params.len() != cm.params.len() {
            out.push(format!("`{at}`: caller passes ({}), callee takes ({})", types(&m.params), types(&cm.params)));
        } else {
            for (i, (a, b)) in m.params.iter().zip(&cm.params).enumerate() {
                if let Some(d) = diff_ty(&a.ty, &b.ty) {
                    out.push(format!("`{at}`, parameter {} (`{}`): {d}", i + 1, b.name));
                }
            }
        }
        match (&m.result, &cm.result) {
            (None, None) => {}
            (Some(a), Some(b)) => {
                if let Some(d) = diff_ty(a, b) {
                    out.push(format!("`{at}`, result: {d}"));
                }
            }
            (a, b) => out.push(format!("`{at}`: caller expects {}, callee returns {}", opt(a), opt(b))),
        }
    }
    out
}

fn opt(t: &Option<Ty>) -> String {
    t.as_ref().map(fmt_ty).unwrap_or_else(|| "nothing".into())
}

fn sig_params(ps: &[crate::manifest::Param]) -> String {
    ps.iter().map(|p| format!("{}: {}", p.name, fmt_ty(&p.ty))).collect::<Vec<_>>().join(", ")
}

fn norm_sql(s: &str) -> String {
    s.replace("\r\n", "\n").trim().to_string()
}

/// Describes how `b` differs from `a` (pointing at the innermost named type
/// that changed), or `None` if they are identical.
fn diff_ty(a: &Ty, b: &Ty) -> Option<String> {
    if a == b {
        return None;
    }
    let first = |pairs: Vec<(&Ty, &Ty)>| pairs.into_iter().find_map(|(x, y)| diff_ty(x, y));
    let changed = || Some(format!("type changed from {} to {}", fmt_ty(a), fmt_ty(b)));
    match (a, b) {
        (Ty::List { element: x }, Ty::List { element: y }) | (Ty::Option { inner: x }, Ty::Option { inner: y }) => diff_ty(x, y),
        (Ty::Result { ok: o1, err: e1 }, Ty::Result { ok: o2, err: e2 })
            if o1.is_some() == o2.is_some() && e1.is_some() == e2.is_some() =>
        {
            let ok = o1.as_deref().zip(o2.as_deref());
            let err = e1.as_deref().zip(e2.as_deref());
            first(ok.into_iter().chain(err).collect())
        }
        (Ty::Tuple { items: x }, Ty::Tuple { items: y }) if x.len() == y.len() => first(x.iter().zip(y).collect()),
        (Ty::Record { name: n, fields: x }, Ty::Record { name: m, fields: y }) if n == m => {
            let names = |f: &[crate::manifest::Field]| f.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
            if names(x) == names(y) {
                if let Some(d) = first(x.iter().zip(y).map(|(p, q)| (&p.ty, &q.ty)).collect()) {
                    return Some(d);
                }
            }
            Some(format!("type `{n}` changed from {} to {}", shape(a), shape(b)))
        }
        (Ty::Variant { name: n, cases: x }, Ty::Variant { name: m, cases: y }) if n == m => {
            let names = |c: &[crate::manifest::Case]| c.iter().map(|c| (c.name.clone(), c.ty.is_some())).collect::<Vec<_>>();
            if names(x) == names(y) {
                let pairs = x.iter().zip(y).filter_map(|(p, q)| p.ty.as_ref().zip(q.ty.as_ref())).collect();
                if let Some(d) = first(pairs) {
                    return Some(d);
                }
            }
            Some(format!("type `{n}` changed from {} to {}", shape(a), shape(b)))
        }
        (Ty::Enum { name: n, .. }, Ty::Enum { name: m, .. }) | (Ty::Flags { name: n, .. }, Ty::Flags { name: m, .. }) if n == m => {
            Some(format!("type `{n}` changed from {} to {}", shape(a), shape(b)))
        }
        _ => changed(),
    }
}

/// The definition of a named type, e.g. `{sku: string, qty: u32}`.
fn shape(t: &Ty) -> String {
    match t {
        Ty::Record { fields, .. } => {
            format!("{{{}}}", fields.iter().map(|f| format!("{}: {}", f.name, fmt_ty(&f.ty))).collect::<Vec<_>>().join(", "))
        }
        Ty::Variant { cases, .. } => cases
            .iter()
            .map(|c| match &c.ty {
                Some(t) => format!("{}({})", c.name, fmt_ty(t)),
                None => c.name.clone(),
            })
            .collect::<Vec<_>>()
            .join(" | "),
        Ty::Enum { cases, .. } => cases.join(" | "),
        Ty::Flags { flags, .. } => flags.join(" | "),
        other => fmt_ty(other),
    }
}

/// Formats a type the way it is written in WIT; named types by name.
pub fn fmt_ty(t: &Ty) -> String {
    match t {
        Ty::List { element } => format!("list<{}>", fmt_ty(element)),
        Ty::Option { inner } => format!("option<{}>", fmt_ty(inner)),
        Ty::Result { ok, err } => {
            let f = |x: &Option<Box<Ty>>| x.as_deref().map(fmt_ty).unwrap_or_else(|| "_".into());
            format!("result<{}, {}>", f(ok), f(err))
        }
        Ty::Tuple { items } => format!("tuple<{}>", items.iter().map(fmt_ty).collect::<Vec<_>>().join(", ")),
        Ty::Record { name, .. } | Ty::Variant { name, .. } | Ty::Enum { name, .. } | Ty::Flags { name, .. } => name.clone(),
        other => serde_json::to_value(other).unwrap()["kind"].as_str().unwrap_or("?").to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Field, Method, Param};

    fn ty(methods: Vec<Method>) -> ActorType {
        ActorType { name: "cart".into(), export: "x:y/cart".into(), docs: None, methods, alarm: None }
    }
    fn m(name: &str, params: Vec<(&str, Ty)>, result: Option<Ty>) -> Method {
        Method {
            name: name.into(),
            params: params.into_iter().map(|(n, t)| Param { name: n.into(), ty: t }).collect(),
            result,
            docs: None,
        }
    }
    fn item(fields: &[(&str, Ty)]) -> Ty {
        Ty::Record { name: "item".into(), fields: fields.iter().map(|(n, t)| Field { name: (*n).into(), ty: t.clone() }).collect() }
    }
    fn migs(list: &[(&str, &str)]) -> BTreeMap<String, Vec<Migration>> {
        BTreeMap::from([(
            "cart".to_string(),
            list.iter().map(|(n, s)| Migration { name: (*n).into(), sql: (*s).into() }).collect(),
        )])
    }
    fn check(old: (&[ActorType], &BTreeMap<String, Vec<Migration>>), new: (&[ActorType], &BTreeMap<String, Vec<Migration>>)) -> Vec<String> {
        breaking_changes(Surface { types: old.0, migrations: old.1 }, Surface { types: new.0, migrations: new.1 })
    }

    #[test]
    fn additions_are_compatible() {
        let old = [ty(vec![m("add", vec![("sku", Ty::String)], Some(Ty::U32))])];
        let new = [
            ty(vec![m("add", vec![("sku", Ty::String)], Some(Ty::U32)), m("clear", vec![], None)]),
            ActorType { name: "wishlist".into(), ..ty(vec![]) },
        ];
        let (mo, mn) = (migs(&[("0001_init.sql", "CREATE TABLE a (x);")]), migs(&[("0001_init.sql", "CREATE TABLE a (x);\n"), ("0002_more.sql", "x")]));
        assert!(check((&old, &mo), (&new, &mn)).is_empty());
    }

    #[test]
    fn detects_breaking_changes() {
        let old = [ty(vec![
            m("add", vec![("sku", Ty::String)], Some(Ty::U32)),
            m("items", vec![], Some(Ty::List { element: Box::new(item(&[("sku", Ty::String)])) })),
            m("gone", vec![], None),
        ])];
        let new = [ty(vec![
            m("add", vec![("sku", Ty::String), ("qty", Ty::U32)], Some(Ty::U32)),
            m("items", vec![], Some(Ty::List { element: Box::new(item(&[("sku", Ty::String), ("qty", Ty::U32)])) })),
        ])];
        let mo = migs(&[("0001_init.sql", "a"), ("0003_x.sql", "c")]);
        let mn = migs(&[("0001_init.sql", "a2"), ("0002_late.sql", "b"), ("0003_x.sql", "c")]);
        let out = check((&old, &mo), (&new, &mn)).join("\n");
        assert!(out.contains("`cart.add`: parameters changed from (sku: string) to (sku: string, qty: u32)"), "{out}");
        assert!(out.contains("type `item` changed from {sku: string} to {sku: string, qty: u32}"), "{out}");
        assert!(out.contains("`cart.gone` was removed"), "{out}");
        assert!(out.contains("`cart/0001_init.sql` was edited"), "{out}");
        assert!(out.contains("`cart/0002_late.sql` sorts before"), "{out}");
        assert!(check((&old, &mo), (&[], &BTreeMap::new())).join("").contains("actor type `cart` was removed"));
    }
}
