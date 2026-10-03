//! Client SDK generation from an app manifest.

use std::collections::BTreeMap;
use std::fmt::Write;

use anyhow::Result;
use serde_json::{json, Value as J};
use statex_runtime::Manifest;

const PY_RUNTIME: &str = include_str!("../../../sdk/python/statex_client/_runtime.py");

const PY_KEYWORDS: &[&str] = &[
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "finally", "for", "from", "global", "if", "import", "in", "is", "lambda", "nonlocal", "not", "or",
    "pass", "raise", "return", "try", "while", "with", "yield", "self", "key", "create", "delete",
];

fn pascal(s: &str) -> String {
    s.split(['-', '_', '/']).filter(|p| !p.is_empty()).map(|p| p[..1].to_uppercase() + &p[1..]).collect()
}

fn ident(s: &str) -> String {
    let s = s.replace('-', "_");
    if PY_KEYWORDS.contains(&s.as_str()) {
        format!("{s}_")
    } else {
        s
    }
}

fn is_named(t: &J) -> bool {
    matches!(t["kind"].as_str(), Some("record" | "variant" | "enum")) && t["name"].is_string()
}

/// Assigns Python class names to named types (records, variants, enums),
/// de-duplicating identical types across interfaces.
struct Names {
    by_shape: BTreeMap<String, String>,
    taken: BTreeMap<String, String>,
    order: Vec<(String, J)>,
}

impl Names {
    fn assign(&mut self, iface: &str, t: &mut J) {
        // Children first, so dependencies are emitted before their users.
        match t["kind"].as_str().unwrap_or("") {
            "list" => self.assign(iface, &mut t["element"]),
            "option" => self.assign(iface, &mut t["inner"]),
            "result" => {
                for k in ["ok", "err"] {
                    if !t[k].is_null() {
                        self.assign(iface, &mut t[k]);
                    }
                }
            }
            "tuple" => t["items"].as_array_mut().unwrap().iter_mut().for_each(|x| self.assign(iface, x)),
            "record" => {
                for f in t["fields"].as_array_mut().unwrap() {
                    self.assign(iface, &mut f["ty"]);
                    let py = ident(f["name"].as_str().unwrap());
                    f["py"] = json!(py);
                }
            }
            "variant" => {
                for c in t["cases"].as_array_mut().unwrap() {
                    if !c["ty"].is_null() {
                        self.assign(iface, &mut c["ty"]);
                    }
                }
            }
            _ => {}
        }
        if !is_named(t) {
            return;
        }
        let shape = t.to_string();
        let py = if let Some(n) = self.by_shape.get(&shape) {
            n.clone()
        } else {
            let base = pascal(t["name"].as_str().unwrap());
            let mut n = if self.taken.contains_key(&base) { format!("{}{base}", pascal(iface)) } else { base };
            while self.taken.contains_key(&n) {
                n.push('_');
            }
            self.taken.insert(n.clone(), shape.clone());
            self.by_shape.insert(shape, n.clone());
            self.order.push((n.clone(), t.clone()));
            n
        };
        t["py"] = json!(py);
    }
}

fn py_ty(t: &J) -> String {
    if let Some(py) = t["py"].as_str() {
        return py.to_string();
    }
    match t["kind"].as_str().unwrap_or("") {
        "bool" => "bool".into(),
        "u8" | "u16" | "u32" | "u64" | "s8" | "s16" | "s32" | "s64" => "int".into(),
        "f32" | "f64" => "float".into(),
        "char" | "string" => "str".into(),
        "list" if t["element"]["kind"] == "u8" => "bytes".into(),
        "list" => format!("List[{}]", py_ty(&t["element"])),
        "option" => format!("Optional[{}]", py_ty(&t["inner"])),
        "tuple" => {
            let items: Vec<_> = t["items"].as_array().unwrap().iter().map(py_ty).collect();
            if items.is_empty() {
                "Tuple[()]".into()
            } else {
                format!("Tuple[{}]", items.join(", "))
            }
        }
        "flags" => "List[str]".into(),
        _ => "Any".into(),
    }
}

fn ret_ty(t: &J) -> String {
    if t.is_null() {
        return "None".into();
    }
    if t["kind"] == "result" {
        return if t["ok"].is_null() { "None".into() } else { py_ty(&t["ok"]) };
    }
    py_ty(t)
}

fn docstring(out: &mut String, indent: &str, docs: Option<&str>) {
    if let Some(d) = docs.filter(|d| !d.trim().is_empty()) {
        let d = d.trim().replace("\"\"\"", "'''").replace('\\', "\\\\");
        let body = d.lines().collect::<Vec<_>>().join(&format!("\n{indent}"));
        let _ = writeln!(out, "{indent}\"\"\"{body}\"\"\"");
    }
}

pub fn python(manifest: &Manifest, default_url: &str) -> Result<String> {
    let mut names = Names { by_shape: BTreeMap::new(), taken: BTreeMap::new(), order: vec![] };
    for t in &manifest.types {
        names.taken.insert(format!("{}Actor", pascal(&t.name)), String::new());
    }
    names.taken.insert(format!("{}App", pascal(&manifest.app)), String::new());
    let mut types: Vec<(String, Option<String>, Vec<(String, Option<String>, J)>)> = vec![];
    for t in &manifest.types {
        let mut methods = vec![];
        for m in &t.methods {
            let mut sig = json!({ "params": [], "result": null });
            for p in &m.params {
                let mut ty = serde_json::to_value(&p.ty)?;
                names.assign(&t.name, &mut ty);
                sig["params"].as_array_mut().unwrap().push(json!({ "name": p.name, "py": ident(&p.name), "ty": ty }));
            }
            if let Some(r) = &m.result {
                let mut ty = serde_json::to_value(r)?;
                names.assign(&t.name, &mut ty);
                sig["result"] = ty;
            }
            methods.push((m.name.clone(), m.docs.clone(), sig));
        }
        types.push((t.name.clone(), t.docs.clone(), methods));
    }

    let mut o = String::new();
    let _ = writeln!(
        o,
        "# Generated by `statex codegen python` for app {app:?} (sha256 {sha}).\n# Do not edit by hand; re-run codegen after changing the WIT.\n\nfrom __future__ import annotations\n",
        app = manifest.app,
        sha = &manifest.sha256[..12]
    );
    o.push_str(PY_RUNTIME);
    o.push_str("\n\n# --- generated types ---------------------------------------------------------\n\nimport dataclasses as _dc  # noqa: E402\nfrom typing import Tuple  # noqa: E402,F811\n\n");

    for (py, t) in &names.order {
        match t["kind"].as_str().unwrap() {
            "record" => {
                let _ = writeln!(o, "@_dc.dataclass\nclass {py}:");
                let fields = t["fields"].as_array().unwrap();
                if fields.is_empty() {
                    o.push_str("    pass\n");
                }
                for f in fields {
                    let _ = writeln!(o, "    {}: {}", f["py"].as_str().unwrap(), py_ty(&f["ty"]));
                }
            }
            "enum" => {
                let _ = writeln!(o, "class {py}(str, enum.Enum):");
                for c in t["cases"].as_array().unwrap() {
                    let c = c.as_str().unwrap();
                    let _ = writeln!(o, "    {} = {c:?}", ident(c).to_uppercase());
                }
            }
            "variant" => {
                let _ = writeln!(
                    o,
                    "class {py}:\n    \"\"\"Variant; construct with {py}.<case>(...), inspect `.tag` and `.value`.\"\"\"\n\n    __slots__ = (\"tag\", \"value\")\n\n    def __init__(self, tag: str, value: Any = None):\n        self.tag = tag\n        self.value = value\n\n    def __eq__(self, other: object) -> bool:\n        return isinstance(other, {py}) and (self.tag, self.value) == (other.tag, other.value)\n\n    def __repr__(self) -> str:\n        return \"{py}.%s(%r)\" % (self.tag.replace(\"-\", \"_\"), self.value) if self.value is not None else \"{py}.%s()\" % self.tag.replace(\"-\", \"_\")\n"
                );
                for c in t["cases"].as_array().unwrap() {
                    let name = c["name"].as_str().unwrap();
                    if c["ty"].is_null() {
                        let _ = writeln!(o, "    @classmethod\n    def {}(cls) -> \"{py}\":\n        return cls({name:?})\n", ident(name));
                    } else {
                        let _ = writeln!(
                            o,
                            "    @classmethod\n    def {}(cls, value: {}) -> \"{py}\":\n        return cls({name:?}, value)\n",
                            ident(name),
                            py_ty(&c["ty"])
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
        let _ = writeln!(o, "\n_TYPES[{py:?}] = {py}\n\n");
    }

    for (ty, docs, methods) in &types {
        let cls = format!("{}Actor", pascal(ty));
        let _ = writeln!(o, "class {cls}(Actor):");
        docstring(&mut o, "    ", docs.as_deref());
        let sigs: serde_json::Map<String, J> = methods.iter().map(|(n, _, s)| (n.clone(), s.clone())).collect();
        let _ = writeln!(o, "    _type = {ty:?}\n    _methods = json.loads({:?})\n", J::Object(sigs).to_string());
        for (name, docs, sig) in methods {
            let params = sig["params"].as_array().unwrap();
            let mut decl = String::from("self");
            let mut pass = vec![];
            let mut seen_optional = false;
            // Trailing option<..> parameters default to None.
            let trailing_opt = params.iter().rev().take_while(|p| p["ty"]["kind"] == "option").count();
            for (i, p) in params.iter().enumerate() {
                let py = p["py"].as_str().unwrap();
                if i >= params.len() - trailing_opt {
                    seen_optional = true;
                    let _ = write!(decl, ", {py}: {} = None", py_ty(&p["ty"]));
                } else {
                    let _ = write!(decl, ", {py}: {}", py_ty(&p["ty"]));
                }
                pass.push(format!("{py:?}: {py}"));
            }
            let _ = seen_optional;
            let _ = writeln!(o, "    def {}({decl}) -> {}:", ident(name), ret_ty(&sig["result"]));
            let mut d = docs.clone().unwrap_or_default();
            if sig["result"]["kind"] == "result" && !sig["result"]["err"].is_null() {
                if !d.is_empty() {
                    d.push_str("\n\n");
                }
                let _ = write!(d, "Raises MethodError whose `.error` is a {}.", py_ty(&sig["result"]["err"]).trim_matches('"'));
            }
            docstring(&mut o, "        ", Some(&d));
            let _ = writeln!(o, "        return self._call({name:?}, {{{}}})\n", pass.join(", "));
        }
        o.push('\n');
    }

    let app_cls = format!("{}App", pascal(&manifest.app));
    let _ = writeln!(
        o,
        "class {app_cls}(App):\n    \"\"\"Typed client for app {app:?}. Any node URL works; calls are routed to the actor's owner.\n\n    Example: {app_cls}().{first}(\"alice\")\n    \"\"\"\n\n    _name = {app:?}\n\n    def __init__(self, base_url: str = {url:?}, timeout: float = 30.0, retries: int = 3,\n                 transport: Optional[Transport] = None):\n        super().__init__(None, base_url, timeout, retries, transport)\n",
        app = manifest.app,
        url = default_url,
        first = manifest.types.first().map(|t| ident(&t.name)).unwrap_or_default()
    );
    for (ty, _, _) in &types {
        let cls = format!("{}Actor", pascal(ty));
        let _ = writeln!(o, "    def {}(self, key: str) -> {cls}:\n        return {cls}(self, key)\n", ident(ty));
    }
    let _ = writeln!(o, "\nClient = {app_cls}");
    Ok(o)
}
