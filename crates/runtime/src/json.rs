//! JSON <-> component value mapping.
//!
//! | WIT                  | JSON                                   |
//! |----------------------|----------------------------------------|
//! | bool / ints / floats | boolean / number (64-bit ints also accept strings) |
//! | char, string         | string                                 |
//! | list<u8>             | base64 string                          |
//! | list<T>, tuple       | array                                  |
//! | option<T>            | `null` or the value                    |
//! | record               | object keyed by field name             |
//! | enum                 | case name string                       |
//! | flags                | array of set flag names                |
//! | variant              | `{"tag": "case", "value": ...}`        |
//! | result<T, E>         | `{"ok": ...}` or `{"err": ...}`        |

use base64::Engine;
use serde_json::{json, Map, Value as J};
use wasmtime::component::Val;

use crate::manifest::{Method, Ty};

type R<T> = std::result::Result<T, String>;

fn int<T: TryFrom<i128>>(v: &J, path: &str) -> R<T> {
    let n: i128 = match v {
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                i as i128
            } else if let Some(u) = n.as_u64() {
                u as i128
            } else {
                return Err(format!("{path}: expected an integer, got {n}"));
            }
        }
        J::String(s) => s.parse().map_err(|_| format!("{path}: expected an integer, got {s:?}"))?,
        _ => return Err(format!("{path}: expected an integer, got {v}")),
    };
    T::try_from(n).map_err(|_| format!("{path}: integer {n} out of range"))
}

fn float(v: &J, path: &str) -> R<f64> {
    v.as_f64().ok_or_else(|| format!("{path}: expected a number, got {v}"))
}

pub fn json_to_val(ty: &Ty, v: &J, path: &str) -> R<Val> {
    Ok(match ty {
        Ty::Bool => Val::Bool(v.as_bool().ok_or_else(|| format!("{path}: expected a boolean"))?),
        Ty::U8 => Val::U8(int(v, path)?),
        Ty::U16 => Val::U16(int(v, path)?),
        Ty::U32 => Val::U32(int(v, path)?),
        Ty::U64 => Val::U64(int(v, path)?),
        Ty::S8 => Val::S8(int(v, path)?),
        Ty::S16 => Val::S16(int(v, path)?),
        Ty::S32 => Val::S32(int(v, path)?),
        Ty::S64 => Val::S64(int(v, path)?),
        Ty::F32 => Val::Float32(float(v, path)? as f32),
        Ty::F64 => Val::Float64(float(v, path)?),
        Ty::Char => {
            let s = v.as_str().ok_or_else(|| format!("{path}: expected a 1-char string"))?;
            let mut it = s.chars();
            match (it.next(), it.next()) {
                (Some(c), None) => Val::Char(c),
                _ => return Err(format!("{path}: expected exactly one character")),
            }
        }
        Ty::String => {
            Val::String(v.as_str().ok_or_else(|| format!("{path}: expected a string, got {v}"))?.to_string())
        }
        Ty::List { element } if **element == Ty::U8 && v.is_string() => {
            let b = base64::engine::general_purpose::STANDARD
                .decode(v.as_str().unwrap())
                .map_err(|e| format!("{path}: invalid base64: {e}"))?;
            Val::List(b.into_iter().map(Val::U8).collect())
        }
        Ty::List { element } => {
            let a = v.as_array().ok_or_else(|| format!("{path}: expected an array, got {v}"))?;
            Val::List(
                a.iter()
                    .enumerate()
                    .map(|(i, x)| json_to_val(element, x, &format!("{path}[{i}]")))
                    .collect::<R<_>>()?,
            )
        }
        Ty::Tuple { items } => {
            let a = v.as_array().ok_or_else(|| format!("{path}: expected an array (tuple)"))?;
            if a.len() != items.len() {
                return Err(format!("{path}: expected a tuple of {} items", items.len()));
            }
            Val::Tuple(
                items
                    .iter()
                    .zip(a)
                    .enumerate()
                    .map(|(i, (t, x))| json_to_val(t, x, &format!("{path}[{i}]")))
                    .collect::<R<_>>()?,
            )
        }
        Ty::Option { inner } => match v {
            J::Null => Val::Option(None),
            x => Val::Option(Some(Box::new(json_to_val(inner, x, path)?))),
        },
        Ty::Record { fields, .. } => {
            let o = v.as_object().ok_or_else(|| format!("{path}: expected an object, got {v}"))?;
            let mut out = Vec::with_capacity(fields.len());
            for f in fields {
                let x = lookup(o, &f.name);
                let fp = format!("{path}.{}", f.name);
                let val = match (x, &f.ty) {
                    (Some(x), t) => json_to_val(t, x, &fp)?,
                    (None, Ty::Option { .. }) => Val::Option(None),
                    (None, _) => return Err(format!("{fp}: missing field")),
                };
                out.push((f.name.clone(), val));
            }
            Val::Record(out)
        }
        Ty::Enum { cases, .. } => {
            let s = v.as_str().ok_or_else(|| format!("{path}: expected one of {cases:?}"))?;
            let c = find_case(cases.iter().map(String::as_str), s)
                .ok_or_else(|| format!("{path}: {s:?} is not one of {cases:?}"))?;
            Val::Enum(c.to_string())
        }
        Ty::Flags { flags, .. } => {
            let a = v.as_array().ok_or_else(|| format!("{path}: expected an array of flags"))?;
            let mut out = Vec::new();
            for x in a {
                let s = x.as_str().unwrap_or_default();
                let f = find_case(flags.iter().map(String::as_str), s)
                    .ok_or_else(|| format!("{path}: unknown flag {x}"))?;
                out.push(f.to_string());
            }
            Val::Flags(out)
        }
        Ty::Variant { cases, .. } => {
            let (tag, payload) = match v {
                J::String(s) => (s.as_str(), None),
                J::Object(o) => (
                    o.get("tag").and_then(J::as_str).ok_or_else(|| format!("{path}: variant needs a \"tag\""))?,
                    o.get("value"),
                ),
                _ => return Err(format!("{path}: expected {{\"tag\": ..., \"value\": ...}}")),
            };
            let case = cases
                .iter()
                .find(|c| same_name(&c.name, tag))
                .ok_or_else(|| format!("{path}: unknown variant case {tag:?}"))?;
            let p = match (&case.ty, payload) {
                (Some(t), Some(x)) => Some(Box::new(json_to_val(t, x, &format!("{path}.value"))?)),
                (Some(_), None) => return Err(format!("{path}: case {tag} requires a \"value\"")),
                (None, _) => None,
            };
            Val::Variant(case.name.clone(), p)
        }
        Ty::Result { ok, err } => {
            let o = v.as_object().ok_or_else(|| format!("{path}: expected {{\"ok\": ...}} or {{\"err\": ...}}"))?;
            let conv = |t: &Option<Box<Ty>>, x: Option<&J>, p: &str| -> R<Option<Box<Val>>> {
                match t {
                    Some(t) => Ok(Some(Box::new(json_to_val(t, x.unwrap_or(&J::Null), p)?))),
                    None => Ok(None),
                }
            };
            if o.contains_key("ok") {
                Val::Result(Ok(conv(ok, o.get("ok"), &format!("{path}.ok"))?))
            } else if o.contains_key("err") {
                Val::Result(Err(conv(err, o.get("err"), &format!("{path}.err"))?))
            } else {
                return Err(format!("{path}: expected {{\"ok\": ...}} or {{\"err\": ...}}"));
            }
        }
    })
}

/// WIT names are kebab-case; also accept snake_case and camelCase from clients.
fn normalize(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c == '_' {
            out.push('-');
        } else if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('-');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn same_name(wit: &str, given: &str) -> bool {
    wit == given || wit == normalize(given)
}

fn find_case<'a>(mut it: impl Iterator<Item = &'a str>, s: &str) -> Option<&'a str> {
    it.find(|c| same_name(c, s))
}

fn lookup<'a>(o: &'a Map<String, J>, wit_name: &str) -> Option<&'a J> {
    o.get(wit_name).or_else(|| o.iter().find(|(k, _)| same_name(wit_name, k)).map(|(_, v)| v))
}

/// Converts call arguments (object by parameter name, or positional array).
pub fn args_to_vals(m: &Method, args: &J) -> R<Vec<Val>> {
    match args {
        J::Null if m.params.is_empty() => Ok(vec![]),
        J::Array(a) => {
            if a.len() != m.params.len() {
                return Err(format!("{} expects {} arguments, got {}", m.name, m.params.len(), a.len()));
            }
            m.params.iter().zip(a).map(|(p, x)| json_to_val(&p.ty, x, &p.name)).collect()
        }
        J::Object(o) => {
            for k in o.keys() {
                if !m.params.iter().any(|p| same_name(&p.name, k)) {
                    return Err(format!("{}: unknown argument {k:?}", m.name));
                }
            }
            m.params
                .iter()
                .map(|p| match (lookup(o, &p.name), &p.ty) {
                    (Some(x), t) => json_to_val(t, x, &p.name),
                    (None, Ty::Option { .. }) => Ok(Val::Option(None)),
                    (None, _) => Err(format!("missing argument {:?}", p.name)),
                })
                .collect()
        }
        J::Null => Err(format!("{} expects arguments {:?}", m.name, m.params.iter().map(|p| &p.name).collect::<Vec<_>>())),
        _ => Err("arguments must be a JSON object or array".into()),
    }
}

pub fn val_to_json(ty: &Ty, v: &Val) -> J {
    match (ty, v) {
        (_, Val::Bool(b)) => J::Bool(*b),
        (_, Val::U8(n)) => json!(n),
        (_, Val::U16(n)) => json!(n),
        (_, Val::U32(n)) => json!(n),
        (_, Val::U64(n)) => json!(n),
        (_, Val::S8(n)) => json!(n),
        (_, Val::S16(n)) => json!(n),
        (_, Val::S32(n)) => json!(n),
        (_, Val::S64(n)) => json!(n),
        (_, Val::Float32(f)) => json!(f),
        (_, Val::Float64(f)) => json!(f),
        (_, Val::Char(c)) => J::String(c.to_string()),
        (_, Val::String(s)) => J::String(s.clone()),
        (Ty::List { element }, Val::List(items)) if **element == Ty::U8 => {
            let bytes: Vec<u8> = items.iter().map(|x| if let Val::U8(b) = x { *b } else { 0 }).collect();
            J::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        (Ty::List { element }, Val::List(items)) => {
            J::Array(items.iter().map(|x| val_to_json(element, x)).collect())
        }
        (Ty::Tuple { items: tys }, Val::Tuple(items)) => {
            J::Array(tys.iter().zip(items).map(|(t, x)| val_to_json(t, x)).collect())
        }
        (Ty::Option { inner }, Val::Option(o)) => match o {
            None => J::Null,
            Some(x) => val_to_json(inner, x),
        },
        (Ty::Record { fields, .. }, Val::Record(vals)) => {
            let mut m = Map::new();
            for (f, (name, x)) in fields.iter().zip(vals) {
                m.insert(name.clone(), val_to_json(&f.ty, x));
            }
            J::Object(m)
        }
        (_, Val::Enum(c)) => J::String(c.clone()),
        (_, Val::Flags(f)) => json!(f),
        (Ty::Variant { cases, .. }, Val::Variant(tag, payload)) => {
            let mut m = Map::new();
            m.insert("tag".into(), J::String(tag.clone()));
            if let (Some(p), Some(case)) = (payload, cases.iter().find(|c| &c.name == tag)) {
                if let Some(t) = &case.ty {
                    m.insert("value".into(), val_to_json(t, p));
                }
            }
            J::Object(m)
        }
        (Ty::Result { ok, err }, Val::Result(r)) => {
            let (k, t, p) = match r {
                Ok(p) => ("ok", ok, p),
                Err(p) => ("err", err, p),
            };
            let inner = match (t, p) {
                (Some(t), Some(p)) => val_to_json(t, p),
                _ => J::Null,
            };
            json!({ k: inner })
        }
        (_, other) => J::String(format!("<unsupported value {other:?}>")),
    }
}

/// Default value placeholder used to size the results buffer.
pub fn placeholder() -> Val {
    Val::Bool(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Case, Field, Param};

    fn entry_ty() -> Ty {
        Ty::Record {
            name: "entry".into(),
            fields: vec![
                Field { name: "id".into(), ty: Ty::U64 },
                Field { name: "kind".into(), ty: Ty::Enum { name: "kind".into(), cases: vec!["deposit".into(), "withdrawal".into()] } },
                Field { name: "memo-text".into(), ty: Ty::Option { inner: Box::new(Ty::String) } },
                Field { name: "raw".into(), ty: Ty::List { element: Box::new(Ty::U8) } },
            ],
        }
    }

    #[test]
    fn roundtrip_record() {
        let ty = entry_ty();
        let j = json!({"id": 7, "kind": "withdrawal", "memo_text": "hi", "raw": "AQID"});
        let v = json_to_val(&ty, &j, "x").unwrap();
        let back = val_to_json(&ty, &v);
        assert_eq!(back, json!({"id": 7, "kind": "withdrawal", "memo-text": "hi", "raw": "AQID"}));
        // Optional field may be omitted.
        let v = json_to_val(&ty, &json!({"id": "9", "kind": "deposit", "raw": ""}), "x").unwrap();
        assert_eq!(val_to_json(&ty, &v)["memo-text"], J::Null);
    }

    #[test]
    fn variants_and_results() {
        let err = Ty::Variant {
            name: "tx-error".into(),
            cases: vec![Case { name: "insufficient-funds".into(), ty: Some(Ty::U64) }, Case { name: "invalid-amount".into(), ty: None }],
        };
        let ty = Ty::Result { ok: Some(Box::new(Ty::U64)), err: Some(Box::new(err)) };
        let v = json_to_val(&ty, &json!({"err": {"tag": "insufficient_funds", "value": 5}}), "r").unwrap();
        assert_eq!(val_to_json(&ty, &v), json!({"err": {"tag": "insufficient-funds", "value": 5}}));
        let v = json_to_val(&ty, &json!({"err": "invalid-amount"}), "r").unwrap();
        assert_eq!(val_to_json(&ty, &v), json!({"err": {"tag": "invalid-amount"}}));
        assert!(json_to_val(&Ty::U8, &json!(300), "n").is_err());
    }

    #[test]
    fn args_by_name_or_position() {
        let m = Method {
            name: "deposit".into(),
            params: vec![
                Param { name: "amount".into(), ty: Ty::U64 },
                Param { name: "memo".into(), ty: Ty::Option { inner: Box::new(Ty::String) } },
            ],
            result: None,
            docs: None,
        };
        assert_eq!(args_to_vals(&m, &json!({"amount": 5})).unwrap().len(), 2);
        assert_eq!(args_to_vals(&m, &json!([5, "x"])).unwrap().len(), 2);
        assert!(args_to_vals(&m, &json!({"amount": 5, "bogus": 1})).is_err());
        assert!(args_to_vals(&m, &json!({})).is_err());
    }
}
