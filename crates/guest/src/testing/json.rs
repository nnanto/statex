use crate::CallError;
use serde_json::{Number, Value};

/// WIT JSON encoding for native test doubles. Production encoding lives in
/// the runtime; generated guests pass typed WIT arguments directly.
pub trait ToJson {
    fn to_json(&self) -> Result<Value, CallError>;
}

pub fn to_json(value: &(impl ToJson + ?Sized)) -> Result<Value, CallError> {
    value.to_json()
}

impl<T: ToJson + ?Sized> ToJson for &T {
    fn to_json(&self) -> Result<Value, CallError> {
        (*self).to_json()
    }
}

impl ToJson for () {
    fn to_json(&self) -> Result<Value, CallError> { Ok(Value::Null) }
}

impl ToJson for str {
    fn to_json(&self) -> Result<Value, CallError> { Ok(Value::String(self.into())) }
}

impl ToJson for String {
    fn to_json(&self) -> Result<Value, CallError> { self.as_str().to_json() }
}

macro_rules! scalar {
    ($($ty:ty),*) => {$(
        impl ToJson for $ty {
            fn to_json(&self) -> Result<Value, CallError> { Ok(Value::from(*self)) }
        }
    )*};
}
scalar!(bool, u8, u16, u32, u64, i8, i16, i32, i64);

impl ToJson for char {
    fn to_json(&self) -> Result<Value, CallError> { self.to_string().to_json() }
}

macro_rules! float {
    ($($ty:ty),*) => {$(
        impl ToJson for $ty {
            fn to_json(&self) -> Result<Value, CallError> {
                Number::from_f64(*self as f64).map(Value::Number)
                    .ok_or_else(|| CallError::Rejected("non-finite float argument".into()))
            }
        }
    )*};
}
float!(f32, f64);

impl<T: ToJson> ToJson for [T] {
    fn to_json(&self) -> Result<Value, CallError> {
        self.iter().map(to_json).collect::<Result<Vec<_>, _>>().map(Value::Array)
    }
}

impl<T: ToJson> ToJson for Vec<T> {
    fn to_json(&self) -> Result<Value, CallError> { self.as_slice().to_json() }
}

impl<T: ToJson> ToJson for Option<T> {
    fn to_json(&self) -> Result<Value, CallError> {
        match self { Some(value) => to_json(value), None => Ok(Value::Null) }
    }
}

impl<T: ToJson, E: ToJson> ToJson for Result<T, E> {
    fn to_json(&self) -> Result<Value, CallError> {
        Ok(match self {
            Ok(value) => serde_json::json!({ "ok": to_json(value)? }),
            Err(value) => serde_json::json!({ "err": to_json(value)? }),
        })
    }
}

macro_rules! tuples {
    ($(($($ty:ident:$index:tt),+)),+ $(,)?) => {$(
        impl<$($ty: ToJson),+> ToJson for ($($ty,)+) {
            fn to_json(&self) -> Result<Value, CallError> {
                Ok(Value::Array(vec![$(to_json(&self.$index)?),+]))
            }
        }
    )+};
}
tuples!(
    (A:0), (A:0,B:1), (A:0,B:1,C:2), (A:0,B:1,C:2,D:3),
    (A:0,B:1,C:2,D:3,E:4), (A:0,B:1,C:2,D:3,E:4,F:5),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6), (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14),
    (A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14,P:15),
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_results_and_nonfinite_options() {
        assert_eq!(to_json(&Ok::<_, String>(Some(("x", vec![1u8, 2])))).unwrap(),
            serde_json::json!({"ok": ["x", [1, 2]]}));
        assert!(to_json(&Some(f64::NAN)).is_err());
        assert!(to_json(&vec![Some(f32::INFINITY)]).is_err());
    }
}
