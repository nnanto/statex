use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{json, Value};
use statex_runtime::{ActorIdentity, CallError, Manifest, Runtime};

#[test]
fn stateless_instances_are_fresh_and_cannot_use_stateful_capabilities() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/stateless");
    assert!(std::process::Command::new("cargo")
        .args(["build", "--release", "--target", "wasm32-wasip2"])
        .current_dir(&root).status().unwrap().success());
    let wasm = std::fs::read(root.join("target/wasm32-wasip2/release/stateless.wasm")).unwrap();
    let mut manifest = Manifest::build(
        &wasm, "stateless", BTreeMap::new(), Default::default(), Default::default(),
    ).unwrap();
    manifest.types[0].stateless = true;
    let code = Runtime::shared().unwrap().load(&wasm, manifest).unwrap();
    let identity = || ActorIdentity {
        app: "stateless".into(), actor_type: "worker".into(), key: "same".into(), epoch: 0,
    };
    // Keeping an instance explicitly proves the fixture has mutable memory.
    let mut instance = code.instantiate_stateless(identity(), None).unwrap();
    assert_eq!(instance.call("worker", "increment", &Value::Null).unwrap().value, json!(1));
    assert_eq!(instance.call("worker", "increment", &Value::Null).unwrap().value, json!(2));
    for _ in 0..3 {
        assert_eq!(code.call_stateless(identity(), "increment", &Value::Null, &[], None).unwrap().value, json!(1));
    }
    assert!(matches!(code.call_stateless(identity(), "missing", &Value::Null, &[], None), Err(CallError::NotFound(_))));
    assert!(matches!(code.call_stateless(identity(), "increment", &json!([1]), &[], None), Err(CallError::BadArgs(_))));
    for method in ["sql", "schedule", "spawn"] {
        let out = code.call_stateless(identity(), method, &Value::Null, &[], None).unwrap();
        assert!(out.is_err);
        assert!(out.value.as_str().unwrap().contains("stateless"), "{}", out.value);
    }
    assert!(matches!(code.call_stateless(identity(), "fail", &Value::Null, &[], None), Err(CallError::Trap(_))));
    assert_eq!(code.call_stateless(identity(), "increment", &Value::Null, &[], None).unwrap().value, json!(1));
}
