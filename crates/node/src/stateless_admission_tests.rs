use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn overload_does_not_allocate_another_stateless_instance() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/async");
    assert!(std::process::Command::new("cargo")
        .args(["build", "--quiet", "--release", "--target", "wasm32-wasip2"])
        .current_dir(&fixture).status().unwrap().success());
    let wasm = std::fs::read(fixture.join("target/wasm32-wasip2/release/async_fixture.wasm")).unwrap();
    let mut manifest = statex_runtime::Manifest::build(
        &wasm, "async-test", Default::default(), Default::default(), Default::default(),
    ).unwrap();
    manifest.types.iter_mut().find(|ty| ty.name == "worker").unwrap().stateless = true;
    let dir = tempfile::tempdir().unwrap();
    let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
    let mut cfg = NodeConfig::new("a", store, dir.path().join("node"));
    cfg.max_stateless_calls = 1;
    let handle = crate::start(cfg).await.unwrap();
    let code = handle.node.runtime.load(&wasm, manifest).unwrap();
    let invocation = Invocation {
        app: "async-test".into(), ty: "worker".into(), key: "worker".into(),
        op: InvOp::Call { method: "fresh".into(), args: serde_json::json!([]) }, chain: vec![],
    };
    let permit = handle.node.stateless_slots.clone().try_acquire_owned().unwrap();
    let refused = handle.node.run_stateless(&invocation, code.clone()).await;
    assert_eq!(refused.status, 503);
    assert!(refused.body["error"]["message"].as_str().unwrap().contains("limit"));
    drop(permit);
    let accepted = handle.node.run_stateless(&invocation, code).await;
    assert_eq!(accepted.status, 200);
    assert_eq!(accepted.body["result"], 0);
    handle.shutdown().await;
}
