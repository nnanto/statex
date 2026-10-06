use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use statex_runtime::invocation::{
    Caller, ExecutionExtension, HookError, InvocationContext, InvocationOperation,
};
use statex_runtime::{
    ActorIdentity, ActorInstance, ActorRef, ActorType, AppCode, CallError, HostLimits, Limits,
    Manifest, Method, Runtime, Ty,
};

const COMPONENT: &str = r#"
(component
  (core module $m
    (memory $a 8 9)
    (memory $b 8)
    (func (export "run") (result i32) i32.const 7)
    (func (export "spin") (result i32) (loop $forever br $forever) i32.const 0)
    (func (export "work") (result i32) (local $n i32)
      i32.const 1000 local.set $n
      (loop $work local.get $n i32.const 1 i32.sub local.tee $n br_if $work)
      i32.const 1)
    (func (export "grow-a") (result i32) i32.const 1 memory.grow $a)
    (func (export "fail-a") (result i32) i32.const 2 memory.grow $a)
    (func (export "grow-b") (result i32) i32.const 8 memory.grow $b))
  (core instance $i (instantiate $m))
  (func $run (result u32) (canon lift (core func $i "run")))
  (func $spin (result u32) (canon lift (core func $i "spin")))
  (func $work (result u32) (canon lift (core func $i "work")))
  (func $a (result u32) (canon lift (core func $i "grow-a")))
  (func $fail (result u32) (canon lift (core func $i "fail-a")))
  (func $b (result u32) (canon lift (core func $i "grow-b")))
  (instance $actor
    (export "run" (func $run)) (export "spin" (func $spin)) (export "work" (func $work))
    (export "grow-a" (func $a)) (export "fail-a" (func $fail)) (export "grow-b" (func $b)))
  (export "test:limits/counter@0.1.0" (instance $actor)))
"#;

fn manifest(limits: Limits) -> Manifest {
    Manifest {
        format: 1,
        app: "limits".into(),
        sha256: String::new(),
        types: vec![ActorType {
            name: "counter".into(),
            export: "test:limits/counter@0.1.0".into(),
            docs: None,
            methods: ["run", "spin", "work", "grow-a", "fail-a", "grow-b"]
                .into_iter()
                .map(|name| Method {
                    name: name.into(),
                    params: vec![],
                    result: Some(Ty::U32),
                    docs: None,
                })
                .collect(),
            alarm: None,
        }],
        migrations: BTreeMap::new(),
        http: Default::default(),
        limits,
        calls: vec![],
    }
}

fn db() -> statex_runtime::database::DatabaseHandle {
    statex_runtime::database::sqlite_handle(Arc::new(Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    )))
}

fn identity(key: &str) -> ActorIdentity {
    ActorIdentity {
        app: "limits".into(),
        actor_type: "counter".into(),
        key: key.into(),
        epoch: 1,
    }
}

fn instance(code: &Arc<AppCode>, key: &str) -> ActorInstance {
    code.instantiate(identity(key), db()).unwrap()
}

fn context() -> InvocationContext {
    InvocationContext::new(
        ActorRef {
            app: "limits".into(),
            actor_type: "counter".into(),
            key: "a".into(),
        },
        InvocationOperation::Call {
            method: "run".into(),
            args: Value::Null,
        },
        Caller::Embedded,
        Duration::from_secs(5),
    )
    .unwrap()
}

fn trapped(result: Result<statex_runtime::CallOutput, CallError>, message: &str) {
    assert!(
        matches!(&result, Err(CallError::Trap(error)) if error.contains(message)),
        "{result:?}"
    );
}

#[test]
fn validate_untrusted_load_and_build_and_host_caps() {
    let rt = Runtime::new().unwrap();
    let invalid = Limits {
        fuel: Some(0),
        ..Limits::default()
    };
    assert!(rt
        .load(COMPONENT.as_bytes(), manifest(invalid.clone()))
        .is_err());
    assert!(rt
        .build_manifest(
            &wat::parse_str(COMPONENT).unwrap(),
            "limits",
            BTreeMap::new(),
            Default::default(),
            invalid
        )
        .is_err());
    assert!(Runtime::builder()
        .host_limits(HostLimits {
            max_fuel: Some(0),
            ..Default::default()
        })
        .build()
        .is_err());
    assert!(rt
        .for_node(HostLimits {
            max_timeout_ms: Some(u64::MAX),
            ..Default::default()
        })
        .is_err());
    let rt = Runtime::builder()
        .host_limits(HostLimits {
            max_timeout_ms: Some(100),
            max_memory_mb: Some(2),
            max_fuel: Some(12_000),
            max_rps: Some(4),
            max_burst: Some(3),
            max_concurrent: Some(2),
        })
        .build()
        .unwrap();
    let node = rt
        .for_node(HostLimits {
            max_timeout_ms: Some(200),
            max_memory_mb: Some(1),
            max_fuel: Some(13_000),
            max_rps: Some(2),
            max_burst: Some(4),
            max_concurrent: Some(3),
        })
        .unwrap();
    let code = node
        .load(COMPONENT.as_bytes(), manifest(Limits::default()))
        .unwrap();
    assert_eq!(
        code.effective_limits(),
        &Limits {
            timeout_ms: 100,
            memory_mb: 1,
            fuel: Some(12_000),
            rps: Some(2),
            burst: Some(2),
            max_concurrent: Some(2),
        }
    );
}

#[test]
fn fuel_resets_after_traps_and_each_successful_invocation() {
    let rt = Runtime::builder()
        .configure_engine(|config| {
            config.consume_fuel(false);
        })
        .build()
        .unwrap();
    let code = rt
        .load(
            COMPONENT.as_bytes(),
            manifest(Limits {
                fuel: Some(12_000),
                ..Default::default()
            }),
        )
        .unwrap();
    let mut actor = instance(&code, "a");
    for _ in 0..3 {
        assert_eq!(
            actor.call("counter", "work", &Value::Null).unwrap().value,
            json!(1)
        );
    }
    trapped(
        actor.call("counter", "spin", &Value::Null),
        "fuel budget exhausted",
    );
    // Canonical ABI traps poison an instance; the node discards it.
    let mut actor = instance(&code, "recovered");
    assert_eq!(
        actor.call("counter", "work", &Value::Null).unwrap().value,
        json!(1)
    );
    let host_code = Runtime::builder()
        .host_limits(HostLimits {
            max_fuel: Some(12_000),
            ..Default::default()
        })
        .build()
        .unwrap()
        .load(COMPONENT.as_bytes(), manifest(Limits::default()))
        .unwrap();
    trapped(
        instance(&host_code, "host").call("counter", "spin", &Value::Null),
        "fuel budget exhausted",
    );
}

#[test]
fn initialization_is_fuel_and_deadline_bounded_and_gets_separate_budget() {
    let bounded_start = r#"(func $start (local $n i32)
        i32.const 1000 local.set $n
        (loop $work local.get $n i32.const 1 i32.sub local.tee $n br_if $work)) (start $start)"#;
    let finite = COMPONENT.replace("(memory $a", &format!("{bounded_start} (memory $a"));
    let rt = Runtime::new().unwrap();
    let code = rt
        .load(
            finite.as_bytes(),
            manifest(Limits {
                fuel: Some(7_000),
                ..Default::default()
            }),
        )
        .unwrap();
    assert_eq!(
        instance(&code, "a")
            .call("counter", "work", &Value::Null)
            .unwrap()
            .value,
        json!(1)
    );
    let infinite = COMPONENT.replace(
        "(memory $a",
        "(func $start (loop $forever br $forever)) (start $start) (memory $a",
    );
    let code = rt
        .load(
            infinite.as_bytes(),
            manifest(Limits {
                fuel: Some(1000),
                ..Default::default()
            }),
        )
        .unwrap();
    let error = code.instantiate(identity("fuel"), db()).err().unwrap();
    assert!(format!("{error:#}").contains("fuel"), "{error:#}");
    let code = rt
        .load(
            infinite.as_bytes(),
            manifest(Limits {
                timeout_ms: 10,
                ..Default::default()
            }),
        )
        .unwrap();
    let error = code.instantiate(identity("timeout"), db()).err().unwrap();
    assert!(
        format!("{error:#}").contains("timed out") || format!("{error:#}").contains("interrupt"),
        "{error:#}"
    );
}

#[test]
fn aggregate_memories_and_failed_growth_accounting_on_reused_instance() {
    let rt = Runtime::new().unwrap();
    let code = rt
        .load(
            COMPONENT.as_bytes(),
            manifest(Limits {
                memory_mb: 1,
                ..Default::default()
            }),
        )
        .unwrap();
    let mut actor = instance(&code, "a");
    trapped(
        actor.call("counter", "grow-a", &Value::Null),
        "aggregate linear memory limit exceeded",
    );
    let oversized = COMPONENT.replace("(memory $b 8)", "(memory $b 9)");
    let code = rt
        .load(
            oversized.as_bytes(),
            manifest(Limits {
                memory_mb: 1,
                ..Default::default()
            }),
        )
        .unwrap();
    assert!(format!(
        "{:#}",
        code.instantiate(identity("init"), db()).err().unwrap()
    )
    .contains("aggregate linear memory"));
    let code = rt
        .load(
            COMPONENT.as_bytes(),
            manifest(Limits {
                memory_mb: 2,
                ..Default::default()
            }),
        )
        .unwrap();
    let mut actor = instance(&code, "b");
    trapped(
        actor.call("counter", "fail-a", &Value::Null),
        "linear memory growth failed",
    );
    let mut actor = instance(&code, "recovered");
    assert_eq!(
        actor.call("counter", "grow-b", &Value::Null).unwrap().value,
        json!(8)
    );
    assert_eq!(
        actor.call("counter", "grow-a", &Value::Null).unwrap().value,
        json!(8)
    );
    trapped(
        actor.call("counter", "grow-b", &Value::Null),
        "aggregate linear memory limit exceeded",
    );
}

#[test]
fn engine_tuning_cannot_allow_unaccounted_shared_memory() {
    let rt = Runtime::builder()
        .configure_engine(|config| {
            config.shared_memory(true);
        })
        .build()
        .unwrap();
    let shared = COMPONENT.replace("(memory $b 8)", "(memory $b 8 8 shared)");
    if let Ok(code) = rt.load(shared.as_bytes(), manifest(Limits::default())) {
        assert!(code.instantiate(identity("shared"), db()).is_err());
    }
}

#[test]
fn quotas_shared_across_instances_versions_and_clones_but_not_nodes() {
    let rt = Runtime::new().unwrap();
    let limits = Limits {
        rps: Some(1),
        burst: Some(2),
        max_concurrent: Some(1),
        ..Default::default()
    };
    let mut deployed = manifest(limits.clone());
    let mut second_type = deployed.types[0].clone();
    second_type.name = "other".into();
    deployed.types.push(second_type);
    let code = rt.load(COMPONENT.as_bytes(), deployed.clone()).unwrap();
    assert_eq!(code.active_executions(), 0);
    let a = instance(&code, "a");
    let mut b = instance(&code, "b");
    let ctx = context();
    assert!(matches!(
        code.admit_call("missing", "run", &Value::Null, &ctx),
        Err(CallError::NotFound(_))
    ));
    assert!(matches!(
        code.admit_call("counter", "run", &json!([1]), &ctx),
        Err(CallError::BadArgs(_))
    ));
    let permit = code
        .admit_call("counter", "run", &Value::Null, &ctx)
        .unwrap();
    let initialization_error = code
        .instantiate(identity("overloaded-init"), db())
        .err()
        .unwrap();
    assert_eq!(code.active_executions(), 1);
    assert!(matches!(
        initialization_error.downcast_ref::<HookError>(),
        Some(HookError::Overloaded(_))
    ));
    let redeployed = rt.clone().load(COMPONENT.as_bytes(), deployed).unwrap();
    assert_eq!(redeployed.active_executions(), 1);
    assert!(matches!(
        redeployed.admit_call("counter", "run", &Value::Null, &ctx),
        Err(CallError::Rejected(HookError::Overloaded(_)))
    ));
    drop(permit);
    assert_eq!(code.active_executions(), 0);
    assert_eq!(redeployed.active_executions(), 0);
    b.call("other", "run", &Value::Null).unwrap();
    assert!(matches!(
        redeployed.admit_call("counter", "run", &Value::Null, &ctx),
        Err(CallError::Rejected(HookError::RateLimited(_)))
    ));
    let other = rt
        .for_node(HostLimits::default())
        .unwrap()
        .load(COMPONENT.as_bytes(), manifest(limits))
        .unwrap();
    instance(&other, "independent")
        .call("counter", "run", &Value::Null)
        .unwrap();
    // A reservation cannot be moved to another scope or reused for a different operation.
    let permit = other
        .admit_call("counter", "run", &Value::Null, &ctx)
        .unwrap();
    let mut a = a;
    assert!(matches!(
        a.call_admitted("counter", "run", &Value::Null, &[], &ctx, permit),
        Err(CallError::Rejected(HookError::Invalid(_)))
    ));
}

struct ToggleDeny(Arc<AtomicBool>);
impl ExecutionExtension for ToggleDeny {
    fn before_guest(&self, _: &InvocationContext) -> Result<(), HookError> {
        if self.0.load(Ordering::SeqCst) {
            Err(HookError::Denied("policy".into()))
        } else {
            Ok(())
        }
    }
}

#[test]
fn invalid_calls_and_policy_denials_do_not_spend_rate_tokens_but_alarms_do() {
    let deny = Arc::new(AtomicBool::new(true));
    let rt = Runtime::builder()
        .execution_extension(Arc::new(ToggleDeny(deny.clone())))
        .build()
        .unwrap();
    let mut manifest = manifest(Limits {
        rps: Some(1),
        burst: Some(1),
        ..Default::default()
    });
    manifest.types[0].alarm = Some(Method {
        name: "run".into(),
        params: vec![],
        result: Some(Ty::U32),
        docs: None,
    });
    let code = rt.load(COMPONENT.as_bytes(), manifest).unwrap();
    let mut actor = instance(&code, "a");
    assert!(matches!(
        actor.call("counter", "missing", &Value::Null),
        Err(CallError::NotFound(_))
    ));
    assert!(matches!(
        actor.call("counter", "run", &json!([1])),
        Err(CallError::BadArgs(_))
    ));
    assert!(matches!(
        actor.call("counter", "run", &Value::Null),
        Err(CallError::Rejected(HookError::Denied(_)))
    ));
    deny.store(false, Ordering::SeqCst);
    actor.call_alarm("counter", 0).unwrap();
    assert!(matches!(
        actor.call("counter", "run", &Value::Null),
        Err(CallError::Rejected(HookError::RateLimited(_)))
    ));
}

struct Deny;
impl ExecutionExtension for Deny {
    fn before_guest(&self, _: &InvocationContext) -> Result<(), HookError> {
        Err(HookError::Denied("policy".into()))
    }
}

#[test]
fn denial_before_fresh_initialization_and_permit_release_after_init_failure() {
    let infinite = COMPONENT.replace(
        "(memory $a",
        "(func $start (loop $forever br $forever)) (start $start) (memory $a",
    );
    let rt = Runtime::builder()
        .execution_extension(Arc::new(Deny))
        .build()
        .unwrap();
    let code = rt
        .load(
            infinite.as_bytes(),
            manifest(Limits {
                fuel: Some(100),
                rps: Some(1),
                ..Default::default()
            }),
        )
        .unwrap();
    assert!(matches!(
        code.admit_call("counter", "run", &Value::Null, &context()),
        Err(CallError::Rejected(HookError::Denied(_)))
    ));
    let rt = Runtime::new().unwrap();
    let code = rt
        .load(
            infinite.as_bytes(),
            manifest(Limits {
                fuel: Some(100),
                max_concurrent: Some(1),
                ..Default::default()
            }),
        )
        .unwrap();
    let ctx = context();
    for _ in 0..2 {
        assert_eq!(code.active_executions(), 0);
        let permit = code
            .admit_call("counter", "run", &Value::Null, &ctx)
            .unwrap();
        assert_eq!(code.active_executions(), 1);
        assert!(matches!(
            code.instantiate_admitted_with(identity("a"), db(), None, &permit, &ctx),
            Err(CallError::Trap(_))
        ));
        drop(permit);
        assert_eq!(code.active_executions(), 0);
    }
}
