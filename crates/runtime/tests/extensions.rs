use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::json;
use statex_runtime::{ActorIdentity, ActorType, Manifest, Method, Runtime, Ty};
use wasmtime::component::Val;

// A minimal component exposing the return value of a custom host function.
const COMPONENT: &str = r#"
(component
  (type $host-type (instance
    (export "next" (func (result u32)))
  ))
  (import "example:extension/sequence@0.1.0" (instance $host (type $host-type)))
  (alias export $host "next" (func $next))
  (core func $lower (canon lower (func $next)))
  (core module $module
    (import "host" "next" (func $next (result i32)))
    (func (export "next") (result i32) call $next)
  )
  (core instance $host-core (export "next" (func $lower)))
  (core instance $core (instantiate $module (with "host" (instance $host-core))))
  (func $run (result u32) (canon lift (core func $core "next")))
  (instance $actor (export "next" (func $run)))
  (export "example:extension/counter@0.1.0" (instance $actor))
)
"#;

fn manifest() -> Manifest {
    Manifest {
        format: 1,
        app: "extension".into(),
        sha256: String::new(),
        types: vec![ActorType {
            name: "counter".into(),
            export: "example:extension/counter@0.1.0".into(),
            docs: None,
            methods: vec![Method {
                name: "next".into(),
                params: vec![],
                result: Some(Ty::U32),
                docs: None,
            }],
            alarm: None,
        }],
        migrations: BTreeMap::new(),
        http: Default::default(),
        limits: Default::default(),
        calls: vec![],
    }
}

fn identity(key: &str) -> ActorIdentity {
    ActorIdentity {
        app: "extension".into(),
        actor_type: "counter".into(),
        key: key.into(),
        epoch: 1,
    }
}

#[test]
fn registered_host_capability_runs_with_actor_local_state() {
    let rt = Runtime::builder()
        .host_interface("example:extension/sequence@0.1.0", |linker| {
            linker
                .instance("example:extension/sequence@0.1.0")?
                .func_new("next", |mut store, _, _, results| {
                    let value = store.data_mut().extensions.get_mut::<u32>().unwrap();
                    *value += 1;
                    results[0] = Val::U32(*value);
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap()
        .initialize_host(|host| {
            host.extensions.insert(40u32);
            Ok(())
        })
        .build()
        .unwrap();
    let bytes = wat::parse_str(COMPONENT).unwrap();
    assert!(Manifest::build(
        &bytes,
        "extension",
        BTreeMap::new(),
        Default::default(),
        Default::default()
    )
    .is_err());
    let built = rt
        .build_manifest(
            &bytes,
            "extension",
            BTreeMap::new(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(built.types[0].name, "counter");
    let manifest = manifest();
    let code = rt.load(COMPONENT.as_bytes(), manifest.clone()).unwrap();
    let instantiate = |key: &str| {
        code.instantiate(
            identity(key),
            statex_runtime::database::sqlite_handle(Arc::new(Mutex::new(
                rusqlite::Connection::open_in_memory().unwrap(),
            ))),
        )
        .unwrap()
    };
    let mut a = instantiate("a");
    let mut b = instantiate("b");
    assert_eq!(
        a.call("counter", "next", &json!({})).unwrap().value,
        json!(41)
    );
    assert_eq!(
        a.call("counter", "next", &json!({})).unwrap().value,
        json!(42)
    );
    assert_eq!(
        b.call("counter", "next", &json!({})).unwrap().value,
        json!(41)
    );
    assert!(Runtime::new()
        .unwrap()
        .load(COMPONENT.as_bytes(), manifest)
        .is_err());
}

#[test]
fn reserved_and_duplicate_interfaces_are_rejected() {
    assert!(Runtime::builder()
        .host_interface("statex:host/sql@0.1.0", |_| Ok(()))
        .is_err());
    assert!(Runtime::builder()
        .host_interface("wasi:io/streams@0.2.0", |_| Ok(()))
        .is_err());
    assert!(Runtime::builder().host_interface("", |_| Ok(())).is_err());
    let builder = Runtime::builder()
        .host_interface("example:extension/sequence@0.1.0", |_| Ok(()))
        .unwrap();
    assert!(builder
        .host_interface("example:extension/sequence@0.1.0", |_| Ok(()))
        .is_err());
}

#[test]
fn registration_and_initializer_failures_are_not_swallowed() {
    let error = Runtime::builder()
        .host_interface("example:extension/sequence@0.1.0", |_| {
            anyhow::bail!("registration failed")
        })
        .unwrap()
        .build()
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("registration failed"));

    let rt = Runtime::builder()
        .host_interface("example:extension/sequence@0.1.0", |linker| {
            linker
                .instance("example:extension/sequence@0.1.0")?
                .func_new("next", |_, _, _, results| {
                    results[0] = Val::U32(1);
                    Ok(())
                })?;
            Ok(())
        })
        .unwrap()
        .initialize_host(|_| anyhow::bail!("initialization failed"))
        .build()
        .unwrap();
    let code = rt.load(COMPONENT.as_bytes(), manifest()).unwrap();
    let error = code
        .instantiate(
            identity("failure"),
            statex_runtime::database::sqlite_handle(Arc::new(Mutex::new(
                rusqlite::Connection::open_in_memory().unwrap(),
            ))),
        )
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("initialization failed"));
}

use statex_runtime::invocation::{Caller, ExecutionExtension, HookError, InvocationContext, InvocationOperation};
use statex_runtime::{CallError, CallOutput};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

struct Hook {
    id: u8,
    events: Arc<Mutex<Vec<u8>>>,
    mode: Arc<AtomicU8>,
}

impl ExecutionExtension for Hook {
    fn before_guest(&self, context: &InvocationContext) -> Result<(), HookError> {
        self.events.lock().unwrap().push(self.id);
        context.data.lock().unwrap().insert(self.id);
        match self.mode.load(Ordering::SeqCst) {
            1 => Err(HookError::Denied("denied".into())),
            2 => panic!("hook panic"),
            3 => { std::thread::sleep(Duration::from_millis(20)); Ok(()) }
            _ => Ok(()),
        }
    }
    fn after_guest(&self, _: &InvocationContext, _: &Result<CallOutput, CallError>) -> Result<(), HookError> {
        self.events.lock().unwrap().push(self.id + 10);
        if self.mode.load(Ordering::SeqCst) == 4 { panic!("observer panic"); }
        Err(HookError::Internal("observer failed".into()))
    }
}

#[test]
fn execution_hooks_veto_order_observe_and_reuse_context() {
    let mode = Arc::new(AtomicU8::new(0));
    let events = Arc::new(Mutex::new(vec![]));
    let mut builder = Runtime::builder();
    for id in [1, 2] {
        builder = builder.execution_extension(Arc::new(Hook { id, events: events.clone(), mode: mode.clone() }));
    }
    let rt = builder.host_interface("example:extension/sequence@0.1.0", |linker| {
        linker.instance("example:extension/sequence@0.1.0")?.func_new("next", |mut store, _, _, results| {
            let context = store.data().invocation_context().expect("installed");
            assert_eq!(context.data.lock().unwrap().get::<u8>(), Some(&2));
            let request = context.request.request_id.clone();
            assert_ne!(store.data().extensions.get::<String>(), Some(&request), "stale context");
            store.data_mut().extensions.insert(request);
            let value = store.data_mut().extensions.get_mut::<u32>().unwrap();
            *value += 1;
            results[0] = Val::U32(*value);
            Ok(())
        })?;
        Ok(())
    }).unwrap().initialize_host(|host| {
        assert!(host.invocation_context().is_none());
        host.extensions.insert(0u32);
        Ok(())
    }).build().unwrap();
    let code = rt.load(COMPONENT.as_bytes(), manifest()).unwrap();
    let mut actor = code.instantiate(identity("hooks"), statex_runtime::database::sqlite_handle(
        Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()))
    )).unwrap();
    assert_eq!(actor.call("counter", "next", &json!({})).unwrap().value, 1);
    assert_eq!(*events.lock().unwrap(), vec![1, 2, 11, 12]);
    events.lock().unwrap().clear();
    mode.store(1, Ordering::SeqCst);
    assert!(matches!(actor.call("counter", "next", &json!({})), Err(CallError::Rejected(HookError::Denied(_)))));
    assert_eq!(*events.lock().unwrap(), vec![1]);
    mode.store(2, Ordering::SeqCst);
    assert!(matches!(actor.call("counter", "next", &json!({})), Err(CallError::Rejected(HookError::Internal(_)))));
    mode.store(3, Ordering::SeqCst);
    let context = InvocationContext::new(
        statex_runtime::ActorRef { app: "extension".into(), actor_type: "counter".into(), key: "hooks".into() },
        InvocationOperation::Call { method: "next".into(), args: json!({}) }, Caller::Embedded, Duration::from_millis(10),
    ).unwrap();
    assert!(matches!(actor.call_with_context("counter", "next", &json!({}), &[], &context), Err(CallError::Rejected(HookError::Timeout))));
    mode.store(4, Ordering::SeqCst);
    assert_eq!(actor.call("counter", "next", &json!({})).unwrap().value, 2);
    assert!(matches!(actor.call("counter", "missing", &json!({})), Err(CallError::NotFound(_))));
    assert!(matches!(actor.call("counter", "next", &json!([1])), Err(CallError::BadArgs(_))));
    mode.store(0, Ordering::SeqCst);
    assert_eq!(actor.call("counter", "next", &json!({})).unwrap().value, 3);
}
