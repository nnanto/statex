use super::*;
use queue::Guest as _;
use sink::Guest as _;
use statex_guest::testing::{call, spawned, try_call};
use std::{cell::RefCell, rc::Rc};
use worker::Guest as _;

const T: u64 = 1_000_000;

#[test]
fn dispatch_caps_encoded_json_not_only_opaque_bytes() {
    setup();
    let mut c = config();
    c.max_batch_size = 100;
    c.max_concurrency = 4;
    c.batch_timeout_ms = 0;
    configure(c);
    let messages = (0..16)
        .map(|i| Input {
            id: format!("{i}-{}", "\"".repeat(900)),
            body: vec![255; MAX_BODY],
            delay_ms: 0,
        })
        .collect();
    let accepted = try_call("queue", "q", || send_at(messages, T)).unwrap();
    assert_eq!(accepted, vec![true; 16]);
    tick(T);
    let dispatched = spawned("queue", "q");
    assert_eq!(dispatched.len(), 2);
    let mut count = 0;
    let mut encoded = Vec::new();
    for request in &dispatched {
        assert!(request.args_json.len() <= statex_guest::spawn::MAX_ARGS_BYTES);
        let args: serde_json::Value = serde_json::from_str(&request.args_json).unwrap();
        count += args["messages"].as_array().unwrap().len();
        encoded.push(args);
    }
    assert_eq!(count, 16);
    let next = encoded[1]["messages"][0].to_string().len();
    assert!(dispatched[0].args_json.len() + next + 1 > statex_guest::spawn::MAX_ARGS_BYTES);
    assert_eq!(info().pending, 0);
    assert_eq!(info().in_flight, 16);
}

fn setup() {
    statex_guest::testing::reset();
    statex_guest::testing::set_app("queue");
    statex_guest::testing::set_migrations_dir(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
    );
}

fn config() -> Config {
    Config {
        max_batch_size: 2,
        batch_timeout_ms: 10,
        max_retries: 1,
        retry_delay_ms: 5,
        lease_ms: 100,
        max_concurrency: 1,
        consumer_key: Some("sample".into()),
        dlq_key: None,
    }
}

fn configure(c: Config) {
    try_call("queue", "q", || App::configure(c)).unwrap();
}

fn send(id: &str, delay: u64) -> bool {
    try_call("queue", "q", || {
        send_at(
            vec![Input {
                id: id.into(),
                body: vec![0, 255, 42],
                delay_ms: delay,
            }],
            T,
        )
    })
    .unwrap()[0]
}

fn tick(at: u64) {
    try_call("queue", "q", || alarm_at(at)).unwrap();
}

fn token(index: usize) -> String {
    let requests = spawned("queue", "q");
    let args: serde_json::Value = serde_json::from_str(&requests[index].args_json).unwrap();
    args["token"].as_str().unwrap().to_owned()
}

fn settle(token: String, ids: &[(&str, bool)], at: u64) -> Result<(), String> {
    try_call("queue", "q", || {
        settle_at(
            token,
            ids.iter()
                .map(|(id, ack)| Outcome {
                    id: (*id).into(),
                    ack: *ack,
                })
                .collect(),
            at,
        )
    })
}

fn info() -> Status {
    call("queue", "q", App::info)
}

#[test]
fn delay_batch_timeout_and_persistent_receipts() {
    setup();
    configure(config());
    assert!(send("a", 20));
    assert!(!send("a", 0));
    tick(T + 19);
    assert!(spawned("queue", "q").is_empty());
    tick(T + 20);
    assert!(spawned("queue", "q").is_empty());
    assert_eq!(statex_guest::testing::alarm("queue", "q"), Some(T + 30));
    tick(T + 30);
    assert_eq!(info().in_flight, 1);
    let request = &spawned("queue", "q")[0];
    assert!(!request.id.is_empty());
    assert_eq!(
        (
            &*request.app,
            &*request.actor_type,
            &*request.key,
            &*request.method
        ),
        ("queue", "worker", "sample", "process")
    );
    let args: serde_json::Value = serde_json::from_str(&request.args_json).unwrap();
    assert_eq!(args["messages"][0]["attempt"], 1);
    assert_eq!(
        STANDARD
            .decode(args["messages"][0]["body"].as_str().unwrap())
            .unwrap(),
        vec![0, 255, 42]
    );
    settle(token(0), &[("a", true)], T + 31).unwrap();
    statex_guest::testing::reactivate("queue", "q");
    assert!(!send("a", 0));
    assert_eq!(
        (info().pending, info().in_flight, info().receipts),
        (0, 0, 1)
    );
}

#[test]
fn full_batches_concurrency_mixed_outcomes_and_retry_delay() {
    setup();
    configure(config());
    for id in ["a", "b", "c", "d"] {
        send(id, 0);
    }
    tick(T);
    assert_eq!(
        (info().pending, info().in_flight, info().active_batches),
        (2, 2, 1)
    );
    assert_eq!(statex_guest::testing::alarm("queue", "q"), Some(T + 100));
    settle(token(0), &[("a", true), ("b", false)], T + 1).unwrap();
    tick(T + 1);
    assert_eq!(spawned("queue", "q").len(), 2);
    settle(token(1), &[("c", true), ("d", true)], T + 2).unwrap();
    tick(T + 5);
    assert_eq!(spawned("queue", "q").len(), 2);
    tick(T + 16);
    let requests = spawned("queue", "q");
    let args: serde_json::Value = serde_json::from_str(&requests[2].args_json).unwrap();
    assert_eq!(args["messages"][0]["id"], "b");
    assert_eq!(args["messages"][0]["attempt"], 2);
    settle(token(2), &[("b", false)], T + 17).unwrap();
    assert_eq!(info().pending + info().in_flight, 0);
}

#[test]
fn trapped_worker_expiry_and_stale_settlement_cannot_touch_new_lease() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    configure(c);
    send("a", 0);
    tick(T);
    let old = token(0);
    // A trap has no settlement. The same lease can be replayed safely.
    assert!(
        std::panic::catch_unwind(|| call("worker", "sample", || panic!("consumer trap"))).is_err()
    );
    assert_eq!(settle(old.clone(), &[("a", true)], T + 100), Ok(()));
    assert_eq!(info().in_flight, 1);
    tick(T + 100);
    assert_eq!(info().pending, 1);
    tick(T + 105);
    assert_ne!(token(1), old);
    assert_eq!(settle(old, &[("a", true)], T + 106), Ok(()));
    assert_eq!(info().in_flight, 1);
    tick(T + 205);
    assert_eq!(info().pending + info().in_flight, 0);
    assert!(!send("a", 0));
}

#[test]
fn settlement_rejects_invalid_active_outcomes_and_accepts_completed_replays() {
    setup();
    configure(config());
    send("a", 0);
    send("b", 0);
    tick(T);
    let t = token(0);
    for outcomes in [
        vec![("a", true)],
        vec![("a", true), ("a", false)],
        vec![("a", true), ("x", true)],
    ] {
        assert!(settle(t.clone(), &outcomes, T + 1).is_err());
        assert_eq!(info().in_flight, 2);
    }
    assert_eq!(
        settle("unknown-token".into(), &[("unknown-message", false)], T + 1),
        Ok(())
    );
    assert_eq!(info().in_flight, 2);
    settle(t.clone(), &[("a", true), ("b", true)], T + 1).unwrap();
    assert_eq!(settle(t, &[("a", true), ("b", true)], T + 2), Ok(()));
    assert_eq!(info().pending + info().in_flight, 0);
}

#[test]
fn dlq_transfer_is_transactional_and_destination_deduplicates() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    c.max_retries = 0;
    c.dlq_key = Some("dead".into());
    configure(c);
    send("a", 0);
    tick(T);
    let t = token(0);
    let result: Result<(), String> = try_call("queue", "q", || {
        settle_at(
            t.clone(),
            vec![Outcome {
                id: "a".into(),
                ack: false,
            }],
            T + 1,
        )?;
        Err("simulate failed commit".into())
    });
    assert!(result.is_err());
    assert_eq!(info().in_flight, 1);
    assert_eq!(spawned("queue", "q").len(), 1);
    settle(t, &[("a", false)], T + 1).unwrap();
    let requests = spawned("queue", "q");
    assert_eq!(requests.len(), 2);
    assert_eq!(
        (
            &*requests[1].actor_type,
            &*requests[1].key,
            &*requests[1].method
        ),
        ("queue", "dead", "send")
    );
    let args: serde_json::Value = serde_json::from_str(&requests[1].args_json).unwrap();
    let id = args["id"].as_str().unwrap().to_owned();
    let body = STANDARD.decode(args["body"].as_str().unwrap()).unwrap();
    assert_eq!(
        try_call("queue", "dead", || App::send(id.clone(), body.clone(), 0)),
        Ok(true)
    );
    assert_eq!(
        try_call("queue", "dead", || App::send(id, body, 0)),
        Ok(false)
    );
    assert_eq!(info().pending + info().in_flight, 0);
    assert!(!send("a", 0));
}

#[test]
fn retention_sweeps_while_paused_or_without_consumer() {
    for consumer in [Some("sample".into()), None] {
        setup();
        let mut c = config();
        let pause = consumer.is_some();
        c.consumer_key = consumer;
        configure(c);
        if pause {
            call("queue", "q", App::pause);
        }
        send("a", 0);
        assert_eq!(
            statex_guest::testing::alarm("queue", "q"),
            Some(T + RETENTION_MS)
        );
        tick(T + RETENTION_MS);
        assert_eq!(info().pending, 0);
        assert_eq!(info().receipts, 1);
        assert!(spawned("queue", "q").is_empty());
        assert_eq!(statex_guest::testing::alarm("queue", "q"), None);
    }
}

#[test]
fn retention_preempts_even_an_unexpired_lease() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    configure(c);
    send("a", RETENTION_MS - 10);
    tick(T + RETENTION_MS - 10);
    let t = token(0);
    assert_eq!(info().in_flight, 1);
    assert_eq!(
        statex_guest::testing::alarm("queue", "q"),
        Some(T + RETENTION_MS)
    );
    tick(T + RETENTION_MS);
    assert_eq!(info().active_batches, 0);
    assert_eq!(settle(t, &[("a", false)], T + RETENTION_MS + 1), Ok(()));
    assert!(!send("a", 0));
}

#[test]
fn multiple_batches_dispatch_up_to_configured_concurrency() {
    setup();
    let mut c = config();
    c.max_concurrency = 2;
    configure(c);
    for id in ["a", "b", "c", "d", "e", "f"] {
        send(id, 0);
    }
    tick(T);
    assert_eq!(
        (info().active_batches, info().in_flight, info().pending),
        (2, 4, 2)
    );
    assert_ne!(token(0), token(1));
    settle(token(0), &[("a", true), ("b", true)], T + 1).unwrap();
    tick(T + 1);
    assert_eq!(
        (info().active_batches, info().in_flight, info().pending),
        (2, 4, 0)
    );
}

#[test]
fn alarm_failure_rolls_back_leases_and_spawn_requests() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    configure(c);
    send("a", 0);
    let result: Result<(), String> = try_call("queue", "q", || {
        alarm_at(T)?;
        Err("commit failed".into())
    });
    assert!(result.is_err());
    assert!(spawned("queue", "q").is_empty());
    assert_eq!((info().pending, info().active_batches), (1, 0));
    tick(T);
    assert_eq!(token(0), "e1-1");
}

fn fill_outbox() {
    call("queue", "q", || {
        let existing = spawned("queue", "q").len();
        for _ in existing..256 {
            statex_guest::spawn::send(
                "queue",
                "worker",
                "sample",
                "process",
                r#"{"queue-key":"q","token":"unused","messages":[]}"#,
            )
            .unwrap();
        }
    });
}

fn drain_outbox() {
    call("queue", "q", || {
        sql::execute("DELETE FROM _statex_outbox", &[]).unwrap();
    });
}

#[test]
fn saturated_outbox_rearms_alarm_without_leases_or_attempts() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    configure(c);
    send("a", 0);
    fill_outbox();
    for n in 0..9 {
        let at = T + n * OUTBOX_BACKOFF_MS;
        tick(at);
        assert_eq!(
            (info().pending, info().in_flight, info().active_batches),
            (1, 0, 0)
        );
        assert_eq!(
            statex_guest::testing::alarm("queue", "q"),
            Some(at + OUTBOX_BACKOFF_MS)
        );
    }
    assert_eq!(spawned("queue", "q").len(), 256);
    assert_eq!(
        call("queue", "q", || count(
            "SELECT attempts FROM messages WHERE id='a'"
        )),
        0
    );
    drain_outbox();
    tick(T + 9 * OUTBOX_BACKOFF_MS);
    assert_eq!((info().pending, info().in_flight), (0, 1));
    assert_eq!(token(0), "e1-1");
    let args: serde_json::Value =
        serde_json::from_str(&spawned("queue", "q")[0].args_json).unwrap();
    assert_eq!(args["messages"][0]["attempt"], 1);
}

#[test]
fn saturated_dlq_transfer_remains_pending_without_extra_attempt_even_when_paused() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    c.max_retries = 0;
    c.dlq_key = Some("dead".into());
    configure(c);
    send("a", 0);
    tick(T);
    let lease = token(0);
    fill_outbox();
    call("queue", "q", App::pause);
    settle(lease, &[("a", false)], T + 1).unwrap();
    assert_eq!(
        (info().pending, info().in_flight, info().active_batches),
        (1, 0, 0)
    );
    assert_eq!(
        statex_guest::testing::alarm("queue", "q"),
        Some(T + 1 + OUTBOX_BACKOFF_MS)
    );
    tick(T + 1 + OUTBOX_BACKOFF_MS);
    assert_eq!(
        call("queue", "q", || count(
            "SELECT attempts FROM messages WHERE id='a'"
        )),
        1
    );
    drain_outbox();
    tick(T + 1 + 2 * OUTBOX_BACKOFF_MS);
    assert_eq!(info().pending, 0);
    let requests = spawned("queue", "q");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        (
            &*requests[0].actor_type,
            &*requests[0].key,
            &*requests[0].method
        ),
        ("queue", "dead", "send")
    );
    assert_eq!(statex_guest::testing::alarm("queue", "q"), None);
}

#[test]
fn saturated_outbox_does_not_prevent_retention_sweep() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    configure(c);
    send("a", 0);
    fill_outbox();
    tick(T + RETENTION_MS - 1);
    assert_eq!(
        statex_guest::testing::alarm("queue", "q"),
        Some(T + RETENTION_MS)
    );
    tick(T + RETENTION_MS);
    assert_eq!(info().pending, 0);
    assert_eq!(info().receipts, 1);
    assert_eq!(statex_guest::testing::alarm("queue", "q"), None);
}

#[test]
fn saturated_dlq_maintenance_is_bounded_and_rearms_remaining_expired_batches() {
    setup();
    let mut c = config();
    c.max_batch_size = 100;
    c.max_concurrency = 2;
    c.max_retries = 0;
    c.dlq_key = Some("dead".into());
    configure(c);
    for n in 0..200 {
        send(&format!("{n:03}"), 0);
    }
    tick(T);
    assert_eq!(info().in_flight, 200);
    fill_outbox();
    tick(T + 100);
    assert_eq!(
        (info().pending, info().in_flight, info().active_batches),
        (100, 100, 1)
    );
    assert_eq!(statex_guest::testing::alarm("queue", "q"), Some(T + 100));
    tick(T + 100);
    assert_eq!(
        (info().pending, info().in_flight, info().active_batches),
        (200, 0, 0)
    );
    assert_eq!(
        statex_guest::testing::alarm("queue", "q"),
        Some(T + 100 + OUTBOX_BACKOFF_MS)
    );
    assert_eq!(spawned("queue", "q").len(), 256);
}

#[test]
fn pause_blocks_new_batches_but_allows_active_settlement_and_resume() {
    setup();
    let mut c = config();
    c.max_batch_size = 1;
    configure(c);
    send("a", 0);
    tick(T);
    call("queue", "q", App::pause);
    send("b", 0);
    settle(token(0), &[("a", true)], T + 1).unwrap();
    tick(T + 1);
    assert_eq!(spawned("queue", "q").len(), 1);
    call("queue", "q", App::resume);
    tick(T + 2);
    assert_eq!(spawned("queue", "q").len(), 2);
}

#[test]
fn bounds_and_transaction_rollback() {
    setup();
    let mut c = config();
    c.max_concurrency = 0;
    assert!(try_call("queue", "q", || App::configure(c)).is_err());
    let mut c = config();
    c.dlq_key = Some("q".into());
    assert!(try_call("queue", "q", || App::configure(c)).is_err());
    configure(config());
    let input = || Input {
        id: "a".into(),
        body: vec![],
        delay_ms: 0,
    };
    assert!(try_call("queue", "q", || send_at(
        (0..101).map(|_| input()).collect(),
        T
    ))
    .is_err());
    assert!(try_call("queue", "q", || send_at(
        vec![Input {
            body: vec![0; MAX_BODY + 1],
            ..input()
        }],
        T
    ))
    .is_err());
    assert!(try_call("queue", "q", || send_at(
        vec![Input {
            delay_ms: RETENTION_MS,
            ..input()
        }],
        T
    ))
    .is_err());
    let result: Result<(), String> = try_call("queue", "q", || {
        send_at(vec![input()], T)?;
        Err("producer rollback".into())
    });
    assert!(result.is_err());
    assert_eq!(info().receipts, 0);
    let result = try_call("queue", "q", || send_at(vec![input(), input()], T)).unwrap();
    assert_eq!(result, vec![true, false]);
}

#[test]
fn worker_uses_synchronous_clients_and_sink_is_idempotent() {
    setup();
    use statex_calls::statex::queue::{queue as client, sink as consumer};
    type Writes = Rc<RefCell<Vec<(String, Vec<u8>)>>>;
    struct SinkStub(Writes);
    impl consumer::Sink for SinkStub {
        fn record(
            &self,
            actor: &str,
            id: &str,
            body: &[u8],
        ) -> Result<Result<bool, String>, statex_guest::CallError> {
            assert_eq!(actor, "q");
            if id == "failed" {
                return Ok(Err("consumer failure".into()));
            }
            self.0.borrow_mut().push((id.into(), body.into()));
            Ok(Ok(true))
        }
    }
    type Settlements = Rc<RefCell<Vec<(String, Vec<client::Outcome>)>>>;
    struct QueueStub(Settlements);
    impl client::Queue for QueueStub {
        fn settle(
            &self,
            actor: &str,
            token: &str,
            outcomes: &[client::Outcome],
        ) -> Result<Result<(), String>, statex_guest::CallError> {
            assert_eq!(actor, "q");
            if token == "expired" {
                return Ok(Err("stale or expired batch token".into()));
            }
            if token == "transport" {
                return Err(statex_guest::CallError::Unavailable(
                    "queue unavailable".into(),
                ));
            }
            self.0.borrow_mut().push((token.into(), outcomes.to_vec()));
            Ok(Ok(()))
        }
    }
    let writes = Writes::default();
    let settlements = Settlements::default();
    consumer::stub(SinkStub(writes.clone()));
    client::stub(QueueStub(settlements.clone()));
    configure(config());
    send("a", 0);
    send("failed", 0);
    tick(T);
    let request = &spawned("queue", "q")[0];
    let args: serde_json::Value = serde_json::from_str(&request.args_json).unwrap();
    let delivery = || {
        args["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| worker::Delivery {
                id: m["id"].as_str().unwrap().into(),
                body: STANDARD.decode(m["body"].as_str().unwrap()).unwrap(),
                attempt: m["attempt"].as_u64().unwrap() as u32,
            })
            .collect()
    };
    // Native mocks forbid nested testing::call. Clients capture calls; apply
    // their stateful effects in separate transactions after the worker returns.
    for _ in 0..2 {
        try_call("worker", "sample", || {
            App::process("q".into(), token(0), delivery())
        })
        .unwrap();
    }
    assert!(spawned("worker", "sample").is_empty());
    for (id, body) in writes.borrow().iter() {
        try_call("sink", "q", || App::record(id.clone(), body.clone())).unwrap();
    }
    assert_eq!(call("sink", "q", App::entries).len(), 1);
    let settlements = settlements.borrow();
    assert_eq!(
        settlements[0]
            .1
            .iter()
            .map(|o| (&*o.id, o.ack))
            .collect::<Vec<_>>(),
        vec![("a", true), ("failed", false)]
    );
    settle(
        settlements[0].0.clone(),
        &[("a", true), ("failed", false)],
        T + 1,
    )
    .unwrap();
    assert_eq!(
        settle(
            settlements[1].0.clone(),
            &[("a", true), ("failed", false)],
            T + 2
        ),
        Ok(())
    );
    assert_eq!((info().pending, info().in_flight), (1, 0));
    assert_eq!(
        try_call("worker", "sample", || App::process(
            "q".into(),
            "expired".into(),
            vec![]
        )),
        Ok(())
    );
    assert!(try_call("worker", "sample", || App::process(
        "q".into(),
        "transport".into(),
        vec![]
    ))
    .is_err());
    consumer::clear_stub();
    client::clear_stub();
}
