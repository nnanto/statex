//! A queue implemented entirely in ordinary actor SQL, alarms and durable spawn.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use sha2::{Digest, Sha256};
use statex_guest::{alarm, context, params, sql};
use std::collections::HashSet;

mod statex_calls;

wit_bindgen::generate!({
    path: "wit",
    world: "app",
    additional_derives: [PartialEq],
    with: {
        "statex:host/actors@0.1.0": statex_guest::actors,
        "statex:queue/queue": crate::statex_calls::statex::queue::queue,
        "statex:queue/sink": crate::statex_calls::statex::queue::sink,
    },
});

use exports::example::queue::{queue, sink, worker};
use queue::{Config, Input, Outcome, Status};

struct App;

const RETENTION_MS: u64 = 4 * 24 * 60 * 60 * 1000;
const MAX_BATCH: usize = 100;
const MAX_BODY: usize = 64 * 1024;
const MAX_BATCH_BYTES: usize = 1024 * 1024;
const OUTBOX_BACKOFF_MS: u64 = 1000;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn configuration() -> Config {
    let (size, timeout, retries, delay, lease, concurrency) =
        sql::query_row::<(u32, u64, u32, u64, u64, u32)>(
            "SELECT max_batch_size, batch_timeout_ms, max_retries, retry_delay_ms,
                    lease_ms, max_concurrency FROM configuration WHERE id=0",
            &[],
        )
        .unwrap()
        .unwrap();
    let (consumer_key, dlq_key) = sql::query_row(
        "SELECT consumer_key, dlq_key FROM configuration WHERE id=0",
        &[],
    )
    .unwrap()
    .unwrap();
    Config {
        max_batch_size: size,
        batch_timeout_ms: timeout,
        max_retries: retries,
        retry_delay_ms: delay,
        lease_ms: lease,
        max_concurrency: concurrency,
        consumer_key,
        dlq_key,
    }
}

fn paused() -> bool {
    sql::query_scalar("SELECT paused FROM configuration WHERE id=0", &[])
        .unwrap()
        .unwrap()
}

fn count(statement: &str) -> u64 {
    sql::query_scalar(statement, &[]).unwrap().unwrap_or(0)
}

fn validate_config(c: &Config) -> Result<(), String> {
    if !(1..=MAX_BATCH as u32).contains(&c.max_batch_size)
        || !(1..=32).contains(&c.max_concurrency)
        || !(1..=RETENTION_MS).contains(&c.lease_ms)
        || c.batch_timeout_ms >= RETENTION_MS
        || c.retry_delay_ms >= RETENTION_MS
        || c.max_retries > 100
        || c.consumer_key
            .as_ref()
            .is_some_and(|k| k.is_empty() || k.len() > 256)
        || c.dlq_key
            .as_ref()
            .is_some_and(|k| k.is_empty() || k.len() > 256 || *k == context::key())
    {
        return Err("invalid configuration bounds or self-referential DLQ".into());
    }
    Ok(())
}

fn send_at(messages: Vec<Input>, at: u64) -> Result<Vec<bool>, String> {
    if messages.len() > MAX_BATCH
        || messages.iter().map(|m| m.body.len()).sum::<usize>() > MAX_BATCH_BYTES
        || messages.iter().any(|m| {
            m.id.is_empty()
                || m.id.len() > 1024
                || m.body.len() > MAX_BODY
                || m.delay_ms >= RETENTION_MS
        })
    {
        return Err(
            "batch exceeds 100 messages/1 MiB, or invalid id/body/delay (64 KiB, <4 days)".into(),
        );
    }
    let mut accepted = Vec::with_capacity(messages.len());
    for m in messages {
        let inserted = sql::execute(
            "INSERT OR IGNORE INTO receipts(id) VALUES(?1)",
            params![&m.id],
        )
        .unwrap()
            != 0;
        if inserted {
            sql::execute(
                "INSERT INTO messages(id,body,created_at,available_at,expires_at)
                 VALUES(?1,?2,?3,?4,?5)",
                params![m.id, m.body, at, at + m.delay_ms, at + RETENTION_MS],
            )
            .unwrap();
        }
        accepted.push(inserted);
    }
    schedule(&configuration(), at);
    Ok(accepted)
}

fn remove_empty_batches() {
    sql::execute(
        "DELETE FROM batches WHERE NOT EXISTS(SELECT 1 FROM messages WHERE messages.token=batches.token)",
        &[],
    )
    .unwrap();
}

fn retry_or_dead_letter(id: &str, c: &Config, at: u64) -> Result<(), String> {
    let (body, attempts) = sql::query_row::<(Vec<u8>, u32)>(
        "SELECT body, attempts FROM messages WHERE id=?1",
        params![id],
    )
    .unwrap()
    .unwrap();
    if attempts > c.max_retries {
        if let Some(dlq) = &c.dlq_key {
            // Stable across outbox replay, with no ambiguity between source keys.
            let source = serde_json::to_vec(&(context::key(), id)).unwrap();
            let dlq_id = format!("dlq:{:x}", Sha256::digest(source));
            let args = serde_json::json!({
                "id": dlq_id, "body": STANDARD.encode(body), "delay-ms": 0
            });
            if let Err(error) =
                statex_guest::spawn::send(&context::app(), "queue", dlq, "send", &args.to_string())
            {
                statex_guest::log::warn(&format!("deferring DLQ transfer for {id}: {error}"));
                sql::execute(
                    "UPDATE messages SET token=NULL,available_at=?2 WHERE id=?1",
                    params![id, at + OUTBOX_BACKOFF_MS],
                )
                .unwrap();
                return Ok(());
            }
        }
        sql::execute("DELETE FROM messages WHERE id=?1", params![id]).unwrap();
    } else {
        sql::execute(
            "UPDATE messages SET token=NULL,available_at=?2 WHERE id=?1",
            params![id, at + c.retry_delay_ms],
        )
        .unwrap();
    }
    Ok(())
}

fn maintain(c: &Config, at: u64) -> Result<(), String> {
    sql::execute("DELETE FROM messages WHERE expires_at<=?1", params![at]).unwrap();
    let expired = sql::query_as::<(String,)>(
        "SELECT messages.id FROM messages LEFT JOIN batches USING(token)
         WHERE deadline<=?1 OR (messages.token IS NULL AND attempts>?2 AND available_at<=?1)
         ORDER BY available_at,messages.id LIMIT ?3",
        params![at, c.max_retries, MAX_BATCH],
    )
    .unwrap();
    for (id,) in expired {
        retry_or_dead_letter(&id, c, at)?;
    }
    remove_empty_batches();
    Ok(())
}

fn ready(c: &Config, at: u64) -> (u64, Option<u64>) {
    sql::query_row(
        "SELECT count(*),min(available_at) FROM messages
         WHERE token IS NULL AND available_at<=?1 AND attempts<=?2",
        params![at, c.max_retries],
    )
    .unwrap()
    .unwrap()
}

fn dispatch(c: &Config, at: u64) -> Result<(), String> {
    let Some(consumer) = &c.consumer_key else {
        return Ok(());
    };
    if paused() {
        return Ok(());
    }
    while count("SELECT count(*) FROM batches") < u64::from(c.max_concurrency) {
        let (n, first) = ready(c, at);
        if n == 0 || (n < u64::from(c.max_batch_size) && at < first.unwrap() + c.batch_timeout_ms) {
            break;
        }
        let messages = sql::query_as::<(String, Vec<u8>, u32)>(
            "SELECT id,body,attempts FROM messages
             WHERE token IS NULL AND available_at<=?1 AND attempts<=?3
             ORDER BY available_at,id LIMIT ?2",
            params![at, c.max_batch_size, c.max_retries],
        )
        .unwrap();
        let sequence: u64 =
            sql::query_scalar("SELECT sequence+1 FROM configuration WHERE id=0", &[])
                .unwrap()
                .unwrap();
        let token = format!("e{}-{sequence}", context::epoch());
        let mut deliveries = Vec::new();
        let mut leased_ids = Vec::new();
        let mut bytes = 0;
        let mut json_bytes = serde_json::json!({
            "queue-key": context::key(), "token": token, "messages": [],
        })
        .to_string()
        .len();
        for (id, body, attempts) in messages {
            if bytes + body.len() > MAX_BATCH_BYTES {
                break;
            }
            let delivery = serde_json::json!({
                "id": id, "body": STANDARD.encode(&body), "attempt": attempts + 1,
            });
            let encoded_len = delivery.to_string().len() + usize::from(!deliveries.is_empty());
            // Spawn's cap applies to encoded JSON, including base64 and ids,
            // not just to the original opaque payload.
            if json_bytes + encoded_len > statex_guest::spawn::MAX_ARGS_BYTES {
                break;
            }
            bytes += body.len();
            json_bytes += encoded_len;
            leased_ids.push(id);
            deliveries.push(delivery);
        }
        let args = serde_json::json!({
            "queue-key": context::key(), "token": token, "messages": deliveries,
        });
        let queued = if leased_ids.is_empty() {
            Err(statex_guest::Error(
                "delivery envelope exceeds spawn argument limit".into(),
            ))
        } else {
            statex_guest::spawn::send(
                &context::app(),
                "worker",
                consumer,
                "process",
                &args.to_string(),
            )
        };
        if let Err(error) = queued {
            statex_guest::log::warn(&format!("deferring queue dispatch: {error}"));
            // Commit maintenance and a future alarm even when the outbox is
            // full; returning Err could exhaust the host's alarm retry budget.
            sql::execute(
                "UPDATE messages SET available_at=?2
                 WHERE token IS NULL AND available_at<=?1 AND attempts<=?3",
                params![at, at + OUTBOX_BACKOFF_MS, c.max_retries],
            )
            .unwrap();
            break;
        }
        sql::execute(
            "UPDATE configuration SET sequence=?1 WHERE id=0",
            params![sequence],
        )
        .unwrap();
        sql::execute(
            "INSERT INTO batches(token,deadline) VALUES(?1,?2)",
            params![&token, at + c.lease_ms],
        )
        .unwrap();
        for id in leased_ids {
            sql::execute(
                "UPDATE messages SET token=?2,attempts=attempts+1 WHERE id=?1",
                params![id, &token],
            )
            .unwrap();
        }
    }
    Ok(())
}

fn schedule(c: &Config, at: u64) {
    let mut next: Option<u64> = sql::query_scalar("SELECT min(expires_at) FROM messages", &[])
        .unwrap()
        .flatten();
    let mut earlier = |candidate: Option<u64>| {
        if let Some(t) = candidate {
            next = Some(next.map_or(t, |n| n.min(t)));
        }
    };
    earlier(
        sql::query_scalar("SELECT min(deadline) FROM batches", &[])
            .unwrap()
            .flatten(),
    );
    earlier(
        sql::query_scalar(
            "SELECT min(available_at) FROM messages WHERE token IS NULL AND attempts>?1",
            params![c.max_retries],
        )
        .unwrap()
        .flatten(),
    );
    if !paused()
        && c.consumer_key.is_some()
        && count("SELECT count(*) FROM batches") < u64::from(c.max_concurrency)
    {
        let (n, first) = ready(c, at);
        earlier(if n >= u64::from(c.max_batch_size) {
            Some(at)
        } else {
            first.map(|t| t + c.batch_timeout_ms)
        });
        // Future arrivals may fill a batch before its current timeout.
        earlier(
            sql::query_scalar(
                "SELECT min(available_at) FROM messages
                 WHERE token IS NULL AND available_at>?1 AND attempts<=?2",
                params![at, c.max_retries],
            )
            .unwrap()
            .flatten(),
        );
    }
    match next {
        Some(t) => alarm::set(t.max(at)).unwrap(),
        None => alarm::clear(),
    }
}

fn alarm_at(at: u64) -> Result<(), String> {
    let c = configuration();
    maintain(&c, at)?;
    dispatch(&c, at)?;
    schedule(&c, at);
    Ok(())
}

fn settle_at(token: String, outcomes: Vec<Outcome>, at: u64) -> Result<(), String> {
    let deadline: Option<u64> = sql::query_scalar(
        "SELECT deadline FROM batches WHERE token=?1",
        params![&token],
    )
    .unwrap();
    if !deadline.is_some_and(|deadline| deadline > at) {
        return Ok(());
    }
    let ids: HashSet<String> =
        sql::query_as::<(String,)>("SELECT id FROM messages WHERE token=?1", params![&token])
            .unwrap()
            .into_iter()
            .map(|(id,)| id)
            .collect();
    let outcome_ids: HashSet<&str> = outcomes.iter().map(|o| o.id.as_str()).collect();
    if outcomes.len() != ids.len()
        || outcome_ids.len() != ids.len()
        || outcome_ids.iter().any(|id| !ids.contains(*id))
    {
        return Err("settlement must contain exactly one outcome per leased message".into());
    }
    let c = configuration();
    for o in outcomes {
        let expires: u64 = sql::query_scalar(
            "SELECT expires_at FROM messages WHERE id=?1",
            params![&o.id],
        )
        .unwrap()
        .unwrap();
        if o.ack || expires <= at {
            sql::execute("DELETE FROM messages WHERE id=?1", params![o.id]).unwrap();
        } else {
            retry_or_dead_letter(&o.id, &c, at)?;
        }
    }
    remove_empty_batches();
    schedule(&c, at);
    Ok(())
}

impl queue::Guest for App {
    fn configure(c: Config) -> Result<(), String> {
        validate_config(&c)?;
        sql::execute(
            "UPDATE configuration SET max_batch_size=?1,batch_timeout_ms=?2,max_retries=?3,
             retry_delay_ms=?4,lease_ms=?5,max_concurrency=?6,consumer_key=?7,dlq_key=?8 WHERE id=0",
            params![c.max_batch_size, c.batch_timeout_ms, c.max_retries, c.retry_delay_ms,
                    c.lease_ms, c.max_concurrency, c.consumer_key.as_deref(), c.dlq_key.as_deref()],
        ).unwrap();
        schedule(&c, now());
        Ok(())
    }

    fn send(id: String, body: Vec<u8>, delay_ms: u64) -> Result<bool, String> {
        Ok(send_at(vec![Input { id, body, delay_ms }], now())?[0])
    }

    fn send_batch(messages: Vec<Input>) -> Result<Vec<bool>, String> {
        send_at(messages, now())
    }

    fn settle(token: String, outcomes: Vec<Outcome>) -> Result<(), String> {
        settle_at(token, outcomes, now())
    }

    fn pause() {
        sql::execute("UPDATE configuration SET paused=1 WHERE id=0", &[]).unwrap();
        schedule(&configuration(), now());
    }

    fn resume() {
        sql::execute("UPDATE configuration SET paused=0 WHERE id=0", &[]).unwrap();
        schedule(&configuration(), now());
    }

    fn info() -> Status {
        Status {
            configuration: configuration(),
            paused: paused(),
            pending: count("SELECT count(*) FROM messages WHERE token IS NULL"),
            in_flight: count("SELECT count(*) FROM messages WHERE token IS NOT NULL"),
            active_batches: count("SELECT count(*) FROM batches"),
            receipts: count("SELECT count(*) FROM receipts"),
            alarm_at: alarm::get(),
        }
    }

    fn alarm(_: u32) -> Result<(), String> {
        alarm_at(now())
    }
}

impl worker::Guest for App {
    fn process(
        queue_key: String,
        token: String,
        messages: Vec<worker::Delivery>,
    ) -> Result<(), String> {
        use statex_calls::statex::queue::{queue as client, sink as consumer};
        let mut outcomes = Vec::with_capacity(messages.len());
        for m in messages {
            let ack = match consumer::record(&queue_key, &m.id, &m.body) {
                Ok(Ok(_)) => true,
                Ok(Err(error)) => {
                    statex_guest::log::warn(&format!("queue message {} failed: {error}", m.id));
                    false
                }
                Err(error) => {
                    statex_guest::log::warn(&format!(
                        "queue message {} call failed: {error}",
                        m.id
                    ));
                    false
                }
            };
            outcomes.push(client::Outcome { id: m.id, ack });
        }
        match client::settle(&queue_key, &token, &outcomes).map_err(|e| e.to_string())? {
            Ok(()) => Ok(()),
            Err(error) if error == "stale or expired batch token" => {
                statex_guest::log::warn(&format!(
                    "discarding obsolete queue dispatch {queue_key}/{token}"
                ));
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

impl sink::Guest for App {
    fn record(id: String, body: Vec<u8>) -> Result<bool, String> {
        Ok(sql::execute(
            "INSERT OR IGNORE INTO entries(id,body) VALUES(?1,?2)",
            params![id, body],
        )
        .unwrap()
            != 0)
    }

    fn entries() -> Vec<sink::Entry> {
        sql::query_as::<(String, Vec<u8>)>("SELECT id,body FROM entries ORDER BY id", &[])
            .unwrap()
            .into_iter()
            .map(|(id, body)| sink::Entry { id, body })
            .collect()
    }
}

export!(App);

#[cfg(test)]
mod tests;
