use statex_guest::{alarm, log, params, sql};

wit_bindgen::generate!({
    path: "wit",
    world: "app",
    additional_derives: [PartialEq],
});

use exports::example::counter::account::{self, Entry, Kind, TxError};
use exports::example::counter::counter;

struct App;

impl counter::Guest for App {
    fn increment(by: i64) -> i64 {
        sql::execute("UPDATE counter SET value = value + ?1 WHERE id = 0", params![by]).unwrap();
        Self::get()
    }

    fn get() -> i64 {
        sql::query_scalar("SELECT value FROM counter WHERE id = 0", &[]).unwrap().unwrap_or(0)
    }

    fn reset() {
        sql::execute("UPDATE counter SET value = 0 WHERE id = 0", &[]).unwrap();
    }

    fn enqueue(key: String, by: i64, delay_ms: u64, fail: bool) -> Result<String, String> {
        let id = statex_guest::actors::spawn_after(
            std::time::Duration::from_millis(delay_ms),
            &statex_guest::context::app(),
            "counter",
            &key,
            "increment",
            &[by.into()],
        )
        .map_err(|error| error.to_string())?;
        if fail {
            return Err("planned outbox rollback".into());
        }
        Ok(id)
    }

    fn job(id: String) -> Result<Option<String>, String> {
        statex_guest::actors::job(&id)
            .map(|job| job.map(|job| job.to_string()))
            .map_err(|error| error.to_string())
    }

    fn schedule(delay_ms: u64, fail_times: u32) {
        sql::execute("UPDATE alarm_demo SET fail_times = ?1 WHERE id = 0", params![fail_times]).unwrap();
        alarm::set_in(std::time::Duration::from_millis(delay_ms)).unwrap();
    }

    fn cancel() {
        alarm::clear();
    }

    fn alarm_at() -> Option<u64> {
        alarm::get()
    }

    fn fired() -> i64 {
        sql::query_scalar("SELECT fired FROM alarm_demo WHERE id = 0", &[]).unwrap().unwrap_or(0)
    }

    fn alarm(retry_count: u32) -> Result<(), String> {
        let fail_times: u32 = sql::query_scalar("SELECT fail_times FROM alarm_demo WHERE id = 0", &[]).unwrap().unwrap_or(0);
        if retry_count < fail_times {
            return Err(format!("planned failure {} of {fail_times}", retry_count + 1));
        }
        sql::execute("UPDATE alarm_demo SET fired = fired + 1 WHERE id = 0", &[]).unwrap();
        log::info(&format!("alarm fired (after {retry_count} failed attempts)"));
        Ok(())
    }
}

fn balance() -> u64 {
    sql::query_scalar::<i64>(
        "SELECT COALESCE(SUM(CASE kind WHEN 'deposit' THEN amount ELSE -amount END), 0) FROM entries",
        &[],
    )
    .unwrap()
    .unwrap_or(0) as u64
}

fn record(kind: &str, amount: u64, memo: Option<String>) {
    sql::execute(
        "INSERT INTO entries(kind, amount, memo) VALUES(?1, ?2, ?3)",
        params![kind, amount as i64, memo],
    )
    .unwrap();
}

impl account::Guest for App {
    fn deposit(amount: u64, memo: Option<String>) -> Result<u64, TxError> {
        if amount == 0 {
            return Err(TxError::InvalidAmount);
        }
        record("deposit", amount, memo);
        Ok(balance())
    }

    fn withdraw(amount: u64) -> Result<u64, TxError> {
        if amount == 0 {
            return Err(TxError::InvalidAmount);
        }
        let bal = balance();
        // Written before validating: returning `Err` rolls the whole call back.
        record("withdrawal", amount, None);
        if amount > bal {
            log::warn(&format!("insufficient funds: balance {bal}, wanted {amount}"));
            return Err(TxError::InsufficientFunds(bal));
        }
        Ok(balance())
    }

    fn balance() -> u64 {
        balance()
    }

    fn history(limit: u32) -> Vec<Entry> {
        sql::query_as::<(i64, String, i64, Option<String>)>(
            "SELECT id, kind, amount, memo FROM entries ORDER BY id DESC LIMIT ?1",
            params![limit],
        )
        .unwrap()
        .into_iter()
        .map(|(id, kind, amount, memo)| Entry {
            id: id as u64,
            kind: if kind == "deposit" { Kind::Deposit } else { Kind::Withdrawal },
            amount: amount as u64,
            memo,
        })
        .collect()
    }
}

export!(App);

#[cfg(test)]
mod tests {
    use super::*;
    use account::Guest as _;
    use counter::Guest as _;
    use statex_guest::testing::{call, try_call};

    #[test]
    fn counter_increments_per_key() {
        assert_eq!(call("counter", "alice", || App::increment(2)), 2);
        assert_eq!(call("counter", "alice", || App::increment(3)), 5);
        assert_eq!(call("counter", "bob", App::get), 0);
        call("counter", "alice", App::reset);
        assert_eq!(call("counter", "alice", App::get), 0);
    }

    #[test]
    fn background_call_is_recorded_without_running_target() {
        let id = try_call("counter", "source", || {
            App::enqueue("target".into(), 7, 30_000, false)
        }).unwrap();
        let record = call("counter", "source", || {
            statex_guest::actors::job(&id).unwrap().unwrap()
        });
        assert_eq!(record["method"], "increment");
        assert_eq!(record["target"]["type"], "counter");
        assert_eq!(record["target"]["key"], "target");
        assert_eq!(record["args"][0], 7);
        assert_eq!(call("counter", "target", App::get), 0);
        assert_eq!(call("counter", "target", || App::job(id.clone())), Ok(None));
    }

    #[test]
    fn background_call_rolls_back_with_source_transaction() {
        let mut id = String::new();
        let result = try_call("counter", "source", || {
            id = App::enqueue("target".into(), 7, 0, false).unwrap();
            Err::<(), _>("planned rollback")
        });
        assert_eq!(result, Err("planned rollback"));
        assert_eq!(call("counter", "source", || App::job(id)), Ok(None));
    }

    #[test]
    fn delayed_background_call_waits_until_due() {
        use statex_guest::testing::{drain_jobs, mock_spawn};
        use std::time::Duration;

        let app = call("counter", "source", statex_guest::context::app);
        mock_spawn(&app, "counter", "increment", |_, args| {
            Ok(App::increment(args[0].as_i64().unwrap()).into())
        });
        let id = try_call("counter", "source", || {
            App::enqueue("target".into(), 7, 30_000, false)
        }).unwrap();
        assert_eq!(drain_jobs(Duration::from_millis(29_999)), 0);
        assert_eq!(call("counter", "target", App::get), 0);
        assert_eq!(drain_jobs(Duration::from_millis(1)), 1);
        assert_eq!(call("counter", "target", App::get), 7);
        let record = call("counter", "source", || {
            statex_guest::actors::job(&id).unwrap().unwrap()
        });
        assert_eq!(record["status"], "succeeded");
        assert_eq!(drain_jobs(Duration::ZERO), 0);
    }

    #[test]
    fn alarm_schedules_and_fires() {
        call("counter", "t", || App::schedule(60_000, 1));
        let at = statex_guest::testing::alarm("counter", "t").unwrap();
        assert_eq!(call("counter", "t", App::alarm_at), Some(at));
        assert!(try_call("counter", "t", || App::alarm(0)).is_err());
        assert_eq!(try_call("counter", "t", || App::alarm(1)), Ok(()));
        assert_eq!(call("counter", "t", App::fired), 1);
        call("counter", "t", App::cancel);
        assert_eq!(statex_guest::testing::alarm("counter", "t"), None);
    }

    #[test]
    fn account_rules() {
        assert_eq!(try_call("account", "a", || App::deposit(100, Some("salary".into()))), Ok(100));
        assert_eq!(try_call("account", "a", || App::withdraw(30)), Ok(70));
        assert_eq!(try_call("account", "a", || App::withdraw(500)), Err(TxError::InsufficientFunds(70)));
        assert_eq!(try_call("account", "a", || App::deposit(0, None)), Err(TxError::InvalidAmount));
        let h = call("account", "a", || App::history(10));
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].kind, Kind::Withdrawal);
        assert_eq!(h[1].memo.as_deref(), Some("salary"));
    }
}
