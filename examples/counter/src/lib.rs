use statex_guest::{log, params, sql};

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
