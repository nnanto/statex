use statex_guest::{params, sql};

wit_bindgen::generate!({ path: "wit", world: "app", additional_derives: [PartialEq] });

use exports::local::demo_rustdemo::counter as counter;

struct App;

// Each call runs inside its own transaction against this actor's database.
impl counter::Guest for App {
    fn increment(by: i64) -> i64 {
        sql::execute("UPDATE counter SET value = value + ?1 WHERE id = 0", params![by]).unwrap();
        Self::get()
    }

    fn get() -> i64 {
        sql::query_scalar("SELECT value FROM counter WHERE id = 0", &[]).unwrap().unwrap_or(0)
    }
}

export!(App);

#[cfg(test)]
mod tests {
    use super::*;
    use counter::Guest as _;
    use statex_guest::testing::call;

    #[test]
    fn increments_per_key() {
        assert_eq!(call("counter", "alice", || App::increment(2)), 2);
        assert_eq!(call("counter", "alice", || App::increment(3)), 5);
        assert_eq!(call("counter", "bob", App::get), 0);
    }
}
