use std::sync::atomic::{AtomicU64, Ordering};
use statex_guest::{alarm, context, sql};

wit_bindgen::generate!({
    path: "wit",
    world: "app",
});

struct App;
static COUNT: AtomicU64 = AtomicU64::new(0);

impl exports::example::stateless::worker::Guest for App {
    fn increment() -> u64 {
        COUNT.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn epoch() -> u64 {
        context::epoch()
    }

    fn sql() -> Result<u64, String> {
        sql::execute("CREATE TABLE forbidden (value INTEGER)", &[]).map_err(|e| e.to_string())
    }

    fn schedule() -> Result<(), String> {
        alarm::set(1).map_err(|e| e.to_string())
    }

    fn spawn() -> Result<String, String> {
        statex_guest::spawn::send("stateless", "worker", "same", "increment", "null").map_err(|e| e.to_string())
    }

    fn fail() {
        panic!("planned trap");
    }
}

export!(App);
