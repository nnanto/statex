use statex_guest::{context, spawn, sql};

wit_bindgen::generate!({ path: "wit", world: "app" });

struct App;

fn value() -> i64 {
    sql::query_scalar("SELECT value FROM tally WHERE id=0", &[]).unwrap().unwrap()
}

fn increment() -> i64 {
    sql::execute("UPDATE tally SET value=value+1 WHERE id=0", &[]).unwrap();
    value()
}

impl exports::test::dispatch::source::Guest for App {
    fn submit(target: String, fail: bool) -> Result<String, String> {
        increment();
        let id = spawn::send(&context::app(), "target", &target, "increment", "[]").map_err(|e| e.to_string())?;
        if fail { return Err("rollback source and spawn".into()); }
        Ok(id)
    }
    fn value() -> i64 { value() }
}

impl exports::test::dispatch::target::Guest for App {
    fn increment() -> i64 { increment() }
    fn value() -> i64 { value() }
}

impl exports::test::dispatch::worker::Guest for App {
    fn fresh() -> u32 {
        static VALUE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        VALUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    fn storage() -> Result<(), String> {
        sql::execute("CREATE TABLE forbidden(v)", &[]).map(|_| ()).map_err(|e| e.to_string())
    }
    fn spawn_call() -> Result<String, String> {
        spawn::send(&context::app(), "target", "forbidden", "increment", "[]").map_err(|e| e.to_string())
    }
}

export!(App);
