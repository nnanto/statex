//! Host ceilings and per-app, per-runtime-scope execution admission.
//!
//! Fuel meters WASM instructions, not native SQL/custom callbacks. Memory
//! accounts for aggregate WASM linear memory, not native allocations, JIT or
//! databases; allocated memory remains charged while an instance is reused.
//! Shared linear memories are disabled because Wasmtime does not meter their
//! growth through ResourceLimiter.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use wasmtime::{ResourceLimiter, StoreLimits, StoreLimitsBuilder};

use crate::{invocation::HookError, Limits};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostLimits {
    pub max_timeout_ms: Option<u64>,
    pub max_memory_mb: Option<u64>,
    pub max_fuel: Option<u64>,
    pub max_rps: Option<u32>,
    pub max_burst: Option<u32>,
    pub max_concurrent: Option<u32>,
}

pub(crate) fn validate_timeout(ms: u64) -> Result<()> {
    ensure!(ms > 0, "timeout_ms must be positive");
    ensure!(
        Instant::now()
            .checked_add(Duration::from_millis(ms))
            .is_some(),
        "timeout_ms exceeds supported monotonic duration"
    );
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    ensure!(
        now_ms + u128::from(ms) <= u128::from(u64::MAX),
        "timeout_ms exceeds supported invocation deadline range"
    );
    Ok(())
}

pub(crate) fn memory_bytes(mb: u64) -> Result<usize> {
    ensure!(mb > 0, "memory_mb must be positive");
    mb.checked_mul(1 << 20)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| anyhow::anyhow!("memory_mb exceeds supported byte range"))
}

fn cap<T: Ord + Copy>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

impl HostLimits {
    pub fn validate(&self) -> Result<()> {
        if let Some(ms) = self.max_timeout_ms {
            validate_timeout(ms)?;
        }
        if let Some(mb) = self.max_memory_mb {
            memory_bytes(mb)?;
        }
        ensure!(self.max_fuel != Some(0), "max_fuel must be positive");
        ensure!(self.max_rps != Some(0), "max_rps must be positive");
        ensure!(self.max_burst != Some(0), "max_burst must be positive");
        ensure!(
            self.max_concurrent != Some(0),
            "max_concurrent must be positive"
        );
        Ok(())
    }

    pub(crate) fn intersect(&self, other: &Self) -> Self {
        Self {
            max_timeout_ms: cap(self.max_timeout_ms, other.max_timeout_ms),
            max_memory_mb: cap(self.max_memory_mb, other.max_memory_mb),
            max_fuel: cap(self.max_fuel, other.max_fuel),
            max_rps: cap(self.max_rps, other.max_rps),
            max_burst: cap(self.max_burst, other.max_burst),
            max_concurrent: cap(self.max_concurrent, other.max_concurrent),
        }
    }

    pub(crate) fn effective(&self, app: &Limits) -> Result<Limits> {
        self.validate()?;
        app.validate()?;
        let rps = cap(app.rps, self.max_rps);
        let burst = rps.map(|rps| {
            app.burst
                .unwrap_or(rps)
                .min(self.max_burst.unwrap_or(u32::MAX))
        });
        let limits = Limits {
            timeout_ms: app.timeout_ms.min(self.max_timeout_ms.unwrap_or(u64::MAX)),
            memory_mb: app.memory_mb.min(self.max_memory_mb.unwrap_or(u64::MAX)),
            fuel: cap(app.fuel, self.max_fuel),
            rps,
            burst,
            max_concurrent: cap(app.max_concurrent, self.max_concurrent),
        };
        limits.validate()?;
        Ok(limits)
    }
}

#[derive(Default)]
pub(crate) struct QuotaRegistry(Mutex<HashMap<String, Arc<Quota>>>);

impl QuotaRegistry {
    pub(crate) fn register(&self, app: &str, limits: &Limits) -> Arc<Quota> {
        let mut apps = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let quota = apps
            .entry(app.into())
            .or_insert_with(|| Arc::new(Quota(Mutex::new(Bucket::new(limits, Instant::now())))))
            .clone();
        quota
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .update(limits, Instant::now());
        quota
    }
}

const TOKEN: u128 = 1_000_000_000;

struct Bucket {
    rps: Option<u32>,
    burst: u32,
    tokens: u128,
    updated: Instant,
    running: u64,
    max_concurrent: Option<u32>,
}

impl Bucket {
    fn new(limits: &Limits, now: Instant) -> Self {
        let burst = limits.burst.or(limits.rps).unwrap_or(0);
        Self {
            rps: limits.rps,
            burst,
            tokens: u128::from(burst) * TOKEN,
            updated: now,
            running: 0,
            max_concurrent: limits.max_concurrent,
        }
    }

    fn refill(&mut self, now: Instant) {
        if let Some(rps) = self.rps {
            self.tokens = self
                .tokens
                .saturating_add(
                    now.saturating_duration_since(self.updated)
                        .as_nanos()
                        .saturating_mul(u128::from(rps)),
                )
                .min(u128::from(self.burst) * TOKEN);
        }
        self.updated = now;
    }

    fn update(&mut self, limits: &Limits, now: Instant) {
        self.refill(now);
        self.rps = limits.rps;
        self.burst = limits.burst.or(limits.rps).unwrap_or(0);
        self.tokens = self.tokens.min(u128::from(self.burst) * TOKEN);
        self.max_concurrent = limits.max_concurrent;
    }

    fn acquire(&mut self, now: Instant) -> Result<(), HookError> {
        self.reserve(now, true)
    }

    fn reserve(&mut self, now: Instant, charge_rate: bool) -> Result<(), HookError> {
        self.refill(now);
        if self
            .max_concurrent
            .is_some_and(|max| self.running >= u64::from(max))
        {
            return Err(HookError::Overloaded(
                "app concurrent execution limit reached".into(),
            ));
        }
        if charge_rate && self.rps.is_some() && self.tokens < TOKEN {
            return Err(HookError::RateLimited(
                "app request rate limit reached".into(),
            ));
        }
        let running = self
            .running
            .checked_add(1)
            .ok_or_else(|| HookError::Overloaded("app execution count overflow".into()))?;
        if charge_rate && self.rps.is_some() {
            self.tokens -= TOKEN;
        }
        self.running = running;
        Ok(())
    }
}

pub(crate) struct Quota(Mutex<Bucket>);

impl Quota {
    pub(crate) fn active_executions(&self) -> u32 {
        let running = self.0.lock().unwrap_or_else(|e| e.into_inner()).running;
        u32::try_from(running).unwrap_or(u32::MAX)
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> Result<Permit, HookError> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .acquire(Instant::now())?;
        Ok(Permit(self.clone()))
    }

    pub(crate) fn acquire_initialization(self: &Arc<Self>) -> Result<Permit, HookError> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reserve(Instant::now(), false)?;
        Ok(Permit(self.clone()))
    }
}

pub(crate) struct Permit(Arc<Quota>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0 .0.lock().unwrap_or_else(|e| e.into_inner()).running -= 1;
    }
}

pub(crate) struct MemoryEnvelope {
    cap: usize,
    used: usize,
    pending: usize,
    tables: StoreLimits,
}

#[derive(Debug)]
pub(crate) struct MemoryLimit(pub String);

impl std::fmt::Display for MemoryLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for MemoryLimit {}

impl MemoryEnvelope {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            used: 0,
            pending: 0,
            tables: StoreLimitsBuilder::new().build(),
        }
    }
}

impl Default for MemoryEnvelope {
    fn default() -> Self {
        Self::new(64 << 20)
    }
}

impl ResourceLimiter for MemoryEnvelope {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.pending = 0;
        let growth = desired
            .checked_sub(current)
            .ok_or_else(|| wasmtime::Error::msg("invalid memory growth"))?;
        let used = self
            .used
            .checked_add(growth)
            .filter(|used| *used <= self.cap)
            .ok_or_else(|| {
                wasmtime::Error::new(MemoryLimit(format!(
                    "guest aggregate linear memory limit exceeded ({} bytes)",
                    self.cap
                )))
            })?;
        self.used = used;
        self.pending = growth;
        Ok(true)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.used -= self.pending;
        self.pending = 0;
        Err(wasmtime::Error::new(MemoryLimit(format!(
            "guest linear memory growth failed: {error}"
        ))))
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.tables.table_growing(current, desired, maximum)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.tables.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.tables.instances()
    }
    fn tables(&self) -> usize {
        self.tables.tables()
    }
    fn memories(&self) -> usize {
        self.tables.memories()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_and_optional_serialization() {
        let limits: Limits = serde_json::from_str("{}").unwrap();
        assert_eq!(limits, Limits::default());
        assert_eq!(
            serde_json::to_value(&limits).unwrap(),
            serde_json::json!({"timeout_ms": 5000, "memory_mb": 64})
        );
        assert!(serde_json::from_str::<Limits>(r#"{"cpu_ms":10}"#).is_err());
        for limits in [
            Limits {
                timeout_ms: 0,
                ..Limits::default()
            },
            Limits {
                timeout_ms: u64::MAX,
                ..Limits::default()
            },
            Limits {
                memory_mb: 0,
                ..Limits::default()
            },
            Limits {
                memory_mb: u64::MAX,
                ..Limits::default()
            },
            Limits {
                fuel: Some(0),
                ..Limits::default()
            },
            Limits {
                rps: Some(0),
                ..Limits::default()
            },
            Limits {
                rps: Some(1),
                burst: Some(0),
                ..Limits::default()
            },
            Limits {
                burst: Some(1),
                ..Limits::default()
            },
            Limits {
                max_concurrent: Some(0),
                ..Limits::default()
            },
        ] {
            assert!(limits.validate().is_err(), "{limits:?}");
        }
        for caps in [
            HostLimits {
                max_timeout_ms: Some(0),
                ..HostLimits::default()
            },
            HostLimits {
                max_timeout_ms: Some(u64::MAX),
                ..HostLimits::default()
            },
            HostLimits {
                max_memory_mb: Some(0),
                ..HostLimits::default()
            },
            HostLimits {
                max_memory_mb: Some(u64::MAX),
                ..HostLimits::default()
            },
            HostLimits {
                max_fuel: Some(0),
                ..HostLimits::default()
            },
            HostLimits {
                max_rps: Some(0),
                ..HostLimits::default()
            },
            HostLimits {
                max_burst: Some(0),
                ..HostLimits::default()
            },
            HostLimits {
                max_concurrent: Some(0),
                ..HostLimits::default()
            },
        ] {
            assert!(caps.validate().is_err(), "{caps:?}");
        }
        HostLimits {
            max_burst: Some(1),
            ..HostLimits::default()
        }
        .validate()
        .unwrap();
        for (error, code) in [
            (HookError::RateLimited("rate".into()), "rate_limited"),
            (HookError::Overloaded("concurrency".into()), "overloaded"),
        ] {
            assert_eq!(error.status(), 429);
            assert_eq!(error.code(), code);
        }
    }

    #[test]
    fn effective_caps_default_and_intersection() {
        let caps = HostLimits {
            max_timeout_ms: Some(40),
            max_memory_mb: Some(2),
            max_fuel: Some(10),
            max_rps: Some(3),
            max_burst: Some(2),
            max_concurrent: Some(4),
        };
        let effective = caps.effective(&Limits::default()).unwrap();
        assert_eq!(
            effective,
            Limits {
                timeout_ms: 40,
                memory_mb: 2,
                fuel: Some(10),
                rps: Some(3),
                burst: Some(2),
                max_concurrent: Some(4)
            }
        );
        let tighter = caps.intersect(&HostLimits {
            max_timeout_ms: Some(50),
            max_memory_mb: Some(1),
            max_fuel: None,
            max_rps: Some(1),
            max_burst: Some(4),
            max_concurrent: Some(2),
        });
        assert_eq!(
            tighter,
            HostLimits {
                max_timeout_ms: Some(40),
                max_memory_mb: Some(1),
                max_fuel: Some(10),
                max_rps: Some(1),
                max_burst: Some(2),
                max_concurrent: Some(2)
            }
        );
        assert_eq!(
            HostLimits::default().effective(&Limits::default()).unwrap(),
            Limits::default()
        );
        let app = Limits {
            rps: Some(10),
            ..Limits::default()
        };
        assert_eq!(caps.effective(&app).unwrap().burst, Some(2));
        assert_eq!(
            HostLimits {
                max_rps: Some(3),
                ..Default::default()
            }
            .effective(&app)
            .unwrap()
            .burst,
            Some(3)
        );
        let app = Limits {
            rps: Some(10),
            burst: Some(20),
            ..Limits::default()
        };
        assert_eq!(
            HostLimits {
                max_rps: Some(3),
                ..Default::default()
            }
            .effective(&app)
            .unwrap()
            .burst,
            Some(20)
        );
    }

    #[test]
    fn token_boundaries_and_redeploy_are_deterministic() {
        let start = Instant::now();
        let limits = Limits {
            rps: Some(3),
            burst: Some(1),
            ..Default::default()
        };
        let mut bucket = Bucket::new(&limits, start);
        bucket.acquire(start).unwrap();
        assert!(matches!(
            bucket.acquire(start + Duration::from_nanos(333_333_333)),
            Err(HookError::RateLimited(_))
        ));
        bucket
            .acquire(start + Duration::from_nanos(333_333_334))
            .unwrap();
        assert_eq!(bucket.running, 2);
        bucket.update(
            &Limits {
                rps: Some(1),
                burst: Some(8),
                max_concurrent: Some(1),
                ..Default::default()
            },
            start + Duration::from_nanos(333_333_334),
        );
        assert_eq!(bucket.running, 2);
        assert_eq!(bucket.tokens, 0);
        assert!(matches!(
            bucket.acquire(start + Duration::from_secs(1)),
            Err(HookError::Overloaded(_))
        ));
        bucket.running = 0;
        bucket
            .acquire(start + Duration::from_nanos(1_333_333_334))
            .unwrap();
    }

    #[test]
    fn overload_does_not_spend_tokens_and_permits_release_on_unwind() {
        let limits = Limits {
            rps: Some(1),
            burst: Some(2),
            max_concurrent: Some(1),
            ..Default::default()
        };
        let quota = Arc::new(Quota(Mutex::new(Bucket::new(&limits, Instant::now()))));
        let permit = quota.acquire().unwrap();
        assert!(matches!(quota.acquire(), Err(HookError::Overloaded(_))));
        drop(permit);
        let tokens = quota.0.lock().unwrap().tokens;
        assert!(tokens >= TOKEN);
        let _ = std::panic::catch_unwind(|| {
            let _permit = quota.acquire().unwrap();
            panic!("cancel executing task");
        });
        assert_eq!(quota.0.lock().unwrap().running, 0);
    }

    #[test]
    fn memory_failure_rolls_back_aggregate_growth_and_never_overflows() {
        let mut envelope = MemoryEnvelope::new(4 * 65536);
        assert!(envelope.memory_growing(0, 65536, None).unwrap());
        assert!(envelope.memory_growing(0, 2 * 65536, None).unwrap());
        assert_eq!(envelope.used, 3 * 65536);
        assert!(envelope.memory_growing(65536, 3 * 65536, None).is_err());
        assert_eq!(envelope.used, 3 * 65536);
        assert!(envelope
            .memory_growing(65536, 2 * 65536, Some(65536))
            .unwrap());
        assert!(envelope
            .memory_grow_failed(wasmtime::Error::msg("allocation failed"))
            .is_err());
        assert_eq!(envelope.used, 3 * 65536);
        assert!(envelope.memory_growing(65536, 2 * 65536, None).unwrap());
        assert_eq!(envelope.used, 4 * 65536);
        assert!(envelope.memory_growing(0, usize::MAX, None).is_err());
    }

    #[test]
    fn wasmtime_reports_failed_growth_without_leaking_aggregate_reservation() {
        let engine = wasmtime::Engine::default();
        let mut store = wasmtime::Store::new(&engine, MemoryEnvelope::new(4 * 65536));
        store.limiter(|limits| limits);
        let a = wasmtime::Memory::new(&mut store, wasmtime::MemoryType::new(1, Some(1))).unwrap();
        let b = wasmtime::Memory::new(&mut store, wasmtime::MemoryType::new(1, None)).unwrap();
        assert_eq!(store.data().used, 2 * 65536);
        assert!(a.grow(&mut store, 1).is_err());
        assert_eq!(store.data().used, 2 * 65536);
        b.grow(&mut store, 2).unwrap();
        assert_eq!(store.data().used, 4 * 65536);
        assert!(b.grow(&mut store, 1).is_err());
        assert_eq!(store.data().used, 4 * 65536);
    }
}
