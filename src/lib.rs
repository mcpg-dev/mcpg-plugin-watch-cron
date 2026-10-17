//! Cron / interval `watch_strategy` plugin (`dev.mcpg.watch.cron`).
//!
//! Emits a periodic resource-change tick on an operator schedule, so the
//! gateway re-reads / re-notifies a watched resource on a cron expression or a
//! fixed interval (e.g. "re-poll this resource every 5 minutes"). Each watcher
//! runs a private background thread that fires until the host calls `cancel`.
//! Pure timekeeping — no network, no host services.
//!
//! The per-watch `spec` selects exactly one schedule:
//! `{ "cron": "0 */5 * * * *" }` (6/7-field, seconds-first) or
//! `{ "interval_secs": 300 }` or `{ "interval_ms": 500 }`, with an optional
//! `max_fires` cap. A malformed spec fails the `watch()` call (`InvalidSpec`).

use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::Utc;
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const PLUGIN_ID: &str = "dev.mcpg.watch.cron";
/// The `WatchStrategy` spec variant name the gateway routes to this plugin.
const WATCH_KIND: &str = "cron";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// Cron expression (the `cron` crate's 6/7-field, seconds-first form).
    #[serde(default)]
    cron: Option<String>,
    /// Fixed interval in whole seconds.
    #[serde(default)]
    interval_secs: Option<u64>,
    /// Fixed interval in milliseconds (sub-second polling).
    #[serde(default)]
    interval_ms: Option<u64>,
    /// Optional cap on the number of ticks before the watcher self-stops.
    #[serde(default)]
    max_fires: Option<u64>,
}

/// The event type of one fire of the schedule.
pub const SCHEDULE_FIRED_EVENT_TYPE: &str = "schedule.fired";

/// The most missed fires a resumed event source raises; past that it goes
/// on from the next slot.
const MAX_CATCH_UP: u32 = 100;

/// An event source's `upstream_cursor`: the slot it fired last.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct CronResume {
    /// Unix milliseconds of the slot.
    last: i64,
}

/// A compiled schedule of fire slots.
enum Schedule {
    Cron(Box<cron::Schedule>),
    Every(Duration),
}

impl Schedule {
    /// The first slot after `after`, both in Unix milliseconds. `None` when
    /// the expression has no later occurrence.
    fn slot_after(&self, after: i64) -> Option<i64> {
        match self {
            Schedule::Every(d) => after.checked_add(i64::try_from(d.as_millis()).ok()?),
            Schedule::Cron(s) => {
                let at = chrono::DateTime::<Utc>::from_timestamp_millis(after)?;
                s.after(&at).next().map(|t| t.timestamp_millis())
            }
        }
    }
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn rfc3339(millis: i64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// The `schedule.fired` event of `slot`. Its id names the slot, so the same
/// fire raised twice is one occurrence.
fn fire_event(slot: i64, catch_up: bool) -> WatchEvent {
    WatchEvent {
        event_id: Some(format!("fire:{slot}")),
        event_type: Some(SCHEDULE_FIRED_EVENT_TYPE.to_owned()),
        occurred_at: mcpg_plugin_protocol::occurred_at_from_millis(slot),
        data: Some(serde_json::json!({
            "scheduledAt": rfc3339(slot),
            "firedAt": rfc3339(now_ms()),
            "catchUp": catch_up,
        })),
        upstream_cursor: serde_json::to_value(CronResume { last: slot }).ok(),
        ..WatchEvent::default()
    }
}

impl WatchSpec {
    fn compile(&self) -> Result<Schedule, WatchError> {
        let set = [
            self.cron.is_some(),
            self.interval_secs.is_some(),
            self.interval_ms.is_some(),
        ]
        .iter()
        .filter(|b| **b)
        .count();
        if set != 1 {
            return Err(WatchError::InvalidSpec {
                message: "exactly one of `cron`, `interval_secs`, `interval_ms` is required".into(),
            });
        }
        if let Some(expr) = &self.cron {
            let schedule = cron::Schedule::from_str(expr).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid cron expression: {e}"),
            })?;
            return Ok(Schedule::Cron(Box::new(schedule)));
        }
        let millis = if let Some(s) = self.interval_secs {
            s.checked_mul(1000)
        } else {
            self.interval_ms
        };
        match millis {
            Some(ms) if ms > 0 => Ok(Schedule::Every(Duration::from_millis(ms))),
            _ => Err(WatchError::InvalidSpec {
                message: "interval must be > 0".into(),
            }),
        }
    }
}

/// Shared stop signal between the watcher thread and `cancel`.
struct Stop {
    stopped: Mutex<bool>,
    cv: Condvar,
}

impl Stop {
    fn new() -> Self {
        Self {
            stopped: Mutex::new(false),
            cv: Condvar::new(),
        }
    }
    fn is_stopped(&self) -> bool {
        *self.stopped.lock().expect("stop mutex")
    }
    fn signal(&self) {
        *self.stopped.lock().expect("stop mutex") = true;
        self.cv.notify_all();
    }
    /// Block until `deadline` or a stop signal. Returns true if stopped.
    fn wait_until(&self, deadline: Instant) -> bool {
        let mut guard = self.stopped.lock().expect("stop mutex");
        while !*guard {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (g, _) = self
                .cv
                .wait_timeout(guard, deadline - now)
                .expect("cv wait");
            guard = g;
        }
        true
    }
}

/// Boxed behind the opaque [`WatchHandleBox`] pointer; `cancel` reconstructs it.
struct CronCancelState {
    stop: Arc<Stop>,
    thread: Option<JoinHandle<()>>,
}

pub struct CronWatchPlugin {
    manifest: PluginManifest,
}

impl CronWatchPlugin {
    /// SDK factory. The cron watcher carries no plugin-level config (the
    /// schedule arrives per-watch in the `spec`), so the config JSON is ignored.
    pub fn from_config_json(_config_json: &str) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: PLUGIN_ID,
                name: "Cron Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for CronWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, resume) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid watch spec: {e}"),
            })?;
        let schedule = parsed.compile()?;
        let max_fires = parsed.max_fires;
        let event_source = mcpg_plugin_protocol::is_event_source(resource_uri);
        // Where an event source stopped, handed back by the host; a resource
        // watch has none.
        let resume: Option<CronResume> = match resume.filter(|_| event_source) {
            Some(position) => {
                Some(
                    serde_json::from_value(position).map_err(|e| WatchError::InvalidSpec {
                        message: format!("invalid resume position: {e}"),
                    })?,
                )
            }
            None => None,
        };

        let stop = Arc::new(Stop::new());
        let thread_stop = Arc::clone(&stop);
        // A change tick carries no principal/session — the host treats the
        // resource as changed and re-notifies subscribers.
        let tick =
            serde_json::to_string(&WatchEvent::default()).unwrap_or_else(|_| "{}".to_owned());

        let thread = std::thread::Builder::new()
            .name("mcpg-watch-cron".into())
            .spawn(move || {
                let mut fires: u64 = 0;
                let mut catch_up_left = if resume.is_some() { MAX_CATCH_UP } else { 0 };
                let mut next = schedule.slot_after(resume.map_or_else(now_ms, |r| r.last));
                loop {
                    let Some(slot) = next else {
                        // No later occurrence (e.g. a year-pinned past expr): idle.
                        if thread_stop.wait_until(Instant::now() + Duration::from_secs(3600)) {
                            return;
                        }
                        next = schedule.slot_after(now_ms());
                        continue;
                    };
                    let now = now_ms();
                    let missed = slot <= now;
                    if missed && catch_up_left == 0 {
                        next = schedule.slot_after(now);
                        continue;
                    }
                    if missed {
                        catch_up_left -= 1;
                    } else {
                        let wait = Duration::from_millis(u64::try_from(slot - now).unwrap_or(0));
                        if thread_stop.wait_until(Instant::now() + wait) {
                            return; // cancelled
                        }
                    }
                    if thread_stop.is_stopped() {
                        return;
                    }
                    if event_source {
                        let event = serde_json::to_string(&fire_event(slot, missed))
                            .unwrap_or_else(|_| tick.clone());
                        emit_event(&event);
                    } else {
                        emit_event(&tick);
                    }
                    next = schedule.slot_after(slot);
                    fires += 1;
                    if let Some(max) = max_fires
                        && fires >= max
                    {
                        return;
                    }
                }
            })
            .map_err(|e| WatchError::Subscribe {
                message: format!("failed to spawn cron watcher thread: {e}"),
            })?;

        let state = Box::new(CronCancelState {
            stop,
            thread: Some(thread),
        });
        Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: pointer produced by `Box::into_raw` in `watch`, round-tripped
        // by the host exactly once.
        let mut state = unsafe { Box::from_raw(watch_handle.0 as *mut CronCancelState) };
        state.stop.signal();
        if let Some(t) = state.thread.take() {
            let _ = t.join();
        }
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.watch.cron",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: CronWatchPlugin,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| CronWatchPlugin::from_config_json(cfg),
        },
    ],
}

#[cfg(test)]
mod tests;
