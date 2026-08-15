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
use serde::Deserialize;
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

/// A compiled schedule that yields the delay until its next fire.
enum Schedule {
    Cron(Box<cron::Schedule>),
    Every(Duration),
}

impl Schedule {
    /// Delay from now until the next fire. For cron this is recomputed against
    /// the wall clock each tick (so it stays aligned to the expression).
    fn next_delay(&self) -> Duration {
        match self {
            Schedule::Every(d) => *d,
            Schedule::Cron(s) => {
                let now = Utc::now();
                match s.upcoming(Utc).next() {
                    Some(next) => (next - now).to_std().unwrap_or(Duration::from_millis(1)),
                    // No future occurrence (e.g. a year-pinned past expr): idle.
                    None => Duration::from_secs(3600),
                }
            }
        }
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
        _resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let parsed: WatchSpec =
            serde_json::from_value(spec.clone()).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid watch spec: {e}"),
            })?;
        let schedule = parsed.compile()?;
        let max_fires = parsed.max_fires;

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
                loop {
                    let deadline = Instant::now() + schedule.next_delay();
                    if thread_stop.wait_until(deadline) {
                        return; // cancelled
                    }
                    if thread_stop.is_stopped() {
                        return;
                    }
                    emit_event(&tick);
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
