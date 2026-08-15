use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use mcpg_plugin_protocol::backend::WatchError;
use mcpg_plugin_sdk::ffi::SyncWatchStrategyPlugin;
use serde_json::{Value, json};

use super::{CronWatchPlugin, PLUGIN_ID, WATCH_KIND};

type Sink = Arc<Mutex<Vec<String>>>;

fn plugin() -> CronWatchPlugin {
    CronWatchPlugin::from_config_json("{}")
}

fn sink() -> Sink {
    Arc::new(Mutex::new(Vec::new()))
}

fn emit_for(s: &Sink) -> Box<dyn Fn(&str) + Send + Sync + 'static> {
    let s = Arc::clone(s);
    Box::new(move |ev: &str| s.lock().unwrap().push(ev.to_owned()))
}

fn count(s: &Sink) -> usize {
    s.lock().unwrap().len()
}

fn watch(
    p: &CronWatchPlugin,
    spec: Value,
    s: &Sink,
) -> Result<mcpg_plugin_sdk::ffi::WatchHandleBox, WatchError> {
    p.watch("res://x", &spec, emit_for(s))
}

#[test]
fn manifest_and_kind_are_correct() {
    use mcpg_plugin_protocol::PluginClass;
    let p = plugin();
    let m = SyncWatchStrategyPlugin::manifest(&p);
    assert_eq!(m.id, PLUGIN_ID);
    assert_eq!(m.plugin_class, PluginClass::WatchStrategy);
    assert_eq!(p.kind(), WATCH_KIND);
    assert!(m.required_capabilities.is_empty());
}

#[test]
fn interval_fires_repeatedly_then_cancel_stops() {
    let p = plugin();
    let s = sink();
    let handle = watch(&p, json!({ "interval_ms": 100 }), &s).unwrap();
    sleep(Duration::from_millis(350));
    let fired = count(&s);
    assert!(fired >= 2, "expected >=2 ticks in 350ms, got {fired}");
    p.cancel(handle);
    let after_cancel = count(&s);
    sleep(Duration::from_millis(300));
    assert_eq!(count(&s), after_cancel, "no ticks after cancel");
}

#[test]
fn emitted_event_is_watch_event_json() {
    let p = plugin();
    let s = sink();
    let handle = watch(&p, json!({ "interval_ms": 60 }), &s).unwrap();
    sleep(Duration::from_millis(150));
    p.cancel(handle);
    let evs = s.lock().unwrap();
    assert!(!evs.is_empty());
    // Each tick is a serialised WatchEvent (empty object — no principal/session).
    let v: Value = serde_json::from_str(&evs[0]).unwrap();
    assert!(v.is_object(), "tick must be a JSON object: {}", evs[0]);
}

#[test]
fn max_fires_caps_emission() {
    let p = plugin();
    let s = sink();
    let handle = watch(&p, json!({ "interval_ms": 50, "max_fires": 3 }), &s).unwrap();
    sleep(Duration::from_millis(500));
    assert_eq!(count(&s), 3, "watcher must self-stop after max_fires");
    // Cancelling an already-finished watcher is safe (joins immediately).
    p.cancel(handle);
    assert_eq!(count(&s), 3);
}

#[test]
fn cron_every_second_fires() {
    let p = plugin();
    let s = sink();
    let handle = watch(&p, json!({ "cron": "* * * * * *" }), &s).unwrap();
    sleep(Duration::from_millis(2300));
    p.cancel(handle);
    assert!(count(&s) >= 1, "cron '* * * * * *' should fire within 2.3s");
}

#[test]
fn invalid_spec_both_schedules_set() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({ "interval_ms": 100, "cron": "* * * * * *" }), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn invalid_spec_no_schedule() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({}), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn invalid_cron_expression_rejected() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({ "cron": "not a cron" }), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn zero_interval_rejected() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({ "interval_secs": 0 }), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn unknown_spec_field_rejected() {
    let p = plugin();
    let s = sink();
    assert!(matches!(
        watch(&p, json!({ "interval_ms": 100, "bogus": 1 }), &s),
        Err(WatchError::InvalidSpec { .. })
    ));
}

#[test]
fn cancel_null_handle_is_safe() {
    let p = plugin();
    p.cancel(mcpg_plugin_sdk::ffi::WatchHandleBox(std::ptr::null_mut()));
}
