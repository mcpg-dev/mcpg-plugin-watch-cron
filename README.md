# Cron Watch Strategy — `dev.mcpg.watch.cron`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-cron` · artifact `libmcpg_plugin_watch_cron.so` · Apache-2.0

Emits a resource-change tick on an operator schedule, so an MCP gateway
re-notifies subscribers of a watched resource on a cron expression or a fixed
interval. Bind it to a resource whose upstream has no change feed at all and
whose freshness is a matter of policy rather than detection — a nightly report,
a rate-limited vendor API you may only call every five minutes, a document that
is simply declared stale on a cadence. Reach for it when you want the cadence
itself to be the trigger; if you want a tick only when the content actually
changed, the gateway's built-in `poll` strategy hashes each read and suppresses
no-op notifications.

## What it does
- Fires a change tick on a cron expression, a whole-second interval, or a
  millisecond interval — exactly one per watch.
- Runs one private background thread per watched resource, sleeping until the
  next fire and waking immediately on cancellation.
- Recomputes each cron delay against the wall clock, so firing stays aligned to
  the expression rather than drifting by the handler's own runtime.
- Stops itself after `max_fires` ticks when that cap is set.
- Rejects a spec that names zero or several schedules, a zero interval, a
  malformed cron expression, or an unknown field, failing the watch rather than
  starting a watcher that never fires.
- Joins its thread during cancellation, so no tick can arrive after cancel
  returns.
- Declares no `required_capabilities` — pure timekeeping, with no network,
  filesystem, or host-service access.

## Configuration
Loaded from the flat top-level `plugins:` list. The plugin itself takes no
instance config; the schedule is chosen per watched resource.

```yaml
plugins:
  - id: dev.mcpg.watch.cron
    class: watch_strategy
    source:
      oci: ghcr.io/mcpg-dev/source-code/plugins/watch-cron:protocol-1
```

Each resource that should tick on this schedule selects it under
`mcp.capabilities.resources[].watch.strategy` with the generic `type: plugin`
form, where `kind` names the plugin's watch kind — `cron` — and the remaining
fields flatten into the spec passed to the plugin.

```yaml
mcp:
  capabilities:
    resources:
      - name: daily-report
        description: Daily report, re-notified on a fixed cadence.
        uri: "report://daily"
        mime_type: application/json
        backend:
          kind: http
          url: https://reports.example.com/daily.json
          method: get
        watch:
          strategy:
            type: plugin
            kind: cron
            cron: "0 */5 * * * *"     # every five minutes
```

| Field | Type | Default | Description |
|---|---|---|---|
| `cron` | string | *(unset)* | Cron expression in seconds-first 6/7-field form (`sec min hour day month day-of-week [year]`), evaluated in UTC. |
| `interval_secs` | integer > 0 | *(unset)* | Fixed interval in whole seconds. |
| `interval_ms` | integer > 0 | *(unset)* | Fixed interval in milliseconds, for sub-second cadence. |
| `max_fires` | integer | *(unlimited)* | Stop the watcher after this many ticks. |

Exactly one of `cron`, `interval_secs`, and `interval_ms` must be set. Unknown
fields are rejected.

## Change-watching
The gateway starts one watcher per resource URI when a session first calls
`resources/subscribe` on it, shares that watcher across every later subscriber,
and cancels it when the last subscriber goes away — an unsubscribed resource
consumes no timer thread.

A tick carries no principal and no session: it says the resource changed, not
who changed it. The gateway turns each tick into
`notifications/resources/updated` for that URI's subscribers. Because the tick
carries no identity, a `notification_filter` scoped to `subject_id` or
`session_id` has nothing to narrow on and falls back to fanning out to every
subscriber; an `expression` filter still evaluates per subscriber against
`subscriber.*` and `event.uri`.

Only one plugin may register a given watch kind, so `kind: cron` resolves to
this plugin. A resource that names a kind no loaded plugin serves gets a
watcher that idles until cancelled rather than an error at boot.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-watch-cron --features cdylib-export --release   # → target/release/libmcpg_plugin_watch_cron.so
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Plugin classes and the ABI: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- Resource bindings and their watch block: <https://mcpg.dev/docs/reference/configuration>
- Sibling watch strategy: `libs/plugins/watch/file` (ticks on filesystem changes)
