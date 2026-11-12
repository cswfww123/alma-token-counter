# Token Counter Plus (Alma plugin)

A fork of Alma's built-in **Token Counter** plugin with three additions the
original lacks: a **prompt-cache hit rate**, **live streaming throughput**, and
**per-feature display toggles**.

Independent plugin id (`token-counter-plus`), so installing this never touches
or gets overwritten by the bundled `token-counter`.

## What it shows

```
247.4k tok · 99.7% cache · 172 t/s · $0.0764
```

| Segment | Meaning |
| --- | --- |
| `247.4k tok` | Total tokens for the thread, accumulated across turns |
| `99.7% cache` | Prompt-cache hit rate: `cached / prompt`. One decimal near 100% so 99.8% is never rounded up to a lie |
| `172 t/s` | Generation speed. Live sample while streaming, persisted value afterwards |
| `$0.0764` | Estimated cost, using the model's real pricing when available |

Hover for a full breakdown: input/output/reasoning split, cached vs. uncached
prompt tokens, TTFT, stream duration, average speed, and per-1M pricing.
Click for a compact detail panel.

## Why a fork

The bundled plugin only had token totals and a rough cost estimate. It also had
no live data source for speed — Alma persists `tokensPerSecond` on the assistant
message *after* a turn completes, so a naive implementation shows a frozen number
while text is actively streaming. This fork reads the streaming WebSocket to
sample throughput in real time.

## How the numbers are derived

- **Token counts and cost** — `chat.message.didReceive` and `thread.activated`
  events, matching Alma's own `input × price.input + output × price.output +
  cached × price.cacheRead + cacheWrite × price.cacheWrite` convention.
- **Cache hit rate** — `cachedInputTokens / promptTokens`. Cached tokens are a
  subset of prompt tokens, so this is a true hit rate rather than a ratio of two
  independent quantities.
- **Live throughput** — connects to `ws://127.0.0.1:${ALMA_PORT}/ws/threads` and
  sums the character length of incoming `text_append` deltas in a 3-second
  rolling window. The stream carries characters, not tokens, so characters are
  divided by a per-thread calibration ratio measured on the previous turn
  (defaults to 3.5 chars/token until one is available).
- **Settled throughput** — read back from the message metadata through the local
  HTTP API, which is the authoritative `tokensPerSecond` Alma computed.

Live values are estimates and are marked with `~` once the stream ends but before
the persisted record lands. Chinese text estimates less accurately than English
because characters-per-token varies more.

## Install

Alma → Plugins → Install Plugin → paste this repository's URL.

Or point it at a local checkout:

```bash
curl -X POST http://localhost:23001/api/plugins \
  -H 'Content-Type: application/json' \
  -d '{"sourcePath":"/path/to/alma-token-counter"}'
```

## Settings

All four segments can be toggled independently:

| Key | Default | Description |
| --- | --- | --- |
| `tokenCounter.showTokens` | `true` | Total token count |
| `tokenCounter.showCacheRate` | `true` | Prompt-cache hit rate |
| `tokenCounter.showThroughput` | `true` | Generation speed |
| `tokenCounter.showCost` | `true` | Estimated cost |
| `tokenCounter.costPerMillionTokens` | `3.0` | Fallback price when the model has no pricing data |

## Development

`main.ts` is the source. Alma compiles TypeScript plugins on activation, but a
built `main.js` is committed so the plugin also loads when no Bun runtime is
available:

```bash
bun build main.ts --outfile main.js --target node --format esm \
  --external electron --external zod
```

Then reload: `curl -X POST http://localhost:23001/api/plugins/refresh`.

## Known behavior

- Speed shows the **last turn**, not the conversation average. A turn where the
  model thinks for minutes and emits a short answer will legitimately read
  1–2 t/s.
- Background threads (cron, subagents) do not hijack the bar; only the thread on
  screen is sampled.
- Repaints are throttled to ~4/s so the number stays readable.

## License

MIT
