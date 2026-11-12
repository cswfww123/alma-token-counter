# Token Counter Plus (Alma plugin)

A fork of Alma's built-in **Token Counter** plugin with three additions the
original lacks: a **prompt-cache hit rate**, **live streaming throughput**, and
**per-feature display toggles**.

Independent plugin id (`token-counter-plus`), so installing this never touches
or gets overwritten by the bundled `token-counter`.

## What it shows

The inline bar carries only what moves — cache hit rate, speed, and running
agents:

```
99.8% cache · 172 t/s · 2 agents · 696 t/s
```

| Segment | Meaning |
| --- | --- |
| `99.8% cache` | Prompt-cache hit rate: `cached / prompt`. One decimal near 100% so 99.8% is never rounded up to a lie |
| `172 t/s` | This thread's generation speed. Live sample while streaming, persisted value afterwards |
| `2 agents · 696 t/s` | Running delegated subagents and their combined throughput |

Token totals and cost are **hidden by default** — they change slowly and mostly
add noise next to the composer. Toggle them back on with `showTokens` /
`showCost` in settings.

Everything is still reachable two ways:

- **Hover** — input/output/reasoning split, cached vs. uncached prompt tokens,
  TTFT, stream duration, average speed, per-1M pricing, and a per-agent
  breakdown.
- **Click** — a detail panel with the same numbers plus a cost breakdown
  (input / cache / output).

## Delegated subagents

Task subagents stream on their own channel (`subagent_message_*`), tagged with a
task ID and the parent thread. Each one gets an independent 3-second rolling
window, so several concurrent agents do not blur into one misleading average.

While an agent runs, the bar shows `1 agent · 394 t/s`. Hover to see which agent
is which:

```
Delegated agents:
  Researcher task — 465 t/s · ~617 tok · TTFT 0.12s
  Reviewer task — done in 1.50s · 411 t/s avg · ~617 tok
```

Finished agents keep their row for a minute so their final numbers can be read,
then drop out. Late deltas arriving after a completion event are ignored, so a
finished run cannot appear to restart.

Note: Alma renders the subagent row inside the chat transcript itself, and the
plugin manifest only allows `sidebar` / `settingsPanels` / `toolbar` /
`contextMenus` / `artifactRenderers` / `statusBar` contributions. There is no
hook to inject text into that row, which is why this lives in the status bar
instead.

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
| `tokenCounter.showCacheRate` | `true` | Prompt-cache hit rate |
| `tokenCounter.showThroughput` | `true` | Generation speed and running agents |
| `tokenCounter.showTokens` | `false` | Total token count inline |
| `tokenCounter.showCost` | `false` | Estimated cost inline |
| `tokenCounter.costPerMillionTokens` | `3.0` | Fallback price when the model has no pricing data |

Hiding a segment only affects the inline text — it never removes that data from
the tooltip or the click panel.

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
