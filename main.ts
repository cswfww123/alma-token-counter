import type { PluginContext, PluginActivation } from 'alma-plugin-api';

/**
 * Token Counter Plus
 *
 * Shows live token usage, prompt-cache hit rate, throughput (tokens/s)
 * and estimated cost for the current thread in the status bar.
 *
 * Token counts and pricing arrive via chat.message.didReceive /
 * thread.activated. Generation speed metrics (tokensPerSecond,
 * timeToFirstTokenMs, streamingDurationMs) and reasoning tokens are only
 * persisted on the assistant message metadata, so they are read back from
 * Alma's local HTTP API. Event-side accumulation provides instant feedback
 * and a wall-clock throughput estimate while the request is in flight.
 */

interface Pricing {
    input?: number;  // $ per million tokens
    output?: number;
    cacheRead?: number;  // $ per million cached tokens
    cacheWrite?: number;
}

interface TurnMetrics {
    tps?: number;            // authoritative tokens/s for the last turn
    estimatedTps?: number;   // wall-clock fallback while API is unavailable
    ttftMs?: number;
    streamingMs?: number;
}

/**
 * Live throughput sampled from the streaming WebSocket while a turn is
 * generating. Tokens are estimated from emitted characters using a ratio
 * calibrated on the last completed turn of the same thread.
 */
interface LiveState {
    threadId: string;
    startedAt: number;
    firstTextAt: number | null;
    window: Array<{ t: number; tokens: number }>;
    tokensThisTurn: number;
    charsThisTurn: number;
    tps: number | null;
    ttftMs: number | null;
}

/**
 * One delegated Task/subagent run. Alma streams these on their own event
 * channel (subagent_message_*), tagged with a taskId and the parent thread.
 */
interface SubagentState {
    taskId: string;
    threadId: string;
    label?: string;
    startedAt: number;
    firstTextAt: number | null;
    window: Array<{ t: number; tokens: number }>;
    charsTotal: number;
    tokensTotal: number;
    tps: number | null;
    done: boolean;
    finishedAt?: number;
}

interface ThreadStats {
    promptTokens: number;
    cachedTokens: number;
    cacheWriteTokens: number;
    reasoningTokens: number;
    totalTokens: number;
    inputCost: number;
    outputCost: number;
    cacheReadCost: number;
    cacheWriteCost: number;
    totalCost: number;
    pricing?: Pricing;
    lastTurn?: TurnMetrics;
    avgTps?: number;
    turns: number;
    // Characters emitted per output token, calibrated per thread so live
    // throughput can be estimated from streamed text length.
    charsPerToken?: number;
    // Throughput measured from the stream itself; carried over after the turn
    // ends so the display never blanks while waiting for the persisted value.
    streamTps?: number;
    streamTpsAt?: number;
}

interface ApiUsage {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    reasoningTokens?: number;
    cachedInputTokens?: number;
    cacheWriteInputTokens?: number;
    timeToFirstTokenMs?: number;
    streamingDurationMs?: number;
    tokensPerSecond?: number;
}

export async function activate(context: PluginContext): Promise<PluginActivation> {
    const { logger, events, ui, settings } = context;

    logger.info('Token Counter Plus activated!');

    const statusBarItem = ui.createStatusBarItem({
        id: 'token-counter-plus',
        alignment: 'right',
        priority: 100,
    });

    const threadStats = new Map<string, ThreadStats>();
    let currentThreadId: string | null = null;
    let disposed = false;
    let turnStartedAt: number | null = null;
    let refreshTimer: ReturnType<typeof setTimeout> | null = null;
    let refreshSeq = 0;

    // Live streaming state (null when no turn is generating)
    let live: LiveState | null = null;
    let liveSocket: WebSocket | null = null;
    let liveReconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let liveReconnectDelay = 1000;
    const LIVE_WINDOW_MS = 3000;
    const DEFAULT_CHARS_PER_TOKEN = 3.5;

    // Delegated subagent runs, keyed by taskId. Kept per parent thread so
    // switching threads doesn't show another thread's agents.
    const subagents = new Map<string, SubagentState>();
    const SUBAGENT_RETENTION_MS = 60_000;

    // ---- environment --------------------------------------------------------

    const env = (): Record<string, string | undefined> => {
        const g = globalThis as unknown as { process?: { env?: Record<string, string | undefined> } };
        return g.process?.env ?? {};
    };

    // The plugin is loaded inside Alma's server process, so the API port is
    // available in the environment.
    const apiBaseUrl = (): string | null => {
        const e = env();
        if (e.ALMA_PORT) return `http://127.0.0.1:${e.ALMA_PORT}`;
        if (e.ALMA_API_URL) return e.ALMA_API_URL.replace(/\/+$/, '');
        return null;
    };

    const getSettings = () => ({
        // Tokens and cost are deliberately off by default: they change slowly and
        // just add noise next to the composer. The full numbers are one click
        // away in the detail panel and always present in the tooltip.
        showTokens: settings.get<boolean>('tokenCounter.showTokens', false),
        showCacheRate: settings.get<boolean>('tokenCounter.showCacheRate', true),
        showThroughput: settings.get<boolean>('tokenCounter.showThroughput', true),
        showCost: settings.get<boolean>('tokenCounter.showCost', false),
        fallbackCostPerMillion: settings.get<number>(
            'tokenCounter.costPerMillionTokens',
            settings.get<number>('tokenCounter.fallbackCostPerMillion', 3.0),
        ),
    });

    // ---- stats model --------------------------------------------------------

    const createEmptyStats = (): ThreadStats => ({
        promptTokens: 0,
        completionTokens: 0,
        cachedTokens: 0,
        cacheWriteTokens: 0,
        reasoningTokens: 0,
        totalTokens: 0,
        inputCost: 0,
        outputCost: 0,
        cacheReadCost: 0,
        cacheWriteCost: 0,
        totalCost: 0,
        turns: 0,
    });

    const getCurrentStats = (): ThreadStats => {
        if (!currentThreadId) return createEmptyStats();
        return threadStats.get(currentThreadId) || createEmptyStats();
    };

    const calculateCost = (
        stats: ThreadStats,
        pricing?: Pricing,
    ): Pick<ThreadStats, 'inputCost' | 'outputCost' | 'cacheReadCost' | 'cacheWriteCost' | 'totalCost'> => {
        const { fallbackCostPerMillion } = getSettings();

        if (!pricing) {
            const totalCost =
                ((stats.promptTokens + stats.completionTokens) / 1_000_000) * fallbackCostPerMillion;
            return {
                inputCost: totalCost / 2,
                outputCost: totalCost / 2,
                cacheReadCost: 0,
                cacheWriteCost: 0,
                totalCost,
            };
        }

        const inputCost = pricing.input ? (stats.promptTokens / 1_000_000) * pricing.input : 0;
        const outputCost = pricing.output ? (stats.completionTokens / 1_000_000) * pricing.output : 0;
        const cacheReadCost = pricing.cacheRead ? (stats.cachedTokens / 1_000_000) * pricing.cacheRead : 0;
        const cacheWriteCost = pricing.cacheWrite
            ? (stats.cacheWriteTokens / 1_000_000) * pricing.cacheWrite
            : 0;

        return {
            inputCost,
            outputCost,
            cacheReadCost,
            cacheWriteCost,
            totalCost: inputCost + outputCost + cacheReadCost + cacheWriteCost,
        };
    };

    const withRecalculatedCost = (stats: ThreadStats): ThreadStats => ({
        ...stats,
        ...calculateCost(stats, stats.pricing),
    });

    // ---- formatting ---------------------------------------------------------

    const formatTokens = (num: number): string => {
        if (!Number.isFinite(num) || num <= 0) return '0';
        if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
        if (num >= 10_000) return `${(num / 1_000).toFixed(1)}k`;
        return Math.round(num).toLocaleString();
    };

    const formatExact = (num: number): string =>
        Math.round(Number.isFinite(num) ? num : 0).toLocaleString();

    const formatCost = (num: number): string => {
        if (num >= 1) return `$${num.toFixed(3)}`;
        if (num >= 0.01) return `$${num.toFixed(4)}`;
        return `$${num.toFixed(5)}`;
    };

    const formatSeconds = (ms: number): string => `${(ms / 1000).toFixed(ms >= 60_000 ? 1 : 2)}s`;

    const formatTps = (tps: number): string =>
        tps >= 100 ? tps.toFixed(0) : tps.toFixed(tps >= 10 ? 1 : 2);

    // Cached tokens are a subset of prompt tokens, so this is a true hit rate.
    const cacheRate = (stats: ThreadStats): number | null => {
        if (stats.promptTokens <= 0) return null;
        const rate = (stats.cachedTokens / stats.promptTokens) * 100;
        return Math.max(0, Math.min(100, rate));
    };

    // Implausible wall-clock estimates (very short turns, instant tool replies)
    // are dropped rather than displayed as a misleading number.
    const MAX_PLAUSIBLE_TPS = 2000;

    const currentTps = (stats: ThreadStats): number | null => {
        const last = stats.lastTurn;
        if (last) {
            if (typeof last.tps === 'number' && last.tps > 0) return last.tps;
            if (
                typeof last.estimatedTps === 'number' &&
                last.estimatedTps > 0 &&
                last.estimatedTps <= MAX_PLAUSIBLE_TPS
            ) {
                return last.estimatedTps;
            }
        }
        // Between the end of a turn and the persisted usage landing, keep
        // showing what the stream measured so the segment doesn't blink out.
        if (streamTpsIsFresh(stats)) return stats.streamTps as number;
        return null;
    };

    // The stream value only bridges a short gap; after that it is stale news.
    const STREAM_TPS_TTL_MS = 5 * 60_000;

    const streamTpsIsFresh = (stats: ThreadStats): boolean => {
        if (typeof stats.streamTps !== 'number' || stats.streamTps <= 0) return false;
        if (typeof stats.streamTpsAt !== 'number') return true;
        return Date.now() - stats.streamTpsAt < STREAM_TPS_TTL_MS;
    };

    // The persisted tokens/s always wins; the stream value is only a bridge.
    const tpsIsFromStream = (stats: ThreadStats): boolean =>
        !(typeof stats.lastTurn?.tps === 'number' && stats.lastTurn.tps > 0) &&
        streamTpsIsFresh(stats);

    const isEstimatedTps = (stats: ThreadStats): boolean =>
        !(typeof stats.lastTurn?.tps === 'number' && stats.lastTurn.tps > 0) &&
        typeof stats.lastTurn?.estimatedTps === 'number' &&
        stats.lastTurn.estimatedTps > 0 &&
        stats.lastTurn.estimatedTps <= MAX_PLAUSIBLE_TPS;

    // Rounding 99.8% to "100%" would overstate the cache, so high-but-not-full
    // rates keep one decimal place.
    const formatRate = (rate: number): string => {
        if (rate >= 99.9 || rate <= 0) return `${Math.round(rate)}%`;
        if (rate >= 99) return `${rate.toFixed(1)}%`;
        return `${Math.round(rate)}%`;
    };

    // ---- status bar ---------------------------------------------------------

    const buildTooltip = (stats: ThreadStats): string => {
        const { showCost } = getSettings();
        const lines: string[] = [];
        const rate = cacheRate(stats);

        if (stats.cachedTokens > 0 || stats.cacheWriteTokens > 0) {
            const extra: string[] = [];
            if (stats.cachedTokens > 0) extra.push(`cached ${formatExact(stats.cachedTokens)}`);
            if (stats.cacheWriteTokens > 0) extra.push(`written ${formatExact(stats.cacheWriteTokens)}`);
            lines.push(`Input: ${formatExact(stats.promptTokens)} (${extra.join(', ')})`);
        } else {
            lines.push(`Input: ${formatExact(stats.promptTokens)}`);
        }

        if (stats.reasoningTokens > 0) {
            lines.push(`Output: ${formatExact(stats.completionTokens)} (reasoning ${formatExact(stats.reasoningTokens)})`);
        } else {
            lines.push(`Output: ${formatExact(stats.completionTokens)}`);
        }

        lines.push(`Total: ${formatExact(stats.totalTokens)}`);

        if (rate !== null) {
            const fresh = Math.max(0, stats.promptTokens - stats.cachedTokens);
            lines.push(`Cache hit: ${rate.toFixed(1)}%  (${formatExact(fresh)} uncached prompt tokens)`);
        }

        const liveForThread = live && live.threadId === currentThreadId ? live : null;
        if (liveForThread?.tps) {
            lines.push(`Speed: ${formatTps(liveForThread.tps)} tok/s  (live, last 3s)`);
        }

        const tps = currentTps(stats);
        if (tps !== null) {
            let line = liveForThread?.tps
                ? `Last turn: ${formatTps(tps)} tok/s`
                : `Speed: ${formatTps(tps)} tok/s`;
            if (tpsIsFromStream(stats)) line += ' (measured from stream)';
            else if (isEstimatedTps(stats)) line += ' (estimated)';
            if (typeof stats.avgTps === 'number' && stats.avgTps > 0) {
                line += `  ·  avg ${formatTps(stats.avgTps)} tok/s`;
            }
            lines.push(line);
        }

        if (stats.lastTurn) {
            const timing: string[] = [];
            if (typeof stats.lastTurn.ttftMs === 'number') timing.push(`TTFT ${formatSeconds(stats.lastTurn.ttftMs)}`);
            if (typeof stats.lastTurn.streamingMs === 'number') {
                timing.push(`stream ${formatSeconds(stats.lastTurn.streamingMs)}`);
            }
            if (timing.length > 0) lines.push(timing.join('  ·  '));
        }

        if (showCost && stats.totalCost > 0) {
            lines.push('');
            if (stats.pricing) {
                if (stats.inputCost > 0) lines.push(`Input cost: ${formatCost(stats.inputCost)}`);
                if (stats.cacheReadCost > 0) lines.push(`Cache read cost: ${formatCost(stats.cacheReadCost)}`);
                if (stats.cacheWriteCost > 0) lines.push(`Cache write cost: ${formatCost(stats.cacheWriteCost)}`);
                if (stats.outputCost > 0) lines.push(`Output cost: ${formatCost(stats.outputCost)}`);
            }
            lines.push(`Total cost: ${formatCost(stats.totalCost)}`);

            const p = stats.pricing;
            if (p) {
                const parts: string[] = [];
                if (p.input !== undefined) parts.push(`in $${p.input}`);
                if (p.output !== undefined) parts.push(`out $${p.output}`);
                if (p.cacheRead !== undefined) parts.push(`cache read $${p.cacheRead}`);
                if (p.cacheWrite !== undefined) parts.push(`cache write $${p.cacheWrite}`);
                if (parts.length > 0) {
                    lines.push('');
                    lines.push(`Pricing / 1M: ${parts.join(' · ')}`);
                }
            }
        }

        // Per-agent breakdown. The status bar only has room for an aggregate;
        // hovering is where you see which agent is doing what.
        const agentList = visibleSubagents();
        if (agentList.length > 0) {
            lines.push('');
            lines.push(agentList.some((a) => !a.done) ? 'Delegated agents:' : 'Recent delegated agents:');
            for (const agent of agentList) {
                const bits: string[] = [];
                if (agent.done) {
                    const elapsed = (agent.finishedAt ?? Date.now()) - agent.startedAt;
                    bits.push(`done in ${formatSeconds(elapsed)}`);
                    if (agent.tokensTotal > 0 && elapsed > 0) {
                        // Average over the whole run, since a finished agent has
                        // no instantaneous rate.
                        bits.push(`${formatTps((agent.tokensTotal / elapsed) * 1000)} t/s avg`);
                    }
                } else if (agent.tps !== null && agent.tps > 0) {
                    bits.push(`${formatTps(agent.tps)} t/s`);
                } else {
                    bits.push('starting');
                }
                if (agent.tokensTotal > 0) bits.push(`~${formatTokens(agent.tokensTotal)} tok`);
                if (!agent.done && agent.firstTextAt !== null) {
                    bits.push(`TTFT ${formatSeconds(agent.firstTextAt - agent.startedAt)}`);
                }
                lines.push(`  ${subagentLabel(agent)} — ${bits.join(' · ')}`);
            }
        }

        lines.push('');
        lines.push('Click for details');
        return lines.join('\n');
    };

    // ---- delegated subagents ------------------------------------------------

    // Subagents belonging to the thread on screen, newest activity first.
    const visibleSubagents = (): SubagentState[] => {
        if (!currentThreadId) return [];
        const now = Date.now();
        const list: SubagentState[] = [];
        for (const state of subagents.values()) {
            if (state.threadId !== currentThreadId) continue;
            // Finished runs stay listed for a while so their final numbers can
            // be read, then drop out.
            if (state.done) {
                const age = now - (state.finishedAt ?? state.startedAt);
                if (age > SUBAGENT_RETENTION_MS) {
                    subagents.delete(state.taskId);
                    continue;
                }
            }
            list.push(state);
        }
        // Running agents first, then most recently finished.
        list.sort((a, b) => {
            if (a.done !== b.done) return a.done ? 1 : -1;
            return (b.finishedAt ?? b.startedAt) - (a.finishedAt ?? a.startedAt);
        });
        return list;
    };

    const activeSubagents = (): SubagentState[] => visibleSubagents().filter((s) => !s.done);

    // Aggregate throughput across running subagents. Reported as a combined
    // figure because several agents stream concurrently and a single number per
    // agent would not fit in the status bar.
    const subagentSummary = (): { running: number; tps: number | null; tokens: number } | null => {
        const running = activeSubagents();
        if (running.length === 0) return null;

        let tps = 0;
        let sawTps = false;
        let tokens = 0;
        for (const state of running) {
            if (state.tps !== null && state.tps > 0) {
                tps += state.tps;
                sawTps = true;
            }
            tokens += state.tokensTotal;
        }
        return { running: running.length, tps: sawTps ? tps : null, tokens };
    };

    const subagentLabel = (state: SubagentState): string => {
        const name = state.label?.trim();
        if (name) return name;
        return state.taskId.slice(0, 8);
    };

    // Streaming emits hundreds of deltas per second; repainting the status bar
    // on every one is wasteful and makes the number jitter unreadably. Coalesce
    // live repaints to ~4/s while letting discrete events paint immediately.
    let statusBarTimer: ReturnType<typeof setTimeout> | null = null;
    const MIN_PAINT_INTERVAL_MS = 250;
    let lastPaintAt = 0;

    const scheduleStatusBarUpdate = () => {
        if (disposed || statusBarTimer) return;
        const since = Date.now() - lastPaintAt;
        const wait = since >= MIN_PAINT_INTERVAL_MS ? 0 : MIN_PAINT_INTERVAL_MS - since;
        statusBarTimer = setTimeout(() => {
            statusBarTimer = null;
            updateStatusBar();
        }, wait);
    };

    const updateStatusBar = () => {
        if (disposed) return;
        lastPaintAt = Date.now();
        const stats = getCurrentStats();
        const { showTokens, showCacheRate, showThroughput, showCost } = getSettings();

        // Agents can be running before this thread has counted any tokens of
        // its own, so they have to keep the bar alive too.
        const hasAgents = visibleSubagents().length > 0;
        if (stats.totalTokens === 0 && !stats.lastTurn && !live && !hasAgents) {
            // Nothing to report yet. Hiding the item entirely beats flashing a
            // placeholder every time the plugin reloads or a thread is opening.
            statusBarItem.hide();
            statusBarItem.tooltip = 'No token usage in this thread yet';
            return;
        }
        statusBarItem.show();

        const segments: string[] = [];

        // Suppress a literal "0 tok" next to a live speed reading — during the
        // first seconds of a turn there is throughput but nothing counted yet,
        // and "0 tok · 160 t/s" reads like a contradiction.
        if (showTokens && stats.totalTokens > 0) segments.push(`${formatTokens(stats.totalTokens)} tok`);

        const rate = cacheRate(stats);
        if (showCacheRate && rate !== null) segments.push(`${formatRate(rate)} cache`);

        // While a turn is streaming, the live sample describes what is
        // happening right now; otherwise fall back to the last turn's value.
        const liveTps = live && live.threadId === currentThreadId ? live.tps : null;
        if (showThroughput) {
            if (liveTps !== null) {
                segments.push(`${formatTps(liveTps)} t/s`);
            } else {
                const tps = currentTps(stats);
                if (tps !== null) {
                    const approximate = isEstimatedTps(stats) || tpsIsFromStream(stats);
                    segments.push(`${formatTps(tps)}${approximate ? '~' : ''} t/s`);
                }
            }
        }

        if (showCost && stats.totalCost > 0) segments.push(formatCost(stats.totalCost));

        // Delegated agents get their own segment so it's obvious the throughput
        // belongs to subagents and not to this thread's own turn.
        const subs = subagentSummary();
        if (showThroughput && subs) {
            const agents = subs.running === 1 ? '1 agent' : `${subs.running} agents`;
            const speed = subs.tps !== null ? ` · ${formatTps(subs.tps)} t/s` : '';
            segments.push(`${agents}${speed}`);
        }

        // With tokens and cost hidden the remaining segments can all be empty
        // (no cache data, no speed yet). Fall back to a neutral marker instead
        // of resurfacing the token count the user asked to hide.
        if (segments.length > 0) {
            statusBarItem.text = segments.join(' · ');
        } else if (showTokens) {
            statusBarItem.text = `${formatTokens(stats.totalTokens)} tok`;
        } else {
            // Every visible segment was toggled off: keep the item out of the
            // way rather than showing a meaningless placeholder.
            statusBarItem.hide();
            statusBarItem.tooltip = buildTooltip(stats);
            return;
        }
        statusBarItem.tooltip = buildTooltip(stats);
    };

    // ---- live streaming throughput (WebSocket) ------------------------------

    const wsUrl = (): string | null => {
        const e = env();
        const port = e.ALMA_PORT;
        if (port) return `ws://127.0.0.1:${port}/ws/threads`;
        if (e.ALMA_API_URL) {
            return `${e.ALMA_API_URL.replace(/^http/, 'ws').replace(/\/+$/, '')}/ws/threads`;
        }
        return null;
    };

    const charsPerTokenFor = (threadId: string): number => {
        const calibrated = threadStats.get(threadId)?.charsPerToken;
        if (calibrated && calibrated > 0.5 && calibrated < 20) return calibrated;
        // CJK-heavy output runs near 1 char/token, English near 4. Split the
        // difference until this thread has a measured sample.
        return DEFAULT_CHARS_PER_TOKEN;
    };

    const ensureLive = (threadId: string): LiveState => {
        if (!live || live.threadId !== threadId) {
            live = {
                threadId,
                startedAt: Date.now(),
                firstTextAt: null,
                window: [],
                tokensThisTurn: 0,
                charsThisTurn: 0,
                tps: null,
                ttftMs: null,
            };
        }
        return live;
    };

    // Rolling-window throughput: tokens emitted in the last LIVE_WINDOW_MS.
    const recordLiveChars = (threadId: string, chars: number) => {
        if (chars <= 0) return;
        const state = ensureLive(threadId);
        const now = Date.now();

        if (state.firstTextAt === null) state.firstTextAt = now;

        state.charsThisTurn += chars;
        const tokens = chars / charsPerTokenFor(threadId);
        state.tokensThisTurn += tokens;
        state.window.push({ t: now, tokens });

        const cutoff = now - LIVE_WINDOW_MS;
        while (state.window.length > 0 && state.window[0].t < cutoff) state.window.shift();

        const tokenSum = state.window.reduce((sum, s) => sum + s.tokens, 0);
        const spanMs = state.window.length > 1 ? now - state.window[0].t : 0;

        // Need at least ~1s of samples before the number means anything;
        // before that, fall back to chars since the first token.
        if (spanMs >= 800) {
            state.tps = (tokenSum / spanMs) * 1000;
        } else if (state.firstTextAt !== null) {
            const elapsed = now - state.firstTextAt;
            if (elapsed >= 400) state.tps = (state.tokensThisTurn / elapsed) * 1000;
        }

        if (state.firstTextAt !== null) state.ttftMs = state.startedAt ? state.firstTextAt - state.startedAt : null;

        scheduleStatusBarUpdate();
    };

    // Ending a turn must not blank the speed segment: hand the measured value
    // to the thread stats first so currentTps() keeps something to show.
    const resetLive = (threadId?: string) => {
        if (threadId && live && live.threadId !== threadId) return;
        if (!live) return;

        const ended = live;
        const stats = threadStats.get(ended.threadId);
        if (stats && ended.tps !== null && ended.tps > 0) {
            threadStats.set(ended.threadId, {
                ...stats,
                streamTps: ended.tps,
                streamTpsAt: Date.now(),
            });
        }
        live = null;
        updateStatusBar();
    };

    /**
     * Which thread the user is looking at, as far as the plugin can tell.
     *
     * thread.activated is authoritative but only fires on an explicit switch,
     * and it can be missed (plugin reloaded mid-thread, app restored with a
     * thread already open). So the plugin also watches for generation activity
     * and adopts a thread the first time it streams — guarded by a short window
     * so a background cron or subagent run cannot steal the bar.
     */
    const adoptThread = (threadId: string) => {
        if (currentThreadId === threadId) return;
        currentThreadId = threadId;
        live = null;
        void refreshServerActiveThread();
        scheduleSync(threadId, 0);
    };

    // Which thread the server considers active, refreshed on a slow cadence and
    // consulted when a delta arrives for a thread we aren't showing. Null means
    // "not known yet", in which case deltas are trusted so the first stream
    // after a reload is not dropped.
    let serverActiveThread: string | null = null;
    // Set once the plugin has seen a real thread.activated. From then on that
    // event is the only thing allowed to move the bar between threads; before
    // it, deltas are the only evidence available and are trusted.
    let sawExplicitSwitch = false;

    const refreshServerActiveThread = async (): Promise<string | null> => {
        try {
            const thread = await context.chat?.getActiveThread?.();
            const id = (thread as { id?: string } | undefined)?.id ?? null;
            serverActiveThread = id;
            return id;
        } catch {
            return serverActiveThread;
        }
    };

    // A delta for another thread is only adopted when it matches the thread the
    // server says is active. That keeps cron runs and subagents from hijacking
    // the bar while still recovering from a missed thread.activated.
    //
    // The cached value can lag behind a switch, so a recent explicit switch wins
    // and a live query settles the rest.
    const pendingConfirmations = new Map<string, Promise<boolean>>();

    const confirmThreadFromServer = (threadId: string): Promise<boolean> => {
        const inFlight = pendingConfirmations.get(threadId);
        if (inFlight) return inFlight;

        const check = refreshServerActiveThread()
            .then((active) => active === threadId)
            .catch(() => false)
            .finally(() => pendingConfirmations.delete(threadId));

        pendingConfirmations.set(threadId, check);
        return check;
    };

    const isThreadConfirmed = (threadId: string): boolean => {
        // An explicit thread.activated always wins over the cached value: it is
        // the user's own action, and currentThreadId already reflects it.
        if (sawExplicitSwitch) return threadId === currentThreadId;
        if (serverActiveThread === null) return true;
        return serverActiveThread === threadId;
    };

    const ensureSubagent = (context: any): SubagentState | null => {
        const taskId: string | undefined = context?.taskId;
        const threadId: string | undefined = context?.threadId;
        if (!taskId || !threadId) return null;

        const existing = subagents.get(taskId);
        if (existing) {
            // A retried run reuses the taskId; treat it as a fresh start.
            if (!existing.done) return existing;
            subagents.delete(taskId);
        }

        const fresh: SubagentState = {
            taskId,
            threadId,
            startedAt: Date.now(),
            firstTextAt: null,
            window: [],
            charsTotal: 0,
            tokensTotal: 0,
            tps: null,
            done: false,
        };
        subagents.set(taskId, fresh);
        return fresh;
    };

    // Subagents emit characters, not tokens. Reuse the parent thread's
    // calibration when it exists, since the same model is usually involved.
    const subagentCharsPerToken = (threadId: string): number => charsPerTokenFor(threadId);

    const handleSubagentDelta = (data: any) => {
        const context = data?.context;
        if (!context) return;

        // Late deltas can arrive after the completion event; reviving a finished
        // run would make it look like it restarted.
        const finished = subagents.get(context.taskId);
        if (finished?.done) return;

        const state = ensureSubagent(context);
        if (!state || state.done) return;

        let chars = 0;
        for (const delta of data?.deltas ?? []) {
            if (delta?.type === 'text_append' && typeof delta.text === 'string') {
                // Reasoning counts toward generation speed the same way it does
                // for the main thread: it is emitted by the same stream.
                chars += delta.text.length;
            }
        }
        if (chars <= 0) return;

        const now = Date.now();
        if (state.firstTextAt === null) state.firstTextAt = now;

        state.charsTotal += chars;
        const tokens = chars / subagentCharsPerToken(state.threadId);
        state.tokensTotal += tokens;

        state.window.push({ t: now, tokens });
        const cutoff = now - LIVE_WINDOW_MS;
        while (state.window.length > 0 && state.window[0].t < cutoff) state.window.shift();

        const tokenSum = state.window.reduce((sum, sample) => sum + sample.tokens, 0);
        const spanMs = state.window.length > 1 ? now - state.window[0].t : 0;

        if (spanMs >= 800) {
            state.tps = (tokenSum / spanMs) * 1000;
        } else if (state.firstTextAt !== null) {
            const elapsed = now - state.firstTextAt;
            if (elapsed >= 400) state.tps = (state.tokensTotal / elapsed) * 1000;
        }

        scheduleStatusBarUpdate();
    };

    const handleSubagentLifecycle = (type: string, data: any) => {
        const context = data?.context;
        if (!context) return;

        if (type === 'subagent_message_added') {
            const state = ensureSubagent(context);
            // The agent's display name lives on the message parts; fall back to
            // the task description if nothing better is available.
            const label =
                data?.message?.parts?.find((p: any) => typeof p?.text === 'string' && p.text.trim())?.text?.slice(0, 40);
            if (state && label && !state.label) state.label = label.replace(/\s+/g, ' ').trim();
            if (state) scheduleStatusBarUpdate();
            return;
        }

        // subagent_message_completed
        const state = subagents.get(context.taskId);
        if (!state) return;
        state.done = true;
        state.finishedAt = Date.now();
        state.tps = null;
        state.window = [];
        scheduleStatusBarUpdate();
    };

    const handleLiveMessage = (raw: string) => {
        let msg: any;
        try {
            msg = JSON.parse(raw);
        } catch {
            return;
        }

        const type = msg?.type;

        // Delegated agents stream on their own channel, tagged with the parent
        // thread, so they never interfere with this thread's own turn metrics.
        if (type === 'subagent_message_added' || type === 'subagent_message_completed') {
            handleSubagentLifecycle(type, msg.data);
            return;
        }

        if (type === 'subagent_message_delta') {
            handleSubagentDelta(msg.data);
            return;
        }

        // A new generation starting somewhere is our cue to begin sampling.
        if (type === 'thread_generating') {
            const data = msg.data ?? {};
            if (data.generating === false && data.threadId === currentThreadId) {
                resetLive(data.threadId);
            }
            return;
        }

        if (type === 'generation_completed' || type === 'generation_stopped') {
            if (msg.data?.threadId === currentThreadId) resetLive(msg.data.threadId);
            return;
        }

        if (type !== 'message_delta') return;

        const data = msg.data;
        if (!data) return;
        const threadId = data.threadId;
        if (!threadId) return;

        // The socket carries deltas for every thread — background work,
        // subagents, cron runs. Sampling those produced nonsense like
        // "0 tok · 171 t/s" on an idle thread, so only the displayed thread counts.
        //
        // The exception is the thread we're unsure about: right after a switch
        // the plugin may not have received thread.activated yet, and refusing
        // the deltas would leave the bar blank for a thread that is plainly
        // streaming. Those deltas are what prove which thread the user is on.
        if (threadId !== currentThreadId) {
            // Adopt freely until the plugin has a thread to show. After that a
            // different thread is only taken over when the switch was explicit
            // or the server confirms it, so background runs stay out.
            const confirmed = isThreadConfirmed(threadId);
            if (currentThreadId !== null && !confirmed) {
                // Fire-and-forget verification; if the server agrees, adopt on
                // the next delta rather than blocking this one.
                if (!pendingConfirmations.has(threadId)) {
                    void confirmThreadFromServer(threadId).then((ok) => {
                        if (ok && !disposed) adoptThread(threadId);
                    });
                }
                return;
            }
            adoptThread(threadId);
        }

        let chars = 0;
        for (const delta of data.deltas ?? []) {
            // Count both visible text and reasoning: they share the token budget.
            if (delta?.type === 'text_append' && typeof delta.text === 'string') {
                chars += delta.text.length;
            }
        }

        if (chars > 0) recordLiveChars(threadId, chars);
    };

    const connectLiveSocket = () => {
        if (disposed) return;
        const url = wsUrl();
        if (!url) return;
        if (liveSocket && (liveSocket.readyState === 0 || liveSocket.readyState === 1)) return;

        try {
            const socket = new WebSocket(url);
            liveSocket = socket;

            socket.onopen = () => {
                liveReconnectDelay = 1000;
            };

            socket.onmessage = (event: MessageEvent) => {
                if (typeof event.data === 'string') handleLiveMessage(event.data);
            };

            socket.onclose = () => {
                liveSocket = null;
                if (disposed) return;
                // Back off quietly; this is a best-effort telemetry channel.
                liveReconnectTimer = setTimeout(connectLiveSocket, liveReconnectDelay);
                liveReconnectDelay = Math.min(liveReconnectDelay * 2, 30_000);
            };

            socket.onerror = () => {
                // onclose follows; nothing else to do.
            };
        } catch (error) {
            logger.debug(`Token Counter: live socket failed (${String(error)})`);
            liveSocket = null;
        }
    };

    const disconnectLiveSocket = () => {
        if (liveReconnectTimer) {
            clearTimeout(liveReconnectTimer);
            liveReconnectTimer = null;
        }
        if (liveSocket) {
            const socket = liveSocket;
            liveSocket = null;
            socket.onclose = null;
            socket.onerror = null;
            socket.onmessage = null;
            try {
                socket.close();
            } catch {
                /* ignore */
            }
        }
    };

    // ---- authoritative metrics from the local API ---------------------------

    interface ApiSnapshot {
        stats: Partial<ThreadStats>;
    }

    const readThreadFromApi = async (threadId: string): Promise<ApiSnapshot | null> => {
        const base = apiBaseUrl();
        if (!base) return null;

        const seq = ++refreshSeq;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 5000);

        try {
            const url = `${base}/api/threads/${encodeURIComponent(threadId)}/messages`;
            const res = await fetch(url, { signal: controller.signal });
            if (!res.ok) return null;

            const payload: unknown = await res.json();
            const rows: any[] = Array.isArray(payload)
                ? (payload as any[])
                : ((payload as any)?.messages ?? []);

            if (seq !== refreshSeq) return null;

            let promptTokens = 0;
            let completionTokens = 0;
            let cachedTokens = 0;
            let cacheWriteTokens = 0;
            let reasoningTokens = 0;
            let totalTokens = 0;
            let turns = 0;

            let outputSum = 0;
            let durationSum = 0;
            let lastTurn: TurnMetrics | undefined;

            for (const row of rows) {
                const usage: ApiUsage | undefined = row?.metadata?.usage;
                if (!usage) continue;
                const hasTokens =
                    usage.inputTokens !== undefined ||
                    usage.outputTokens !== undefined ||
                    usage.totalTokens !== undefined;
                if (!hasTokens) continue;

                turns += 1;
                promptTokens += usage.inputTokens ?? 0;
                completionTokens += usage.outputTokens ?? 0;
                cachedTokens += usage.cachedInputTokens ?? 0;
                cacheWriteTokens += usage.cacheWriteInputTokens ?? 0;
                reasoningTokens += usage.reasoningTokens ?? 0;
                totalTokens += usage.totalTokens ?? (usage.inputTokens ?? 0) + (usage.outputTokens ?? 0);

                if (
                    typeof usage.streamingDurationMs === 'number' &&
                    usage.streamingDurationMs > 0 &&
                    typeof usage.outputTokens === 'number' &&
                    usage.outputTokens > 0
                ) {
                    outputSum += usage.outputTokens;
                    durationSum += usage.streamingDurationMs;
                }

                if (typeof usage.tokensPerSecond === 'number' && usage.tokensPerSecond > 0) {
                    lastTurn = {
                        tps: usage.tokensPerSecond,
                        ttftMs:
                            typeof usage.timeToFirstTokenMs === 'number' ? usage.timeToFirstTokenMs : undefined,
                        streamingMs:
                            typeof usage.streamingDurationMs === 'number'
                                ? usage.streamingDurationMs
                                : undefined,
                    };
                }
            }

            return {
                stats: {
                    promptTokens,
                    completionTokens,
                    cachedTokens,
                    cacheWriteTokens,
                    reasoningTokens,
                    totalTokens,
                    turns,
                    lastTurn,
                    avgTps: durationSum > 0 ? (outputSum / durationSum) * 1000 : undefined,
                },
            };
        } catch (error) {
            logger.debug(`Token Counter: local API unavailable (${String(error)})`);
            return null;
        } finally {
            clearTimeout(timer);
        }
    };

    const syncFromApi = async (threadId: string) => {
        const snapshot = await readThreadFromApi(threadId);
        if (disposed || !snapshot) {
            if (!disposed) updateStatusBar();
            return;
        }

        const existing = threadStats.get(threadId) || createEmptyStats();
        const incoming = snapshot.stats;

        // Event deltas are authoritative for token totals. The persisted
        // snapshot can lag behind a turn that is still being written, so it is
        // only allowed to refine totals when event stats are empty or smaller.
        const apiTotal = incoming.totalTokens ?? 0;
        const useApiTotals = existing.totalTokens === 0 || apiTotal >= existing.totalTokens;

        const merged: ThreadStats = {
            ...existing,
            ...(useApiTotals
                ? {
                      promptTokens: incoming.promptTokens ?? existing.promptTokens,
                      completionTokens: incoming.completionTokens ?? existing.completionTokens,
                      cachedTokens: incoming.cachedTokens ?? existing.cachedTokens,
                      cacheWriteTokens: incoming.cacheWriteTokens ?? existing.cacheWriteTokens,
                      reasoningTokens: incoming.reasoningTokens ?? existing.reasoningTokens,
                      totalTokens: apiTotal,
                      turns: incoming.turns ?? existing.turns,
                  }
                : {}),
            // Speed metrics always come from the API — the event payload has none.
            lastTurn: incoming.lastTurn ?? existing.lastTurn,
            avgTps: incoming.avgTps ?? existing.avgTps,
            pricing: existing.pricing,
        };

        threadStats.set(threadId, withRecalculatedCost(merged));
        updateStatusBar();
    };

    const scheduleSync = (threadId: string, delayMs = 400) => {
        if (refreshTimer) clearTimeout(refreshTimer);
        refreshTimer = setTimeout(() => {
            refreshTimer = null;
            if (!disposed) void syncFromApi(threadId);
        }, delayMs);
    };

    // ---- events -------------------------------------------------------------

    const willSendDisposable = events.on('chat.message.willSend', () => {
        turnStartedAt = Date.now();
    });

    const messageDisposable = events.on('chat.message.didReceive', (input, _output) => {
        const { threadId, pricing } = input;
        // Only adopt this thread as "current" if nothing is being displayed
        // yet. A cron run or a background thread finishing must not hijack the
        // bar away from the thread the user is actually reading.
        if (!currentThreadId) currentThreadId = threadId;

        const usage = input.response?.usage as
            | (typeof input.response.usage & {
                  cacheWriteInputTokens?: number;
                  reasoningTokens?: number;
              })
            | undefined;

        if (!usage) {
            updateStatusBar();
            return;
        }

        const existing = threadStats.get(threadId) || createEmptyStats();
        const currentPricing = pricing || existing.pricing;

        // Optimistic accumulation so the status bar reacts immediately.
        const optimistic: ThreadStats = {
            ...existing,
            promptTokens: existing.promptTokens + usage.promptTokens,
            completionTokens: existing.completionTokens + usage.completionTokens,
            cachedTokens: existing.cachedTokens + (usage.cachedInputTokens ?? 0),
            cacheWriteTokens: existing.cacheWriteTokens + (usage.cacheWriteInputTokens ?? 0),
            reasoningTokens: existing.reasoningTokens + (usage.reasoningTokens ?? 0),
            totalTokens: existing.totalTokens + usage.totalTokens,
            turns: existing.turns + 1,
            pricing: currentPricing,
        };

        // Wall-clock throughput estimate; replaced by the authoritative value.
        if (turnStartedAt !== null && usage.completionTokens > 0) {
            const seconds = Math.max(0.05, (Date.now() - turnStartedAt) / 1000);
            optimistic.lastTurn = { estimatedTps: usage.completionTokens / seconds };
        }
        turnStartedAt = null;

        threadStats.set(threadId, withRecalculatedCost(optimistic));
        // Background threads update their stats silently; repaint only when the
        // finished turn belongs to the thread on screen.
        if (threadId === currentThreadId) updateStatusBar();

        // Replace with authoritative tokens/s, TTFT and reasoning counts.
        scheduleSync(threadId);

        // The turn is over: keep the stream measurement as a bridge until the
        // persisted usage record lands, and recalibrate chars-per-token.
        if (live && live.threadId === threadId) {
            const merged = threadStats.get(threadId);
            if (merged) {
                const patch: Partial<ThreadStats> = {};
                if (usage.completionTokens > 0 && live.charsThisTurn > 0) {
                    patch.charsPerToken = live.charsThisTurn / usage.completionTokens;
                }
                if (live.tps !== null && live.tps > 0) {
                    patch.streamTps = live.tps;
                    patch.streamTpsAt = Date.now();
                }
                if (Object.keys(patch).length > 0) {
                    threadStats.set(threadId, { ...merged, ...patch });
                }
            }
            live = null;
            updateStatusBar();
        }
    });

    const threadDisposable = events.on('thread.activated', (input, _output) => {
        const { threadId, usage, pricing } = input;
        // Switching threads invalidates any in-flight live sample.
        if (live && live.threadId !== threadId) live = null;
        currentThreadId = threadId;
        serverActiveThread = threadId;
        sawExplicitSwitch = true;

        if (usage) {
            const base: ThreadStats = {
                ...createEmptyStats(),
                promptTokens: usage.promptTokens,
                completionTokens: usage.completionTokens,
                cachedTokens: usage.cachedInputTokens ?? 0,
                totalTokens: usage.totalTokens,
                pricing,
            };
            threadStats.set(threadId, withRecalculatedCost(base));
        }

        updateStatusBar();
        // Pull per-turn speed history for the newly opened thread.
        scheduleSync(threadId, 50);
    });

    const showDetails = async () => {
        const stats = getCurrentStats();
        const agentList = visibleSubagents();
        if (stats.totalTokens === 0 && !stats.lastTurn && agentList.length === 0) {
            ui.showNotification('No token usage in this thread yet', { type: 'info' });
            return;
        }

        const rate = cacheRate(stats);
        const tps = currentTps(stats);

        const items: Array<{ label: string; description: string }> = [
            { label: 'Total tokens', description: formatExact(stats.totalTokens) },
            { label: 'Input', description: formatExact(stats.promptTokens) },
            { label: 'Output', description: formatExact(stats.completionTokens) },
        ];

        if (stats.reasoningTokens > 0) {
            items.push({ label: 'of which reasoning', description: formatExact(stats.reasoningTokens) });
        }
        if (rate !== null) {
            items.push({
                label: 'Prompt cache hit',
                description: `${rate.toFixed(1)}%  (${formatExact(stats.cachedTokens)} of ${formatExact(stats.promptTokens)})`,
            });
        }
        if (tps !== null) {
            items.push({
                label: 'Last turn speed',
                description: `${formatTps(tps)} tok/s${isEstimatedTps(stats) ? ' (estimated)' : ''}`,
            });
        }
        if (typeof stats.avgTps === 'number' && stats.avgTps > 0) {
            items.push({ label: 'Average speed', description: `${formatTps(stats.avgTps)} tok/s` });
        }
        if (typeof stats.lastTurn?.ttftMs === 'number') {
            items.push({ label: 'Time to first token', description: formatSeconds(stats.lastTurn.ttftMs) });
        }
        if (stats.totalCost > 0) {
            const parts: string[] = [];
            if (stats.inputCost > 0) parts.push(`in ${formatCost(stats.inputCost)}`);
            if (stats.cacheReadCost > 0) parts.push(`cache ${formatCost(stats.cacheReadCost)}`);
            if (stats.outputCost > 0) parts.push(`out ${formatCost(stats.outputCost)}`);
            items.push({
                label: 'Estimated cost',
                description: parts.length > 0 ? `${formatCost(stats.totalCost)}  (${parts.join(' · ')})` : formatCost(stats.totalCost),
            });
        }

        // Delegated agents: the status bar only carries an aggregate.
        for (const agent of agentList) {
            const bits: string[] = [];
            if (agent.done) {
                bits.push(`done in ${formatSeconds((agent.finishedAt ?? Date.now()) - agent.startedAt)}`);
            } else if (agent.tps !== null && agent.tps > 0) {
                bits.push(`${formatTps(agent.tps)} tok/s`);
            } else {
                bits.push('starting');
            }
            if (agent.tokensTotal > 0) bits.push(`~${formatTokens(agent.tokensTotal)} tok`);
            items.push({ label: `Agent · ${subagentLabel(agent)}`, description: bits.join(' · ') });
        }

        await ui.showQuickPick(items, { title: 'Token usage — this thread' });
    };

    const commandDisposable = context.commands.register('showDetails', {
        title: 'Token Counter: Show Details',
        description: 'Show token, cache hit rate, throughput and cost details',
        handler: showDetails,
    });

    // Status bar clicks are dispatched through the fully qualified command id.
    statusBarItem.command = `${context.id}.showDetails`;
    // updateStatusBar decides whether there is anything worth showing; the
    // initial paint must not force the item visible.
    updateStatusBar();

    // Stream throughput updates live; the HTTP endpoint only has finished turns.
    connectLiveSocket();

    // A plugin reload (or app start) happens with a thread already open and no
    // fresh thread.activated event, which would leave the bar stuck at zero.
    // Prime the bar on activation. This is only a best guess — getActiveThread
    // can point at a background thread — so it deliberately does NOT mark the
    // thread as confirmed. If a different thread turns out to be the one
    // streaming, the delta handler will adopt it.
    const restoreActiveThread = async () => {
        try {
            const thread = await context.chat?.getActiveThread?.();
            const threadId = (thread as { id?: string } | undefined)?.id;
            if (threadId) serverActiveThread = threadId;
            if (!threadId || disposed || currentThreadId) return;
            currentThreadId = threadId;
            await syncFromApi(threadId);
        } catch (error) {
            logger.debug(`Token Counter: could not restore active thread (${String(error)})`);
        }
    };
    void restoreActiveThread();

    // The plugin cannot observe thread switches directly; ask the server who is
    // active on a slow cadence as a fallback, so a missed thread.activated does
    // not pin the bar to a stale thread forever.
    let activePollTimer: ReturnType<typeof setInterval> | null = null;
    activePollTimer = setInterval(async () => {
        if (disposed) return;
        const threadId = await refreshServerActiveThread();
        if (!threadId || disposed || threadId === currentThreadId) return;
        // Only follow the reported active thread when nothing is streaming
        // here, so a live sample is never yanked away mid-turn.
        if (live) return;
        currentThreadId = threadId;
        // The server confirmed this switch, so trust it the same way an
        // explicit thread.activated would be trusted.
        sawExplicitSwitch = true;
        void scheduleSync(threadId, 0);
    }, 5000);

    const settingsDisposable = settings.onDidChange(() => {
        for (const [threadId, stats] of threadStats.entries()) {
            threadStats.set(threadId, withRecalculatedCost(stats));
        }
        updateStatusBar();
    });

    return {
        dispose: () => {
            disposed = true;
            logger.info('Token Counter plugin deactivated');
            if (refreshTimer) clearTimeout(refreshTimer);
            if (statusBarTimer) clearTimeout(statusBarTimer);
            if (activePollTimer) clearInterval(activePollTimer);
            disconnectLiveSocket();
            willSendDisposable.dispose();
            messageDisposable.dispose();
            threadDisposable.dispose();
            settingsDisposable.dispose();
            commandDisposable.dispose();
            statusBarItem.dispose();
        },
    };
}
