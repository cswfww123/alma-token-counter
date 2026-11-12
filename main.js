// main.ts
async function activate(context) {
  const { logger, events, ui, settings } = context;
  logger.info("Token Counter Plus activated!");
  const statusBarItem = ui.createStatusBarItem({
    id: "token-counter-plus",
    alignment: "right",
    priority: 100
  });
  const threadStats = new Map;
  let currentThreadId = null;
  let disposed = false;
  let turnStartedAt = null;
  let refreshTimer = null;
  let refreshSeq = 0;
  let live = null;
  let liveSocket = null;
  let liveReconnectTimer = null;
  let liveReconnectDelay = 1000;
  const LIVE_WINDOW_MS = 3000;
  const DEFAULT_CHARS_PER_TOKEN = 3.5;
  const subagents = new Map;
  const SUBAGENT_RETENTION_MS = 60000;
  const env = () => {
    const g = globalThis;
    return g.process?.env ?? {};
  };
  const apiBaseUrl = () => {
    const e = env();
    if (e.ALMA_PORT)
      return `http://127.0.0.1:${e.ALMA_PORT}`;
    if (e.ALMA_API_URL)
      return e.ALMA_API_URL.replace(/\/+$/, "");
    return null;
  };
  const getSettings = () => ({
    showTokens: settings.get("tokenCounter.showTokens", false),
    showCacheRate: settings.get("tokenCounter.showCacheRate", true),
    showThroughput: settings.get("tokenCounter.showThroughput", true),
    showCost: settings.get("tokenCounter.showCost", false),
    fallbackCostPerMillion: settings.get("tokenCounter.costPerMillionTokens", settings.get("tokenCounter.fallbackCostPerMillion", 3))
  });
  const createEmptyStats = () => ({
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
    turns: 0
  });
  const getCurrentStats = () => {
    if (!currentThreadId)
      return createEmptyStats();
    return threadStats.get(currentThreadId) || createEmptyStats();
  };
  const calculateCost = (stats, pricing) => {
    const { fallbackCostPerMillion } = getSettings();
    if (!pricing) {
      const totalCost = (stats.promptTokens + stats.completionTokens) / 1e6 * fallbackCostPerMillion;
      return {
        inputCost: totalCost / 2,
        outputCost: totalCost / 2,
        cacheReadCost: 0,
        cacheWriteCost: 0,
        totalCost
      };
    }
    const inputCost = pricing.input ? stats.promptTokens / 1e6 * pricing.input : 0;
    const outputCost = pricing.output ? stats.completionTokens / 1e6 * pricing.output : 0;
    const cacheReadCost = pricing.cacheRead ? stats.cachedTokens / 1e6 * pricing.cacheRead : 0;
    const cacheWriteCost = pricing.cacheWrite ? stats.cacheWriteTokens / 1e6 * pricing.cacheWrite : 0;
    return {
      inputCost,
      outputCost,
      cacheReadCost,
      cacheWriteCost,
      totalCost: inputCost + outputCost + cacheReadCost + cacheWriteCost
    };
  };
  const withRecalculatedCost = (stats) => ({
    ...stats,
    ...calculateCost(stats, stats.pricing)
  });
  const formatTokens = (num) => {
    if (!Number.isFinite(num) || num <= 0)
      return "0";
    if (num >= 1e6)
      return `${(num / 1e6).toFixed(2)}M`;
    if (num >= 1e4)
      return `${(num / 1000).toFixed(1)}k`;
    return Math.round(num).toLocaleString();
  };
  const formatExact = (num) => Math.round(Number.isFinite(num) ? num : 0).toLocaleString();
  const formatCost = (num) => {
    if (num >= 1)
      return `$${num.toFixed(3)}`;
    if (num >= 0.01)
      return `$${num.toFixed(4)}`;
    return `$${num.toFixed(5)}`;
  };
  const formatSeconds = (ms) => `${(ms / 1000).toFixed(ms >= 60000 ? 1 : 2)}s`;
  const formatTps = (tps) => tps >= 100 ? tps.toFixed(0) : tps.toFixed(tps >= 10 ? 1 : 2);
  const cacheRate = (stats) => {
    if (stats.promptTokens <= 0)
      return null;
    const rate = stats.cachedTokens / stats.promptTokens * 100;
    return Math.max(0, Math.min(100, rate));
  };
  const MAX_PLAUSIBLE_TPS = 2000;
  const currentTps = (stats) => {
    const last = stats.lastTurn;
    if (last) {
      if (typeof last.tps === "number" && last.tps > 0)
        return last.tps;
      if (typeof last.estimatedTps === "number" && last.estimatedTps > 0 && last.estimatedTps <= MAX_PLAUSIBLE_TPS) {
        return last.estimatedTps;
      }
    }
    if (streamTpsIsFresh(stats))
      return stats.streamTps;
    return null;
  };
  const STREAM_TPS_TTL_MS = 5 * 60000;
  const streamTpsIsFresh = (stats) => {
    if (typeof stats.streamTps !== "number" || stats.streamTps <= 0)
      return false;
    if (typeof stats.streamTpsAt !== "number")
      return true;
    return Date.now() - stats.streamTpsAt < STREAM_TPS_TTL_MS;
  };
  const tpsIsFromStream = (stats) => !(typeof stats.lastTurn?.tps === "number" && stats.lastTurn.tps > 0) && streamTpsIsFresh(stats);
  const isEstimatedTps = (stats) => !(typeof stats.lastTurn?.tps === "number" && stats.lastTurn.tps > 0) && typeof stats.lastTurn?.estimatedTps === "number" && stats.lastTurn.estimatedTps > 0 && stats.lastTurn.estimatedTps <= MAX_PLAUSIBLE_TPS;
  const formatRate = (rate) => {
    if (rate >= 99.9 || rate <= 0)
      return `${Math.round(rate)}%`;
    if (rate >= 99)
      return `${rate.toFixed(1)}%`;
    return `${Math.round(rate)}%`;
  };
  const buildTooltip = (stats) => {
    const { showCost } = getSettings();
    const lines = [];
    const rate = cacheRate(stats);
    if (stats.cachedTokens > 0 || stats.cacheWriteTokens > 0) {
      const extra = [];
      if (stats.cachedTokens > 0)
        extra.push(`cached ${formatExact(stats.cachedTokens)}`);
      if (stats.cacheWriteTokens > 0)
        extra.push(`written ${formatExact(stats.cacheWriteTokens)}`);
      lines.push(`Input: ${formatExact(stats.promptTokens)} (${extra.join(", ")})`);
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
      let line = liveForThread?.tps ? `Last turn: ${formatTps(tps)} tok/s` : `Speed: ${formatTps(tps)} tok/s`;
      if (tpsIsFromStream(stats))
        line += " (measured from stream)";
      else if (isEstimatedTps(stats))
        line += " (estimated)";
      if (typeof stats.avgTps === "number" && stats.avgTps > 0) {
        line += `  ·  avg ${formatTps(stats.avgTps)} tok/s`;
      }
      lines.push(line);
    }
    if (stats.lastTurn) {
      const timing = [];
      if (typeof stats.lastTurn.ttftMs === "number")
        timing.push(`TTFT ${formatSeconds(stats.lastTurn.ttftMs)}`);
      if (typeof stats.lastTurn.streamingMs === "number") {
        timing.push(`stream ${formatSeconds(stats.lastTurn.streamingMs)}`);
      }
      if (timing.length > 0)
        lines.push(timing.join("  ·  "));
    }
    if (showCost && stats.totalCost > 0) {
      lines.push("");
      if (stats.pricing) {
        if (stats.inputCost > 0)
          lines.push(`Input cost: ${formatCost(stats.inputCost)}`);
        if (stats.cacheReadCost > 0)
          lines.push(`Cache read cost: ${formatCost(stats.cacheReadCost)}`);
        if (stats.cacheWriteCost > 0)
          lines.push(`Cache write cost: ${formatCost(stats.cacheWriteCost)}`);
        if (stats.outputCost > 0)
          lines.push(`Output cost: ${formatCost(stats.outputCost)}`);
      }
      lines.push(`Total cost: ${formatCost(stats.totalCost)}`);
      const p = stats.pricing;
      if (p) {
        const parts = [];
        if (p.input !== undefined)
          parts.push(`in $${p.input}`);
        if (p.output !== undefined)
          parts.push(`out $${p.output}`);
        if (p.cacheRead !== undefined)
          parts.push(`cache read $${p.cacheRead}`);
        if (p.cacheWrite !== undefined)
          parts.push(`cache write $${p.cacheWrite}`);
        if (parts.length > 0) {
          lines.push("");
          lines.push(`Pricing / 1M: ${parts.join(" · ")}`);
        }
      }
    }
    const agentList = visibleSubagents();
    if (agentList.length > 0) {
      lines.push("");
      lines.push(agentList.some((a) => !a.done) ? "Delegated agents:" : "Recent delegated agents:");
      for (const agent of agentList) {
        const bits = [];
        if (agent.done) {
          const elapsed = (agent.finishedAt ?? Date.now()) - agent.startedAt;
          bits.push(`done in ${formatSeconds(elapsed)}`);
          if (agent.tokensTotal > 0 && elapsed > 0) {
            bits.push(`${formatTps(agent.tokensTotal / elapsed * 1000)} t/s avg`);
          }
        } else if (agent.tps !== null && agent.tps > 0) {
          bits.push(`${formatTps(agent.tps)} t/s`);
        } else {
          bits.push("starting");
        }
        if (agent.tokensTotal > 0)
          bits.push(`~${formatTokens(agent.tokensTotal)} tok`);
        if (!agent.done && agent.firstTextAt !== null) {
          bits.push(`TTFT ${formatSeconds(agent.firstTextAt - agent.startedAt)}`);
        }
        lines.push(`  ${subagentLabel(agent)} — ${bits.join(" · ")}`);
      }
    }
    lines.push("");
    lines.push("Click for details");
    return lines.join(`
`);
  };
  const visibleSubagents = () => {
    if (!currentThreadId)
      return [];
    const now = Date.now();
    const list = [];
    for (const state of subagents.values()) {
      if (state.threadId !== currentThreadId)
        continue;
      if (state.done) {
        const age = now - (state.finishedAt ?? state.startedAt);
        if (age > SUBAGENT_RETENTION_MS) {
          subagents.delete(state.taskId);
          continue;
        }
      }
      list.push(state);
    }
    list.sort((a, b) => {
      if (a.done !== b.done)
        return a.done ? 1 : -1;
      return (b.finishedAt ?? b.startedAt) - (a.finishedAt ?? a.startedAt);
    });
    return list;
  };
  const activeSubagents = () => visibleSubagents().filter((s) => !s.done);
  const subagentSummary = () => {
    const running = activeSubagents();
    if (running.length === 0)
      return null;
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
  const subagentLabel = (state) => {
    const name = state.label?.trim();
    if (name)
      return name;
    return state.taskId.slice(0, 8);
  };
  let statusBarTimer = null;
  const MIN_PAINT_INTERVAL_MS = 250;
  let lastPaintAt = 0;
  const scheduleStatusBarUpdate = () => {
    if (disposed || statusBarTimer)
      return;
    const since = Date.now() - lastPaintAt;
    const wait = since >= MIN_PAINT_INTERVAL_MS ? 0 : MIN_PAINT_INTERVAL_MS - since;
    statusBarTimer = setTimeout(() => {
      statusBarTimer = null;
      updateStatusBar();
    }, wait);
  };
  const updateStatusBar = () => {
    if (disposed)
      return;
    lastPaintAt = Date.now();
    const stats = getCurrentStats();
    const { showTokens, showCacheRate, showThroughput, showCost } = getSettings();
    const hasAgents = visibleSubagents().length > 0;
    if (stats.totalTokens === 0 && !stats.lastTurn && !live && !hasAgents) {
      statusBarItem.text = showTokens ? "Tokens: 0" : "…";
      statusBarItem.tooltip = "No token usage in this thread yet";
      return;
    }
    const segments = [];
    if (showTokens && stats.totalTokens > 0)
      segments.push(`${formatTokens(stats.totalTokens)} tok`);
    const rate = cacheRate(stats);
    if (showCacheRate && rate !== null)
      segments.push(`${formatRate(rate)} cache`);
    const liveTps = live && live.threadId === currentThreadId ? live.tps : null;
    if (showThroughput) {
      if (liveTps !== null) {
        segments.push(`${formatTps(liveTps)} t/s`);
      } else {
        const tps = currentTps(stats);
        if (tps !== null) {
          const approximate = isEstimatedTps(stats) || tpsIsFromStream(stats);
          segments.push(`${formatTps(tps)}${approximate ? "~" : ""} t/s`);
        }
      }
    }
    if (showCost && stats.totalCost > 0)
      segments.push(formatCost(stats.totalCost));
    const subs = subagentSummary();
    if (showThroughput && subs) {
      const agents = subs.running === 1 ? "1 agent" : `${subs.running} agents`;
      const speed = subs.tps !== null ? ` · ${formatTps(subs.tps)} t/s` : "";
      segments.push(`${agents}${speed}`);
    }
    if (segments.length > 0) {
      statusBarItem.text = segments.join(" · ");
    } else {
      statusBarItem.text = showTokens ? `${formatTokens(stats.totalTokens)} tok` : "…";
    }
    statusBarItem.tooltip = buildTooltip(stats);
  };
  const wsUrl = () => {
    const e = env();
    const port = e.ALMA_PORT;
    if (port)
      return `ws://127.0.0.1:${port}/ws/threads`;
    if (e.ALMA_API_URL) {
      return `${e.ALMA_API_URL.replace(/^http/, "ws").replace(/\/+$/, "")}/ws/threads`;
    }
    return null;
  };
  const charsPerTokenFor = (threadId) => {
    const calibrated = threadStats.get(threadId)?.charsPerToken;
    if (calibrated && calibrated > 0.5 && calibrated < 20)
      return calibrated;
    return DEFAULT_CHARS_PER_TOKEN;
  };
  const ensureLive = (threadId) => {
    if (!live || live.threadId !== threadId) {
      live = {
        threadId,
        startedAt: Date.now(),
        firstTextAt: null,
        window: [],
        tokensThisTurn: 0,
        charsThisTurn: 0,
        tps: null,
        ttftMs: null
      };
    }
    return live;
  };
  const recordLiveChars = (threadId, chars) => {
    if (chars <= 0)
      return;
    const state = ensureLive(threadId);
    const now = Date.now();
    if (state.firstTextAt === null)
      state.firstTextAt = now;
    state.charsThisTurn += chars;
    const tokens = chars / charsPerTokenFor(threadId);
    state.tokensThisTurn += tokens;
    state.window.push({ t: now, tokens });
    const cutoff = now - LIVE_WINDOW_MS;
    while (state.window.length > 0 && state.window[0].t < cutoff)
      state.window.shift();
    const tokenSum = state.window.reduce((sum, s) => sum + s.tokens, 0);
    const spanMs = state.window.length > 1 ? now - state.window[0].t : 0;
    if (spanMs >= 800) {
      state.tps = tokenSum / spanMs * 1000;
    } else if (state.firstTextAt !== null) {
      const elapsed = now - state.firstTextAt;
      if (elapsed >= 400)
        state.tps = state.tokensThisTurn / elapsed * 1000;
    }
    if (state.firstTextAt !== null)
      state.ttftMs = state.startedAt ? state.firstTextAt - state.startedAt : null;
    scheduleStatusBarUpdate();
  };
  const resetLive = (threadId) => {
    if (threadId && live && live.threadId !== threadId)
      return;
    if (!live)
      return;
    const ended = live;
    const stats = threadStats.get(ended.threadId);
    if (stats && ended.tps !== null && ended.tps > 0) {
      threadStats.set(ended.threadId, {
        ...stats,
        streamTps: ended.tps,
        streamTpsAt: Date.now()
      });
    }
    live = null;
    updateStatusBar();
  };
  const ensureSubagent = (context2) => {
    const taskId = context2?.taskId;
    const threadId = context2?.threadId;
    if (!taskId || !threadId)
      return null;
    const existing = subagents.get(taskId);
    if (existing) {
      if (!existing.done)
        return existing;
      subagents.delete(taskId);
    }
    const fresh = {
      taskId,
      threadId,
      startedAt: Date.now(),
      firstTextAt: null,
      window: [],
      charsTotal: 0,
      tokensTotal: 0,
      tps: null,
      done: false
    };
    subagents.set(taskId, fresh);
    return fresh;
  };
  const subagentCharsPerToken = (threadId) => charsPerTokenFor(threadId);
  const handleSubagentDelta = (data) => {
    const context2 = data?.context;
    if (!context2)
      return;
    const finished = subagents.get(context2.taskId);
    if (finished?.done)
      return;
    const state = ensureSubagent(context2);
    if (!state || state.done)
      return;
    let chars = 0;
    for (const delta of data?.deltas ?? []) {
      if (delta?.type === "text_append" && typeof delta.text === "string") {
        chars += delta.text.length;
      }
    }
    if (chars <= 0)
      return;
    const now = Date.now();
    if (state.firstTextAt === null)
      state.firstTextAt = now;
    state.charsTotal += chars;
    const tokens = chars / subagentCharsPerToken(state.threadId);
    state.tokensTotal += tokens;
    state.window.push({ t: now, tokens });
    const cutoff = now - LIVE_WINDOW_MS;
    while (state.window.length > 0 && state.window[0].t < cutoff)
      state.window.shift();
    const tokenSum = state.window.reduce((sum, sample) => sum + sample.tokens, 0);
    const spanMs = state.window.length > 1 ? now - state.window[0].t : 0;
    if (spanMs >= 800) {
      state.tps = tokenSum / spanMs * 1000;
    } else if (state.firstTextAt !== null) {
      const elapsed = now - state.firstTextAt;
      if (elapsed >= 400)
        state.tps = state.tokensTotal / elapsed * 1000;
    }
    scheduleStatusBarUpdate();
  };
  const handleSubagentLifecycle = (type, data) => {
    const context2 = data?.context;
    if (!context2)
      return;
    if (type === "subagent_message_added") {
      const state2 = ensureSubagent(context2);
      const label = data?.message?.parts?.find((p) => typeof p?.text === "string" && p.text.trim())?.text?.slice(0, 40);
      if (state2 && label && !state2.label)
        state2.label = label.replace(/\s+/g, " ").trim();
      if (state2)
        scheduleStatusBarUpdate();
      return;
    }
    const state = subagents.get(context2.taskId);
    if (!state)
      return;
    state.done = true;
    state.finishedAt = Date.now();
    state.tps = null;
    state.window = [];
    scheduleStatusBarUpdate();
  };
  const handleLiveMessage = (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    const type = msg?.type;
    if (type === "subagent_message_added" || type === "subagent_message_completed") {
      handleSubagentLifecycle(type, msg.data);
      return;
    }
    if (type === "subagent_message_delta") {
      handleSubagentDelta(msg.data);
      return;
    }
    if (type === "thread_generating") {
      const data2 = msg.data ?? {};
      if (data2.generating === false && data2.threadId === currentThreadId) {
        resetLive(data2.threadId);
      }
      return;
    }
    if (type === "generation_completed" || type === "generation_stopped") {
      if (msg.data?.threadId === currentThreadId)
        resetLive(msg.data.threadId);
      return;
    }
    if (type !== "message_delta")
      return;
    const data = msg.data;
    if (!data)
      return;
    const threadId = data.threadId;
    if (!threadId)
      return;
    if (threadId !== currentThreadId)
      return;
    let chars = 0;
    for (const delta of data.deltas ?? []) {
      if (delta?.type === "text_append" && typeof delta.text === "string") {
        chars += delta.text.length;
      }
    }
    if (chars > 0)
      recordLiveChars(threadId, chars);
  };
  const connectLiveSocket = () => {
    if (disposed)
      return;
    const url = wsUrl();
    if (!url)
      return;
    if (liveSocket && (liveSocket.readyState === 0 || liveSocket.readyState === 1))
      return;
    try {
      const socket = new WebSocket(url);
      liveSocket = socket;
      socket.onopen = () => {
        liveReconnectDelay = 1000;
      };
      socket.onmessage = (event) => {
        if (typeof event.data === "string")
          handleLiveMessage(event.data);
      };
      socket.onclose = () => {
        liveSocket = null;
        if (disposed)
          return;
        liveReconnectTimer = setTimeout(connectLiveSocket, liveReconnectDelay);
        liveReconnectDelay = Math.min(liveReconnectDelay * 2, 30000);
      };
      socket.onerror = () => {};
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
      } catch {}
    }
  };
  const readThreadFromApi = async (threadId) => {
    const base = apiBaseUrl();
    if (!base)
      return null;
    const seq = ++refreshSeq;
    const controller = new AbortController;
    const timer = setTimeout(() => controller.abort(), 5000);
    try {
      const url = `${base}/api/threads/${encodeURIComponent(threadId)}/messages`;
      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok)
        return null;
      const payload = await res.json();
      const rows = Array.isArray(payload) ? payload : payload?.messages ?? [];
      if (seq !== refreshSeq)
        return null;
      let promptTokens = 0;
      let completionTokens = 0;
      let cachedTokens = 0;
      let cacheWriteTokens = 0;
      let reasoningTokens = 0;
      let totalTokens = 0;
      let turns = 0;
      let outputSum = 0;
      let durationSum = 0;
      let lastTurn;
      for (const row of rows) {
        const usage = row?.metadata?.usage;
        if (!usage)
          continue;
        const hasTokens = usage.inputTokens !== undefined || usage.outputTokens !== undefined || usage.totalTokens !== undefined;
        if (!hasTokens)
          continue;
        turns += 1;
        promptTokens += usage.inputTokens ?? 0;
        completionTokens += usage.outputTokens ?? 0;
        cachedTokens += usage.cachedInputTokens ?? 0;
        cacheWriteTokens += usage.cacheWriteInputTokens ?? 0;
        reasoningTokens += usage.reasoningTokens ?? 0;
        totalTokens += usage.totalTokens ?? (usage.inputTokens ?? 0) + (usage.outputTokens ?? 0);
        if (typeof usage.streamingDurationMs === "number" && usage.streamingDurationMs > 0 && typeof usage.outputTokens === "number" && usage.outputTokens > 0) {
          outputSum += usage.outputTokens;
          durationSum += usage.streamingDurationMs;
        }
        if (typeof usage.tokensPerSecond === "number" && usage.tokensPerSecond > 0) {
          lastTurn = {
            tps: usage.tokensPerSecond,
            ttftMs: typeof usage.timeToFirstTokenMs === "number" ? usage.timeToFirstTokenMs : undefined,
            streamingMs: typeof usage.streamingDurationMs === "number" ? usage.streamingDurationMs : undefined
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
          avgTps: durationSum > 0 ? outputSum / durationSum * 1000 : undefined
        }
      };
    } catch (error) {
      logger.debug(`Token Counter: local API unavailable (${String(error)})`);
      return null;
    } finally {
      clearTimeout(timer);
    }
  };
  const syncFromApi = async (threadId) => {
    const snapshot = await readThreadFromApi(threadId);
    if (disposed || !snapshot) {
      if (!disposed)
        updateStatusBar();
      return;
    }
    const existing = threadStats.get(threadId) || createEmptyStats();
    const incoming = snapshot.stats;
    const apiTotal = incoming.totalTokens ?? 0;
    const useApiTotals = existing.totalTokens === 0 || apiTotal >= existing.totalTokens;
    const merged = {
      ...existing,
      ...useApiTotals ? {
        promptTokens: incoming.promptTokens ?? existing.promptTokens,
        completionTokens: incoming.completionTokens ?? existing.completionTokens,
        cachedTokens: incoming.cachedTokens ?? existing.cachedTokens,
        cacheWriteTokens: incoming.cacheWriteTokens ?? existing.cacheWriteTokens,
        reasoningTokens: incoming.reasoningTokens ?? existing.reasoningTokens,
        totalTokens: apiTotal,
        turns: incoming.turns ?? existing.turns
      } : {},
      lastTurn: incoming.lastTurn ?? existing.lastTurn,
      avgTps: incoming.avgTps ?? existing.avgTps,
      pricing: existing.pricing
    };
    threadStats.set(threadId, withRecalculatedCost(merged));
    updateStatusBar();
  };
  const scheduleSync = (threadId, delayMs = 400) => {
    if (refreshTimer)
      clearTimeout(refreshTimer);
    refreshTimer = setTimeout(() => {
      refreshTimer = null;
      if (!disposed)
        syncFromApi(threadId);
    }, delayMs);
  };
  const willSendDisposable = events.on("chat.message.willSend", () => {
    turnStartedAt = Date.now();
  });
  const messageDisposable = events.on("chat.message.didReceive", (input, _output) => {
    const { threadId, pricing } = input;
    if (!currentThreadId)
      currentThreadId = threadId;
    const usage = input.response?.usage;
    if (!usage) {
      updateStatusBar();
      return;
    }
    const existing = threadStats.get(threadId) || createEmptyStats();
    const currentPricing = pricing || existing.pricing;
    const optimistic = {
      ...existing,
      promptTokens: existing.promptTokens + usage.promptTokens,
      completionTokens: existing.completionTokens + usage.completionTokens,
      cachedTokens: existing.cachedTokens + (usage.cachedInputTokens ?? 0),
      cacheWriteTokens: existing.cacheWriteTokens + (usage.cacheWriteInputTokens ?? 0),
      reasoningTokens: existing.reasoningTokens + (usage.reasoningTokens ?? 0),
      totalTokens: existing.totalTokens + usage.totalTokens,
      turns: existing.turns + 1,
      pricing: currentPricing
    };
    if (turnStartedAt !== null && usage.completionTokens > 0) {
      const seconds = Math.max(0.05, (Date.now() - turnStartedAt) / 1000);
      optimistic.lastTurn = { estimatedTps: usage.completionTokens / seconds };
    }
    turnStartedAt = null;
    threadStats.set(threadId, withRecalculatedCost(optimistic));
    if (threadId === currentThreadId)
      updateStatusBar();
    scheduleSync(threadId);
    if (live && live.threadId === threadId) {
      const merged = threadStats.get(threadId);
      if (merged) {
        const patch = {};
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
  const threadDisposable = events.on("thread.activated", (input, _output) => {
    const { threadId, usage, pricing } = input;
    if (live && live.threadId !== threadId)
      live = null;
    currentThreadId = threadId;
    if (usage) {
      const base = {
        ...createEmptyStats(),
        promptTokens: usage.promptTokens,
        completionTokens: usage.completionTokens,
        cachedTokens: usage.cachedInputTokens ?? 0,
        totalTokens: usage.totalTokens,
        pricing
      };
      threadStats.set(threadId, withRecalculatedCost(base));
    }
    updateStatusBar();
    scheduleSync(threadId, 50);
  });
  const showDetails = async () => {
    const stats = getCurrentStats();
    const agentList = visibleSubagents();
    if (stats.totalTokens === 0 && !stats.lastTurn && agentList.length === 0) {
      ui.showNotification("No token usage in this thread yet", { type: "info" });
      return;
    }
    const rate = cacheRate(stats);
    const tps = currentTps(stats);
    const items = [
      { label: "Total tokens", description: formatExact(stats.totalTokens) },
      { label: "Input", description: formatExact(stats.promptTokens) },
      { label: "Output", description: formatExact(stats.completionTokens) }
    ];
    if (stats.reasoningTokens > 0) {
      items.push({ label: "of which reasoning", description: formatExact(stats.reasoningTokens) });
    }
    if (rate !== null) {
      items.push({
        label: "Prompt cache hit",
        description: `${rate.toFixed(1)}%  (${formatExact(stats.cachedTokens)} of ${formatExact(stats.promptTokens)})`
      });
    }
    if (tps !== null) {
      items.push({
        label: "Last turn speed",
        description: `${formatTps(tps)} tok/s${isEstimatedTps(stats) ? " (estimated)" : ""}`
      });
    }
    if (typeof stats.avgTps === "number" && stats.avgTps > 0) {
      items.push({ label: "Average speed", description: `${formatTps(stats.avgTps)} tok/s` });
    }
    if (typeof stats.lastTurn?.ttftMs === "number") {
      items.push({ label: "Time to first token", description: formatSeconds(stats.lastTurn.ttftMs) });
    }
    if (stats.totalCost > 0) {
      const parts = [];
      if (stats.inputCost > 0)
        parts.push(`in ${formatCost(stats.inputCost)}`);
      if (stats.cacheReadCost > 0)
        parts.push(`cache ${formatCost(stats.cacheReadCost)}`);
      if (stats.outputCost > 0)
        parts.push(`out ${formatCost(stats.outputCost)}`);
      items.push({
        label: "Estimated cost",
        description: parts.length > 0 ? `${formatCost(stats.totalCost)}  (${parts.join(" · ")})` : formatCost(stats.totalCost)
      });
    }
    for (const agent of agentList) {
      const bits = [];
      if (agent.done) {
        bits.push(`done in ${formatSeconds((agent.finishedAt ?? Date.now()) - agent.startedAt)}`);
      } else if (agent.tps !== null && agent.tps > 0) {
        bits.push(`${formatTps(agent.tps)} tok/s`);
      } else {
        bits.push("starting");
      }
      if (agent.tokensTotal > 0)
        bits.push(`~${formatTokens(agent.tokensTotal)} tok`);
      items.push({ label: `Agent · ${subagentLabel(agent)}`, description: bits.join(" · ") });
    }
    await ui.showQuickPick(items, { title: "Token usage — this thread" });
  };
  const commandDisposable = context.commands.register("showDetails", {
    title: "Token Counter: Show Details",
    description: "Show token, cache hit rate, throughput and cost details",
    handler: showDetails
  });
  statusBarItem.command = `${context.id}.showDetails`;
  updateStatusBar();
  statusBarItem.show();
  connectLiveSocket();
  const restoreActiveThread = async () => {
    try {
      const thread = await context.chat?.getActiveThread?.();
      const threadId = thread?.id;
      if (!threadId || disposed || currentThreadId)
        return;
      currentThreadId = threadId;
      await syncFromApi(threadId);
    } catch (error) {
      logger.debug(`Token Counter: could not restore active thread (${String(error)})`);
    }
  };
  restoreActiveThread();
  const settingsDisposable = settings.onDidChange(() => {
    for (const [threadId, stats] of threadStats.entries()) {
      threadStats.set(threadId, withRecalculatedCost(stats));
    }
    updateStatusBar();
  });
  return {
    dispose: () => {
      disposed = true;
      logger.info("Token Counter plugin deactivated");
      if (refreshTimer)
        clearTimeout(refreshTimer);
      if (statusBarTimer)
        clearTimeout(statusBarTimer);
      disconnectLiveSocket();
      willSendDisposable.dispose();
      messageDisposable.dispose();
      threadDisposable.dispose();
      settingsDisposable.dispose();
      commandDisposable.dispose();
      statusBarItem.dispose();
    }
  };
}
export {
  activate
};
