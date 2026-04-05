#!/usr/bin/env node
"use strict";

const { createRequire } = require("module");
const { execFileSync } = require("child_process");

const requireFromRoot = createRequire("/Users/aliathar/polymarket-bot/package.json");
const WebSocket = requireFromRoot("ws");

function parseArgs(argv) {
  const out = {
    pageUrl: "http://127.0.0.1:28888/",
    apiBase: "http://127.0.0.1:28888",
    cdpBase: "http://127.0.0.1:9222",
    instanceId: "",
    runs: 1,
    waitMs: 7000,
    allowNoUi: false,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const a = String(argv[i] || "");
    const b = String(argv[i + 1] || "");
    if (a === "--page-url" && b) {
      out.pageUrl = b;
      i += 1;
    } else if (a === "--api-base" && b) {
      out.apiBase = b.replace(/\/$/, "");
      i += 1;
    } else if (a === "--cdp-base" && b) {
      out.cdpBase = b.replace(/\/$/, "");
      i += 1;
    } else if (a === "--instance-id" && b) {
      out.instanceId = b;
      i += 1;
    } else if (a === "--runs" && b) {
      const n = Number(b);
      if (Number.isFinite(n) && n > 0) out.runs = Math.max(1, Math.floor(n));
      i += 1;
    } else if (a === "--wait-ms" && b) {
      const n = Number(b);
      if (Number.isFinite(n) && n > 0) out.waitMs = Math.max(1000, Math.floor(n));
      i += 1;
    } else if (a === "--allow-no-ui") {
      out.allowNoUi = true;
    }
  }
  return out;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getJson(url, init) {
  if (init && Object.keys(init).length) {
    throw new Error("getJson only supports simple GET requests in this verifier");
  }
  const stdout = execFileSync("curl", ["-fsS", url], {
    encoding: "utf8",
    maxBuffer: 25 * 1024 * 1024,
  });
  return JSON.parse(String(stdout || ""));
}

function ok(status, details) {
  return { status: "pass", ...details };
}

function fail(reason, details) {
  return { status: "fail", reason, ...details };
}

function partial(reason, details) {
  return { status: "partial", reason, ...details };
}

async function detectInstanceId(apiBase, explicitInstanceId) {
  if (String(explicitInstanceId || "").trim()) return String(explicitInstanceId || "").trim();
  const bots = await getJson(`${apiBase}/api/v2/bots?limit=3`);
  const inst = Array.isArray(bots?.items) ? bots.items[0] : null;
  const id = String(inst?.instanceId || "").trim();
  if (!id) throw new Error("instanceId not found");
  return id;
}

function summarizeSessionsPayload(payload) {
  const sessions = Array.isArray(payload?.sessions) ? payload.sessions : [];
  const recent = sessions.slice(0, 12);
  const hasReasons = recent.every((row) => row?.noTrade !== true || String(row?.noTradeReason || "").trim());
  const hasAuditLinks = recent.every((row) => !String(row?.slug || "").trim() || String(row?.sessionAuditPath || "").trim());
  let chronological = true;
  for (let i = 1; i < recent.length; i += 1) {
    const prev = Number(recent[i - 1]?.startMs || 0);
    const cur = Number(recent[i]?.startMs || 0);
    if (!(prev > cur)) {
      chronological = false;
      break;
    }
  }
  return { count: sessions.length, recent, hasReasons, hasAuditLinks, chronological };
}

async function verifyData(apiBase, instanceId) {
  const card = await getJson(`${apiBase}/api/v2/bots/${encodeURIComponent(instanceId)}/latest-session-card?maxSessions=6&refresh=1`);
  const summary = await getJson(`${apiBase}/api/v2/bots/${encodeURIComponent(instanceId)}/session-artifacts-summary?limit=12`);
  const focused = await getJson(`${apiBase}/api/v2/bots/${encodeURIComponent(instanceId)}/focused-live-session`);
  const sessionSummary = summarizeSessionsPayload(summary);
  const cardTraceLen = Array.isArray(card?.selectedSession?.trace?.xMs) ? card.selectedSession.trace.xMs.length : 0;
  const historyCard = cardTraceLen >= 2
    ? ok({
        slug: String(card?.selectedSlug || ""),
        traceLen: cardTraceLen,
        traceSource: String(card?.selectedSession?.traceSource || card?.traceSource || ""),
      })
    : fail("historical card trace missing", {
        slug: String(card?.selectedSlug || ""),
        traceLen: cardTraceLen,
      });
  const last100 = sessionSummary.count > 0 && sessionSummary.hasReasons && sessionSummary.hasAuditLinks && sessionSummary.chronological
    ? ok({
        count: sessionSummary.count,
        topSlug: String(sessionSummary.recent[0]?.slug || ""),
      })
    : fail("last 100 sessions payload incomplete", {
        count: sessionSummary.count,
        hasReasons: sessionSummary.hasReasons,
        hasAuditLinks: sessionSummary.hasAuditLinks,
        chronological: sessionSummary.chronological,
        topSlug: String(sessionSummary.recent[0]?.slug || ""),
      });
  const liveTrace = (() => {
    const gapMax = Number(focused?.invariants?.rawTraceGapMaxMs);
    const staleMs = Number(focused?.invariants?.traceStaleVsRuntimeTickMs);
    if (Number.isFinite(gapMax) && gapMax <= 200 && Number.isFinite(staleMs) && staleMs <= 2000) {
      return ok({ gapMaxMs: gapMax, staleMs });
    }
    return fail("live raw trace cadence too slow", { gapMaxMs: Number.isFinite(gapMax) ? gapMax : null, staleMs: Number.isFinite(staleMs) ? staleMs : null });
  })();
  return {
    instanceId,
    endpoints: { card, summary, focused },
    sections: {
      historicalSessionCards_data: historyCard,
      last100Sessions_data: last100,
      liveSessionTrace_data: liveTrace,
    },
  };
}

async function isCdpAvailable(cdpBase) {
  try {
    const version = await getJson(`${cdpBase}/json/version`);
    return !!version?.webSocketDebuggerUrl;
  } catch {
    return false;
  }
}

async function createTarget(cdpBase, url) {
  try {
    const stdout = execFileSync("curl", ["-fsS", "-X", "PUT", `${cdpBase}/json/new?${encodeURIComponent(url)}`], {
      encoding: "utf8",
      maxBuffer: 5 * 1024 * 1024,
    });
    return JSON.parse(String(stdout || ""));
  } catch {
    return await getJson(`${cdpBase}/json/new?${encodeURIComponent(url)}`);
  }
}

async function withCdp(target, fn) {
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  let id = 0;
  const pending = new Map();
  ws.on("message", (buf) => {
    const msg = JSON.parse(String(buf));
    if (msg.id && pending.has(msg.id)) {
      const handlers = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) handlers.reject(new Error(msg.error.message || "cdp error"));
      else handlers.resolve(msg.result);
    }
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    id += 1;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });
  try {
    await send("Page.enable");
    await send("Runtime.enable");
    return await fn(send);
  } finally {
    try { ws.close(); } catch {}
  }
}

async function evalJson(send, expression) {
  const result = await send("Runtime.evaluate", {
    expression,
    returnByValue: true,
    awaitPromise: true,
  });
  return result?.result?.value;
}

const uiProbeExpression = `(() => {
  const historyCard = document.querySelector('#history .chartCard[data-slug]');
  const compactRows = Array.from(document.querySelectorAll('.sessCompactPnlTable tbody tr'));
  const compactFirst = compactRows[0]
    ? Array.from(compactRows[0].querySelectorAll('td')).map((td) => String(td.textContent || '').trim())
    : [];
  const compactReason = !!document.querySelector('.sessCompactPnlReason');
  const compactLink = !!document.querySelector('.sessCompactPnlSlugLink[href]');
  const historyPlot = historyCard ? historyCard.querySelector('.plotly, .js-plotly-plot') : null;
  const placeholderText = String(document.body?.innerText || '');
  return {
    historyVisible: !!historyCard,
    historySlug: historyCard ? String(historyCard.getAttribute('data-slug') || '') : '',
    historyPlotVisible: !!historyPlot,
    historyHasJpegPlaceholder: /jpeg not found/i.test(placeholderText),
    historyHasTraceAuditFailed: /trace audit failed/i.test(placeholderText),
    compactRows: compactRows.length,
    compactFirst,
    compactHasReason: compactReason,
    compactHasAuditLink: compactLink,
    compactEmptyMessage: /No recent session-history rows loaded for this card yet\\./i.test(placeholderText),
    compactUnavailableMessage: /Recent session history unavailable\\./i.test(placeholderText)
  };
})()`;

async function verifyUi(cdpBase, pageUrl, runs, waitMs) {
  if (!(await isCdpAvailable(cdpBase))) {
    return {
      available: false,
      runs: [],
      status: "unavailable",
      reason: "cdp unavailable",
    };
  }
  const target = await createTarget(cdpBase, pageUrl);
  const runRows = [];
  await withCdp(target, async (send) => {
    for (let i = 0; i < runs; i += 1) {
      await send("Page.navigate", { url: pageUrl });
      await delay(waitMs);
      runRows.push(await evalJson(send, uiProbeExpression));
    }
  });
  const every = (predicate) => runRows.length > 0 && runRows.every(predicate);
  const historical = every((row) => row.historyVisible && row.historyPlotVisible && !row.historyHasJpegPlaceholder && !row.historyHasTraceAuditFailed)
    ? ok({ runs: runRows.length, slug: String(runRows[runRows.length - 1]?.historySlug || "") })
    : fail("historical card not visibly rendered", { runs: runRows });
  const last100 = every((row) => row.compactRows > 0 && row.compactHasReason && row.compactHasAuditLink && !row.compactEmptyMessage && !row.compactUnavailableMessage)
    ? ok({ runs: runRows.length, firstRow: runRows[runRows.length - 1]?.compactFirst || [] })
    : fail("last 100 sessions not visibly rendered", { runs: runRows });
  return {
    available: true,
    runs: runRows,
    status: "ok",
    sections: {
      historicalSessionCards_ui: historical,
      last100Sessions_ui: last100,
    },
  };
}

function mergeOverall(dataVerification, uiVerification, allowNoUi) {
  const sections = {
    ...dataVerification.sections,
  };
  if (uiVerification.available && uiVerification.sections) {
    Object.assign(sections, uiVerification.sections);
  } else {
    sections.uiTransport = allowNoUi
      ? partial("ui verification unavailable", { reason: uiVerification.reason || "cdp unavailable" })
      : fail("ui verification unavailable", { reason: uiVerification.reason || "cdp unavailable" });
  }
  const failing = Object.entries(sections).filter(([, value]) => String(value?.status) !== "pass");
  return {
    ok: failing.length === 0,
    failingSections: failing.map(([key, value]) => ({ key, status: value.status, reason: value.reason || null })),
    sections,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const instanceId = await detectInstanceId(args.apiBase, args.instanceId);
  const dataVerification = await verifyData(args.apiBase, instanceId);
  const uiVerification = await verifyUi(args.cdpBase, args.pageUrl, args.runs, args.waitMs);
  const overall = mergeOverall(dataVerification, uiVerification, args.allowNoUi);
  const out = {
    ok: overall.ok,
    generatedAt: new Date().toISOString(),
    instanceId,
    pageUrl: args.pageUrl,
    apiBase: args.apiBase,
    cdpBase: args.cdpBase,
    dataVerification,
    uiVerification,
    overall,
  };
  process.stdout.write(JSON.stringify(out, null, 2) + "\n");
  process.exit(overall.ok ? 0 : 1);
}

main().catch((err) => {
  const out = {
    ok: false,
    error: String(err?.stack || err?.message || err || "unknown error"),
  };
  process.stdout.write(JSON.stringify(out, null, 2) + "\n");
  process.exit(2);
});
