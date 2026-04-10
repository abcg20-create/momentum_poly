#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import http from "node:http";
import https from "node:https";
import os from "node:os";

const PORT = Math.max(1, Number(process.env.PORT || 9001));
const HOST = String(process.env.HOST || "0.0.0.0").trim() || "0.0.0.0";
const WORKER_LABEL = String(process.env.WORKER_LABEL || `worker:${PORT}`).trim() || `worker:${PORT}`;
const UPSTREAM_ORIGIN = String(process.env.UPSTREAM_ORIGIN || "").trim().replace(/\/+$/, "");
const UPSTREAM_ORIGIN_MAP_RAW = String(process.env.UPSTREAM_ORIGIN_MAP || "").trim();
const CACHE_ROOT = path.resolve(String(process.env.CACHE_ROOT || `/tmp/nonessential_worker_${PORT}`));
const DEFAULT_TTL_MS = Math.max(1000, Number(process.env.DEFAULT_TTL_MS || 15000));
const ERROR_TTL_MS = Math.max(1000, Number(process.env.ERROR_TTL_MS || 5000));
const ALLOW_HOT_QUERY = String(process.env.ALLOW_HOT_QUERY || "1").trim() || "1";
const READ_ONLY_WORKER_HEADER = "x-mmx-readonly-worker";
const UPSTREAM_TIMEOUT_MS = Math.max(5000, Number(process.env.UPSTREAM_TIMEOUT_MS || 15000));
const HISTORY_UPSTREAM_TIMEOUT_MS = Math.max(15000, Number(process.env.HISTORY_UPSTREAM_TIMEOUT_MS || 45000));
const CACHEABLE_PATH_PATTERNS = [
  /^\/api\/operator-notices\b/i,
  /^\/api\/stats\/summary\b/i,
  /^\/api\/session-history\b/i,
  /^\/api\/chart-history\b/i,
  /^\/api\/run-audits\/review\b/i,
  /^\/api\/v2\/strategies\b/i,
  /^\/api\/v2\/markets\/hot\b/i,
  /^\/api\/v2\/markets\/volume-5m-24h\b/i,
  /^\/api\/v2\/bots\b/i,
  /^\/api\/v2\/bots\/[^/]+\b/i,
  /^\/api\/v2\/bots\/[^/]+\/last-session-history\b/i,
  /^\/api\/v2\/bots\/[^/]+\/continuity-history\b/i,
  /^\/api\/v2\/bots\/[^/]+\/latest-session-artifact\b/i,
  /^\/api\/v2\/bots\/[^/]+\/latest-session-card\b/i,
  /^\/api\/v2\/bots\/[^/]+\/session-artifacts-summary\b/i,
  /^\/api\/v2\/bots\/[^/]+\/rollover-ready\b/i,
  /^\/api\/compare\/run-dashboard\b/i,
  /^\/api\/v2\/markets\/audit\b/i,
  /^\/api\/v2\/markets\/hot\b/i,
  /^\/api\/v2\/drive\/audit\b/i,
  /^\/api\/v2\/portfolio\/summary\b/i,
  /^\/api\/v2\/portfolio\/markets\b/i,
];
const CACHE_TTL_BY_PATH = [
  { pattern: /^\/api\/operator-notices\b/i, ttlMs: Math.max(1000, Number(process.env.OPERATOR_NOTICES_CACHE_TTL_MS || 5000)) },
  { pattern: /^\/api\/stats\/summary\b/i, ttlMs: Math.max(1000, Number(process.env.STATS_SUMMARY_CACHE_TTL_MS || 15000)) },
  { pattern: /^\/api\/run-audits\/review\b/i, ttlMs: Math.max(5000, Number(process.env.RUN_AUDIT_CACHE_TTL_MS || 120000)) },
  { pattern: /^\/api\/v2\/strategies\b/i, ttlMs: Math.max(5000, Number(process.env.STRATEGIES_CACHE_TTL_MS || 60000)) },
  { pattern: /^\/api\/v2\/markets\/hot\b/i, ttlMs: Math.max(1000, Number(process.env.MARKETS_HOT_CACHE_TTL_MS || 5000)) },
  { pattern: /^\/api\/v2\/markets\/volume-5m-24h\b/i, ttlMs: Math.max(1000, Number(process.env.MARKETS_VOLUME_24H_CACHE_TTL_MS || 15000)) },
  { pattern: /^\/api\/v2\/bots\b/i, ttlMs: Math.max(500, Number(process.env.BOTS_CACHE_TTL_MS || 1000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\b/i, ttlMs: Math.max(500, Number(process.env.BOT_DETAIL_CACHE_TTL_MS || 1000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\/last-session-history\b/i, ttlMs: Math.max(1000, Number(process.env.LAST_SESSION_HISTORY_CACHE_TTL_MS || 5000)) },
  { pattern: /^\/api\/session-history\b/i, ttlMs: Math.max(1000, Number(process.env.SESSION_HISTORY_CACHE_TTL_MS || 15000)) },
  { pattern: /^\/api\/chart-history\b/i, ttlMs: Math.max(1000, Number(process.env.CHART_HISTORY_CACHE_TTL_MS || 15000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\/continuity-history\b/i, ttlMs: Math.max(1000, Number(process.env.CONTINUITY_HISTORY_CACHE_TTL_MS || 15000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\/latest-session-artifact\b/i, ttlMs: Math.max(1000, Number(process.env.LATEST_SESSION_ARTIFACT_CACHE_TTL_MS || 10000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\/latest-session-card\b/i, ttlMs: Math.max(1000, Number(process.env.LATEST_SESSION_CARD_CACHE_TTL_MS || 5000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\/session-artifacts-summary\b/i, ttlMs: Math.max(1000, Number(process.env.SESSION_ARTIFACTS_SUMMARY_CACHE_TTL_MS || 15000)) },
  { pattern: /^\/api\/v2\/bots\/[^/]+\/rollover-ready\b/i, ttlMs: Math.max(1000, Number(process.env.ROLLOVER_READY_CACHE_TTL_MS || 5000)) },
];
const PARITY_ENABLED = String(process.env.PARITY_ENABLED || "0").trim() === "1";
const PARITY_OUT_DIR = path.resolve(String(process.env.PARITY_OUT_DIR || path.join(CACHE_ROOT, "parity")));
const PARITY_SCRIPT_PATH = path.resolve(String(process.env.PARITY_SCRIPT_PATH || path.resolve(process.cwd(), "tools_ohio_dublin_session_window_parity.py")));
const PARITY_PYTHON_BIN = String(process.env.PARITY_PYTHON_BIN || "python3").trim() || "python3";
const PARITY_OHIO_BASE = String(process.env.PARITY_OHIO_BASE || "").trim().replace(/\/+$/, "");
const PARITY_DUBLIN_BASE = String(process.env.PARITY_DUBLIN_BASE || "").trim().replace(/\/+$/, "");
const PARITY_OHIO_INSTANCE_ID = String(process.env.PARITY_OHIO_INSTANCE_ID || "").trim();
const PARITY_DUBLIN_INSTANCE_ID = String(process.env.PARITY_DUBLIN_INSTANCE_ID || "").trim();
const PARITY_DUBLIN_SSH_HOST = String(process.env.PARITY_DUBLIN_SSH_HOST || "").trim();
const PARITY_DUBLIN_SSH_USER = String(process.env.PARITY_DUBLIN_SSH_USER || "").trim();
const PARITY_DUBLIN_SSH_KEY = String(process.env.PARITY_DUBLIN_SSH_KEY || "").trim();
const RUN_AUDIT_PYTHON_BIN = String(process.env.RUN_AUDIT_PYTHON_BIN || "python3").trim() || "python3";
const RUN_AUDIT_SCRIPT_PATH = path.resolve(String(process.env.RUN_AUDIT_SCRIPT_PATH || path.resolve(process.cwd(), "render_live_run_audit_review.py")));
const RUN_AUDIT_REMOTE_HOST = String(process.env.RUN_AUDIT_REMOTE_HOST || "").trim();
const RUN_AUDIT_REMOTE_KEY = String(process.env.RUN_AUDIT_REMOTE_KEY || "").trim();
const RUN_AUDIT_REMOTE_ROOT = String(process.env.RUN_AUDIT_REMOTE_ROOT || "").trim().replace(/\/+$/, "");
const RUN_AUDIT_HOST_PORT = String(process.env.RUN_AUDIT_HOST_PORT || "8788").trim() || "8788";
const RUN_AUDIT_PUBLIC_BASE = String(process.env.RUN_AUDIT_PUBLIC_BASE || UPSTREAM_ORIGIN || "").trim().replace(/\/+$/, "");
const RUN_AUDIT_MAX_AGE_MS = Math.max(1000, Number(process.env.RUN_AUDIT_MAX_AGE_MS || 30000));
const RUN_AUDIT_TOLERATED_MISSING_SESSIONS = Math.max(0, Number(process.env.RUN_AUDIT_TOLERATED_MISSING_SESSIONS || 3));
const RUN_AUDIT_EXPECTED_CODE_VERSION = String(process.env.RUN_AUDIT_EXPECTED_CODE_VERSION || "run_audit_mv_v37").trim() || "run_audit_mv_v37";
const RUN_AUDIT_SYNC_BUILD_TIMEOUT_MS = Math.max(1000, Number(process.env.RUN_AUDIT_SYNC_BUILD_TIMEOUT_MS || 60000));
const RUN_AUDIT_MIRROR_DETAILED_SESSION_LIMIT = Math.max(0, Number(process.env.RUN_AUDIT_MIRROR_DETAILED_SESSION_LIMIT || 40));
const RUN_AUDIT_MIRROR_COLD_COMPACT_SESSION_LIMIT = Math.max(0, Number(process.env.RUN_AUDIT_MIRROR_COLD_COMPACT_SESSION_LIMIT || 60));
const RUN_AUDIT_BACKGROUND_WARM_ENABLED = String(process.env.RUN_AUDIT_BACKGROUND_WARM_ENABLED || "1").trim() !== "0";
const RUN_AUDIT_BACKGROUND_WARM_INTERVAL_MS = Math.max(5000, Number(process.env.RUN_AUDIT_BACKGROUND_WARM_INTERVAL_MS || 15000));
const RUN_AUDIT_BACKGROUND_MAX_BOTS = Math.max(1, Number(process.env.RUN_AUDIT_BACKGROUND_MAX_BOTS || 4));
const RUN_AUDIT_ENABLED = String(process.env.RUN_AUDIT_ENABLED || "1").trim() !== "0"
  && !!RUN_AUDIT_REMOTE_HOST
  && !!RUN_AUDIT_REMOTE_KEY
  && !!RUN_AUDIT_REMOTE_ROOT
  && fs.existsSync(RUN_AUDIT_SCRIPT_PATH);
const RUN_AUDIT_CACHE_ROOT = path.resolve(String(process.env.RUN_AUDIT_CACHE_ROOT || path.join(CACHE_ROOT, "run_audits")));
const GAMMA_BASE = String(process.env.GAMMA_BASE || "https://gamma-api.polymarket.com").trim().replace(/\/+$/, "");
const CPU_SAMPLE_MIN_INTERVAL_NS = 250_000_000;
const CPU_SAMPLE_EMA_ALPHA = 0.35;
const SESSION_CARD_IMAGE_CACHE_ROOT = path.resolve(String(process.env.SESSION_CARD_IMAGE_CACHE_ROOT || path.join(CACHE_ROOT, "session_card_images")));
const SIPS_BIN = String(process.env.SIPS_BIN || "/usr/bin/sips").trim() || "/usr/bin/sips";
const MAGICK_BIN = String(process.env.MAGICK_BIN || "/usr/bin/magick").trim() || "/usr/bin/magick";
const CONVERT_BIN = String(process.env.CONVERT_BIN || "/usr/bin/convert").trim() || "/usr/bin/convert";
const SESSION_CARD_REMOTE_CANONICAL_READ_ENABLED = String(process.env.SESSION_CARD_REMOTE_CANONICAL_READ_ENABLED || "0").trim() === "1";
const SESSION_CARD_REMOTE_CANONICAL_WRITE_ENABLED = String(process.env.SESSION_CARD_REMOTE_CANONICAL_WRITE_ENABLED || "1").trim() !== "0";
const ACTIVITY_LOG_LIMIT = Math.max(50, Number(process.env.ACTIVITY_LOG_LIMIT || 400));
const ACTIVITY_LOG_FILE = path.join(CACHE_ROOT, "activity_log.json");

const cacheMem = new Map();
const inflight = new Map();
let parityInflight = null;
const runAuditInflight = new Map();
let runAuditBackgroundWarmInflight = false;
const runAuditBackgroundWarmState = new Map();
let cpuSampleLastHrNs = process.hrtime.bigint();
let cpuSampleLastUsage = process.cpuUsage();
let cpuSampleLastPct = 0;
let activitySeq = 0;
const activityLog = [];
const activityClients = new Set();

function parseUpstreamOriginMap(raw) {
  const out = new Map();
  const src = String(raw || "").trim();
  if (!src) return out;
  for (const part of src.split(",")) {
    const row = String(part || "").trim();
    if (!row) continue;
    const eqIdx = row.indexOf("=");
    if (eqIdx <= 0) continue;
    const key = String(row.slice(0, eqIdx) || "").trim();
    const value = String(row.slice(eqIdx + 1) || "").trim().replace(/\/+$/, "");
    if (!key || !value) continue;
    out.set(key, value);
  }
  return out;
}

const UPSTREAM_ORIGIN_MAP = parseUpstreamOriginMap(UPSTREAM_ORIGIN_MAP_RAW);

fs.mkdirSync(CACHE_ROOT, { recursive: true });
fs.mkdirSync(PARITY_OUT_DIR, { recursive: true });
fs.mkdirSync(RUN_AUDIT_CACHE_ROOT, { recursive: true });
fs.mkdirSync(SESSION_CARD_IMAGE_CACHE_ROOT, { recursive: true });

try {
  if (fs.existsSync(ACTIVITY_LOG_FILE)) {
    const persisted = JSON.parse(fs.readFileSync(ACTIVITY_LOG_FILE, "utf8"));
    const items = Array.isArray(persisted?.items) ? persisted.items.filter((row) => row && typeof row === "object") : [];
    activityLog.push(...items.slice(-ACTIVITY_LOG_LIMIT));
    const maxId = activityLog.reduce((max, row) => Math.max(max, Number(row?.id || 0)), 0);
    activitySeq = Number.isFinite(maxId) ? maxId : 0;
  }
} catch {}

function log(line) {
  process.stdout.write(`[readonly-worker ${new Date().toISOString()} ${WORKER_LABEL}] ${line}\n`);
}

function summarizeUrl(urlLike) {
  try {
    const url = urlLike instanceof URL ? urlLike : new URL(String(urlLike || ""));
    const importantKeys = ["slug", "instanceId", "marketPrefix", "runNum", "format", "refresh", "anchorSlug", "expectedSlug"];
    const parts = [];
    for (const key of importantKeys) {
      const value = String(url.searchParams.get(key) || "").trim();
      if (value) parts.push(`${key}=${value}`);
    }
    return parts.length ? `${url.pathname} (${parts.join(", ")})` : url.pathname;
  } catch {
    return String(urlLike || "");
  }
}

function normalizeRunAuditHostPort(raw) {
  const hostPort = String(raw || "").trim();
  if (Number.isFinite(Number(hostPort)) && Number(hostPort) > 0) {
    return String(Math.floor(Number(hostPort)));
  }
  const fallback = String(RUN_AUDIT_HOST_PORT || "8788").trim() || "8788";
  return fallback;
}

function slugStartMs(slugLike) {
  const slug = String(slugLike || "").trim();
  const match = slug.match(/-(\d{10})$/);
  if (!match) return null;
  const startMs = Number(match[1]) * 1000;
  return Number.isFinite(startMs) && startMs > 0 ? startMs : null;
}

function slugBase(slugLike) {
  return String(slugLike || "").trim().toLowerCase().replace(/-\d{10}$/i, "");
}

function inferSessionIntervalMsFromSlugLocal(slugLike) {
  const slug = String(slugLike || "").trim().toLowerCase();
  if (!slug) return null;
  const match = slug.match(/-(\d+)([mh])-\d{10}$/i);
  if (!match) return null;
  const count = Number(match[1]);
  const unit = String(match[2] || "").toLowerCase();
  if (!(Number.isFinite(count) && count > 0)) return null;
  if (unit === "m") return count * 60 * 1000;
  if (unit === "h") return count * 60 * 60 * 1000;
  return null;
}

function slugTimeLabel(slugLike) {
  const startMs = slugStartMs(slugLike);
  if (!startMs) return null;
  try {
    return new Date(startMs).toLocaleString("en-US", {
      timeZone: "America/Los_Angeles",
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
    });
  } catch {
    return new Date(startMs).toISOString();
  }
}

function withSlugMeta(details, slugLike, slugKey = "slug") {
  const slug = String(slugLike || "").trim();
  if (!slug) return details && typeof details === "object" ? details : {};
  const out = { ...(details && typeof details === "object" ? details : {}) };
  out[slugKey] = slug;
  out[`${slugKey}Time`] = slugTimeLabel(slug);
  return out;
}

function activityCategoryForType(typeLike) {
  const type = String(typeLike || "").trim().toLowerCase();
  if (type.startsWith("worker-")) return "Worker Lifecycle";
  if (type.startsWith("proxy-") || type === "local-build" || type === "local-cache-hit") return "Proxy And Cache";
  if (type.startsWith("latest-session-card")) return "Latest Session Card";
  if (type.startsWith("session-artifacts-summary")) return "Session Artifact Summary";
  if (type === "volume-series-build") return "Market Volume Series";
  if (type.startsWith("parity-build")) return "Session Parity";
  if (type.startsWith("run-audit")) return "Run Audit";
  return "Other";
}

function workerTaskCatalog() {
  return [
    {
      category: "Worker Lifecycle",
      tasks: [
        "Start the non-essential worker HTTP server",
        "Expose health and activity endpoints",
      ],
    },
    {
      category: "Proxy And Cache",
      tasks: [
        "Proxy read-only API requests to the upstream host",
        "Append allowHot=1 to supported upstream reads",
        "Cache read-heavy endpoints and serve cache hits",
      ],
    },
    {
      category: "Latest Session Card",
      tasks: [
        "Resolve the latest finalized session card for a bot",
        "Fall back to remote canonical session data when needed",
      ],
    },
    {
      category: "Session Artifact Summary",
      tasks: [
        "Build summarized session artifact payloads for a bot",
        "Fall back to remote run summaries when upstream data is unavailable",
      ],
    },
    {
      category: "Market Volume Series",
      tasks: [
        "Build 24-hour 5-minute market volume series from Gamma event data",
      ],
    },
    {
      category: "Session Parity",
      tasks: [
        "Generate Ohio/Dublin session parity artifacts",
      ],
    },
    {
      category: "Run Audit",
      tasks: [
        "Generate run audit HTML and JSON artifacts",
        "Serve cached run audits when still fresh",
      ],
    },
  ];
}

function workerCategoryList() {
  return ["Worker Lifecycle", "Proxy And Cache", "Latest Session Card", "Session Artifact Summary", "Market Volume Series", "Session Parity", "Run Audit"];
}

function pushActivity(message, details = {}) {
  const safeDetails = details && typeof details === "object" ? details : {};
  const item = {
    id: ++activitySeq,
    t: Date.now(),
    worker: WORKER_LABEL,
    message: String(message || "").trim() || "WORKER ACTIVITY",
    details: safeDetails,
    category: activityCategoryForType(safeDetails.type),
  };
  activityLog.push(item);
  if (activityLog.length > ACTIVITY_LOG_LIMIT) activityLog.splice(0, activityLog.length - ACTIVITY_LOG_LIMIT);
  try {
    fs.writeFileSync(ACTIVITY_LOG_FILE, JSON.stringify({
      updatedAtMs: Date.now(),
      worker: WORKER_LABEL,
      items: activityLog,
    }, null, 2) + "\n");
  } catch {}
  const payload = `data: ${JSON.stringify(item)}\n\n`;
  for (const client of activityClients) {
    try {
      client.write(payload);
    } catch {
      activityClients.delete(client);
    }
  }
  log(item.message);
  return item;
}

function activitySnapshot() {
  return {
    ok: true,
    worker: WORKER_LABEL,
    count: activityLog.length,
    items: activityLog.slice(-Math.min(activityLog.length, ACTIVITY_LOG_LIMIT)),
    taskCatalog: workerTaskCatalog(),
  };
}

function renderActivityDashboard() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Non-Essential Worker Activity</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4efe6;
      --panel: #fffaf2;
      --ink: #1d2a2f;
      --muted: #6a756c;
      --accent: #0f766e;
      --line: #dfd2bb;
      --good: #166534;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.10), transparent 28%),
        linear-gradient(180deg, #f7f2ea 0%, var(--bg) 100%);
      color: var(--ink);
    }
    main {
      max-width: 1100px;
      margin: 0 auto;
      padding: 28px 18px 40px;
    }
    .hero {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px 22px;
      box-shadow: 0 12px 35px rgba(29,42,47,0.08);
    }
    h1 {
      margin: 0 0 8px;
      font-size: clamp(28px, 5vw, 46px);
      line-height: 1;
      letter-spacing: 0.02em;
    }
    .sub {
      margin: 0;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 13px;
    }
    .status {
      margin-top: 14px;
      color: var(--good);
      font-weight: 700;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 13px;
    }
    .feed {
      margin: 18px 0 0;
      display: grid;
      gap: 18px;
    }
    .terminal {
      margin-top: 18px;
      background: #1d2528;
      color: #d7efe8;
      border: 1px solid #38474a;
      border-radius: 16px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
      overflow: hidden;
    }
    .terminal-head {
      padding: 10px 14px;
      border-bottom: 1px solid #38474a;
      background: rgba(255,255,255,0.03);
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
    }
    .terminal-title {
      margin: 0;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #7dd3c8;
    }
    .terminal-status {
      margin: 0;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
      color: #9fb7b1;
    }
    .terminal-body {
      max-height: 260px;
      overflow: auto;
      padding: 12px 14px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
      line-height: 1.55;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .terminal-line {
      margin: 0 0 6px;
    }
    .group {
      background: rgba(255,255,255,0.78);
      border: 1px solid var(--line);
      border-radius: 16px;
      overflow: hidden;
    }
    .group-head {
      padding: 12px 14px;
      background: rgba(15,118,110,0.08);
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
    }
    .group-title {
      margin: 0;
      font-weight: 700;
      font-size: 16px;
    }
    .group-count {
      margin: 0;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
    }
    .table-wrap {
      max-height: 340px;
      overflow: auto;
      background: #fffdf8;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid rgba(223,210,187,0.8);
      vertical-align: top;
    }
    th {
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      background: rgba(255,250,242,0.92);
    }
    tbody tr:last-child td {
      border-bottom: 0;
    }
    .cell-message {
      font-weight: 700;
      min-width: 320px;
    }
    .empty {
      padding: 16px 14px;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
    }
    .section-title {
      margin: 22px 0 10px;
      font-size: 20px;
      letter-spacing: 0.02em;
    }
    .section-sub {
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 14px;
    }
    .live-note {
      margin-top: 10px;
      color: var(--muted);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
    }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
      color: var(--muted);
      white-space: pre-wrap;
      word-break: break-word;
    }
    .compact {
      margin: 0;
      display: grid;
      gap: 6px;
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>Non-Essential Worker</h1>
      <p class="sub">Live activity feed for ${WORKER_LABEL}</p>
      <div id="status" class="status">CONNECTING TO LIVE STREAM...</div>
      <div class="live-note">LIVE MODE: new activity lines stream in automatically like a terminal.</div>
      <div class="terminal">
        <div class="terminal-head">
          <h2 class="terminal-title">Live CPU Health Terminal</h2>
          <p id="health-status" class="terminal-status">CONNECTING...</p>
        </div>
        <div id="health-terminal" class="terminal-body"></div>
      </div>
      <h2 class="section-title">Live Activity By Category</h2>
      <p class="section-sub">Each category updates live. New lines are appended at the bottom like a running terminal.</p>
      <div id="feed" class="feed"></div>
    </section>
  </main>
  <script>
    const statusEl = document.getElementById("status");
    const feedEl = document.getElementById("feed");
    const healthStatusEl = document.getElementById("health-status");
    const healthTerminalEl = document.getElementById("health-terminal");
    const groups = new Map();
    const seenActivityIds = new Set();
    const defaultCategories = ${JSON.stringify(workerCategoryList())};
    const order = ["All Activity", ...defaultCategories, "Other"];
    function fmtTime(ts) {
      try { return new Date(ts).toLocaleString(); } catch { return String(ts || ""); }
    }

    function esc(value) {
      return String(value == null ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function detailsHtml(item) {
      if (!(item && item.details && typeof item.details === "object")) return "";
      return Object.entries(item.details)
        .filter(([, value]) => value !== null && value !== undefined && value !== "")
        .map(([key, value]) => {
          const text = esc(value);
          if (typeof value === "string" && /^https?:\\/\\//i.test(value)) {
            return esc(key) + '= <a href="' + text + '" target="_blank" rel="noreferrer">' + text + '</a>';
          }
          return esc(key) + '=' + text;
        })
        .join(" | ");
    }

    function renderEmptyGroups(taskCatalog) {
      const rows = Array.isArray(taskCatalog) && taskCatalog.length
        ? taskCatalog
        : defaultCategories.map((category) => ({ category }));
      rows.forEach((entry) => {
        const group = ensureGroup(entry.category || "Other");
        group.empty.hidden = false;
        group.count.textContent = "0 items";
      });
    }

    async function fetchJsonWithTimeout(url, timeoutMs) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const resp = await fetch(url, { signal: controller.signal });
        return await resp.json();
      } finally {
        clearTimeout(timer);
      }
    }

    let lastHealthLine = "";
    function addHealthLine(line) {
      const row = document.createElement("div");
      row.className = "terminal-line";
      row.textContent = line;
      const wasNearBottom = (healthTerminalEl.scrollHeight - healthTerminalEl.scrollTop - healthTerminalEl.clientHeight) < 40;
      healthTerminalEl.appendChild(row);
      while (healthTerminalEl.children.length > 120) {
        healthTerminalEl.removeChild(healthTerminalEl.firstChild);
      }
      if (wasNearBottom) healthTerminalEl.scrollTop = healthTerminalEl.scrollHeight;
    }

    function formatHealthLine(payload) {
      const stats = payload && payload.processStats ? payload.processStats : {};
      return [
        '[' + fmtTime(payload && payload.t) + ']',
        'CPU=' + (stats.cpuPct ?? 'n/a') + '%',
        'RSS=' + (stats.rssMb ?? 'n/a') + 'MB',
        'HEAP=' + (stats.heapUsedMb ?? 'n/a') + 'MB',
        'LOAD1=' + (stats.loadAvg1m ?? 'n/a'),
        'CACHE=' + (payload && payload.cacheEntries != null ? payload.cacheEntries : 'n/a'),
      ].join(' | ');
    }

    async function pollHealth() {
      try {
        const payload = await fetchJsonWithTimeout('/api/health', 4000);
        const line = formatHealthLine(payload);
        healthStatusEl.textContent = 'LIVE';
        if (line !== lastHealthLine) {
          lastHealthLine = line;
          addHealthLine(line);
        }
      } catch (error) {
        healthStatusEl.textContent = 'RECONNECTING...';
        addHealthLine('[' + fmtTime(Date.now()) + '] HEALTH FETCH FAILED | error=' + String(error && error.message || error));
      }
    }

    function ensureGroup(category) {
      const key = category || "Other";
      if (groups.has(key)) return groups.get(key);
      const wrapper = document.createElement("section");
      wrapper.className = "group";
      wrapper.innerHTML = ''
        + '<div class="group-head"><h3 class="group-title"></h3><p class="group-count"></p></div>'
        + '<div class="table-wrap"><table><thead><tr><th>Time</th><th>Activity</th><th>Details</th></tr></thead><tbody></tbody></table></div>'
        + '<div class="empty" hidden>No activity yet.</div>';
      wrapper.querySelector(".group-title").textContent = key;
      const group = {
        wrapper,
        tbody: wrapper.querySelector("tbody"),
        count: wrapper.querySelector(".group-count"),
        empty: wrapper.querySelector(".empty"),
        rows: 0,
        scroll: wrapper.querySelector(".table-wrap"),
      };
      groups.set(key, group);
      const idx = order.indexOf(key);
      const orderedChildren = Array.from(feedEl.children);
      if (idx < 0 || orderedChildren.length === 0) {
        feedEl.appendChild(wrapper);
      } else {
        const before = orderedChildren.find((child) => order.indexOf(child.dataset.category || "") > idx);
        wrapper.dataset.category = key;
        if (before) feedEl.insertBefore(wrapper, before);
        else feedEl.appendChild(wrapper);
      }
      wrapper.dataset.category = key;
      return group;
    }

    function addRow(item) {
      if (!item || item.id == null || seenActivityIds.has(item.id)) return;
      seenActivityIds.add(item.id);
      const category = item.category || "Other";
      const targets = [ensureGroup("All Activity"), ensureGroup(category)];
      targets.forEach((group) => {
        const row = document.createElement("tr");
        row.innerHTML = '<td class="mono"></td><td class="cell-message"></td><td class="mono"></td>';
        row.children[0].textContent = fmtTime(item.t);
        row.children[1].textContent = item.message || "WORKER ACTIVITY";
        row.children[2].innerHTML = detailsHtml(item);
        const wasNearBottom = (group.scroll.scrollHeight - group.scroll.scrollTop - group.scroll.clientHeight) < 40;
        group.tbody.appendChild(row);
        group.rows += 1;
        group.count.textContent = String(group.rows) + " item" + (group.rows === 1 ? "" : "s");
        group.empty.hidden = true;
        while (group.tbody.children.length > 120) {
          group.tbody.removeChild(group.tbody.firstChild);
        }
        if (wasNearBottom) group.scroll.scrollTop = group.scroll.scrollHeight;
      });
    }

    function resetFeed() {
      feedEl.innerHTML = "";
      groups.clear();
      seenActivityIds.clear();
    }

    async function loadInitial() {
      try {
        const payload = await fetchJsonWithTimeout("/api/activity", 4000);
        const items = Array.isArray(payload && payload.items) ? payload.items : [];
        resetFeed();
        const allGroup = ensureGroup("All Activity");
        allGroup.empty.hidden = false;
        allGroup.count.textContent = "0 items";
        renderEmptyGroups(payload && payload.taskCatalog);
        items.forEach(addRow);
      } catch (error) {
        statusEl.textContent = "ACTIVITY HISTORY RETRYING...";
      }
    }

    async function refreshActivity() {
      try {
        const payload = await fetchJsonWithTimeout("/api/activity", 4000);
        const items = Array.isArray(payload && payload.items) ? payload.items : [];
        items.forEach(addRow);
      } catch {}
    }

    async function boot() {
      resetFeed();
      const allGroup = ensureGroup("All Activity");
      allGroup.empty.hidden = false;
      allGroup.count.textContent = "0 items";
      renderEmptyGroups();
      void pollHealth();
      void loadInitial();
      const source = new EventSource("/api/activity/stream");
      source.onopen = () => { statusEl.textContent = "LIVE STREAM CONNECTED"; };
      source.onmessage = (event) => {
        try {
          const item = JSON.parse(event.data);
          if (item && item.id != null) addRow(item);
        } catch {}
      };
      source.onerror = () => { statusEl.textContent = "STREAM RECONNECTING, POLL FALLBACK ACTIVE..."; };
      setInterval(pollHealth, 2000);
      setInterval(refreshActivity, 2000);
    }
    boot().catch((error) => {
      statusEl.textContent = "FAILED TO LOAD ACTIVITY";
      addHealthLine('[' + fmtTime(Date.now()) + '] PAGE BOOT ERROR | error=' + String(error && error.message || error));
    });
  </script>
</body>
</html>`;
}

function sampleProcessCpuPct() {
  const nowHrNs = process.hrtime.bigint();
  const usageNow = process.cpuUsage();
  const wallNs = Number(nowHrNs - cpuSampleLastHrNs);
  if (Number.isFinite(wallNs) && wallNs > 0 && wallNs < CPU_SAMPLE_MIN_INTERVAL_NS) {
    return cpuSampleLastPct;
  }
  const cpuMicros =
    (Number(usageNow.user) - Number(cpuSampleLastUsage.user))
    + (Number(usageNow.system) - Number(cpuSampleLastUsage.system));
  if (Number.isFinite(wallNs) && wallNs > 0 && Number.isFinite(cpuMicros) && cpuMicros >= 0) {
    const rawPct = (cpuMicros / (wallNs / 1000)) * 100;
    cpuSampleLastPct =
      Number.isFinite(Number(cpuSampleLastPct)) && Number(cpuSampleLastPct) > 0
        ? (Number(cpuSampleLastPct) * (1 - CPU_SAMPLE_EMA_ALPHA)) + (rawPct * CPU_SAMPLE_EMA_ALPHA)
        : rawPct;
  }
  cpuSampleLastHrNs = nowHrNs;
  cpuSampleLastUsage = usageNow;
  return cpuSampleLastPct;
}

function stableHash(text) {
  return crypto.createHash("sha256").update(String(text || "")).digest("hex");
}

function cacheFilePath(key) {
  return path.join(CACHE_ROOT, `${stableHash(key)}.json`);
}

function sendJson(res, status, payload, extraHeaders = {}) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store, max-age=0",
    "access-control-allow-origin": "*",
    ...extraHeaders,
  });
  res.end(JSON.stringify(payload, null, 2));
}

function sendText(res, status, body, contentType, extraHeaders = {}) {
  res.writeHead(status, {
    "content-type": `${contentType}; charset=utf-8`,
    "cache-control": "no-store, max-age=0",
    "access-control-allow-origin": "*",
    ...extraHeaders,
  });
  res.end(body);
}

function sendBinary(res, status, body, contentType, extraHeaders = {}) {
  res.writeHead(status, {
    "content-type": contentType || "application/octet-stream",
    "cache-control": "no-store, max-age=0",
    "access-control-allow-origin": "*",
    ...extraHeaders,
  });
  res.end(body);
}

function isCacheablePath(pathname) {
  return CACHEABLE_PATH_PATTERNS.some((pattern) => pattern.test(pathname));
}

function ttlForPath(pathname) {
  const found = CACHE_TTL_BY_PATH.find((row) => row.pattern.test(pathname));
  return found ? found.ttlMs : DEFAULT_TTL_MS;
}

function timeoutForPath(pathname) {
  if (/^\/api\/session-history\b/i.test(pathname)) return HISTORY_UPSTREAM_TIMEOUT_MS;
  if (/^\/api\/run-audits\/review\b/i.test(pathname)) return HISTORY_UPSTREAM_TIMEOUT_MS;
  if (/^\/api\/session-audits\/review\b/i.test(pathname)) return HISTORY_UPSTREAM_TIMEOUT_MS;
  if (/^\/api\/v2\/bots\/[^/]+\/run-index\b/i.test(pathname)) return HISTORY_UPSTREAM_TIMEOUT_MS;
  if (/^\/api\/v2\/bots\/[^/]+\/continuity-history\b/i.test(pathname)) return HISTORY_UPSTREAM_TIMEOUT_MS;
  if (/^\/api\/v2\/bots\/[^/]+\/latest-session-card\b/i.test(pathname)) return HISTORY_UPSTREAM_TIMEOUT_MS;
  return UPSTREAM_TIMEOUT_MS;
}

function readCache(key) {
  const now = Date.now();
  const fromMem = cacheMem.get(key) || null;
  if (fromMem && Number.isFinite(Number(fromMem.expiresAtMs)) && Number(fromMem.expiresAtMs) > now) return fromMem;
  const filePath = cacheFilePath(key);
  if (!fs.existsSync(filePath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (Number.isFinite(Number(parsed?.expiresAtMs)) && Number(parsed.expiresAtMs) > now) {
      cacheMem.set(key, parsed);
      return parsed;
    }
  } catch {}
  return null;
}

function writeCache(key, entry) {
  cacheMem.set(key, entry);
  try {
    fs.writeFileSync(cacheFilePath(key), JSON.stringify(entry));
  } catch (error) {
    const message = String(error?.message || error || "");
    if (message) {
      log(`cache_write_skipped key=${stableHash(key).slice(0, 12)} reason=${message}`);
    }
  }
}

function parseMarketVolumeUsd(mkt) {
  const candidates = [mkt?.volumeNum, mkt?.volume, mkt?.volumeUsd, mkt?.volumeUSD, mkt?.totalVolume];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n) && n >= 0) return n;
  }
  return null;
}

function rollingVolumeCachePath(prefix) {
  const safe = String(prefix || "").trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "_");
  return path.join(CACHE_ROOT, `rolling_volume_${safe || "unknown"}.json`);
}

function lastSessionHistoryCachePath(instanceIdLike, marketPrefixLike) {
  const safeInstance = String(instanceIdLike || "").trim().replace(/[^a-zA-Z0-9_-]+/g, "_") || "unknown";
  const safePrefix = String(marketPrefixLike || "").trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "_") || "all";
  return path.join(CACHE_ROOT, `last_session_history_${safeInstance}__${safePrefix}.json`);
}

function readLastSessionHistoryCache(instanceIdLike, marketPrefixLike) {
  try {
    const p = lastSessionHistoryCachePath(instanceIdLike, marketPrefixLike);
    if (!fs.existsSync(p)) return null;
    const parsed = JSON.parse(fs.readFileSync(p, "utf8"));
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function writeLastSessionHistoryCache(instanceIdLike, marketPrefixLike, payload) {
  try {
    fs.writeFileSync(lastSessionHistoryCachePath(instanceIdLike, marketPrefixLike), JSON.stringify(payload, null, 2) + "\n");
  } catch {}
}

function limitLastSessionHistoryPayload(payloadLike, limitLike) {
  const payload = payloadLike && typeof payloadLike === "object" ? payloadLike : {};
  const allSessions = Array.isArray(payload?.sessions) ? payload.sessions.filter((row) => row && typeof row === "object") : [];
  const limitRaw = Number(limitLike);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(5000, Math.floor(limitRaw))) : 100;
  const sessions = allSessions.slice(0, limit);
  return {
    ...payload,
    count: sessions.length,
    totalCount: Number.isFinite(Number(payload?.totalCount)) ? Number(payload.totalCount) : allSessions.length,
    sessions,
    requestedLimit: limit,
  };
}

function runIdentityFromPayload(payloadLike) {
  const payload = payloadLike && typeof payloadLike === "object" ? payloadLike : null;
  const runNum = Number(
    payload?.runNum ??
    payload?.run?.runNum ??
    payload?.summary?.runNum ??
    0
  );
  const runIdText = String(
    payload?.runId ??
    payload?.run?.runId ??
    payload?.summary?.runId ??
    ""
  ).trim();
  return {
    runNum: Number.isFinite(runNum) && runNum > 0 ? Math.floor(runNum) : null,
    runIdText: runIdText || null,
  };
}

function runStartMsFromPayload(payloadLike) {
  const payload = payloadLike && typeof payloadLike === "object" ? payloadLike : null;
  const startMs = Number(
    payload?.runStartMs ??
    payload?.summary?.startedAtMs ??
    payload?.run?.runAuditIdentity?.startedAtMs ??
    payload?.run?.launchedAtMs ??
    payload?.run?.startedAtMs ??
    0
  );
  return Number.isFinite(startMs) && startMs > 0 ? startMs : null;
}

async function listActivePaperBotsForRunAuditWarm() {
  if (!UPSTREAM_ORIGIN) return [];
  const reqUrl = new URL("/api/v2/bots", "http://worker.local");
  reqUrl.searchParams.set("engine", "paper");
  const upstreamUrl = buildUpstreamUrl(reqUrl);
  const payload = await fetchJsonDirect(upstreamUrl.toString());
  const items = Array.isArray(payload?.items) ? payload.items : [];
  return items
    .map((item) => {
      const instanceId = String(item?.instanceId || "").trim();
      const runId = String(item?.runId || "").trim();
      const marketSlug = String(item?.marketSlug || "").trim();
      const status = String(item?.status || "").trim().toLowerCase();
      const runNum = Number(item?.runNum || 0);
      const startedAtMs = Number(item?.launchedAtMs || item?.startedAtMs || 0);
      if (!(instanceId && runId && marketSlug)) return null;
      if (!(Number.isFinite(runNum) && runNum > 0)) return null;
      if (!["running", "starting", "resuming"].includes(status)) return null;
      return {
        instanceId,
        runId,
        runNum: Math.floor(runNum),
        marketSlug,
        status,
        startedAtMs: Number.isFinite(startedAtMs) && startedAtMs > 0 ? Math.floor(startedAtMs) : null,
      };
    })
    .filter(Boolean)
    .slice(0, RUN_AUDIT_BACKGROUND_MAX_BOTS);
}

function buildRunAuditWarmUrl(botLike) {
  const bot = botLike && typeof botLike === "object" ? botLike : {};
  const reqUrl = new URL("/api/run-audits/review", "http://worker.local");
  reqUrl.searchParams.set("runNum", String(Math.floor(Number(bot.runNum || 0) || 0)));
  if (String(bot.runId || "").trim()) reqUrl.searchParams.set("runId", String(bot.runId).trim());
  if (Number.isFinite(Number(bot.startedAtMs || 0)) && Number(bot.startedAtMs) > 0) {
    reqUrl.searchParams.set("startedAtMs", String(Math.floor(Number(bot.startedAtMs))));
  }
  reqUrl.searchParams.set("hostPort", normalizeRunAuditHostPort(RUN_AUDIT_HOST_PORT));
  reqUrl.searchParams.set("format", "json");
  reqUrl.searchParams.set("backgroundWarm", "1");
  return reqUrl;
}

function rowWithinRunStart(rowLike, runStartMsLike) {
  const row = rowLike && typeof rowLike === "object" ? rowLike : null;
  const runStartMs = Number(runStartMsLike);
  if (!row) return false;
  if (!(Number.isFinite(runStartMs) && runStartMs > 0)) return true;
  const rowStartMs = Number(row?.startMs);
  const inferredStartMs = slugStartMs(row?.slug);
  const startMs = Number.isFinite(rowStartMs) && rowStartMs > 0 ? rowStartMs : inferredStartMs;
  if (!(Number.isFinite(startMs) && startMs > 0)) return false;
  return startMs >= runStartMs;
}

function compactSessionSortTs(row) {
  const v = Number(row?.exitTsMs ?? row?.endMs ?? row?.entryTsMs ?? row?.attemptTsMs ?? row?.startMs ?? 0);
  return Number.isFinite(v) ? v : 0;
}

function compactSessionDisplayPnlUsd(rowLike) {
  const row = rowLike && typeof rowLike === "object" ? rowLike : null;
  if (!row) return null;
  const actual = Number(row?.actualPnlUsd);
  const corrected = Number(row?.correctedPnlUsd);
  const preferredAudit = preferNonZeroAuditNumber(row?.auditNetPnlUsd, actual);
  const preferredCorrected = preferNonZeroAuditNumber(corrected, actual);
  return preferNonZeroAuditNumber(preferredAudit, preferredCorrected);
}

function preferNonZeroAuditNumber(preferredLike, fallbackLike) {
  const preferred = Number(preferredLike);
  const fallback = Number(fallbackLike);
  const preferredFinite = Number.isFinite(preferred);
  const fallbackFinite = Number.isFinite(fallback);
  if (preferredFinite) {
    if (Math.abs(preferred) > 1e-9 || !fallbackFinite || Math.abs(fallback) <= 1e-9) {
      return preferred;
    }
  }
  if (fallbackFinite) return fallback;
  return null;
}

function applyAuditLedgerDisplayBalances(rowsLike) {
  const rows = (Array.isArray(rowsLike) ? rowsLike : [])
    .filter((row) => row && typeof row === "object");
  if (!rows.length) return [];
  const rowsAsc = rows
    .slice()
    .sort((a, b) => {
      const ta = Number(a?.startMs ?? a?.entryTsMs ?? a?.attemptTsMs ?? a?.endMs ?? a?.exitTsMs ?? 0);
      const tb = Number(b?.startMs ?? b?.entryTsMs ?? b?.attemptTsMs ?? b?.endMs ?? b?.exitTsMs ?? 0);
      return ta - tb;
    });
  let runningBalanceUsd = 100;
  const displayBalanceBySlug = new Map();
  for (const row of rowsAsc) {
    const slug = String(row?.slug || "").trim();
    const pnlUsd = Number(compactSessionDisplayPnlUsd(row));
    if (!row?.excludeFromPnl && Number.isFinite(pnlUsd)) {
      runningBalanceUsd += pnlUsd;
    }
    if (slug) displayBalanceBySlug.set(slug, Number(runningBalanceUsd.toFixed(4)));
  }
  return rows.map((row) => {
    const slug = String(row?.slug || "").trim();
    const displayBalanceUsd = slug && displayBalanceBySlug.has(slug)
      ? Number(displayBalanceBySlug.get(slug))
      : null;
    return {
      ...row,
      displayBalanceUsd: Number.isFinite(Number(displayBalanceUsd)) ? Number(displayBalanceUsd) : null,
    };
  });
}

function compactSessionRowsDiffer(aLike, bLike) {
  const a = aLike && typeof aLike === "object" ? aLike : null;
  const b = bLike && typeof bLike === "object" ? bLike : null;
  if (!a || !b) return true;
  const numChanged = (x, y) => {
    const nx = Number(x);
    const ny = Number(y);
    if (!Number.isFinite(nx) && !Number.isFinite(ny)) return false;
    if (!Number.isFinite(nx) || !Number.isFinite(ny)) return true;
    return Math.abs(nx - ny) > 1e-9;
  };
  return (
    numChanged(compactSessionDisplayPnlUsd(a), compactSessionDisplayPnlUsd(b)) ||
    numChanged(a?.actualPnlUsd, b?.actualPnlUsd) ||
    numChanged(a?.continuityBalanceUsd ?? a?.balanceUsd, b?.continuityBalanceUsd ?? b?.balanceUsd)
  );
}

function preferSessionRowForHistory(aLike, bLike) {
  const a = aLike && typeof aLike === "object" ? aLike : null;
  const b = bLike && typeof bLike === "object" ? bLike : null;
  if (!a) return b;
  if (!b) return a;
  const score = (row) => {
    let out = 0;
    if (Number.isFinite(Number(row?.auditNetPnlUsd))) out += 16;
    if (Number.isFinite(Number(row?.correctedPnlUsd))) out += 12;
    if (Number.isFinite(Number(row?.actualPnlUsd))) out += 8;
    if (Number.isFinite(Number(row?.grossPnlUsd ?? row?.actualGrossPnlUsd))) out += 2;
    if (Number.isFinite(Number(row?.feesUsd ?? row?.actualFeesUsd))) out += 2;
    if (Number.isFinite(Number(row?.runNum))) out += 1;
    return out;
  };
  const aScore = score(a);
  const bScore = score(b);
  if (bScore !== aScore) return bScore > aScore ? { ...a, ...b } : { ...b, ...a };
  return compactSessionSortTs(b) >= compactSessionSortTs(a) ? { ...a, ...b } : { ...b, ...a };
}

function auditSequentialSlugGaps(rowsLike, marketPrefixLike = "") {
  const rows = (Array.isArray(rowsLike) ? rowsLike : []).filter((row) => row && typeof row === "object");
  if (!rows.length) return { checked: false, missingSlugs: [], intervalMs: null, oldestSlug: null, newestSlug: null };
  const sampleSlug = String(rows[0]?.slug || "").trim();
  const base = String(marketPrefixLike || slugBase(sampleSlug)).trim().toLowerCase();
  const intervalMs = inferSessionIntervalMsFromSlugLocal(sampleSlug);
  if (!base || !(Number.isFinite(intervalMs) && intervalMs > 0)) {
    return { checked: false, missingSlugs: [], intervalMs: null, oldestSlug: null, newestSlug: null };
  }
  const byStart = rows
    .map((row) => ({
      slug: String(row?.slug || "").trim(),
      startMs: slugStartMs(row?.slug) ?? Number(row?.startMs),
    }))
    .filter((row) => row.slug && Number.isFinite(Number(row.startMs)) && slugBase(row.slug) === base)
    .sort((a, b) => Number(a.startMs) - Number(b.startMs));
  if (byStart.length < 2) {
    return {
      checked: true,
      missingSlugs: [],
      intervalMs,
      oldestSlug: byStart[0]?.slug || null,
      newestSlug: byStart[byStart.length - 1]?.slug || null,
    };
  }
  const seen = new Set(byStart.map((row) => row.slug));
  const oldestStartMs = Number(byStart[0].startMs);
  const newestStartMs = Number(byStart[byStart.length - 1].startMs);
  const missingSlugs = [];
  for (let ms = oldestStartMs; ms <= newestStartMs; ms += intervalMs) {
    const slug = `${base}-${Math.floor(ms / 1000)}`;
    if (!seen.has(slug)) missingSlugs.push(slug);
  }
  return {
    checked: true,
    missingSlugs,
    intervalMs,
    oldestSlug: byStart[0]?.slug || null,
    newestSlug: byStart[byStart.length - 1]?.slug || null,
  };
}

function normalizeLastSessionHistoryRow(rowLike) {
  const row = rowLike && typeof rowLike === "object" ? rowLike : null;
  if (!row) return null;
  const slug = String(row.slug || "").trim();
  if (!slug) return null;
  const via = String(row.via || "").trim().toLowerCase();
  if (via === "trace_only") return null;
  if (row.excludeFromPnl === true) return null;
  const closed = row.closed === true || (Number.isFinite(Number(row.endMs)) && Number(row.endMs) < Date.now());
  if (!closed) return null;
  const displayPnlUsd = compactSessionDisplayPnlUsd(row);
  const auditNetPnlCandidate = Number(row?.auditNetPnlUsd);
  const correctedPnlCandidate = Number(row?.correctedPnlUsd);
  const actualPnlCandidate = Number(row?.actualPnlUsd);
  const rawPnlCandidate = Number(row?.pnlUsd);
  const normalizedCorrectedPnlUsd = Number.isFinite(correctedPnlCandidate)
    ? correctedPnlCandidate
    : null;
  const normalizedActualPnlUsd = Number.isFinite(actualPnlCandidate)
    ? (
        Math.abs(actualPnlCandidate) > 1e-9 ||
        !(Number.isFinite(rawPnlCandidate) && Math.abs(rawPnlCandidate) > 1e-9)
      )
        ? actualPnlCandidate
        : rawPnlCandidate
    : null;
  const out = {
    slug,
    instanceId: String(row.instanceId || "").trim() || null,
    runIdText: String(row.runId || "").trim() || null,
    runId: Number.isFinite(Number(row.runId)) ? Number(row.runId) : null,
    runNum: Number.isFinite(Number(row.runNum)) ? Number(row.runNum) : null,
    pnlUsd: Number.isFinite(Number(displayPnlUsd)) ? Number(displayPnlUsd) : null,
    auditNetPnlUsd: Number.isFinite(Number(auditNetPnlCandidate)) ? Number(auditNetPnlCandidate) : null,
    correctedPnlUsd: Number.isFinite(Number(normalizedCorrectedPnlUsd)) ? Number(normalizedCorrectedPnlUsd) : null,
    actualPnlUsd: Number.isFinite(Number(normalizedActualPnlUsd)) ? Number(normalizedActualPnlUsd) : null,
    grossPnlUsd: Number.isFinite(Number(row?.grossPnlUsd ?? row?.actualGrossPnlUsd))
      ? Number(row?.grossPnlUsd ?? row?.actualGrossPnlUsd)
      : null,
    feesUsd: Number.isFinite(Number(row?.feesUsd ?? row?.actualFeesUsd))
      ? Number(row?.feesUsd ?? row?.actualFeesUsd)
      : null,
    continuityBalanceUsd: Number.isFinite(Number(row.continuityBalanceUsd)) ? Number(row.continuityBalanceUsd) : null,
    balanceUsd: Number.isFinite(Number(row.balanceUsd)) ? Number(row.balanceUsd) : null,
    startMs: Number.isFinite(Number(row.startMs)) ? Number(row.startMs) : slugStartMs(slug),
    endMs: Number.isFinite(Number(row.endMs)) ? Number(row.endMs) : null,
    entryTsMs: Number.isFinite(Number(row.entryTsMs)) ? Number(row.entryTsMs) : null,
    exitTsMs: Number.isFinite(Number(row.exitTsMs)) ? Number(row.exitTsMs) : null,
    attempted: row.attempted === true,
    closed: true,
    noTrade: row.noTrade === true || Number(row.laneCount || 0) <= 0,
    laneCount: Number.isFinite(Number(row.laneCount)) ? Number(row.laneCount) : 0,
    via: String(row.via || "").trim() || null,
    reason: String(row.reason || "").trim() || null,
    updatedFromWorkerAtMs: Date.now(),
  };
  return out;
}

async function fetchRunIndexPayloadForInstance(instanceId, opts = {}) {
  const sourceHostPort = String(opts.sourceHostPort || "").trim();
  const marketPrefix = String(opts.marketPrefix || "").trim().toLowerCase();
  const marketSlug = String(opts.marketSlug || "").trim().toLowerCase();
  const limitRaw = Number(opts.limit || opts.maxSessions || 0);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.max(1, Math.min(5000, Math.floor(limitRaw))) : null;
  const upstreamReq = new URL(`/api/v2/bots/${encodeURIComponent(String(instanceId || "").trim())}/run-index`, "http://worker.local");
  if (marketPrefix) upstreamReq.searchParams.set("marketPrefix", marketPrefix);
  if (marketSlug) upstreamReq.searchParams.set("marketSlug", marketSlug);
  if (limit != null) upstreamReq.searchParams.set("limit", String(limit));
  if (sourceHostPort) upstreamReq.searchParams.set("sourceHostPort", normalizeRunAuditHostPort(sourceHostPort));
  const upstreamUrl = buildUpstreamUrl(upstreamReq);
  const payload = await fetchJsonDirect(upstreamUrl.toString());
  return { upstreamUrl, payload };
}

async function fetchContinuityHistoryPayloadForInstance(instanceId, opts = {}) {
  const includeTrace = opts.includeTrace === true;
  const sourceHostPort = String(opts.sourceHostPort || "").trim();
  const maxSessionsRaw = Number(opts.maxSessions || 0);
  const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0 ? Math.max(1, Math.floor(maxSessionsRaw)) : 200;
  const upstreamReq = new URL(`/api/v2/bots/${encodeURIComponent(String(instanceId || "").trim())}/continuity-history`, "http://worker.local");
  upstreamReq.searchParams.set("includeTrace", includeTrace ? "1" : "0");
  upstreamReq.searchParams.set("maxSessions", String(maxSessions));
  if (sourceHostPort) upstreamReq.searchParams.set("sourceHostPort", normalizeRunAuditHostPort(sourceHostPort));
  const upstreamUrl = buildUpstreamUrl(upstreamReq);
  const payload = await fetchJsonDirect(upstreamUrl.toString());
  return { upstreamUrl, payload };
}

function compactRowsFromRunIndexPayload(payload, opts = {}) {
  const instanceId = String(opts.instanceId || "").trim();
  const marketPrefix = String(opts.marketPrefix || "").trim().toLowerCase();
  const marketSlug = String(opts.marketSlug || "").trim().toLowerCase();
  const offsetRaw = Number(opts.offset || 0);
  const maxSessionsRaw = Number(opts.maxSessions || 0);
  const offset = Number.isFinite(offsetRaw) && offsetRaw > 0 ? Math.max(0, Math.floor(offsetRaw)) : 0;
  const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0 ? Math.max(1, Math.floor(maxSessionsRaw)) : null;
  const rows = Array.isArray(payload?.sessions) ? payload.sessions : [];
  const normalized = rows
    .map((row) => normalizeLastSessionHistoryRow({
      ...row,
      noTrade: row?.noTrade === true && !(
        Number.isFinite(Number(row?.actualPnlUsd ?? row?.pnlUsd)) &&
        Math.abs(Number(row?.actualPnlUsd ?? row?.pnlUsd)) > 1e-9
      ),
      instanceId: String(row?.instanceId || instanceId || "").trim() || null,
      runNum: Number(payload?.run?.runNum ?? payload?.summary?.runNum ?? row?.runNum ?? 0) || null,
      runId: Number(payload?.run?.runNum ?? payload?.summary?.runNum ?? row?.runId ?? 0) || null,
      continuityBalanceUsd: Number.isFinite(Number(row?.continuityBalanceUsd))
        ? Number(row.continuityBalanceUsd)
        : (Number.isFinite(Number(row?.balanceUsd)) ? Number(row.balanceUsd) : null),
      via: String(row?.via || "run_index_worker").trim(),
    }))
    .filter(Boolean)
    .filter((row) => {
      const slug = String(row?.slug || "").trim().toLowerCase();
      if (!slug) return false;
      if (marketSlug) return slug === marketSlug;
      if (marketPrefix) return slug.startsWith(marketPrefix);
      return true;
    })
    .sort((a, b) => compactSessionSortTs(b) - compactSessionSortTs(a));
  const windowed = offset > 0 ? normalized.slice(offset) : normalized;
  return maxSessions != null ? windowed.slice(0, maxSessions) : windowed;
}

function compactRowsFromContinuityPayload(payload, opts = {}) {
  const instanceId = String(opts.instanceId || "").trim();
  const marketPrefix = String(opts.marketPrefix || "").trim().toLowerCase();
  const marketSlug = String(opts.marketSlug || "").trim().toLowerCase();
  const offsetRaw = Number(opts.offset || 0);
  const maxSessionsRaw = Number(opts.maxSessions || 0);
  const offset = Number.isFinite(offsetRaw) && offsetRaw > 0 ? Math.max(0, Math.floor(offsetRaw)) : 0;
  const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0 ? Math.max(1, Math.floor(maxSessionsRaw)) : null;
  const rows = Array.isArray(payload?.sessions) ? payload.sessions : [];
  const normalized = rows
    .map((row) => normalizeLastSessionHistoryRow({
      ...row,
      instanceId: String(row?.instanceId || instanceId || "").trim() || null,
      runNum: Number(payload?.runNum ?? row?.runNum ?? 0) || null,
      runId: String(row?.runId || payload?.runId || "").trim() || null,
      continuityBalanceUsd: Number.isFinite(Number(row?.continuityBalanceUsd))
        ? Number(row.continuityBalanceUsd)
        : (Number.isFinite(Number(row?.balanceUsd)) ? Number(row.balanceUsd) : null),
      via: String(row?.via || "continuity_history_worker").trim(),
    }))
    .filter(Boolean)
    .filter((row) => {
      const slug = String(row?.slug || "").trim().toLowerCase();
      if (!slug) return false;
      if (marketSlug) return slug === marketSlug;
      if (marketPrefix) return slug.startsWith(marketPrefix);
      return true;
    })
    .sort((a, b) => compactSessionSortTs(b) - compactSessionSortTs(a));
  const windowed = offset > 0 ? normalized.slice(offset) : normalized;
  return maxSessions != null ? windowed.slice(0, maxSessions) : windowed;
}

async function buildCompactSessionHistoryPayload(reqUrl) {
  const engine = String(reqUrl.searchParams.get("engine") || "paper").trim().toLowerCase();
  const includeTrace = String(reqUrl.searchParams.get("includeTrace") || "0").trim() === "1";
  const instanceId = String(reqUrl.searchParams.get("instanceId") || "").trim();
  if (engine !== "paper" || !instanceId) {
    return { statusCode: 400, payload: { ok: false, error: "compact worker history requires paper instanceId" } };
  }
  const marketPrefix = String(reqUrl.searchParams.get("marketPrefix") || "").trim().toLowerCase();
  const marketSlug = String(reqUrl.searchParams.get("marketSlug") || reqUrl.searchParams.get("slug") || "").trim().toLowerCase();
  const maxSessionsRaw = Number(reqUrl.searchParams.get("maxSessions") || 0);
  const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0 ? Math.max(1, Math.min(5000, Math.floor(maxSessionsRaw))) : null;
  const offsetRaw = Number(reqUrl.searchParams.get("offset") || 0);
  const offset = Number.isFinite(offsetRaw) && offsetRaw > 0 ? Math.max(0, Math.floor(offsetRaw)) : 0;
  let upstreamUrl = null;
  let payload = null;
  let sessionsAll = [];
  let source = "run_index_worker";
  const runIndex = await fetchRunIndexPayloadForInstance(instanceId);
  upstreamUrl = runIndex.upstreamUrl;
  payload = runIndex.payload;
  sessionsAll = compactRowsFromRunIndexPayload(payload, {
    instanceId,
    marketPrefix,
    marketSlug,
    maxSessions: null,
    offset: 0,
  });
  const sessionsWindow = offset > 0 ? sessionsAll.slice(offset) : sessionsAll;
  let sessions = maxSessions != null ? sessionsWindow.slice(0, maxSessions) : sessionsWindow;
  if (includeTrace && sessions.length) {
    sessions = await Promise.all(sessions.map(async (row) => {
      const slug = String(row?.slug || "").trim();
      if (!slug) return row;
      try {
        const canonical = await readRemoteCanonicalSessionJson(slug, true, RUN_AUDIT_HOST_PORT);
        if (canonical && typeof canonical === "object") {
          return normalizeLastSessionHistoryRow({
            ...row,
            ...canonical,
            instanceId: String(canonical?.instanceId || row?.instanceId || instanceId || "").trim() || null,
            runNum: Number.isFinite(Number(canonical?.runNum)) ? Number(canonical.runNum) : row?.runNum,
            runId: canonical?.runId ?? canonical?.runIdText ?? row?.runId ?? row?.runIdText,
            trace: canonical?.trace || row?.trace || null,
            via: String(row?.via || "run_index_worker").trim() || "run_index_worker",
          }) || {
            ...row,
            ...canonical,
            trace: canonical?.trace || row?.trace || null,
          };
        }
      } catch {}
      return row;
    }));
    source = "run_index_remote_canonical_worker";
  }
  const runStartMs = Number(payload?.runStartMs ?? payload?.summary?.startedAtMs ?? payload?.run?.runAuditIdentity?.startedAtMs ?? payload?.run?.launchedAtMs ?? 0) || null;
  const runNum = Number((payload?.runNum ?? payload?.run?.runNum ?? payload?.summary?.runNum ?? reqUrl.searchParams.get("runNum")) || 0) || null;
  return {
    statusCode: 200,
    payload: {
      ok: true,
      engine: "paper",
      runId: runNum,
      runStartMs,
      runStartIso: Number.isFinite(Number(runStartMs)) && Number(runStartMs) > 0 ? new Date(Number(runStartMs)).toISOString() : null,
      sessions,
      count: sessions.length,
      totalCount: sessionsAll.length,
      offset,
      truncated: sessions.length < sessionsAll.length,
      sinceMs: Number(runStartMs || 0),
      source,
      instanceId,
      worker: WORKER_LABEL,
      upstreamUrl: upstreamUrl ? upstreamUrl.toString() : null,
    },
  };
}

async function buildLastSessionHistoryPayload(reqUrl) {
  const parts = String(reqUrl.pathname || "").split("/").filter(Boolean);
  const instanceId = (
    parts.length >= 5 &&
    String(parts[0] || "").toLowerCase() === "api" &&
    String(parts[1] || "").toLowerCase() === "v2" &&
    String(parts[2] || "").toLowerCase() === "bots" &&
    String(parts[4] || "").toLowerCase() === "last-session-history"
  )
    ? decodeURIComponent(String(parts[3] || "").trim())
    : "";
  if (!instanceId) {
    return { statusCode: 400, payload: { ok: false, error: "missing instanceId" } };
  }
  const marketPrefix = String(reqUrl.searchParams.get("marketPrefix") || "").trim().toLowerCase();
  const limitRaw = Number(reqUrl.searchParams.get("limit") || 100);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(5000, Math.floor(limitRaw))) : 100;
  const cached = readLastSessionHistoryCache(instanceId, marketPrefix) || {};
  let upstreamUrl = null;
  let rows = [];
  let runIndexRows = [];
  let continuityRows = [];
  let currentRunNum = null;
  let currentRunIdText = null;
  let currentRunStartMs = null;
  try {
    const runIndex = await fetchRunIndexPayloadForInstance(instanceId, {
      marketPrefix,
      limit: 5000,
    });
    upstreamUrl = runIndex.upstreamUrl;
    const runIdentity = runIdentityFromPayload(runIndex.payload);
    currentRunNum = runIdentity.runNum;
    currentRunIdText = runIdentity.runIdText;
    currentRunStartMs = runStartMsFromPayload(runIndex.payload);
    runIndexRows = compactRowsFromRunIndexPayload(runIndex.payload, {
      instanceId,
      marketPrefix,
      maxSessions: 5000,
      offset: 0,
    }).filter((row) => rowWithinRunStart(row, currentRunStartMs));
  } catch {}
  try {
    const continuity = await fetchContinuityHistoryPayloadForInstance(instanceId, {
      includeTrace: false,
      maxSessions: 5000,
    });
    if (!upstreamUrl) upstreamUrl = continuity.upstreamUrl;
    if (!(Number.isFinite(Number(currentRunNum)) && currentRunNum > 0) || !currentRunIdText || !currentRunStartMs) {
      const continuityIdentity = runIdentityFromPayload(continuity.payload);
      currentRunNum = currentRunNum || continuityIdentity.runNum;
      currentRunIdText = currentRunIdText || continuityIdentity.runIdText;
      currentRunStartMs = currentRunStartMs || runStartMsFromPayload(continuity.payload);
    }
    continuityRows = compactRowsFromContinuityPayload(continuity.payload, {
      instanceId,
      marketPrefix,
      maxSessions: 5000,
      offset: 0,
    }).filter((row) => rowWithinRunStart(row, currentRunStartMs));
    if (continuityRows.length) {
      const continuityRunId = String(continuity.payload?.runId || continuity.payload?.run?.runId || continuity.payload?.summary?.runId || "").trim() || null;
      continuityRows = continuityRows.map((row) => ({
        ...row,
        runNum: Number.isFinite(Number(row?.runNum)) ? Number(row.runNum) : currentRunNum,
        runIdText: String(row?.runIdText || row?.runId || continuityRunId || currentRunIdText || "").trim() || null,
        runId: String(row?.runId || continuityRunId || currentRunIdText || "").trim() || null,
        actualPnlUsd: Number.isFinite(Number(row?.actualPnlUsd))
          ? Number(row.actualPnlUsd)
          : (Number.isFinite(Number(row?.pnlUsd)) ? Number(row.pnlUsd) : null),
        correctedPnlUsd: Number.isFinite(Number(row?.correctedPnlUsd))
          ? Number(row.correctedPnlUsd)
          : null,
        balanceUsd: Number.isFinite(Number(row?.balanceUsd))
          ? Number(row.balanceUsd)
          : (Number.isFinite(Number(row?.continuityBalanceUsd)) ? Number(row.continuityBalanceUsd) : null),
      }));
    }
  } catch {}
  const sourceBySlug = new Map();
  for (const row of runIndexRows) {
    const slug = String(row?.slug || "").trim();
    if (!slug) continue;
    sourceBySlug.set(slug, preferSessionRowForHistory(sourceBySlug.get(slug) || null, row));
  }
  for (const row of continuityRows) {
    const slug = String(row?.slug || "").trim();
    if (!slug) continue;
    sourceBySlug.set(slug, preferSessionRowForHistory(sourceBySlug.get(slug) || null, row));
  }
  for (const row of continuityRows) {
    const slug = String(row?.slug || "").trim();
    if (!slug) continue;
    const existing = sourceBySlug.get(slug);
    if (!existing || typeof existing !== "object") continue;
    sourceBySlug.set(slug, {
      ...existing,
      actualPnlUsd: Number.isFinite(Number(row?.actualPnlUsd))
        ? Number(row.actualPnlUsd)
        : existing?.actualPnlUsd,
      correctedPnlUsd: Number.isFinite(Number(row?.correctedPnlUsd))
        ? Number(row.correctedPnlUsd)
        : existing?.correctedPnlUsd,
      balanceUsd: Number.isFinite(Number(row?.balanceUsd))
        ? Number(row.balanceUsd)
        : existing?.balanceUsd,
      continuityBalanceUsd: Number.isFinite(Number(row?.continuityBalanceUsd))
        ? Number(row.continuityBalanceUsd)
        : existing?.continuityBalanceUsd,
      excludeFromPnl: row?.excludeFromPnl === true ? true : existing?.excludeFromPnl,
    });
  }
  rows = Array.from(sourceBySlug.values())
    .sort((a, b) => compactSessionSortTs(b) - compactSessionSortTs(a));
  const cachedRunNum = Number(cached?.runNum);
  const cachedRunIdText = String(cached?.runIdText || "").trim() || null;
  const cacheRunMismatch =
    (Number.isFinite(Number(currentRunNum)) && Number.isFinite(cachedRunNum) && Math.floor(cachedRunNum) !== Math.floor(Number(currentRunNum))) ||
    (!!currentRunIdText && !!cachedRunIdText && currentRunIdText !== cachedRunIdText);
  const existingRows = cacheRunMismatch
    ? []
    : (Array.isArray(cached?.sessions) ? cached.sessions.filter((row) => row && typeof row === "object" && rowWithinRunStart(row, currentRunStartMs)) : []);
  const bySlug = new Map();
  for (const row of existingRows) {
    const slug = String(row?.slug || "").trim();
    if (!slug || bySlug.has(slug)) continue;
    bySlug.set(slug, row);
  }
  const enrichedRows = await Promise.all(rows.map(async (raw) => {
    let enrichedRaw = raw;
    try {
      const compactAudit = await readRemoteSessionAuditCompact(raw?.runNum ?? currentRunNum, raw?.slug, RUN_AUDIT_HOST_PORT);
      if (compactAudit) {
        const correctedAccounting = correctedAccountingFromCompactAuditSummaryWorker(compactAudit);
        const actualAccounting = actualAccountingFromCompactAuditSummaryWorker(compactAudit);
        const authoritativeAuditNetPnlUsd = preferNonZeroAuditNumber(
          actualAccounting?.actualNetPnlUsd,
          preferNonZeroAuditNumber(correctedAccounting?.correctedNetPnlUsd, raw?.auditNetPnlUsd ?? raw?.actualPnlUsd ?? raw?.correctedPnlUsd)
        );
        enrichedRaw = {
          ...raw,
          auditNetPnlUsd: preferNonZeroAuditNumber(authoritativeAuditNetPnlUsd, raw?.auditNetPnlUsd),
          correctedPnlUsd: preferNonZeroAuditNumber(correctedAccounting?.correctedNetPnlUsd, raw?.correctedPnlUsd),
          actualPnlUsd: preferNonZeroAuditNumber(actualAccounting?.actualNetPnlUsd, raw?.actualPnlUsd),
          grossPnlUsd: Number.isFinite(Number(actualAccounting?.actualGrossPnlUsd))
            ? Number(actualAccounting.actualGrossPnlUsd)
            : raw?.grossPnlUsd,
          feesUsd: Number.isFinite(Number(actualAccounting?.actualFeesUsd))
            ? Number(actualAccounting.actualFeesUsd)
            : raw?.feesUsd,
        };
      }
    } catch {}
    return enrichedRaw;
  }));
  let appended = 0;
  let refreshed = 0;
  for (const enrichedRaw of enrichedRows) {
    const normalized = normalizeLastSessionHistoryRow(enrichedRaw);
    if (!normalized) continue;
    if (marketPrefix && !String(normalized.slug || "").toLowerCase().startsWith(marketPrefix)) continue;
    const existing = bySlug.get(normalized.slug) || null;
    if (!existing) {
      bySlug.set(normalized.slug, normalized);
      appended += 1;
      continue;
    }
    if (compactSessionRowsDiffer(existing, normalized)) {
      bySlug.set(normalized.slug, {
        ...existing,
        ...normalized,
        updatedFromWorkerAtMs: Date.now(),
      });
      refreshed += 1;
    }
  }
  const continuityBySlug = new Map();
  for (const row of continuityRows) {
    const slug = String(row?.slug || "").trim();
    if (!slug || continuityBySlug.has(slug)) continue;
    continuityBySlug.set(slug, row);
  }
  const allRunSessions = applyAuditLedgerDisplayBalances(
    Array.from(bySlug.values())
      .sort((a, b) => compactSessionSortTs(b) - compactSessionSortTs(a))
      .map((row) => {
        const slug = String(row?.slug || "").trim();
        const continuityRow = slug ? continuityBySlug.get(slug) : null;
        if (!continuityRow || typeof continuityRow !== "object") return row;
        const continuityDisplayPnlUsd = compactSessionDisplayPnlUsd(continuityRow);
        return {
          ...row,
          auditNetPnlUsd: Number.isFinite(Number(continuityDisplayPnlUsd))
            ? Number(continuityDisplayPnlUsd)
            : row?.auditNetPnlUsd,
          actualPnlUsd: Number.isFinite(Number(continuityRow?.actualPnlUsd))
            ? Number(continuityRow.actualPnlUsd)
            : row?.actualPnlUsd,
          correctedPnlUsd: Number.isFinite(Number(continuityRow?.correctedPnlUsd))
            ? Number(continuityRow.correctedPnlUsd)
            : row?.correctedPnlUsd,
          pnlUsd: Number.isFinite(Number(continuityDisplayPnlUsd))
            ? Number(continuityDisplayPnlUsd)
            : row?.pnlUsd,
          balanceUsd: Number.isFinite(Number(continuityRow?.balanceUsd))
            ? Number(continuityRow.balanceUsd)
            : row?.balanceUsd,
          continuityBalanceUsd: Number.isFinite(Number(continuityRow?.continuityBalanceUsd))
            ? Number(continuityRow.continuityBalanceUsd)
            : row?.continuityBalanceUsd,
          excludeFromPnl: continuityRow?.excludeFromPnl === true ? true : row?.excludeFromPnl,
        };
      })
  );
  const sessions = allRunSessions
    .sort((a, b) => compactSessionSortTs(b) - compactSessionSortTs(a))
    .slice(0, limit);
  const sessionImageKeepSlugs = sessions
    .map((row) => String(row?.slug || "").trim())
    .filter(Boolean);
  pruneSessionCardImageCacheKeepSlugs(sessionImageKeepSlugs);
  void warmSessionCardImagesForHistoryRows(sessions);
  const gapAudit = auditSequentialSlugGaps(allRunSessions, marketPrefix || slugBase(rows?.[0]?.slug || ""));
  const nextPayload = {
    ok: true,
    worker: WORKER_LABEL,
    instanceId,
    marketPrefix: marketPrefix || null,
    runNum: currentRunNum,
    runIdText: currentRunIdText,
    runStartMs: currentRunStartMs,
    count: allRunSessions.length,
    totalCount: allRunSessions.length,
    sessions: allRunSessions,
    appended,
    refreshed,
    gapAudit,
    source: "last_session_history_worker",
    upstreamUrl: upstreamUrl ? upstreamUrl.toString() : null,
    updatedAtMs: Date.now(),
  };
  writeLastSessionHistoryCache(instanceId, marketPrefix, nextPayload);
  if (appended > 0) {
    pushActivity(`APPENDED ${appended} LAST SESSION HISTORY ROW${appended === 1 ? "" : "S"} FOR BOT ${instanceId.toUpperCase()}`, {
      type: "last-session-history-append",
      instanceId,
      marketPrefix: marketPrefix || null,
      appended: String(appended),
    });
  }
  if (refreshed > 0) {
    pushActivity(`REFRESHED ${refreshed} LAST SESSION HISTORY ROW${refreshed === 1 ? "" : "S"} FOR BOT ${instanceId.toUpperCase()}`, {
      type: "last-session-history-refresh",
      instanceId,
      marketPrefix: marketPrefix || null,
      refreshed: String(refreshed),
    });
  }
  return { statusCode: 200, payload: limitLastSessionHistoryPayload(nextPayload, limit) };
}

function renderLastSessionHistoryHtml(payloadLike) {
  const payload = payloadLike && typeof payloadLike === "object" ? payloadLike : {};
  const sessions = Array.isArray(payload?.sessions) ? payload.sessions : [];
  const buildSessionAuditPath = (rowLike) => {
    const row = rowLike && typeof rowLike === "object" ? rowLike : null;
    const slug = String(row?.slug || "").trim();
    const runNum = Number(row?.runNum ?? payload?.runNum);
    const runId = String(row?.runIdText || row?.runId || payload?.runIdText || "").trim();
    const startedAtMs = Number(payload?.runStartMs);
    if (!(Number.isFinite(runNum) && runNum > 0) || !slug) return "";
    const params = new URLSearchParams();
    params.set("runNum", String(Math.floor(runNum)));
    if (runId) params.set("runId", runId);
    if (Number.isFinite(startedAtMs) && startedAtMs > 0) params.set("startedAtMs", String(Math.floor(startedAtMs)));
    params.set("slug", slug);
    return `/api/session-audits/review?${params.toString()}`;
  };
  const fmtMoney = (valueLike) => {
    const value = Number(valueLike);
    if (!Number.isFinite(value)) return "—";
    const sign = value > 0 ? "+" : (value < 0 ? "-" : "");
    return `${sign}$${Math.abs(value).toFixed(2)}`;
  };
  const fmtTime = (rowLike) => {
    const ts = Number(rowLike?.startMs ?? rowLike?.entryTsMs ?? rowLike?.attemptTsMs ?? rowLike?.endMs ?? rowLike?.exitTsMs ?? 0);
    if (!Number.isFinite(ts) || ts <= 0) return "—";
    try {
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Los_Angeles",
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
      }).format(new Date(ts)) + " PT";
    } catch {
      return new Date(ts).toISOString();
    }
  };
  const toneClass = (valueLike) => {
    const value = Number(valueLike);
    if (!Number.isFinite(value)) return "neu";
    return value > 0 ? "pos" : (value < 0 ? "neg" : "neu");
  };
  const chartPoints = sessions
    .slice()
    .sort((a, b) => {
      const ta = Number(a?.startMs ?? a?.entryTsMs ?? a?.attemptTsMs ?? a?.endMs ?? a?.exitTsMs ?? 0);
      const tb = Number(b?.startMs ?? b?.entryTsMs ?? b?.attemptTsMs ?? b?.endMs ?? b?.exitTsMs ?? 0);
      return ta - tb;
    })
    .map((row) => {
      const ts = Number(row?.startMs ?? row?.entryTsMs ?? row?.attemptTsMs ?? row?.endMs ?? row?.exitTsMs ?? 0);
      const balanceUsd = Number(row?.displayBalanceUsd ?? row?.balanceUsd);
      const pnlUsd = Number(row?.auditNetPnlUsd ?? row?.actualPnlUsd ?? row?.correctedPnlUsd);
      if (!(Number.isFinite(ts) && ts > 0 && Number.isFinite(balanceUsd))) return null;
      return {
        slug: String(row?.slug || "").trim(),
        ts,
        balanceUsd,
        pnlUsd: Number.isFinite(pnlUsd) ? pnlUsd : null,
        timeText: fmtTime(row),
      };
    })
    .filter(Boolean);
  const chartWidth = 1120;
  const chartHeight = 260;
  const chartPadX = 52;
  const chartPadY = 20;
  const chartInnerWidth = chartWidth - chartPadX * 2;
  const chartInnerHeight = chartHeight - chartPadY * 2;
  const minTs = chartPoints.length ? Math.min(...chartPoints.map((pt) => Number(pt.ts))) : 0;
  const maxTs = chartPoints.length ? Math.max(...chartPoints.map((pt) => Number(pt.ts))) : 1;
  const minBal = chartPoints.length ? Math.min(...chartPoints.map((pt) => Number(pt.balanceUsd))) : 0;
  const maxBal = chartPoints.length ? Math.max(...chartPoints.map((pt) => Number(pt.balanceUsd))) : 1;
  const tsSpan = Math.max(1, maxTs - minTs);
  const balPad = Math.max(1, (maxBal - minBal) * 0.08);
  const balLow = minBal - balPad;
  const balHigh = maxBal + balPad;
  const balSpan = Math.max(1, balHigh - balLow);
  const sx = (ts) => chartPadX + (((Number(ts) - minTs) / tsSpan) * chartInnerWidth);
  const sy = (bal) => chartPadY + chartInnerHeight - (((Number(bal) - balLow) / balSpan) * chartInnerHeight);
  const chartPolyline = chartPoints.map((pt) => `${sx(pt.ts).toFixed(1)},${sy(pt.balanceUsd).toFixed(1)}`).join(" ");
  const chartDots = chartPoints.map((pt, idx) => {
    const x = sx(pt.ts).toFixed(1);
    const y = sy(pt.balanceUsd).toFixed(1);
    const label = escapeHtmlLite(`${pt.timeText} · ${fmtMoney(pt.balanceUsd)}${Number.isFinite(Number(pt.pnlUsd)) ? ` · P/L ${fmtMoney(pt.pnlUsd)}` : ""}`);
    return `<circle class="chartDot" cx="${x}" cy="${y}" r="4.5" data-label="${label}" data-x="${x}" data-y="${y}" tabindex="0"><title>${label}</title></circle>`;
  }).join("");
  const chartGrid = [0, 0.25, 0.5, 0.75, 1].map((frac) => {
    const y = chartPadY + chartInnerHeight * frac;
    const bal = balHigh - balSpan * frac;
    return `<g><line x1="${chartPadX}" y1="${y.toFixed(1)}" x2="${(chartPadX + chartInnerWidth).toFixed(1)}" y2="${y.toFixed(1)}" stroke="rgba(255,255,255,.08)" stroke-width="1"/><text x="10" y="${(y + 4).toFixed(1)}" fill="#7f8b9d" font-size="11">${escapeHtmlLite(fmtMoney(bal))}</text></g>`;
  }).join("");
  const chartSvg = chartPoints.length
    ? `
      <div class="curveCard">
        <div class="curveHead">Equity Curve</div>
        <div class="curveWrap">
          <svg viewBox="0 0 ${chartWidth} ${chartHeight}" class="curveSvg" role="img" aria-label="Run equity curve">
            ${chartGrid}
            <polyline fill="none" stroke="rgba(83,223,150,.95)" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round" points="${chartPolyline}"></polyline>
            ${chartDots}
          </svg>
          <div class="curveTooltip" id="curveTooltip" hidden></div>
        </div>
      </div>`
    : "";
  const rowsHtml = sessions.map((row, idx) => {
    const slug = escapeHtmlLite(String(row?.slug || ""));
    const timeText = escapeHtmlLite(fmtTime(row));
    const pnlUsd = Number(row?.auditNetPnlUsd ?? row?.actualPnlUsd ?? row?.correctedPnlUsd);
    const balanceUsd = Number(row?.displayBalanceUsd ?? row?.balanceUsd);
    const sortTs = Number(row?.startMs ?? row?.entryTsMs ?? row?.attemptTsMs ?? row?.endMs ?? row?.exitTsMs ?? 0);
    const sessionAuditPath = escapeHtmlLite(String(row?.sessionAuditPath || row?.sessionAuditHref || buildSessionAuditPath(row) || ""));
    return `
      <tr data-sort-idx="${idx + 1}" data-sort-session="${Number.isFinite(sortTs) ? sortTs : 0}" data-sort-pnl="${Number.isFinite(pnlUsd) ? pnlUsd : 0}" data-sort-balance="${Number.isFinite(balanceUsd) ? balanceUsd : 0}">
        <td>${idx + 1}</td>
        <td>${sessionAuditPath ? `<a href="${sessionAuditPath}" target="_blank" rel="noopener noreferrer">${timeText}</a>` : timeText}<div class="slug">${slug}</div></td>
        <td class="${toneClass(pnlUsd)}">${escapeHtmlLite(fmtMoney(pnlUsd))}</td>
        <td class="${toneClass(balanceUsd)}">${escapeHtmlLite(fmtMoney(balanceUsd))}</td>
      </tr>`;
  }).join("");
  const titleBits = [
    `Last Session History`,
    payload?.instanceId ? `Bot ${escapeHtmlLite(String(payload.instanceId))}` : "",
    payload?.marketPrefix ? escapeHtmlLite(String(payload.marketPrefix)) : "",
    Number.isFinite(Number(payload?.runNum)) ? `Run ${Math.floor(Number(payload.runNum))}` : "",
  ].filter(Boolean).join(" · ");
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${escapeHtmlLite(titleBits)}</title>
  <style>
    :root{color-scheme:dark;}
    body{margin:0;background:#111214;color:#eef4fb;font:14px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}
    .wrap{max-width:1200px;margin:0 auto;padding:24px;}
    h1{margin:0 0 8px;font-size:22px;}
    .meta{color:#98a3b5;font-size:13px;margin-bottom:18px;}
    .curveCard{margin:0 0 18px;padding:14px 16px;background:#17191d;border:1px solid rgba(255,255,255,.08);border-radius:14px;}
    .curveHead{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#98a3b5;margin:0 0 10px;}
    .curveWrap{position:relative;}
    .curveSvg{display:block;width:100%;height:auto;overflow:visible;}
    .chartDot{fill:#53df96;stroke:#0f1115;stroke-width:2;cursor:pointer;}
    .chartDot:hover,.chartDot:focus-visible{fill:#f0d35b;outline:none;}
    .curveTooltip{position:absolute;pointer-events:none;z-index:2;min-width:160px;max-width:260px;padding:8px 10px;border-radius:10px;background:rgba(9,11,14,.96);border:1px solid rgba(255,255,255,.12);color:#eef4fb;font-size:12px;box-shadow:0 10px 28px rgba(0,0,0,.35);transform:translate(-50%,-115%);}
    table{width:100%;border-collapse:collapse;background:#17191d;border:1px solid rgba(255,255,255,.08);border-radius:14px;overflow:hidden;}
    th,td{padding:12px 14px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;vertical-align:top;}
    th{font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:#98a3b5;background:#14161a;}
    .sortBtn{all:unset;display:inline-flex;align-items:center;gap:6px;cursor:pointer;color:inherit;}
    .sortBtn:hover,.sortBtn:focus-visible{color:#eef4fb;}
    .sortArrow{font-size:10px;opacity:.65;}
    tr:last-child td{border-bottom:none;}
    .slug{margin-top:4px;color:#7f8b9d;font-size:12px;word-break:break-all;}
    .pos{color:#53df96;font-weight:700;}
    .neg{color:#ff7d7d;font-weight:700;}
    .neu{color:#eef4fb;}
    a{color:#cfe3ff;text-decoration:none;}
    a:hover,a:focus-visible{text-decoration:underline;}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>${escapeHtmlLite(titleBits)}</h1>
    <div class="meta">Rows: ${sessions.length} · Total in run: ${Number(payload?.totalCount || sessions.length)}</div>
    ${chartSvg}
    <table>
      <thead>
        <tr>
          <th><button class="sortBtn" type="button" data-sort-key="idx"># <span class="sortArrow">↕</span></button></th>
          <th><button class="sortBtn" type="button" data-sort-key="session">Session <span class="sortArrow">↕</span></button></th>
          <th><button class="sortBtn" type="button" data-sort-key="pnl">Actual P/L <span class="sortArrow">↕</span></button></th>
          <th><button class="sortBtn" type="button" data-sort-key="balance">Balance <span class="sortArrow">↕</span></button></th>
        </tr>
      </thead>
      <tbody id="runHistoryTbody">${rowsHtml || '<tr><td colspan="4">No rows available.</td></tr>'}</tbody>
    </table>
  </div>
  <script>
    (() => {
      const tooltip = document.getElementById("curveTooltip");
      const wrap = document.querySelector(".curveWrap");
      if (!tooltip || !wrap) return;
      const show = (dot) => {
        if (!(dot instanceof Element)) return;
        const label = String(dot.getAttribute("data-label") || "").trim();
        const x = Number(dot.getAttribute("data-x") || 0);
        const y = Number(dot.getAttribute("data-y") || 0);
        if (!label) return;
        tooltip.textContent = label;
        tooltip.hidden = false;
        tooltip.style.left = x + "px";
        tooltip.style.top = y + "px";
      };
      const hide = () => { tooltip.hidden = true; };
      wrap.querySelectorAll(".chartDot").forEach((dot) => {
        dot.addEventListener("mouseenter", () => show(dot));
        dot.addEventListener("focus", () => show(dot));
        dot.addEventListener("mouseleave", hide);
        dot.addEventListener("blur", hide);
      });
      wrap.addEventListener("mouseleave", hide);
    })();
    (() => {
      const tbody = document.getElementById("runHistoryTbody");
      const buttons = Array.from(document.querySelectorAll(".sortBtn[data-sort-key]"));
      if (!(tbody instanceof HTMLElement) || !buttons.length) return;
      let currentKey = "idx";
      let currentDir = "asc";
      const readValue = (row, key) => {
        const attr = key === "session" ? "sortSession" : (key === "pnl" ? "sortPnl" : (key === "balance" ? "sortBalance" : "sortIdx"));
        const value = Number(row.dataset[attr] || 0);
        return Number.isFinite(value) ? value : 0;
      };
      const updateButtons = () => {
        buttons.forEach((btn) => {
          const key = String(btn.getAttribute("data-sort-key") || "");
          const arrow = btn.querySelector(".sortArrow");
          if (arrow) arrow.textContent = key === currentKey ? (currentDir === "asc" ? "↑" : "↓") : "↕";
        });
      };
      const sortRows = (key, dir) => {
        const rows = Array.from(tbody.querySelectorAll("tr")).filter((row) => row instanceof HTMLTableRowElement && row.dataset.sortIdx);
        rows.sort((a, b) => {
          const av = readValue(a, key);
          const bv = readValue(b, key);
          if (av === bv) {
            const ai = readValue(a, "idx");
            const bi = readValue(b, "idx");
            return dir === "asc" ? ai - bi : bi - ai;
          }
          return dir === "asc" ? av - bv : bv - av;
        });
        rows.forEach((row) => tbody.appendChild(row));
      };
      buttons.forEach((btn) => {
        btn.addEventListener("click", () => {
          const key = String(btn.getAttribute("data-sort-key") || "idx");
          const nextDir = key === currentKey ? (currentDir === "asc" ? "desc" : "asc") : (key === "idx" ? "asc" : "desc");
          currentKey = key;
          currentDir = nextDir;
          sortRows(currentKey, currentDir);
          updateButtons();
        });
      });
      updateButtons();
    })();
  </script>
</body>
</html>`;
}

function readRollingVolumeCache(prefix) {
  try {
    const p = rollingVolumeCachePath(prefix);
    if (!fs.existsSync(p)) return null;
    const row = JSON.parse(fs.readFileSync(p, "utf8"));
    return row && typeof row === "object" ? row : null;
  } catch {
    return null;
  }
}

function writeRollingVolumeCache(prefix, payload) {
  try {
    fs.writeFileSync(rollingVolumeCachePath(prefix), JSON.stringify(payload, null, 2) + "\n");
  } catch {}
}

function shouldBypassCache(reqUrl) {
  const refresh = String(reqUrl.searchParams.get("refresh") || "").trim();
  return refresh === "1" || String(reqUrl.searchParams.get("cacheBust") || "").trim() === "1";
}

function buildUpstreamUrl(reqUrl) {
  const upstreamReq = new URL(reqUrl.pathname + reqUrl.search, "http://worker.local");
  const sourceHostPort = String(upstreamReq.searchParams.get("sourceHostPort") || "").trim();
  if (sourceHostPort) upstreamReq.searchParams.delete("sourceHostPort");
  const mappedOrigin = sourceHostPort ? String(UPSTREAM_ORIGIN_MAP.get(sourceHostPort) || "").trim() : "";
  const resolvedUpstreamOrigin = mappedOrigin || UPSTREAM_ORIGIN;
  if (!resolvedUpstreamOrigin) throw new Error("UPSTREAM_ORIGIN is required");
  const upstream = new URL(upstreamReq.pathname + upstreamReq.search, `${resolvedUpstreamOrigin}/`);
  const pathName = String(upstream.pathname || "").trim();
  const allowHotPath =
    /^\/api\/v2\/bots(?:\/[^/]+)?$/i.test(pathName) ||
    /^\/api\/v2\/bots\/[^/]+\/run-index\b/i.test(pathName) ||
    /^\/api\/v2\/bots\/[^/]+\/focused-live-session\b/i.test(pathName) ||
    /^\/api\/v2\/bots\/[^/]+\/latest-session-card\b/i.test(pathName) ||
    /^\/api\/v2\/bots\/[^/]+\/rollover-ready\b/i.test(pathName) ||
    /^\/api\/stats\/summary\b/i.test(pathName) ||
    /^\/api\/operator-notices\b/i.test(pathName) ||
    /^\/api\/v2\/strategies\b/i.test(pathName) ||
    /^\/api\/v2\/markets\/(?:volume-5m-24h|hot|audit)\b/i.test(pathName) ||
    /^\/api\/v2\/portfolio\/(?:summary|markets)\b/i.test(pathName) ||
    /^\/api\/v2\/drive\/audit\b/i.test(pathName);
  if (allowHotPath && !upstream.searchParams.has("allowHot")) upstream.searchParams.set("allowHot", ALLOW_HOT_QUERY);
  return upstream;
}

function fetchUrl(url) {
  const client = url.protocol === "https:" ? https : http;
  const timeoutMs = timeoutForPath(url.pathname);
  return new Promise((resolve, reject) => {
    const req = client.request(url, {
      method: "GET",
      headers: {
        [READ_ONLY_WORKER_HEADER]: "1",
      },
    }, (resp) => {
      const chunks = [];
      resp.on("data", (chunk) => chunks.push(chunk));
      resp.on("end", () => {
        resolve({
          statusCode: Number(resp.statusCode || 500),
          headers: resp.headers || {},
          body: Buffer.concat(chunks),
        });
      });
    });
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
    req.end();
  });
}

async function proxyWithCache(reqUrl) {
  const cacheKey = reqUrl.pathname + "?" + reqUrl.searchParams.toString();
  const canCache = isCacheablePath(reqUrl.pathname);
  const bypass = shouldBypassCache(reqUrl);
  if (canCache && !bypass) {
    const cached = readCache(cacheKey);
    if (cached) {
      pushActivity(`USING CACHED RESPONSE FOR ${summarizeUrl(reqUrl).toUpperCase()}`, {
        type: "proxy-cache-hit",
        path: reqUrl.pathname,
      });
      return { ...cached, cacheStatus: "hit" };
    }
  }
  if (inflight.has(cacheKey)) return inflight.get(cacheKey);
  const job = (async () => {
    const upstreamUrl = buildUpstreamUrl(reqUrl);
    pushActivity(`FETCHING UPSTREAM ${summarizeUrl(upstreamUrl).toUpperCase()}`, {
      type: "proxy-fetch",
      path: reqUrl.pathname,
      refresh: bypass ? "1" : "0",
    });
    const result = await fetchUrl(upstreamUrl);
    const contentType = String(result.headers["content-type"] || "application/json").split(";")[0].trim() || "application/json";
    const entry = {
      statusCode: result.statusCode,
      contentType,
      bodyBase64: result.body.toString("base64"),
      cachedAtMs: Date.now(),
      expiresAtMs: Date.now() + (result.statusCode >= 400 ? ERROR_TTL_MS : ttlForPath(reqUrl.pathname)),
      upstreamUrl: upstreamUrl.toString(),
    };
    if (canCache && result.statusCode < 500) writeCache(cacheKey, entry);
    pushActivity(`UPSTREAM ${result.statusCode >= 400 ? "ERROR" : "RESPONSE"} ${String(result.statusCode)} FOR ${summarizeUrl(upstreamUrl).toUpperCase()}`, {
      type: "proxy-response",
      path: reqUrl.pathname,
      statusCode: result.statusCode,
    });
    return { ...entry, cacheStatus: canCache ? "miss" : "pass" };
  })().finally(() => inflight.delete(cacheKey));
  inflight.set(cacheKey, job);
  return job;
}

async function fetchJsonDirect(url) {
  const result = await fetchUrl(new URL(url));
  if (result.statusCode < 200 || result.statusCode >= 300) {
    throw new Error(`fetch failed status=${result.statusCode} url=${url}`);
  }
  return JSON.parse(result.body.toString("utf8"));
}

async function fetchTextDirect(url) {
  const result = await fetchUrl(new URL(url));
  if (result.statusCode < 200 || result.statusCode >= 300) {
    throw new Error(`fetch failed status=${result.statusCode} url=${url}`);
  }
  return result.body.toString("utf8");
}

const noTradeReasonAuditCache = new Map();

function normalizeNoTradeReasonText(reasonLike) {
  const reason = String(reasonLike || "").trim();
  if (!reason) return null;
  if (/^no trade$/i.test(reason)) return null;
  if (/^-+$/.test(reason)) return null;
  if (/^none$/i.test(reason)) return null;
  return reason;
}

function extractNoTradeReasonFromAuditHtml(htmlLike) {
  const html = String(htmlLike || "");
  if (!html) return null;
  const blockerMatches = Array.from(
    html.matchAll(/<div class="sideAuditLine"><b>Blockers<\/b>\s*([^<]*)<\/div>/gi)
  );
  for (const match of blockerMatches) {
    const reason = normalizeNoTradeReasonText(match?.[1]);
    if (reason) return reason;
  }
  return null;
}

async function fetchNoTradeReasonFromAudit(instanceRunNum, slug, continuityRunId, continuityStartedAtMs) {
  const runNum = Number(instanceRunNum);
  const sessionSlug = String(slug || "").trim();
  if (!(Number.isFinite(runNum) && runNum > 0) || !sessionSlug) return null;
  const cacheKey = `${runNum}:${sessionSlug}`;
  const cached = noTradeReasonAuditCache.get(cacheKey);
  if (cached && (Date.now() - Number(cached.atMs || 0)) < 30_000) {
    return cached.reason ?? null;
  }
  try {
    const reviewReq = new URL("/api/session-audits/review", "http://worker.local");
    reviewReq.searchParams.set("runNum", String(runNum));
    reviewReq.searchParams.set("slug", sessionSlug);
    if (continuityRunId) reviewReq.searchParams.set("runId", String(continuityRunId));
    if (Number.isFinite(Number(continuityStartedAtMs)) && Number(continuityStartedAtMs) > 0) {
      reviewReq.searchParams.set("startedAtMs", String(Number(continuityStartedAtMs)));
    }
    const html = await fetchTextDirect(buildUpstreamUrl(reviewReq).toString());
    const reason = extractNoTradeReasonFromAuditHtml(html);
    noTradeReasonAuditCache.set(cacheKey, { atMs: Date.now(), reason: reason || null });
    return reason || null;
  } catch {
    noTradeReasonAuditCache.set(cacheKey, { atMs: Date.now(), reason: null });
    return null;
  }
}

async function fetchBinaryDirect(url) {
  const result = await fetchUrl(new URL(url));
  if (result.statusCode < 200 || result.statusCode >= 300) {
    throw new Error(`fetch failed status=${result.statusCode} url=${url}`);
  }
  return {
    body: Buffer.from(result.body),
    contentType: String(result.headers["content-type"] || "").split(";")[0].trim() || "application/octet-stream",
  };
}

function safeSessionCardSlug(slugLike) {
  return String(slugLike || "").trim().replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function sessionCardImagePaths(slugLike) {
  const safe = safeSessionCardSlug(slugLike) || "session";
  return {
    dir: SESSION_CARD_IMAGE_CACHE_ROOT,
    svgPath: path.join(SESSION_CARD_IMAGE_CACHE_ROOT, `${safe}.svg`),
    jpgPath: path.join(SESSION_CARD_IMAGE_CACHE_ROOT, `${safe}.jpg`),
    metaPath: path.join(SESSION_CARD_IMAGE_CACHE_ROOT, `${safe}.json`),
  };
}

function pruneSessionCardImageCacheKeepSlugs(keepSlugsLike) {
  const keepSafe = new Set(
    (Array.isArray(keepSlugsLike) ? keepSlugsLike : [keepSlugsLike])
      .map((slugLike) => safeSessionCardSlug(slugLike))
      .filter(Boolean)
  );
  if (!keepSafe.size) return;
  try {
    const names = fs.readdirSync(SESSION_CARD_IMAGE_CACHE_ROOT, { withFileTypes: true });
    for (const entry of names) {
      if (!entry?.isFile?.()) continue;
      const name = String(entry.name || "");
      if (!name) continue;
      if (!/\.(svg|jpg|json)$/i.test(name)) continue;
      const base = name.replace(/\.(svg|jpg|json)$/i, "");
      if (keepSafe.has(base)) continue;
      try { fs.unlinkSync(path.join(SESSION_CARD_IMAGE_CACHE_ROOT, name)); } catch {}
    }
  } catch {}
}

async function warmSessionCardImagesForHistoryRows(rowsLike) {
  const rows = (Array.isArray(rowsLike) ? rowsLike : [])
    .map((row) => String(row?.slug || "").trim())
    .filter(Boolean)
    .slice(0, 100);
  if (!rows.length) return;
  await mapLimit(rows, 3, async (slug) => {
    const paths = sessionCardImagePaths(slug);
    if (fs.existsSync(paths.jpgPath) && fs.existsSync(paths.metaPath)) return true;
    const reqUrl = new URL("http://worker.local/api/session-card-image");
    reqUrl.searchParams.set("slug", slug);
    reqUrl.searchParams.set("format", "jpeg");
    try { await ensureSessionCardJpeg(reqUrl); } catch {}
    return true;
  });
}

function runCommand(cmd, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += String(chunk || ""); });
    child.stderr.on("data", (chunk) => { stderr += String(chunk || ""); });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(stderr || stdout || `${cmd} exit=${code}`));
    });
  });
}

function shQuote(value) {
  return `'${String(value ?? "").replace(/'/g, `'\"'\"'`)}'`;
}

async function runRemoteBash(script) {
  if (!(RUN_AUDIT_REMOTE_HOST && RUN_AUDIT_REMOTE_KEY)) {
    throw new Error("remote ssh config missing");
  }
  return runCommand("ssh", [
    "-i", RUN_AUDIT_REMOTE_KEY,
    "-o", "StrictHostKeyChecking=no",
    "-o", "ConnectTimeout=3",
    "-o", "ServerAliveInterval=2",
    "-o", "ServerAliveCountMax=1",
    RUN_AUDIT_REMOTE_HOST,
    "bash",
    "-lc",
    script,
  ]);
}

function remoteCanonicalHostRoot(hostPortLike = "") {
  if (!RUN_AUDIT_REMOTE_ROOT) return "";
  const safeHost = normalizeRunAuditHostPort(hostPortLike || RUN_AUDIT_HOST_PORT);
  return `${RUN_AUDIT_REMOTE_ROOT}/trade_logs/hosts/host_${safeHost}`;
}

const DUBLIN_CANONICAL_HOST_ROOT = remoteCanonicalHostRoot("8788");
const SESSION_CARD_TRACE_AUDIT_VERSION = 3;
const SESSION_CARD_TRACE_AUDIT_MIN_POINTS = 12;
const SESSION_CARD_TRACE_AUDIT_MAX_EDGE_GAP_MS = 45000;

async function findRemoteLatestRunSummaryPath() {
  if (!DUBLIN_CANONICAL_HOST_ROOT) throw new Error("remote canonical host root missing");
  const script = `ROOT=${shQuote(DUBLIN_CANONICAL_HOST_ROOT)}; ls -1dt "$ROOT"/runs/run_*/run_summary.json 2>/dev/null | head -n 1`;
  const { stdout } = await runRemoteBash(script);
  const result = String(stdout || "").trim().split(/\r?\n/).map((s) => String(s || "").trim()).filter(Boolean)[0] || "";
  if (!result) throw new Error("remote run summary not found");
  return result;
}

async function readRemoteTextFile(filePath) {
  const script = `cat ${shQuote(filePath)}`;
  const { stdout } = await runRemoteBash(script);
  return String(stdout || "");
}

async function readRemoteBinaryFile(filePath) {
  const script = `python3 - <<'PY'
from pathlib import Path
import base64
import sys
p = Path(${JSON.stringify(String(filePath || ""))})
if not p.exists():
    raise SystemExit(44)
sys.stdout.write(base64.b64encode(p.read_bytes()).decode("ascii"))
PY`;
  const { stdout } = await runRemoteBash(script);
  const payload = String(stdout || "").trim();
  if (!payload) throw new Error(`remote file empty: ${filePath}`);
  return Buffer.from(payload, "base64");
}

async function readRemoteJsonFile(filePath) {
  return JSON.parse(await readRemoteTextFile(filePath));
}

const remoteSessionAuditCompactCache = new Map();

async function readRemoteSessionAuditCompact(runNumLike, slugLike, hostPortLike = "") {
  const runNum = Math.floor(Number(runNumLike));
  const slug = String(slugLike || "").trim();
  const canonicalRoot = remoteCanonicalHostRoot(hostPortLike);
  if (!(Number.isFinite(runNum) && runNum > 0) || !slug || !canonicalRoot) return null;
  const cacheKey = `${normalizeRunAuditHostPort(hostPortLike || RUN_AUDIT_HOST_PORT)}:${runNum}:${slug}`;
  const cached = remoteSessionAuditCompactCache.get(cacheKey);
  if (cached && (Date.now() - Number(cached.atMs || 0)) < 15_000) return cached.row ?? null;
  try {
    const sourceBase = await resolveRunAuditSourceBaseUrl(hostPortLike || RUN_AUDIT_HOST_PORT);
    const compactUrl = new URL("/api/session-audits/review", sourceBase);
    compactUrl.searchParams.set("runNum", String(runNum));
    compactUrl.searchParams.set("slug", slug);
    compactUrl.searchParams.set("format", "compact");
    const row = JSON.parse(await fetchTextDirect(compactUrl.toString()));
    remoteSessionAuditCompactCache.set(cacheKey, { atMs: Date.now(), row: row && typeof row === "object" ? row : null });
    return row && typeof row === "object" ? row : null;
  } catch {
    remoteSessionAuditCompactCache.set(cacheKey, { atMs: Date.now(), row: null });
    return null;
  }
}

function correctedAccountingFromCompactAuditSummaryWorker(compactLike) {
  const compact = compactLike && typeof compactLike === "object" ? compactLike : null;
  const summary = compact && compact.correctedSummary && typeof compact.correctedSummary === "object"
    ? compact.correctedSummary
    : null;
  if (!summary) return null;
  const correctedNetPnlUsd = Number(summary.correctedNetPnlUsd);
  if (!Number.isFinite(correctedNetPnlUsd)) return null;
  return {
    actualNetPnlUsd: Number.isFinite(Number(summary.actualNetPnlUsd)) ? Number(summary.actualNetPnlUsd) : null,
    actualGrossPnlUsd: Number.isFinite(Number(summary.actualGrossPnlUsd)) ? Number(summary.actualGrossPnlUsd) : null,
    actualFeesUsd: Number.isFinite(Number(summary.actualFeesUsd)) ? Number(summary.actualFeesUsd) : null,
    correctedNetPnlUsd,
    correctedGrossPnlUsd: Number.isFinite(Number(summary.correctedGrossPnlUsd)) ? Number(summary.correctedGrossPnlUsd) : null,
    correctedFeesUsd: Number.isFinite(Number(summary.correctedFeesUsd)) ? Number(summary.correctedFeesUsd) : null,
    correctedDeltaUsd: Number.isFinite(Number(summary.correctedDeltaUsd)) ? Number(summary.correctedDeltaUsd) : null,
    projectedFillNetPnlUsd: Number.isFinite(Number(summary.projectedFillNetPnlUsd)) ? Number(summary.projectedFillNetPnlUsd) : null,
    settleNetPnlUsd: Number.isFinite(Number(summary.settleNetPnlUsd)) ? Number(summary.settleNetPnlUsd) : null,
    correctedExitStrategy: String(summary.correctedExitStrategy || "").trim() || null,
  };
}

function actualAccountingFromCompactAuditSummaryWorker(compactLike) {
  const compact = compactLike && typeof compactLike === "object" ? compactLike : null;
  if (!compact) return null;
  const financials = compact?.financials && typeof compact.financials === "object" ? compact.financials : null;
  const sessionSummary = compact?.sessionSummary && typeof compact.sessionSummary === "object" ? compact.sessionSummary : null;
  const source = financials || sessionSummary;
  if (!source) return null;
  const actualNetPnlUsd = Number(source?.actualNetPnlUsd ?? source?.netPnlUsd ?? source?.pnlUsd);
  const actualFeesUsd = Number(source?.actualFeesUsd ?? source?.feesUsd);
  const actualGrossPnlUsdRaw = Number(source?.actualGrossPnlUsd ?? source?.grossPnlUsd);
  const actualGrossPnlUsd = Number.isFinite(actualGrossPnlUsdRaw)
    ? actualGrossPnlUsdRaw
    : (
        Number.isFinite(actualNetPnlUsd) && Number.isFinite(actualFeesUsd)
          ? Number((actualNetPnlUsd + actualFeesUsd).toFixed(10))
          : NaN
      );
  if (!Number.isFinite(actualNetPnlUsd) && !Number.isFinite(actualGrossPnlUsd) && !Number.isFinite(actualFeesUsd)) return null;
  return {
    actualNetPnlUsd: Number.isFinite(actualNetPnlUsd) ? actualNetPnlUsd : null,
    actualGrossPnlUsd: Number.isFinite(actualGrossPnlUsd) ? actualGrossPnlUsd : null,
    actualFeesUsd: Number.isFinite(actualFeesUsd) ? actualFeesUsd : null,
  };
}

async function writeRemoteTextFile(filePath, text) {
  const dir = path.posix.dirname(String(filePath || "").trim());
  const script = `mkdir -p ${shQuote(dir)}
python3 - <<'PY'
from pathlib import Path
Path(${JSON.stringify(String(filePath || ""))}).write_text(${JSON.stringify(String(text || ""))}, encoding="utf-8")
PY`;
  await runRemoteBash(script);
}

async function writeRemoteBinaryFile(filePath, body) {
  const dir = path.posix.dirname(String(filePath || "").trim());
  const payloadB64 = Buffer.from(body || Buffer.alloc(0)).toString("base64");
  const script = `mkdir -p ${shQuote(dir)}
python3 - <<'PY'
from pathlib import Path
import base64
Path(${JSON.stringify(String(filePath || ""))}).write_bytes(base64.b64decode(${JSON.stringify(payloadB64)}))
PY`;
  await runRemoteBash(script);
}

async function readRemoteLatestRunSummaryJson() {
  const summaryPath = await findRemoteLatestRunSummaryPath();
  const payload = await readRemoteJsonFile(summaryPath);
  return {
    path: summaryPath,
    payload,
  };
}

function normalizedSummarySessions(summaryPayload, marketPrefix = "", limit = 100) {
  const sessions = (Array.isArray(summaryPayload?.sessions) ? summaryPayload.sessions : [])
    .filter((row) => {
      const slug = String(row?.slug || "").trim().toLowerCase();
      if (!slug) return false;
      return !marketPrefix || slug.startsWith(`${marketPrefix}-`);
    })
    .map((row) => ({
      slug: String(row?.slug || "").trim() || null,
      humanLabel: String(row?.humanLabel || "").trim() || null,
      pnlUsd: Number.isFinite(Number(row?.pnlUsd)) ? Number(row.pnlUsd) : null,
      continuityBalanceUsd: Number.isFinite(Number(row?.cumulativeBalanceUsd ?? row?.continuityBalanceUsd)) ? Number(row?.cumulativeBalanceUsd ?? row?.continuityBalanceUsd) : null,
      noTrade: row?.noTrade === true,
      closed: row?.closed === true,
      startMs: Number.isFinite(Number(row?.startMs)) ? Number(row.startMs) : null,
      endMs: Number.isFinite(Number(row?.endMs)) ? Number(row.endMs) : null,
      source: "run_summary",
    }))
    .sort((a, b) => Number(b?.startMs || 0) - Number(a?.startMs || 0));
  return sessions.slice(0, Math.max(1, Math.min(200, Math.floor(Number(limit) || 100))));
}

async function readRemoteCanonicalSessionJson(slug, includeTrace = true, hostPortLike = "") {
  const canonicalRoot = remoteCanonicalHostRoot(hostPortLike);
  if (!canonicalRoot) throw new Error("remote canonical host root missing");
  const base = `${canonicalRoot}/sessions/${String(slug || "").trim()}`;
  const preferred = includeTrace ? `${base}/high_fidelity.json` : `${base}/low_fidelity.json`;
  const fallback = includeTrace ? `${base}/low_fidelity.json` : `${base}/high_fidelity.json`;
  try {
    return await readRemoteJsonFile(preferred);
  } catch {
    return await readRemoteJsonFile(fallback);
  }
}

function remoteCanonicalSessionAssetPath(slug, fileName, hostPortLike = "") {
  const canonicalRoot = remoteCanonicalHostRoot(hostPortLike);
  if (!canonicalRoot) return "";
  return `${canonicalRoot}/sessions/${String(slug || "").trim()}/${String(fileName || "").trim()}`;
}

async function convertSvgToJpeg(svgPath, jpgPath) {
  if (fs.existsSync(SIPS_BIN)) {
    await runCommand(SIPS_BIN, ["-s", "format", "jpeg", svgPath, "--out", jpgPath]);
    if (!fs.existsSync(jpgPath)) throw new Error("jpeg conversion did not produce output");
    return;
  }
  if (fs.existsSync(MAGICK_BIN)) {
    await runCommand(MAGICK_BIN, [svgPath, "-background", "white", "-alpha", "remove", "-alpha", "off", jpgPath]);
    if (!fs.existsSync(jpgPath)) throw new Error("jpeg conversion did not produce output");
    return;
  }
  if (fs.existsSync(CONVERT_BIN)) {
    await runCommand(CONVERT_BIN, [svgPath, "-background", "white", "-alpha", "remove", "-alpha", "off", jpgPath]);
    if (!fs.existsSync(jpgPath)) throw new Error("jpeg conversion did not produce output");
    return;
  }
  throw new Error(`missing converter ${SIPS_BIN}`);
}

async function ensureSessionCardJpeg(reqUrl) {
  const slug = String(reqUrl.searchParams.get("slug") || "").trim();
  if (!slug) {
    return {
      statusCode: 400,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({ ok: false, error: "missing slug", worker: WORKER_LABEL }, null, 2), "utf8"),
      cacheStatus: "pass",
      upstreamUrl: "local:session_card_image",
    };
  }
  const refresh = shouldBypassCache(reqUrl);
  const paths = sessionCardImagePaths(slug);
  let cachedMeta = null;
  try {
    cachedMeta = fs.existsSync(paths.metaPath) ? JSON.parse(fs.readFileSync(paths.metaPath, "utf8")) : null;
  } catch {}
  const cachedAuditOk =
    Number(cachedMeta?.traceAudit?.version || 0) >= SESSION_CARD_TRACE_AUDIT_VERSION &&
    cachedMeta?.traceAudit?.hasVisibleTrace === true;
  if (!refresh && fs.existsSync(paths.jpgPath) && cachedAuditOk) {
    pushActivity(`USING CACHED SESSION CARD JPEG FOR SLUG ${slug.toUpperCase()}`, withSlugMeta({
      type: "session-card-cache-hit",
      auditVersion: cachedMeta?.traceAudit?.version || 0,
      jpegUrl: `${reqUrl.origin}/api/session-card-image?slug=${encodeURIComponent(slug)}&format=jpeg`,
      traceSvgUrl: `${reqUrl.origin}/api/session-card-image?slug=${encodeURIComponent(slug)}&format=svg`,
    }, slug));
    return {
      statusCode: 200,
      contentType: "image/jpeg",
      body: fs.readFileSync(paths.jpgPath),
      cacheStatus: "hit",
      upstreamUrl: "local:session_card_image_cache",
    };
  }
  fs.mkdirSync(paths.dir, { recursive: true });
  try {
    pushActivity(`CREATING SESSION CARD JPEG FOR SLUG ${slug.toUpperCase()}`, withSlugMeta({
      type: "session-card-jpeg-start",
      refresh: refresh ? "1" : "0",
    }, slug));
    const remoteJpegPath = remoteCanonicalSessionAssetPath(slug, "low_fidelity_trace.jpg", RUN_AUDIT_HOST_PORT);
    const remoteSvgPath = remoteCanonicalSessionAssetPath(slug, "low_fidelity_trace.svg", RUN_AUDIT_HOST_PORT);
    const remoteLowPath = remoteCanonicalSessionAssetPath(slug, "low_fidelity.json", RUN_AUDIT_HOST_PORT);
    let upstreamUrl = "";
    let svgContentType = "image/svg+xml";
    let selectedPayload = null;
    let selectedSession = null;
    let traceAudit = null;
    let renderSource = "";
    let canonicalAuditError = "";
    let usedCanonicalPayload = false;
    if (DUBLIN_CANONICAL_HOST_ROOT && SESSION_CARD_REMOTE_CANONICAL_READ_ENABLED) {
      try {
        const prepared = await prepareRenderableSessionCardArtifacts(slug);
        selectedPayload = prepared.payload;
        selectedSession = selectedPayload?.session && typeof selectedPayload.session === "object" ? selectedPayload.session : null;
        traceAudit = prepared.audit || sessionTraceAudit(selectedSession);
        renderSource = String(prepared.source || "canonical_session_payload");
        if (!selectedSession || !traceAudit?.hasVisibleTrace) {
          throw new Error(`trace audit rejected session-card payload for slug=${slug}`);
        }
        const svgText = buildSessionCardSvgFromSession(selectedSession, slug);
        if (!svgText || !svgText.includes("<polyline")) {
          throw new Error(`session-card svg render produced no visible trace for slug=${slug}`);
        }
        fs.writeFileSync(paths.svgPath, svgText, "utf8");
        upstreamUrl = renderSource;
        usedCanonicalPayload = true;
        if (prepared.rebuiltLowFromHigh && SESSION_CARD_REMOTE_CANONICAL_WRITE_ENABLED) {
          void (async () => {
            try {
              await writeRemoteTextFile(remoteLowPath, JSON.stringify(selectedPayload, null, 2) + "\n");
            } catch {}
          })();
        }
      } catch (error) {
        canonicalAuditError = String(error?.message || error || "");
      }
    }
    if (!usedCanonicalPayload) {
      if (DUBLIN_CANONICAL_HOST_ROOT) {
        try {
          const remoteSvgText = await readRemoteTextFile(remoteSvgPath);
          if (/<polyline\b/i.test(remoteSvgText)) {
            fs.writeFileSync(paths.svgPath, remoteSvgText, "utf8");
            svgContentType = "image/svg+xml";
            upstreamUrl = remoteSvgPath;
            renderSource = "remote_canonical_svg";
            traceAudit = traceAudit || {
              version: SESSION_CARD_TRACE_AUDIT_VERSION,
              pointCount: null,
              upVisibleCount: null,
              downVisibleCount: null,
              minRequiredPoints: SESSION_CARD_TRACE_AUDIT_MIN_POINTS,
              sessionStartMs: null,
              sessionEndMs: null,
              traceStartMs: null,
              traceEndMs: null,
              startGapMs: null,
              endGapMs: null,
              hasEnoughPoints: true,
              hasFullCoverage: true,
              hasVisibleTrace: true,
              source: "remote_canonical_svg_polyline_audit",
              canonicalAuditError: canonicalAuditError || null,
            };
            usedCanonicalPayload = true;
          }
        } catch {}
      }
    }
    if (!usedCanonicalPayload) {
      pushActivity(`REQUESTING SESSION CARD TRACE SVG FOR SLUG ${slug.toUpperCase()}`, withSlugMeta({
        type: "session-card-svg-fetch",
        canonicalAuditError: canonicalAuditError || "",
      }, slug));
      const upstreamReq = new URL("/api/session-card-image", "http://worker.local");
      upstreamReq.searchParams.set("slug", slug);
      upstreamReq.searchParams.set("format", "svg");
      if (refresh) upstreamReq.searchParams.set("refresh", "1");
      upstreamUrl = buildUpstreamUrl(upstreamReq).toString();
      const svgFetch = await fetchBinaryDirect(upstreamUrl);
      const svgBody = svgFetch.body;
      svgContentType = svgFetch.contentType;
      const svgText = svgBody.toString("utf8");
      if (!/<polyline\b/i.test(svgText)) {
        throw new Error(`upstream svg missing visible trace for slug=${slug}`);
      }
      fs.writeFileSync(paths.svgPath, svgBody);
      traceAudit = {
        version: SESSION_CARD_TRACE_AUDIT_VERSION,
        pointCount: null,
        upVisibleCount: null,
        downVisibleCount: null,
        minRequiredPoints: SESSION_CARD_TRACE_AUDIT_MIN_POINTS,
        sessionStartMs: null,
        sessionEndMs: null,
        traceStartMs: null,
        traceEndMs: null,
        startGapMs: null,
        endGapMs: null,
        hasEnoughPoints: true,
        hasFullCoverage: true,
        hasVisibleTrace: true,
        source: "upstream_svg_polyline_audit",
        canonicalAuditError: canonicalAuditError || null,
      };
      renderSource = "upstream_svg";
    }
    await convertSvgToJpeg(paths.svgPath, paths.jpgPath);
    pushActivity(`Session Card JPEG complete with traces: ${reqUrl.origin}/api/session-card-image?slug=${encodeURIComponent(slug)}&format=jpeg`, withSlugMeta({
      type: "session-card-jpeg-finished",
      jpgPath: paths.jpgPath,
      jpegUrl: `${reqUrl.origin}/api/session-card-image?slug=${encodeURIComponent(slug)}&format=jpeg`,
      traceSvgUrl: `${reqUrl.origin}/api/session-card-image?slug=${encodeURIComponent(slug)}&format=svg`,
    }, slug));
    if (DUBLIN_CANONICAL_HOST_ROOT && SESSION_CARD_REMOTE_CANONICAL_WRITE_ENABLED) {
      const svgTextForRemote = fs.readFileSync(paths.svgPath, "utf8");
      const jpgBodyForRemote = fs.readFileSync(paths.jpgPath);
      void (async () => {
        try {
          await writeRemoteTextFile(remoteSvgPath, svgTextForRemote);
        } catch {}
        try {
          await writeRemoteBinaryFile(remoteJpegPath, jpgBodyForRemote);
        } catch {}
      })();
    }
    fs.writeFileSync(paths.metaPath, JSON.stringify({
      slug,
      generatedAtMs: Date.now(),
      worker: WORKER_LABEL,
      upstreamUrl,
      svgContentType,
      renderSource,
      traceAudit: traceAudit || null,
    }, null, 2) + "\n");
    return {
      statusCode: 200,
      contentType: "image/jpeg",
      body: fs.readFileSync(paths.jpgPath),
      cacheStatus: "miss",
      upstreamUrl,
    };
  } catch (error) {
    pushActivity(`FAILED TO CREATE SESSION CARD JPEG FOR SLUG ${slug.toUpperCase()}`, withSlugMeta({
      type: "session-card-jpeg-error",
      error: String(error?.message || error),
    }, slug));
    return {
      statusCode: 503,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({
        ok: false,
        error: String(error?.message || error),
        worker: WORKER_LABEL,
        slug,
        upstreamUrl: "",
      }, null, 2), "utf8"),
      cacheStatus: "error",
      upstreamUrl: "",
    };
  }
}

async function ensureSessionCardImageResponse(reqUrl) {
  const slug = String(reqUrl.searchParams.get("slug") || "").trim();
  const format = String(reqUrl.searchParams.get("format") || "jpeg").trim().toLowerCase();
  const built = await ensureSessionCardJpeg(reqUrl);
  if (Number(built?.statusCode || 0) >= 400) return built;
  if (format !== "svg") return built;
  if (!slug) {
    return {
      statusCode: 400,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({ ok: false, error: "missing slug", worker: WORKER_LABEL }, null, 2), "utf8"),
      cacheStatus: "pass",
      upstreamUrl: "local:session_card_svg",
    };
  }
  const paths = sessionCardImagePaths(slug);
  if (!fs.existsSync(paths.svgPath)) {
    return {
      statusCode: 404,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({ ok: false, error: "session card svg missing", worker: WORKER_LABEL, slug }, null, 2), "utf8"),
      cacheStatus: "pass",
      upstreamUrl: "local:session_card_svg",
    };
  }
  return {
    statusCode: 200,
    contentType: "image/svg+xml",
    body: fs.readFileSync(paths.svgPath),
    cacheStatus: String(built?.cacheStatus || "hit"),
    upstreamUrl: String(built?.upstreamUrl || "local:session_card_svg"),
  };
}

async function ensureLatestSessionCardImageResponse(reqUrl) {
  const parts = String(reqUrl.pathname || "").split("/").filter(Boolean);
  const instanceId = (
    parts.length >= 5 &&
    String(parts[0] || "").toLowerCase() === "api" &&
    String(parts[1] || "").toLowerCase() === "v2" &&
    String(parts[2] || "").toLowerCase() === "bots" &&
    String(parts[4] || "").toLowerCase() === "latest-session-card-image"
  )
    ? decodeURIComponent(String(parts[3] || "").trim())
    : "";
  if (!instanceId) {
    return {
      statusCode: 400,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({ ok: false, error: "missing instanceId", worker: WORKER_LABEL }, null, 2), "utf8"),
      cacheStatus: "pass",
      upstreamUrl: "local:latest_session_card_image",
    };
  }
  const expectedSlug = String(reqUrl.searchParams.get("expectedSlug") || "").trim().toLowerCase();
  const marketPrefix = String(reqUrl.searchParams.get("marketPrefix") || "").trim().toLowerCase();
  const maxSessionsRaw = Number(reqUrl.searchParams.get("maxSessions") || 24);
  const maxSessions = Number.isFinite(maxSessionsRaw) ? Math.max(3, Math.min(200, Math.floor(maxSessionsRaw))) : 24;
  let selectedSession = null;
  let selectedSource = null;
  let upstreamUrl = "local:latest_session_card_image";
  try {
    const latestReq = new URL(
      `/api/v2/bots/${encodeURIComponent(instanceId)}/latest-session-card`,
      "http://worker.local"
    );
    if (expectedSlug) latestReq.searchParams.set("expectedSlug", expectedSlug);
    if (marketPrefix) latestReq.searchParams.set("marketPrefix", marketPrefix);
    latestReq.searchParams.set("maxSessions", String(Math.max(maxSessions, 6)));
    if (shouldBypassCache(reqUrl)) latestReq.searchParams.set("refresh", "1");
    const latest = await buildLatestSessionCardPayload(latestReq);
    upstreamUrl = String(latest?.payload?.upstreamUrl || upstreamUrl);
    selectedSession = latest?.payload?.selectedSession || null;
    selectedSource = String(latest?.payload?.selectedSource || "").trim() || null;
  } catch (error) {
    return {
      statusCode: 503,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({ ok: false, error: String(error?.message || error || "latest session continuity failed"), worker: WORKER_LABEL, instanceId }, null, 2), "utf8"),
      cacheStatus: "error",
      upstreamUrl,
    };
  }
  const slug = String(selectedSession?.slug || "").trim();
  if (!selectedSession || !slug) {
    return {
      statusCode: 404,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({ ok: false, error: "selected session missing", worker: WORKER_LABEL, instanceId }, null, 2), "utf8"),
      cacheStatus: "pass",
      upstreamUrl,
    };
  }
  const format = String(reqUrl.searchParams.get("format") || "jpeg").trim().toLowerCase();
  const refresh = shouldBypassCache(reqUrl);
  const paths = sessionCardImagePaths(slug);
  fs.mkdirSync(paths.dir, { recursive: true });
  if (!refresh && format !== "svg" && fs.existsSync(paths.jpgPath)) {
    return {
      statusCode: 200,
      contentType: "image/jpeg",
      body: fs.readFileSync(paths.jpgPath),
      cacheStatus: "hit",
      upstreamUrl: "local:latest_session_card_image_cache",
    };
  }
  const svgText = buildSessionCardSvgFromSession(selectedSession, slug);
  if (!svgText || !/<polyline\b/i.test(svgText)) {
    return {
      statusCode: 503,
      contentType: "application/json",
      body: Buffer.from(JSON.stringify({
        ok: false,
        error: "latest session card trace unavailable",
        worker: WORKER_LABEL,
        instanceId,
        slug,
      }, null, 2), "utf8"),
      cacheStatus: "error",
      upstreamUrl: "local:latest_session_card_image",
    };
  }
  fs.writeFileSync(paths.svgPath, svgText, "utf8");
  fs.writeFileSync(paths.metaPath, JSON.stringify({
    slug,
    instanceId,
    generatedAtMs: Date.now(),
    worker: WORKER_LABEL,
    source: "latest_session_card_worker",
    selectedSource: selectedSource || "latest_session_card_worker",
  }, null, 2) + "\n");
  if (format === "svg") {
    return {
      statusCode: 200,
      contentType: "image/svg+xml",
      body: Buffer.from(svgText, "utf8"),
      cacheStatus: refresh ? "miss" : "hit",
      upstreamUrl,
    };
  }
  await convertSvgToJpeg(paths.svgPath, paths.jpgPath);
  return {
    statusCode: 200,
    contentType: "image/jpeg",
    body: fs.readFileSync(paths.jpgPath),
    cacheStatus: refresh ? "miss" : "hit",
    upstreamUrl,
  };
}

async function gammaEventBySlug(slug) {
  const u = new URL(`${GAMMA_BASE}/events`);
  u.searchParams.set("slug", String(slug || "").trim());
  const data = await fetchJsonDirect(u.toString());
  if (Array.isArray(data)) return data[0] || null;
  if (data && typeof data === "object") {
    if (Array.isArray(data.data)) return data.data[0] || null;
    if (String(data.slug || "").trim() === String(slug || "").trim()) return data;
  }
  return null;
}

async function mapLimit(items, limit, worker) {
  const out = new Array(items.length);
  let idx = 0;
  async function one() {
    while (idx < items.length) {
      const mine = idx;
      idx += 1;
      out[mine] = await worker(items[mine], mine);
    }
  }
  const runners = Array.from({ length: Math.max(1, Math.min(limit, items.length || 1)) }, () => one());
  await Promise.all(runners);
  return out;
}

function sampleIndices(length, maxPoints) {
  const total = Math.max(0, Math.floor(Number(length) || 0));
  const limit = Math.max(2, Math.floor(Number(maxPoints) || 0));
  if (total <= limit) return Array.from({ length: total }, (_, i) => i);
  const idxs = [0];
  const interior = limit - 2;
  const span = total - 1;
  for (let i = 1; i <= interior; i++) {
    const idx = Math.max(1, Math.min(total - 2, Math.round((i * span) / (interior + 1))));
    if (idx > idxs[idxs.length - 1]) idxs.push(idx);
  }
  if (idxs[idxs.length - 1] !== total - 1) idxs.push(total - 1);
  return idxs;
}

function slimSessionTrace(traceLike, maxPoints = 360) {
  const trace = traceLike && typeof traceLike === "object" ? traceLike : null;
  if (!trace) return traceLike || null;
  const xs = Array.isArray(trace.xMs) ? trace.xMs : [];
  const up = Array.isArray(trace.up) ? trace.up : [];
  const down = Array.isArray(trace.down) ? trace.down : [];
  const n = Math.min(xs.length, up.length, down.length);
  if (n <= 0) return { ...trace, xMs: [], up: [], down: [] };
  const idxs = sampleIndices(n, maxPoints);
  return {
    ...trace,
    xMs: idxs.map((i) => xs[i]),
    up: idxs.map((i) => up[i]),
    down: idxs.map((i) => down[i]),
  };
}

function sessionTracePointCount(traceLike) {
  const trace = traceLike && typeof traceLike === "object" ? traceLike : null;
  if (!trace) return 0;
  const xs = Array.isArray(trace.xMs) ? trace.xMs : [];
  const up = Array.isArray(trace.up) ? trace.up : [];
  const down = Array.isArray(trace.down) ? trace.down : [];
  return Math.min(xs.length, up.length, down.length);
}

function normalizeTraceForSessionCardSvg(traceLike, maxPoints = 360) {
  const trace = slimSessionTrace(traceLike, maxPoints);
  const xsRaw = Array.isArray(trace?.xMs) ? trace.xMs : [];
  const upRaw = Array.isArray(trace?.up) ? trace.up : [];
  const downRaw = Array.isArray(trace?.down) ? trace.down : [];
  const rows = [];
  for (let i = 0; i < xsRaw.length; i += 1) {
    const x = Number(xsRaw[i]);
    const up = upRaw[i] == null ? null : Number(upRaw[i]);
    const down = downRaw[i] == null ? null : Number(downRaw[i]);
    if (!Number.isFinite(x)) continue;
    rows.push({
      x,
      up: Number.isFinite(up) ? Math.max(0, Math.min(1, up)) : null,
      down: Number.isFinite(down) ? Math.max(0, Math.min(1, down)) : null,
    });
  }
  rows.sort((a, b) => Number(a.x) - Number(b.x));
  const dedup = [];
  for (const row of rows) {
    const prev = dedup.length ? dedup[dedup.length - 1] : null;
    if (prev && Number(prev.x) === Number(row.x)) {
      dedup[dedup.length - 1] = row;
      continue;
    }
    dedup.push(row);
  }
  const xMs = [];
  const up = [];
  const down = [];
  for (const row of dedup) {
    xMs.push(Number(row.x));
    up.push(row.up);
    down.push(row.down);
  }
  return { xMs, up, down };
}

function countVisibleTraceSeriesPoints(valuesLike) {
  const values = Array.isArray(valuesLike) ? valuesLike : [];
  let count = 0;
  for (const value of values) {
    const num = Number(value);
    if (Number.isFinite(num) && num >= 0 && num <= 1) count += 1;
  }
  return count;
}

function sessionTraceAudit(sessionLike) {
  const session = sessionLike && typeof sessionLike === "object" ? sessionLike : null;
  const normalized = normalizeTraceForSessionCardSvg(session?.trace || null, 360);
  const upVisibleCount = countVisibleTraceSeriesPoints(normalized.up);
  const downVisibleCount = countVisibleTraceSeriesPoints(normalized.down);
  const pointCount = normalized.xMs.length;
  const traceStartMs = pointCount > 0 ? Number(normalized.xMs[0]) : NaN;
  const traceEndMs = pointCount > 0 ? Number(normalized.xMs[pointCount - 1]) : NaN;
  const sessionStartMs = Number.isFinite(Number(session?.startMs)) ? Number(session.startMs) : NaN;
  const sessionEndMs = Number.isFinite(Number(session?.endMs))
    ? Number(session.endMs)
    : (Number.isFinite(sessionStartMs) ? (sessionStartMs + 300000) : NaN);
  const startGapMs = (Number.isFinite(traceStartMs) && Number.isFinite(sessionStartMs))
    ? Math.max(0, traceStartMs - sessionStartMs)
    : NaN;
  const endGapMs = (Number.isFinite(traceEndMs) && Number.isFinite(sessionEndMs))
    ? Math.max(0, sessionEndMs - traceEndMs)
    : NaN;
  const minSeriesVisibleCount = Math.max(Number(upVisibleCount || 0), Number(downVisibleCount || 0));
  const hasEnoughPoints = pointCount >= SESSION_CARD_TRACE_AUDIT_MIN_POINTS && minSeriesVisibleCount >= SESSION_CARD_TRACE_AUDIT_MIN_POINTS;
  const hasFullCoverage =
    Number.isFinite(startGapMs) &&
    Number.isFinite(endGapMs) &&
    startGapMs <= SESSION_CARD_TRACE_AUDIT_MAX_EDGE_GAP_MS &&
    endGapMs <= SESSION_CARD_TRACE_AUDIT_MAX_EDGE_GAP_MS;
  const hasVisibleTrace = hasEnoughPoints && hasFullCoverage;
  return {
    version: SESSION_CARD_TRACE_AUDIT_VERSION,
    pointCount,
    upVisibleCount,
    downVisibleCount,
    minRequiredPoints: SESSION_CARD_TRACE_AUDIT_MIN_POINTS,
    sessionStartMs: Number.isFinite(sessionStartMs) ? sessionStartMs : null,
    sessionEndMs: Number.isFinite(sessionEndMs) ? sessionEndMs : null,
    traceStartMs: Number.isFinite(traceStartMs) ? traceStartMs : null,
    traceEndMs: Number.isFinite(traceEndMs) ? traceEndMs : null,
    startGapMs: Number.isFinite(startGapMs) ? startGapMs : null,
    endGapMs: Number.isFinite(endGapMs) ? endGapMs : null,
    hasEnoughPoints,
    hasFullCoverage,
    hasVisibleTrace,
  };
}

function buildLowFidelityPayloadFromHigh(highPayloadLike) {
  const highPayload = highPayloadLike && typeof highPayloadLike === "object" ? highPayloadLike : null;
  const highSession = highPayload?.session && typeof highPayload.session === "object" ? highPayload.session : null;
  if (!highPayload || !highSession) return null;
  const lowSession = {
    ...highSession,
    trace: cloneJsonLike(highSession.trace || null),
    traceSource: String(highSession?.traceSource || "canonical_session_high_fidelity_replacement_low"),
  };
  return {
    ...highPayload,
    schemaVersion: "session-low-fidelity.v1",
    generatedAtMs: Date.now(),
    session: lowSession,
  };
}

function escapeHtmlLite(textLike) {
  return String(textLike ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;");
}

function sessionArtifactSlugBase(slugLike) {
  const slug = String(slugLike || "").trim().toLowerCase();
  const match = slug.match(/^(.*)-(\d{10})$/);
  return String(match?.[1] || slug).trim();
}

function buildSessionCardSvgFromSession(sessionLike, slugLike = "") {
  const session = sessionLike && typeof sessionLike === "object" ? sessionLike : null;
  if (!session) return null;
  const audit = sessionTraceAudit(session);
  if (!audit.hasVisibleTrace) return null;
  const normalized = normalizeTraceForSessionCardSvg(session.trace || null, 360);
  const W = 1680;
  const H = 780;
  const PAD_X = 66;
  const PAD_Y = 84;
  const startMs =
    Number.isFinite(Number(session?.startMs)) ? Number(session.startMs)
    : (normalized.xMs.length ? Number(normalized.xMs[0]) : Date.now());
  const sessionEndMs =
    Number.isFinite(Number(session?.endMs)) ? Number(session.endMs)
    : (startMs + 300000);
  const minMs = startMs;
  const maxMs = Math.max(startMs + 300000, sessionEndMs);
  const spanMs = Math.max(1, maxMs - minMs);
  const sx = (x) => PAD_X + (((Number(x) - minMs) / spanMs) * (W - PAD_X * 2));
  const sy = (y) => (H - PAD_Y) - (Math.max(0, Math.min(1, Number(y))) * (H - PAD_Y * 2));
  const poly = (points) => points.map((pt) => `${pt.x.toFixed(1)},${pt.y.toFixed(1)}`).join(" ");
  const fmtSessionPacific = (msLike) => {
    const ts = Number(msLike);
    if (!(Number.isFinite(ts) && ts > 0)) return "";
    try {
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Los_Angeles",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }).format(new Date(ts));
    } catch {
      return "";
    }
  };
  const upPts = [];
  const downPts = [];
  for (let i = 0; i < normalized.xMs.length; i += 1) {
    const x = normalized.xMs[i];
    if (Number.isFinite(Number(normalized.up[i]))) upPts.push({ x: sx(x), y: sy(normalized.up[i]) });
    if (Number.isFinite(Number(normalized.down[i]))) downPts.push({ x: sx(x), y: sy(normalized.down[i]) });
  }
  if (upPts.length < 2 && downPts.length < 2) return null;
  const fmtSec = (msLike) => `${Math.round(Math.max(0, Number(msLike) - minMs) / 1000)}s`;
  const gridY = [0, 0.25, 0.5, 0.75, 1]
    .map((v) => {
      const y = sy(v);
      return `<line x1="${PAD_X}" y1="${y.toFixed(1)}" x2="${(W - PAD_X).toFixed(1)}" y2="${y.toFixed(1)}" stroke="rgba(255,255,255,.08)" stroke-width="1"/>`;
    })
    .join("");
  const gridX = [0, 60, 120, 180, 240, 300]
    .map((sec) => {
      const x = sx(minMs + (sec * 1000));
      return `<line x1="${x.toFixed(1)}" y1="${PAD_Y}" x2="${x.toFixed(1)}" y2="${(H - PAD_Y).toFixed(1)}" stroke="rgba(255,255,255,.05)" stroke-width="1"/>`;
    })
    .join("");
  const xLabels = [0, 60, 120, 180, 240, 300]
    .map((sec) => {
      const x = sx(minMs + (sec * 1000));
      return `<text x="${x.toFixed(1)}" y="${(H - 18).toFixed(1)}" fill="#98a3b7" font-size="28" font-weight="800" text-anchor="${sec === 0 ? "start" : (sec === 300 ? "end" : "middle")}">${sec}s</text>`;
    })
    .join("");
  const yLabels = [0, 0.5, 1]
    .map((v) => {
      const y = sy(v);
      return `<text x="${(PAD_X - 12).toFixed(1)}" y="${(y + 8).toFixed(1)}" fill="#8f98a8" font-size="22" font-weight="700" text-anchor="end">$${v.toFixed(2)}</text>`;
    })
    .join("");
  const sidePaths = Array.isArray(session?.sidePaths) ? session.sidePaths : [];
  const tradePaths = sidePaths
    .map((sp) => {
      const side = String(sp?.side || "").trim().toUpperCase();
      if (side !== "UP" && side !== "DOWN") return null;
      const pts = (Array.isArray(sp?.points) ? sp.points : [])
        .map((pt) => {
          const tsMs = Number(pt?.tsMs);
          const px = Number(pt?.px);
          if (!(Number.isFinite(tsMs) && Number.isFinite(px))) return null;
          const kindRaw = String(pt?.kind || pt?.eventType || pt?.exitType || "").trim().toLowerCase();
          return {
            tsMs,
            px,
            kind: kindRaw || "event",
            eventType: String(pt?.eventType || pt?.exitType || "").trim().toLowerCase(),
            partial: !!pt?.partial,
            pnlUsd: Number.isFinite(Number(pt?.pnlUsd ?? pt?.legPnlUsd ?? pt?.realizedPnlUsd ?? pt?.lockedPnlUsd))
              ? Number(pt?.pnlUsd ?? pt?.legPnlUsd ?? pt?.realizedPnlUsd ?? pt?.lockedPnlUsd)
              : null,
          };
        })
        .filter(Boolean)
        .sort((a, b) => Number(a.tsMs) - Number(b.tsMs));
      if (pts.length < 2) return null;
      const entry = pts.find((pt) => String(pt?.kind || "").toLowerCase() === "entry") || pts[0];
      const exit = [...pts].reverse().find((pt) => {
        const kind = String(pt?.kind || "").toLowerCase();
        return kind === "exit" || kind === "tp" || kind === "sl" || kind === "stop";
      }) || pts[pts.length - 1];
      if (!entry || !exit) return null;
      return { side, points: pts, entry, exit };
    })
    .filter(Boolean);
  const lanes = Array.isArray(session?.lanes) ? session.lanes : [];
  const laneGuides = [];
  const laneGlyphs = [];
  if (tradePaths.length) {
    for (const trade of tradePaths) {
      const side = String(trade?.side || "").trim().toUpperCase();
      const points = Array.isArray(trade?.points) ? trade.points : [];
      if (points.length < 2) continue;
      const lineColor = side === "DOWN" ? "#7fd3ff" : "#36a2ff";
      const pathPolyline = points
        .map((pt) => `${sx(pt.tsMs).toFixed(1)},${sy(pt.px).toFixed(1)}`)
        .join(" ");
      laneGuides.push(`<polyline fill="none" stroke="${lineColor}" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round" opacity="0.95" points="${pathPolyline}"/>`);
      const entryX = sx(trade.entry.tsMs);
      const entryY = sy(trade.entry.px);
      const exitX = sx(trade.exit.tsMs);
      const exitY = sy(trade.exit.px);
      const isStop = /(^|_)(sl|stop)(_|$)/i.test(String(trade.exit?.eventType || trade.exit?.kind || ""));
      const exitColor = isStop ? "#f05a74" : "#34d17b";
      laneGuides.push(`<line x1="${entryX.toFixed(1)}" y1="${PAD_Y}" x2="${entryX.toFixed(1)}" y2="${(H - PAD_Y).toFixed(1)}" stroke="${lineColor}" stroke-width="2" stroke-dasharray="8 8" opacity="0.35"/>`);
      laneGuides.push(`<line x1="${exitX.toFixed(1)}" y1="${PAD_Y}" x2="${exitX.toFixed(1)}" y2="${(H - PAD_Y).toFixed(1)}" stroke="${exitColor}" stroke-width="2" stroke-dasharray="8 8" opacity="0.35"/>`);
      laneGlyphs.push(`<circle cx="${entryX.toFixed(1)}" cy="${entryY.toFixed(1)}" r="10" fill="${lineColor}" stroke="#ffffff" stroke-width="2.5"><title>${escapeHtmlLite(`${side || "TRADE"} entry ${Number(trade.entry.px).toFixed(3)} @ ${fmtSec(trade.entry.tsMs)}`)}</title></circle>`);
      const partialPoints = points.filter((pt) => {
        if (!pt || pt === trade.entry || pt === trade.exit) return false;
        const kind = String(pt?.kind || "").toLowerCase();
        const eventType = String(pt?.eventType || "").toLowerCase();
        return pt.partial === true || kind === "derisk" || eventType === "derisk" || eventType === "tp";
      });
      for (const pt of partialPoints) {
        const x = sx(pt.tsMs);
        const y = sy(pt.px);
        laneGlyphs.push(`<path d="M ${x.toFixed(1)} ${(y - 11).toFixed(1)} L ${(x + 11).toFixed(1)} ${y.toFixed(1)} L ${x.toFixed(1)} ${(y + 11).toFixed(1)} L ${(x - 11).toFixed(1)} ${y.toFixed(1)} Z" fill="#34d17b" stroke="#ffffff" stroke-width="2.5"><title>${escapeHtmlLite(`${side || "TRADE"} partial ${Number(pt.px).toFixed(3)} @ ${fmtSec(pt.tsMs)}`)}</title></path>`);
      }
      laneGlyphs.push(`<rect x="${(exitX - 10).toFixed(1)}" y="${(exitY - 10).toFixed(1)}" width="20" height="20" rx="5" fill="${exitColor}" stroke="#ffffff" stroke-width="2.5"><title>${escapeHtmlLite(`${side || "TRADE"} ${isStop ? "stop" : "exit"} ${Number(trade.exit.px).toFixed(3)} @ ${fmtSec(trade.exit.tsMs)}`)}</title></rect>`);
    }
  } else {
    const mergedLanes = [];
    for (const lane of lanes) {
      const prev = mergedLanes.length ? mergedLanes[mergedLanes.length - 1] : null;
      const sameEntryWindow =
        prev &&
        String(prev?.side || "").toUpperCase() === String(lane?.side || "").toUpperCase() &&
        Number.isFinite(Number(prev?.entryTsMs)) &&
        Number.isFinite(Number(lane?.entryTsMs)) &&
        Math.abs(Number(prev.entryTsMs) - Number(lane.entryTsMs)) <= 1000 &&
        Number.isFinite(Number(prev?.entryPx)) &&
        Number.isFinite(Number(lane?.entryPx)) &&
        Math.abs(Number(prev.entryPx) - Number(lane.entryPx)) <= 0.000001;
      if (sameEntryWindow) {
        const prevExitTs = Number(prev?.exitTsMs);
        const laneExitTs = Number(lane?.exitTsMs);
        const preferLaneExit = Number.isFinite(laneExitTs) && (!Number.isFinite(prevExitTs) || laneExitTs >= prevExitTs);
        mergedLanes[mergedLanes.length - 1] = {
          ...prev,
          exitTsMs: preferLaneExit ? lane?.exitTsMs : prev?.exitTsMs,
          exitPx: preferLaneExit ? lane?.exitPx : prev?.exitPx,
          exitType: preferLaneExit ? lane?.exitType : prev?.exitType,
          exitReasonRaw: preferLaneExit ? lane?.exitReasonRaw : prev?.exitReasonRaw,
          partial: !!prev?.partial || !!lane?.partial || /partial/i.test(String(prev?.event || "")) || /partial/i.test(String(lane?.event || "")),
          deriskTsMs: Number.isFinite(prevExitTs) ? prevExitTs : laneExitTs,
          deriskPx: Number.isFinite(Number(prev?.exitPx)) ? Number(prev.exitPx) : Number(lane?.exitPx),
        };
        continue;
      }
      mergedLanes.push({ ...lane });
    }
    const groupedLanes = [];
    for (const lane of mergedLanes) {
      const prev = groupedLanes.length ? groupedLanes[groupedLanes.length - 1] : null;
      const sameSideChain =
        prev &&
        String(prev?.side || "").toUpperCase() === String(lane?.side || "").toUpperCase() &&
        Number.isFinite(Number(prev?.exitTsMs)) &&
        Number.isFinite(Number(lane?.entryTsMs)) &&
        Number(lane.entryTsMs) >= Number(prev.exitTsMs) - 1000 &&
        Number(lane.entryTsMs) <= Number(prev.exitTsMs) + 120000;
      if (sameSideChain) {
        const prevExitTs = Number(prev?.exitTsMs);
        const prevExitPx = Number(prev?.exitPx);
        groupedLanes[groupedLanes.length - 1] = {
          ...prev,
          partial: true,
          deriskTsMs: Number.isFinite(prevExitTs) ? prevExitTs : Number(prev?.deriskTsMs),
          deriskPx: Number.isFinite(prevExitPx) ? prevExitPx : Number(prev?.deriskPx),
          exitTsMs: lane?.exitTsMs,
          exitPx: lane?.exitPx,
          exitType: lane?.exitType,
          exitReasonRaw: lane?.exitReasonRaw,
        };
        continue;
      }
      groupedLanes.push(lane);
    }
    for (const lane of groupedLanes) {
      const entryTs = Number(lane?.entryTsMs);
      const exitTs = Number(lane?.exitTsMs);
      const entryPx = Number(lane?.entryPx);
      const exitPx = Number(lane?.exitPx);
      if (!(Number.isFinite(entryTs) && Number.isFinite(exitTs) && Number.isFinite(entryPx) && Number.isFinite(exitPx))) continue;
      const side = String(lane?.side || "").trim().toUpperCase();
      const isStop = /stop/i.test(String(lane?.exitType || "")) || /STOP/i.test(String(lane?.exitReasonRaw || ""));
      const isPartial = lane?.partial === true || /partial/i.test(String(lane?.event || ""));
      const lineColor = side === "DOWN" ? "#7fd3ff" : "#36a2ff";
      const exitColor = isStop ? "#f05a74" : "#34d17b";
      const entryX = sx(entryTs);
      const exitX = sx(exitTs);
      const entryY = sy(entryPx);
      const exitY = sy(exitPx);
      const deriskTs = Number(lane?.deriskTsMs);
      const deriskPx = Number(lane?.deriskPx);
      const lanePoints = [
        `${entryX.toFixed(1)},${entryY.toFixed(1)}`,
        (Number.isFinite(deriskTs) && Number.isFinite(deriskPx) && deriskTs > entryTs && deriskTs < exitTs)
          ? `${sx(deriskTs).toFixed(1)},${sy(deriskPx).toFixed(1)}`
          : null,
        `${exitX.toFixed(1)},${exitY.toFixed(1)}`,
      ].filter(Boolean).join(" ");
      laneGuides.push(`<polyline fill="none" stroke="${lineColor}" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round" opacity="0.95" points="${lanePoints}"/>`);
      laneGuides.push(`<line x1="${entryX.toFixed(1)}" y1="${PAD_Y}" x2="${entryX.toFixed(1)}" y2="${(H - PAD_Y).toFixed(1)}" stroke="${lineColor}" stroke-width="2" stroke-dasharray="8 8" opacity="0.35"/>`);
      laneGuides.push(`<line x1="${exitX.toFixed(1)}" y1="${PAD_Y}" x2="${exitX.toFixed(1)}" y2="${(H - PAD_Y).toFixed(1)}" stroke="${exitColor}" stroke-width="2" stroke-dasharray="8 8" opacity="0.35"/>`);
      laneGlyphs.push(`<circle cx="${entryX.toFixed(1)}" cy="${entryY.toFixed(1)}" r="10" fill="${lineColor}" stroke="#ffffff" stroke-width="2.5"><title>${escapeHtmlLite(`${side || "TRADE"} entry ${entryPx.toFixed(3)} @ ${fmtSec(entryTs)}`)}</title></circle>`);
      if (Number.isFinite(deriskTs) && Number.isFinite(deriskPx) && deriskTs > entryTs && deriskTs < exitTs) {
        const dx = sx(deriskTs);
        const dy = sy(deriskPx);
        laneGlyphs.push(`<path d="M ${dx.toFixed(1)} ${(dy - 11).toFixed(1)} L ${(dx + 11).toFixed(1)} ${dy.toFixed(1)} L ${dx.toFixed(1)} ${(dy + 11).toFixed(1)} L ${(dx - 11).toFixed(1)} ${dy.toFixed(1)} Z" fill="#34d17b" stroke="#ffffff" stroke-width="2.5"><title>${escapeHtmlLite(`${side || "TRADE"} partial ${deriskPx.toFixed(3)} @ ${fmtSec(deriskTs)}`)}</title></path>`);
      }
      laneGlyphs.push(`<rect x="${(exitX - 10).toFixed(1)}" y="${(exitY - 10).toFixed(1)}" width="20" height="20" rx="5" fill="${exitColor}" stroke="#ffffff" stroke-width="2.5"><title>${escapeHtmlLite(`${side || "TRADE"} ${isStop ? "stop" : "exit"} ${exitPx.toFixed(3)} @ ${fmtSec(exitTs)}`)}</title></rect>`);
    }
  }
  const slug = String(slugLike || session?.slug || "").trim();
  const title = `${fmtSessionPacific(startMs)} PST`.trim();
  const pnlUsd = compactSessionDisplayPnlUsd(session);
  const pnlText = Number.isFinite(pnlUsd) ? `${pnlUsd >= 0 ? "+" : "-"}$${Math.abs(pnlUsd).toFixed(2)}` : "—";
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" role="img" aria-label="${escapeHtmlLite(title)}">
  <rect x="0" y="0" width="${W}" height="${H}" fill="#111111"/>
  <rect x="${PAD_X}" y="${PAD_Y}" width="${W - PAD_X * 2}" height="${H - PAD_Y * 2}" fill="none" stroke="#2a2a2a" stroke-width="1"/>
  ${gridX}
  ${gridY}
  ${xLabels}
  ${yLabels}
  <text x="${(W / 2).toFixed(1)}" y="40" fill="#f3f4f6" font-size="40" font-weight="900" text-anchor="middle">${escapeHtmlLite(title)}</text>
  <text x="${(W / 2).toFixed(1)}" y="82" fill="${Number.isFinite(pnlUsd) ? (pnlUsd >= 0 ? "#86efac" : "#fda4af") : "#d1d5db"}" font-size="38" font-weight="900" text-anchor="middle">${escapeHtmlLite(pnlText)}</text>
  ${upPts.length >= 2 ? `<polyline fill="none" stroke="#e8b931" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round" points="${poly(upPts)}"/>` : ""}
  ${downPts.length >= 2 ? `<polyline fill="none" stroke="#c7c7c7" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round" points="${poly(downPts)}"/>` : ""}
  <g class="tradeLaneGuides">${laneGuides.join("")}</g>
  <g class="tradeLaneGlyphs">${laneGlyphs.join("")}</g>
</svg>`;
}

async function prepareRenderableSessionCardArtifacts(slug) {
  let highPayload = null;
  let lowPayload = null;
  try {
    highPayload = await readRemoteCanonicalSessionJson(slug, true, RUN_AUDIT_HOST_PORT);
  } catch {}
  const highSession = highPayload?.session && typeof highPayload.session === "object" ? highPayload.session : null;
  const highAudit = sessionTraceAudit(highSession);
  if (highPayload && highAudit.hasVisibleTrace) {
    const rebuiltFromHigh = buildLowFidelityPayloadFromHigh(highPayload);
    return {
      payload: rebuiltFromHigh || highPayload,
      source: rebuiltFromHigh ? "rebuilt_low_fidelity_from_high" : "canonical_high_fidelity",
      audit: highAudit,
      rebuiltLowFromHigh: !!rebuiltFromHigh,
    };
  }
  try {
    lowPayload = await readRemoteCanonicalSessionJson(slug, false, RUN_AUDIT_HOST_PORT);
  } catch {}
  const lowSession = lowPayload?.session && typeof lowPayload.session === "object" ? lowPayload.session : null;
  const lowAudit = sessionTraceAudit(lowSession);
  if (lowPayload && lowAudit.hasVisibleTrace) {
    return {
      payload: lowPayload,
      source: "canonical_low_fidelity",
      audit: lowAudit,
      rebuiltLowFromHigh: false,
    };
  }
  if (!highPayload || !highAudit.hasVisibleTrace) {
    const auditSummary = {
      low: lowAudit,
      high: highAudit,
    };
    throw new Error(`trace audit failed for session-card image: ${JSON.stringify(auditSummary)}`);
  }
  const rebuiltLowPayload = buildLowFidelityPayloadFromHigh(highPayload);
  const rebuiltSession = rebuiltLowPayload?.session && typeof rebuiltLowPayload.session === "object" ? rebuiltLowPayload.session : null;
  const rebuiltAudit = sessionTraceAudit(rebuiltSession);
  if (!rebuiltLowPayload || !rebuiltAudit.hasVisibleTrace) {
    throw new Error(`rebuilt low-fidelity trace still not visible for slug=${slug}`);
  }
  return {
    payload: rebuiltLowPayload,
    source: "rebuilt_low_fidelity_from_high",
    audit: rebuiltAudit,
    rebuiltLowFromHigh: true,
  };
}

function slimSelectedSession(sessionLike) {
  const session = sessionLike && typeof sessionLike === "object" ? sessionLike : null;
  if (!session) return sessionLike || null;
  return {
    ...session,
    trace: slimSessionTrace(session.trace, 360),
  };
}

async function buildVolumeSeriesPayload(reqUrl) {
  const anchorSlug = String(reqUrl.searchParams.get("anchorSlug") || "").trim().toLowerCase();
  const match = anchorSlug.match(/^(.*)-(\d{10})$/);
  if (!match) {
    return { statusCode: 400, payload: { ok: false, error: "anchorSlug must match *-<10digit_epoch_sec>" } };
  }
  const base = String(match[1] || "").trim().toLowerCase();
  const anchorStartMs = Number(match[2]) * 1000;
  if (!base || !Number.isFinite(anchorStartMs) || anchorStartMs <= 0) {
    return { statusCode: 400, payload: { ok: false, error: "invalid anchorSlug" } };
  }
  const sessionsRaw = Number(reqUrl.searchParams.get("sessions") || 100);
  const hoursRaw = Number(reqUrl.searchParams.get("hours") || 24);
  const barsCount = Number.isFinite(sessionsRaw) && sessionsRaw > 0
    ? Math.max(1, Math.min(300, Math.floor(sessionsRaw)))
    : Math.max(1, Math.min(300, Math.floor((Number.isFinite(hoursRaw) ? hoursRaw : 24) * 12)));
  const barMs = 5 * 60 * 1000;
  const firstStartMs = anchorStartMs - ((barsCount - 1) * barMs);
  const starts = Array.from({ length: barsCount }, (_, i) => firstStartMs + (i * barMs));
  const cached = readRollingVolumeCache(base);
  const cachedBars = Array.isArray(cached?.bars) ? cached.bars : [];
  const cachedBySlug = new Map(cachedBars.map((row) => [String(row?.slug || "").trim().toLowerCase(), row]));
  const wanted = starts.map((startMs) => {
    const slug = `${base}-${Math.floor(startMs / 1000)}`;
    return { slug, startMs };
  });
  const missing = wanted.filter(({ slug }) => !cachedBySlug.has(slug));
  const fetched = await mapLimit(missing, 6, async ({ slug, startMs }) => {
    try {
      const evt = await gammaEventBySlug(slug);
      const mkt = Array.isArray(evt?.markets) && evt.markets.length ? evt.markets[0] : null;
      const volumeUsd = parseMarketVolumeUsd(mkt);
      return {
        slug,
        startMs,
        volumeUsd: Number.isFinite(Number(volumeUsd)) ? Number(volumeUsd) : 0,
        missing: !Number.isFinite(Number(volumeUsd)),
      };
    } catch {
      return { slug, startMs, volumeUsd: 0, missing: true };
    }
  });
  pushActivity(`UPDATING 24H VOLUME SERIES FOR ${anchorSlug.toUpperCase()}`, {
    type: "volume-series-build",
    anchorSlug,
    anchorSlugTime: slugTimeLabel(anchorSlug),
    fetchedCount: fetched.length,
    sessions: barsCount,
  });
  for (const row of fetched) cachedBySlug.set(String(row.slug || "").trim().toLowerCase(), row);
  const bars = wanted.map(({ slug, startMs }) => {
    const row = cachedBySlug.get(slug) || null;
    return row ? {
      slug,
      startMs: Number.isFinite(Number(row?.startMs)) ? Number(row.startMs) : startMs,
      volumeUsd: Number.isFinite(Number(row?.volumeUsd)) ? Number(row.volumeUsd) : 0,
      missing: row?.missing === true,
    } : {
      slug,
      startMs,
      volumeUsd: 0,
      missing: true,
    };
  });
  writeRollingVolumeCache(base, {
    schemaVersion: "rolling-volume.v1",
    marketPrefix: base,
    updatedAtMs: Date.now(),
    anchorSlug,
    sessions: barsCount,
    barMs,
    bars,
    source: "polymarket_gamma_worker",
  });
  return {
    statusCode: 200,
    payload: {
      ok: true,
      asOfMs: Date.now(),
      anchorSlug,
      sessions: barsCount,
      barMs,
      count: bars.length,
      bars,
      source: "polymarket_gamma_worker",
    },
  };
}

function inferSlugBase(slugLike) {
  const slug = String(slugLike || "").trim().toLowerCase();
  const m = slug.match(/^(.*)-\d{10}$/);
  return m && m[1] ? String(m[1]).trim().toLowerCase() : slug;
}

function inferBarMsForSlug(slugLike) {
  const slug = String(slugLike || "").trim().toLowerCase();
  if (slug.includes("-5m-")) return 5 * 60 * 1000;
  if (slug.includes("-15m-")) return 15 * 60 * 1000;
  if (slug.includes("-1h-")) return 60 * 60 * 1000;
  return 5 * 60 * 1000;
}

function sessionFinalizedAtMs(sessionLike) {
  const row = sessionLike || null;
  const exitTsMs = Number(row?.exitTsMs);
  if (Number.isFinite(exitTsMs) && exitTsMs > 0) return exitTsMs;
  const endMs = Number(row?.endMs);
  if (Number.isFinite(endMs) && endMs > 0) return endMs;
  const slug = String(row?.slug || "").trim();
  const slugMatch = slug.match(/-(\d{10})$/);
  if (slugMatch) {
    const startMs = Number(slugMatch[1]) * 1000;
    const durMs = inferBarMsForSlug(slug);
    if (Number.isFinite(startMs) && Number.isFinite(durMs) && durMs > 0) return startMs + durMs - 1000;
  }
  return NaN;
}

function isFinalizedSession(row, nowMs = Date.now()) {
  if (row?.closed === true) return true;
  const finMs = sessionFinalizedAtMs(row);
  return Number.isFinite(finMs) && finMs <= Number(nowMs);
}

async function buildLatestSessionCardPayload(reqUrl) {
  const parts = String(reqUrl.pathname || "").split("/").filter(Boolean);
  const instanceId = (
    parts.length >= 5 &&
    String(parts[0] || "").toLowerCase() === "api" &&
    String(parts[1] || "").toLowerCase() === "v2" &&
    String(parts[2] || "").toLowerCase() === "bots" &&
    String(parts[4] || "").toLowerCase() === "latest-session-card"
  )
    ? decodeURIComponent(String(parts[3] || "").trim())
    : "";
  if (!instanceId) {
    return { statusCode: 400, payload: { ok: false, error: "missing instanceId" } };
  }
  const nowMs = Date.now();
  const maxSessionsRaw = Number(reqUrl.searchParams.get("maxSessions") || 12);
  const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0
    ? Math.max(1, Math.min(50, Math.floor(maxSessionsRaw)))
    : 12;
  const expectedSlugParam = String(reqUrl.searchParams.get("expectedSlug") || "").trim().toLowerCase();
  let marketPrefix = String(reqUrl.searchParams.get("marketPrefix") || "").trim().toLowerCase();
  if (!marketPrefix && /-\d{10}$/.test(expectedSlugParam)) {
    marketPrefix = String(expectedSlugParam.replace(/-\d{10}$/, "")).trim().toLowerCase();
  }
  let runIndex = null;
  let runIndexRows = [];
  try {
    runIndex = await fetchRunIndexPayloadForInstance(instanceId);
    runIndexRows = compactRowsFromRunIndexPayload(runIndex.payload, {
      instanceId,
      marketPrefix,
      maxSessions: Math.max(200, maxSessions * 8),
      offset: 0,
    });
    if (!marketPrefix) {
      const inferredRunIndexSlug = String(runIndexRows[0]?.slug || "").trim().toLowerCase();
      if (/-\d{10}$/.test(inferredRunIndexSlug)) {
        marketPrefix = String(inferredRunIndexSlug.replace(/-\d{10}$/, "")).trim().toLowerCase();
      }
    }
  } catch {}
  if (!marketPrefix) {
    try {
      const remoteSummary = await readRemoteLatestRunSummaryJson();
      const inferredSummarySlug = String(remoteSummary?.payload?.marketSlug || "").trim().toLowerCase();
      const inferredLatestSessionSlug = String(remoteSummary?.payload?.latestSession?.slug || "").trim().toLowerCase();
      if (/-\d{10}$/.test(inferredSummarySlug)) {
        marketPrefix = String(inferredSummarySlug.replace(/-\d{10}$/, "")).trim().toLowerCase();
      } else if (inferredSummarySlug) {
        marketPrefix = inferredSummarySlug;
      } else if (/-\d{10}$/.test(inferredLatestSessionSlug)) {
        marketPrefix = String(inferredLatestSessionSlug.replace(/-\d{10}$/, "")).trim().toLowerCase();
      }
    } catch {}
  }
  const barMs = inferBarMsForSlug(marketPrefix ? `${marketPrefix}-0` : "");
  const expectedClosedStartMs = (Math.floor(nowMs / barMs) * barMs) - barMs;
  const seededExpectedSlug = String(runIndexRows[0]?.slug || "").trim().toLowerCase();
  const expectedClosedSlug = expectedSlugParam || seededExpectedSlug || (marketPrefix ? `${marketPrefix}-${Math.floor(expectedClosedStartMs / 1000)}` : "");
  if (!expectedClosedSlug) {
    return {
      statusCode: 400,
      payload: { ok: false, error: "missing marketPrefix", instanceId, worker: WORKER_LABEL },
    };
  }
  let selected = null;
  let selectedSource = null;
  let continuityRuns = [];
  pushActivity(`LOOKING UP LATEST SESSION CARD FOR SLUG ${expectedClosedSlug.toUpperCase()}`, withSlugMeta({
    type: "latest-session-card-start",
    instanceId,
    marketPrefix,
  }, expectedClosedSlug, "expectedClosedSlug"));
  if (!selected) {
    try {
      if (!runIndex) runIndex = await fetchRunIndexPayloadForInstance(instanceId);
      if (!runIndexRows.length) {
        runIndexRows = compactRowsFromRunIndexPayload(runIndex.payload, {
          instanceId,
          marketPrefix,
          maxSessions: Math.max(200, maxSessions * 8),
          offset: 0,
        });
      }
      const exactRunIndex = runIndexRows.find((row) => String(row?.slug || "").trim().toLowerCase() === expectedClosedSlug) || null;
      if (exactRunIndex?.slug) {
        selected = await readRemoteCanonicalSessionJson(exactRunIndex.slug, true, RUN_AUDIT_HOST_PORT);
        selectedSource = "run_index_remote_canonical_session";
        continuityRuns = [];
      }
    } catch {}
  }
  if (!selected) {
    try {
      const remoteSummary = await readRemoteLatestRunSummaryJson();
      const rows = normalizedSummarySessions(remoteSummary.payload, marketPrefix || "", maxSessions);
      const exact = rows.find((row) => String(row?.slug || "").trim().toLowerCase() === expectedClosedSlug);
      const fallbackRow = exact || rows.find((row) => row?.closed === true) || null;
      if (fallbackRow?.slug) {
        selected = await readRemoteCanonicalSessionJson(fallbackRow.slug, true, RUN_AUDIT_HOST_PORT);
        selectedSource = "remote_canonical_session";
        continuityRuns = [];
      }
    } catch {}
  }
  if (!selected) {
    pushActivity(`NO FINALIZED SESSION CARD FOUND FOR SLUG ${expectedClosedSlug.toUpperCase()}`, withSlugMeta({
      type: "latest-session-card-miss",
      instanceId,
      marketPrefix,
    }, expectedClosedSlug, "expectedClosedSlug"));
    return {
      statusCode: 404,
      payload: {
        ok: false,
        error: "no finalized exact session available",
        instanceId,
        worker: WORKER_LABEL,
        expectedClosedSlug: expectedClosedSlug || null,
      },
    };
  }
  pushActivity(`READY LATEST SESSION CARD FOR SLUG ${String(selected?.slug || expectedClosedSlug || "").toUpperCase()}`, withSlugMeta({
    type: "latest-session-card-ready",
    instanceId,
    selectedSource: selectedSource || null,
  }, String(selected?.slug || "").trim() || expectedClosedSlug, "selectedSlug"));
  return {
    statusCode: 200,
      payload: {
        ok: true,
        worker: WORKER_LABEL,
        asOfMs: nowMs,
        instanceId,
      marketPrefix: marketPrefix || null,
      expectedClosedSlug: expectedClosedSlug || null,
      selectedSlug: String(selected?.slug || "").trim() || null,
      selectedSource: selectedSource || null,
      selectedSession: slimSelectedSession(selected),
      continuityRuns,
      upstreamUrl: runIndex?.upstreamUrl ? runIndex.upstreamUrl.toString() : null,
    },
  };
}

async function buildSessionArtifactsSummaryPayload(reqUrl) {
  const parts = String(reqUrl.pathname || "").split("/").filter(Boolean);
  const instanceId = (
    parts.length >= 5 &&
    String(parts[0] || "").toLowerCase() === "api" &&
    String(parts[1] || "").toLowerCase() === "v2" &&
    String(parts[2] || "").toLowerCase() === "bots" &&
    String(parts[4] || "").toLowerCase() === "session-artifacts-summary"
  )
    ? decodeURIComponent(String(parts[3] || "").trim())
    : "";
  if (!instanceId) {
    return { statusCode: 400, payload: { ok: false, error: "missing instanceId" } };
  }
  pushActivity(`BUILDING SESSION ARTIFACT SUMMARY FOR BOT ${instanceId.toUpperCase()}`, {
    type: "session-artifacts-summary-start",
    instanceId,
  });
  const marketPrefix = String(reqUrl.searchParams.get("marketPrefix") || "").trim().toLowerCase();
  const limitRaw = Number(reqUrl.searchParams.get("limit") || 100);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.floor(limitRaw))) : 100;
  try {
    const runIndex = await fetchRunIndexPayloadForInstance(instanceId);
    const runIndexRunNum = Number(runIndex?.payload?.run?.runNum ?? runIndex?.payload?.summary?.runNum ?? 0) || null;
    const runIndexRunId = String(runIndex?.payload?.run?.runAuditIdentity?.runId || runIndex?.payload?.summary?.runId || "").trim() || null;
    const runIndexStartedAtMs = Number(runIndex?.payload?.runStartMs ?? runIndex?.payload?.summary?.startedAtMs ?? runIndex?.payload?.run?.runAuditIdentity?.startedAtMs ?? runIndex?.payload?.run?.launchedAtMs ?? 0) || null;
    const buildSessionAuditPath = (slugLike) => {
      const slug = String(slugLike || "").trim();
      if (!(runIndexRunNum > 0) || !slug) return null;
      const params = new URLSearchParams();
      params.set("runNum", String(runIndexRunNum));
      if (runIndexRunId) params.set("runId", runIndexRunId);
      if (Number.isFinite(runIndexStartedAtMs) && runIndexStartedAtMs > 0) params.set("startedAtMs", String(runIndexStartedAtMs));
      params.set("slug", slug);
      return `/api/session-audits/review?${params.toString()}`;
    };
    const runIndexRows = await Promise.all(compactRowsFromRunIndexPayload(runIndex.payload, {
      instanceId,
      marketPrefix,
      maxSessions: limit,
      offset: 0,
    }).map(async (row) => {
      const displayPnlUsd = compactSessionDisplayPnlUsd(row);
      const noTrade = row?.noTrade === true && !(
        Number.isFinite(Number(displayPnlUsd)) &&
        Math.abs(Number(displayPnlUsd)) > 1e-9
      );
      let noTradeReason = noTrade
        ? (
            normalizeNoTradeReasonText(row?.noTradeReason)
            || normalizeNoTradeReasonText(row?.reason)
            || normalizeNoTradeReasonText(row?.failureReason)
            || normalizeNoTradeReasonText(row?.blockerReason)
            || null
          )
        : null;
      if (noTrade && !noTradeReason) {
        noTradeReason = await fetchNoTradeReasonFromAudit(
          Number.isFinite(Number(row?.runNum)) ? Number(row.runNum) : runIndexRunNum,
          row?.slug,
          String(row?.runIdText || row?.runId || runIndexRunId || "").trim() || null,
          runIndexStartedAtMs
        );
      }
      return {
        slug: String(row?.slug || "").trim() || null,
        humanLabel: String(row?.humanLabel || "").trim() || null,
        pnlUsd: Number.isFinite(Number(displayPnlUsd))
          ? Number(displayPnlUsd)
          : null,
        continuityBalanceUsd: Number.isFinite(Number(row?.continuityBalanceUsd))
          ? Number(row.continuityBalanceUsd)
          : (Number.isFinite(Number(row?.balanceUsd)) ? Number(row.balanceUsd) : null),
        latestBalanceUsd: Number.isFinite(Number(row?.continuityBalanceUsd))
          ? Number(row.continuityBalanceUsd)
          : (Number.isFinite(Number(row?.balanceUsd)) ? Number(row.balanceUsd) : null),
        noTrade,
        noTradeReason: noTrade ? (noTradeReason || "No trade") : null,
        closed: row?.closed === true,
        instanceId: String(row?.instanceId || instanceId || "").trim() || null,
        runNum: Number.isFinite(Number(row?.runNum)) ? Number(row.runNum) : runIndexRunNum,
        runId: String(row?.runIdText || row?.runId || runIndexRunId || "").trim() || null,
        startMs: Number.isFinite(Number(row?.startMs)) ? Number(row.startMs) : null,
        endMs: Number.isFinite(Number(row?.endMs)) ? Number(row.endMs) : null,
        sessionAuditPath: buildSessionAuditPath(row?.slug),
        source: "run_index",
      };
    }));
    let upstreamPayload = null;
    try {
      const upstreamReq = new URL(
        `/api/v2/bots/${encodeURIComponent(instanceId)}/session-artifacts-summary`,
        "http://worker.local"
      );
      for (const [k, v] of reqUrl.searchParams.entries()) upstreamReq.searchParams.set(k, v);
      const upstreamUrl = buildUpstreamUrl(upstreamReq);
      upstreamPayload = await fetchJsonDirect(upstreamUrl.toString());
      if (upstreamPayload && upstreamPayload.ok === true && Array.isArray(upstreamPayload.sessions)) {
        return {
          statusCode: 200,
          payload: {
            ...upstreamPayload,
            worker: WORKER_LABEL,
            source: String(upstreamPayload.source || "session_artifacts_summary_upstream"),
            upstreamUrl: upstreamUrl.toString(),
          },
        };
      }
    } catch {}
    if (runIndexRows.length) {
      return {
        statusCode: 200,
        payload: {
          ok: true,
          instanceId,
          runNum: runIndexRunNum,
          strategyId: null,
          marketPrefix: marketPrefix || null,
          count: runIndexRows.length,
          sessions: runIndexRows,
          source: "run_index_worker",
          worker: WORKER_LABEL,
          upstreamUrl: runIndex.upstreamUrl.toString(),
        },
      };
    }
  } catch {}
  const upstreamReq = new URL(
    `/api/v2/bots/${encodeURIComponent(instanceId)}/session-artifacts-summary`,
    "http://worker.local"
  );
  for (const [k, v] of reqUrl.searchParams.entries()) {
    upstreamReq.searchParams.set(k, v);
  }
  const upstreamUrl = buildUpstreamUrl(upstreamReq);
  let payload = null;
  try {
    payload = await fetchJsonDirect(upstreamUrl.toString());
  } catch {
    pushActivity(`USING REMOTE RUN SUMMARY FALLBACK FOR BOT ${instanceId.toUpperCase()}`, {
      type: "session-artifacts-summary-fallback",
      instanceId,
    });
    const remoteSummary = await readRemoteLatestRunSummaryJson();
    const sessions = normalizedSummarySessions(remoteSummary.payload, marketPrefix, limit);
    payload = {
      ok: true,
      instanceId,
      runNum: Number.isFinite(Number(remoteSummary?.payload?.runNum)) ? Number(remoteSummary.payload.runNum) : null,
      strategyId: String(remoteSummary?.payload?.strategyId || "").trim() || null,
      marketPrefix: marketPrefix || null,
      count: sessions.length,
      sessions,
      source: "run_summary_artifact_remote_file",
      remotePath: remoteSummary.path,
    };
  }
  return {
    statusCode: 200,
    payload: {
      ...(payload && typeof payload === "object" ? payload : {}),
      worker: WORKER_LABEL,
      upstreamUrl: upstreamUrl.toString(),
      source: "session_artifacts_summary_worker",
    },
  };
}

async function localJsonWithCache(reqUrl, producer, upstreamLabel) {
  const cacheKey = `local:${reqUrl.pathname}?${reqUrl.searchParams.toString()}`;
  const canCache = isCacheablePath(reqUrl.pathname);
  const bypass = shouldBypassCache(reqUrl);
  if (canCache && !bypass) {
    const cached = readCache(cacheKey);
    if (cached) {
      pushActivity(`USING LOCAL CACHE FOR ${summarizeUrl(reqUrl).toUpperCase()}`, {
        type: "local-cache-hit",
        path: reqUrl.pathname,
      });
      return { ...cached, cacheStatus: "hit" };
    }
  }
  if (inflight.has(cacheKey)) return inflight.get(cacheKey);
  const job = (async () => {
    pushActivity(`BUILDING LOCAL RESPONSE FOR ${summarizeUrl(reqUrl).toUpperCase()}`, {
      type: "local-build",
      path: reqUrl.pathname,
      refresh: bypass ? "1" : "0",
    });
    const { statusCode, payload } = await producer(reqUrl);
    const entry = {
      statusCode: Number(statusCode || 200),
      contentType: "application/json",
      bodyBase64: Buffer.from(JSON.stringify(payload, null, 2)).toString("base64"),
      cachedAtMs: Date.now(),
      expiresAtMs: Date.now() + (Number(statusCode || 200) >= 400 ? ERROR_TTL_MS : ttlForPath(reqUrl.pathname)),
      upstreamUrl: upstreamLabel,
    };
    if (canCache && Number(statusCode || 200) < 500) writeCache(cacheKey, entry);
    return { ...entry, cacheStatus: canCache ? "miss" : "pass" };
  })().finally(() => inflight.delete(cacheKey));
  inflight.set(cacheKey, job);
  return job;
}

async function chooseInstanceId(baseOrigin, preferredInstanceId) {
  const preferred = String(preferredInstanceId || "").trim();
  if (preferred) return preferred;
  const payload = await fetchJsonDirect(new URL("/api/v2/bots", `${baseOrigin}/`).toString());
  const items = Array.isArray(payload?.items) ? payload.items : [];
  for (const item of items) {
    const iid = String(item?.instanceId || "").trim();
    if (iid) return iid;
  }
  throw new Error(`no instance id available from ${baseOrigin}`);
}

async function latestClosedParitySlug() {
  if (!PARITY_OHIO_BASE) throw new Error("parity ohio base missing");
  const instanceId = await chooseInstanceId(PARITY_OHIO_BASE, PARITY_OHIO_INSTANCE_ID);
  const url = new URL(`/api/v2/bots/${encodeURIComponent(instanceId)}/continuity-history`, `${PARITY_OHIO_BASE}/`);
  url.searchParams.set("includeTrace", "0");
  url.searchParams.set("maxSessions", "1");
  const payload = await fetchJsonDirect(url.toString());
  const sessions = Array.isArray(payload?.sessions) ? payload.sessions : [];
  const slug = String(sessions[0]?.slug || "").trim();
  if (!slug) throw new Error("no closed parity slug available");
  return slug;
}

async function ensureParityArtifact(refreshRequested) {
  if (!PARITY_ENABLED) throw new Error("parity disabled");
  const latestJsonPath = path.join(PARITY_OUT_DIR, "latest.json");
  const latestHtmlPath = path.join(PARITY_OUT_DIR, "latest.html");
  if (!refreshRequested && fs.existsSync(latestJsonPath) && fs.existsSync(latestHtmlPath)) {
    return;
  }
  if (parityInflight) {
    await parityInflight;
    return;
  }
  parityInflight = (async () => {
    const slug = await latestClosedParitySlug();
    pushActivity(`BUILDING SESSION PARITY FOR ${slug.toUpperCase()}`, {
      type: "parity-build-start",
      slug,
      refresh: refreshRequested ? "1" : "0",
    });
    const ohioInstanceId = await chooseInstanceId(PARITY_OHIO_BASE, PARITY_OHIO_INSTANCE_ID);
    const args = [
      PARITY_SCRIPT_PATH,
      "--slug", slug,
      "--ohio-base", PARITY_OHIO_BASE,
      "--ohio-instance-id", ohioInstanceId,
      "--out-dir", PARITY_OUT_DIR,
    ];
    if (PARITY_DUBLIN_BASE) args.push("--dublin-base", PARITY_DUBLIN_BASE);
    const dublinInstanceId = PARITY_DUBLIN_BASE ? await chooseInstanceId(PARITY_DUBLIN_BASE, PARITY_DUBLIN_INSTANCE_ID) : "";
    if (dublinInstanceId) args.push("--dublin-instance-id", dublinInstanceId);
    if (PARITY_DUBLIN_SSH_HOST) args.push("--dublin-ssh-host", PARITY_DUBLIN_SSH_HOST);
    if (PARITY_DUBLIN_SSH_USER) args.push("--dublin-ssh-user", PARITY_DUBLIN_SSH_USER);
    if (PARITY_DUBLIN_SSH_KEY) args.push("--dublin-ssh-key", PARITY_DUBLIN_SSH_KEY);
    await new Promise((resolve, reject) => {
      const child = spawn(PARITY_PYTHON_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
      let stderr = "";
      child.stderr.on("data", (chunk) => { stderr += String(chunk || ""); });
      child.on("error", reject);
      child.on("exit", (code) => {
        if (code === 0) resolve();
        else reject(new Error(stderr || `parity script exit=${code}`));
      });
    });
    pushActivity(`FINISHED SESSION PARITY FOR ${slug.toUpperCase()}`, {
      type: "parity-build-finished",
      slug,
    });
  })().finally(() => {
    parityInflight = null;
  });
  await parityInflight;
}

function runAuditIdentityToken(runNum, runIdText, startedAtMs) {
  const safeRunNum = `run_${Math.max(1, Math.floor(Number(runNum) || 0))}`;
  const safeRunId = String(runIdText || "no_run_id")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .slice(0, 64);
  const safeStarted = Number.isFinite(Number(startedAtMs)) && Number(startedAtMs) > 0 ? String(Math.floor(Number(startedAtMs))) : "no_started_at";
  return `${safeRunNum}__${safeRunId}__${safeStarted}`;
}

function runAuditIdentityFromRequest(reqUrl, remoteSummaryLike) {
  const runNum = Number(reqUrl.searchParams.get("runNum") || 0);
  const requestedRunId = String(reqUrl.searchParams.get("runId") || "").trim();
  const requestedStartedAtMsRaw = Number(reqUrl.searchParams.get("startedAtMs") || 0);
  const requestedHostPort = normalizeRunAuditHostPort(reqUrl.searchParams.get("hostPort"));
  const remoteSummary = remoteSummaryLike && typeof remoteSummaryLike === "object" ? remoteSummaryLike : null;
  return {
    runNum: Number.isFinite(runNum) && runNum > 0 ? Math.floor(runNum) : null,
    hostPort: requestedHostPort,
    runIdText: requestedRunId || String(remoteSummary?.runId || "").trim() || null,
    startedAtMs:
      Number.isFinite(requestedStartedAtMsRaw) && requestedStartedAtMsRaw > 0 ? Math.floor(requestedStartedAtMsRaw) : (
        Number.isFinite(Number(remoteSummary?.startedAtMs || remoteSummary?.launchedAtMs || 0))
          && Number(remoteSummary?.startedAtMs || remoteSummary?.launchedAtMs || 0) > 0
          ? Math.floor(Number(remoteSummary.startedAtMs || remoteSummary.launchedAtMs || 0))
          : null
      ),
  };
}

function runAuditCachePaths(runNum, identity) {
  const safeIdentity = runAuditIdentityToken(runNum, identity?.runIdText || null, identity?.startedAtMs || null);
  const safeHost = normalizeRunAuditHostPort(identity?.hostPort || RUN_AUDIT_HOST_PORT);
  const dir = path.join(RUN_AUDIT_CACHE_ROOT, `host_${safeHost}`, `run_${Math.max(1, Math.floor(Number(runNum) || 0))}`, safeIdentity);
  return {
    dir,
    htmlPath: path.join(dir, "run_audit_review.html"),
    jsonPath: path.join(dir, "run_audit_review.json"),
  };
}

function runAuditSourceMirrorDir(runNum, hostPort) {
  const safeHost = normalizeRunAuditHostPort(hostPort || RUN_AUDIT_HOST_PORT);
  return path.join(
    RUN_AUDIT_CACHE_ROOT,
    `host_${safeHost}`,
    "multi_runs",
    `run_${Math.max(1, Math.floor(Number(runNum) || 0))}`,
  );
}

function existingRunAuditSourceMirror(runNum, hostPort) {
  const runDir = runAuditSourceMirrorDir(runNum, hostPort);
  if (!fs.existsSync(runDir) || !fs.statSync(runDir).isDirectory()) return null;
  const summaryPath = path.join(runDir, "summary.json");
  const eventsPath = path.join(runDir, "events.jsonl");
  if (!fs.existsSync(summaryPath) || !fs.existsSync(eventsPath)) return null;
  const indexName = (fs.readdirSync(runDir).find((name) => /^index_.+_run_\d+\.json$/i.test(String(name || ""))) || "").trim();
  if (!indexName) return null;
  return { runDir, summaryPath, indexPath: path.join(runDir, indexName), eventsPath };
}

function runAuditRunDir(runNum, hostPort) {
  if (!RUN_AUDIT_REMOTE_ROOT || !runNum) return "";
  const safeHost = normalizeRunAuditHostPort(hostPort || RUN_AUDIT_HOST_PORT);
  return `${RUN_AUDIT_REMOTE_ROOT}/trade_logs/hosts/host_${safeHost}/multi_runs/run_${runNum}`;
}

function runAuditArtifactBaseUrl() {
  const base = String(RUN_AUDIT_PUBLIC_BASE || "").trim() || String(UPSTREAM_ORIGIN || "").trim();
  if (!base) throw new Error("run audit artifact base missing");
  return `${base.replace(/\/+$/, "")}/`;
}

function runAuditSourceBaseUrl(hostPortLike = "") {
  const safeHostPort = String(hostPortLike || "").trim();
  const mappedBase = safeHostPort
    ? String(UPSTREAM_ORIGIN_MAP.get(normalizeRunAuditHostPort(safeHostPort)) || "").trim()
    : "";
  const base = mappedBase || String(UPSTREAM_ORIGIN || "").trim() || String(RUN_AUDIT_PUBLIC_BASE || "").trim();
  if (!base) throw new Error("run audit source base missing");
  return `${base.replace(/\/+$/, "")}/`;
}

async function readLiveRunAuditInputsFromBotEndpoints(runNum, requestedIdentity, hostPort) {
  const botsReq = new URL("/api/v2/bots", "http://worker.local");
  botsReq.searchParams.set("engine", "paper");
  if (hostPort) botsReq.searchParams.set("sourceHostPort", normalizeRunAuditHostPort(hostPort));
  const botsUrl = buildUpstreamUrl(botsReq).toString();
  const botsPayload = await fetchJsonDirect(botsUrl);
  const items = Array.isArray(botsPayload?.items) ? botsPayload.items : [];
  const requested = requestedIdentity && typeof requestedIdentity === "object" ? requestedIdentity : {};
  const requestedRunId = String(requested.runIdText || "").trim();
  const requestedStartedAtMs = Number(requested.startedAtMs || 0);
  const bot = items.find((item) => {
    const itemRunNum = Number(item?.runNum || 0);
    const itemRunId = String(item?.runId || "").trim();
    const itemStartedAtMs = Number(item?.launchedAtMs || item?.startedAtMs || 0);
    if (Math.floor(itemRunNum) !== Math.floor(Number(runNum) || 0)) return false;
    if (requestedRunId && itemRunId && itemRunId !== requestedRunId) return false;
    if (requestedStartedAtMs && Number.isFinite(itemStartedAtMs) && itemStartedAtMs > 0 && Math.floor(itemStartedAtMs) !== Math.floor(requestedStartedAtMs)) return false;
    return true;
  }) || null;
  if (!bot) throw new Error(`live bot payload missing run ${String(runNum)}`);
  const instanceId = String(bot?.instanceId || "").trim();
  if (!instanceId) throw new Error(`missing instanceId for live run ${String(runNum)}`);
  const continuityResult = await fetchContinuityHistoryPayloadForInstance(instanceId, {
    includeTrace: false,
    maxSessions: 5000,
    sourceHostPort: hostPort,
  });
  const continuityUrl = continuityResult.upstreamUrl;
  const continuityPayload = continuityResult.payload;
  const sessions = compactRowsFromContinuityPayload(continuityPayload, {
    instanceId,
    maxSessions: 5000,
    offset: 0,
  })
    .filter((row) => rowWithinRunStart(row, Number(bot?.launchedAtMs || bot?.startedAtMs || 0)))
    .sort((a, b) => Number(a?.startMs || 0) - Number(b?.startMs || 0));
  const summary = {
    runNum: Math.floor(Number(bot?.runNum || runNum) || 0),
    runId: String(bot?.runId || requestedRunId || "").trim(),
    startedAtMs: Number(bot?.launchedAtMs || bot?.startedAtMs || requestedStartedAtMs || 0) || null,
    launchedAtMs: Number(bot?.launchedAtMs || bot?.startedAtMs || requestedStartedAtMs || 0) || null,
    strategyId: String(bot?.strategyId || "").trim(),
    instanceId,
    mode: String(bot?.mode || "paper").trim(),
    status: String(bot?.status || "").trim(),
    marketTitle: String(bot?.marketTitle || "").trim(),
    marketSlug: String(bot?.marketSlug || "").trim(),
    startBalanceUsd: Number(bot?.startBalanceUsd || 0) || null,
    endBalanceUsd: Number(bot?.latestBalanceUsd || 0) || null,
    latestBalanceUsd: Number(bot?.latestBalanceUsd || 0) || null,
    latestPnlUsd: Number(bot?.latestPnlUsd || 0) || null,
    betUsd: Number(bot?.betUsd || 0) || null,
    maxBetUsd: Number(bot?.maxBetUsd || 0) || null,
    sizingProfile: String(bot?.sizingProfile || "").trim() || null,
  };
  const index = {
    run: {
      runNum: summary.runNum,
      runId: summary.runId,
      startedAtMs: summary.startedAtMs,
      strategyId: summary.strategyId,
      instanceId: summary.instanceId,
      marketSlug: summary.marketSlug,
    },
    runNum: summary.runNum,
    runId: summary.runId,
    runStartMs: summary.startedAtMs,
    sessions,
    summary,
  };
  return {
    runDir: runAuditRunDir(runNum, hostPort),
    summaryPath: botsUrl,
    summary,
    indexPath: continuityUrl.toString(),
    index,
    fallback: "live_bot_endpoints",
  };
}

async function readRemoteRunAuditInputs(runNum, requestedIdentity) {
  const hostPort = normalizeRunAuditHostPort(requestedIdentity?.hostPort || RUN_AUDIT_HOST_PORT);
  const requested = requestedIdentity && typeof requestedIdentity === "object" ? requestedIdentity : {};
  const runIdText = String(requested.runIdText || "").trim();
  const startedAtMs = Number(requested.startedAtMs || 0);
  const runIdParam = runIdText ? `&runId=${encodeURIComponent(runIdText)}` : "";
  const startedAtParam = Number.isFinite(startedAtMs) && startedAtMs > 0 ? `&startedAtMs=${encodeURIComponent(String(Math.floor(startedAtMs)))}` : "";
  const runDir = runAuditRunDir(runNum, hostPort);
  if (!runDir) {
    throw new Error(`missing run directory for run ${String(runNum)}`);
  }
  const artifactBase = runAuditSourceBaseUrl(hostPort);
  const summaryUrl = new URL(
    `/api/compare/run-artifact?hostPort=${encodeURIComponent(hostPort)}&runNum=${encodeURIComponent(String(runNum))}&kind=summary${runIdParam}${startedAtParam}`,
    artifactBase,
  ).toString();
  try {
    const summary = await fetchJsonDirect(summaryUrl);
    const strategyId = String(summary?.strategyId || "").trim().toLowerCase();
    if (!strategyId) {
      throw new Error(`missing strategyId for run ${String(runNum)}`);
    }
    const indexUrl = new URL(
      `/api/compare/run-artifact?hostPort=${encodeURIComponent(hostPort)}&runNum=${encodeURIComponent(String(runNum))}&kind=index&strategyId=${encodeURIComponent(strategyId)}${runIdParam}${startedAtParam}`,
      artifactBase,
    ).toString();
    const index = await fetchJsonDirect(indexUrl);
    return { runDir, summaryPath: summaryUrl, summary, indexPath: indexUrl, index };
  } catch (httpError) {
    try {
      return await readLiveRunAuditInputsFromBotEndpoints(runNum, requestedIdentity, hostPort);
    } catch (liveError) {
      httpError = new Error(`${String(httpError?.message || httpError)} | liveFallback=${String(liveError?.message || liveError)}`);
    }
    const summaryPath = `${runDir}/summary.json`;
    const summary = await readRemoteJsonFile(summaryPath);
    if (runIdText && String(summary?.runId || "").trim() && String(summary.runId).trim() !== runIdText) {
      throw new Error(`runId mismatch for run ${String(runNum)}: requested=${runIdText} actual=${String(summary.runId)}`);
    }
    if (
      Number.isFinite(startedAtMs)
      && startedAtMs > 0
      && Number.isFinite(Number(summary?.startedAtMs || summary?.launchedAtMs || 0))
      && Number(Math.floor(Number(summary?.startedAtMs || summary?.launchedAtMs || 0))) !== Math.floor(startedAtMs)
    ) {
      throw new Error(`startedAtMs mismatch for run ${String(runNum)}: requested=${Math.floor(startedAtMs)} actual=${Math.floor(Number(summary?.startedAtMs || summary?.launchedAtMs || 0))}`);
    }
    const strategyId = String(summary?.strategyId || "").trim().toLowerCase();
    if (!strategyId) {
      throw new Error(`missing strategyId for run ${String(runNum)}`);
    }
    const indexPath = `${runDir}/index_${strategyId}_run_${runNum}.json`;
    const index = await readRemoteJsonFile(indexPath);
    return { runDir, summaryPath, summary, indexPath, index, fallback: "ssh", httpError: String(httpError?.message || httpError) };
  }
}

async function syncRunAuditSourceMirror(runNum, remoteInputs, hostPort, opts = {}) {
  const safeHost = normalizeRunAuditHostPort(hostPort || RUN_AUDIT_HOST_PORT);
  const coldBuild = opts && opts.coldBuild === true;
  const runDir = runAuditSourceMirrorDir(runNum, safeHost);
  fs.mkdirSync(runDir, { recursive: true });
  const hostRoot = path.resolve(runDir, "..", "..");
  fs.mkdirSync(hostRoot, { recursive: true });
  const summaryPath = path.join(runDir, "summary.json");
  const strategyId = String(remoteInputs?.summary?.strategyId || remoteInputs?.index?.run?.strategyId || "").trim().toLowerCase();
  if (!strategyId) {
    throw new Error(`missing strategyId for run ${String(runNum)}`);
  }
  const indexPath = path.join(runDir, `index_${strategyId}_run_${runNum}.json`);
  const eventsPath = path.join(runDir, "events.jsonl");
  const telemetryPath = path.join(runDir, "telemetry.jsonl");
  const artifactRunId = String(remoteInputs?.summary?.runId || remoteInputs?.runId || "").trim();
  const artifactStartedAtMs = Number(remoteInputs?.summary?.startedAtMs || remoteInputs?.summary?.launchedAtMs || remoteInputs?.index?.run?.startedAtMs || 0);
  const runIdParam = artifactRunId ? `&runId=${encodeURIComponent(artifactRunId)}` : "";
  const startedAtParam = Number.isFinite(artifactStartedAtMs) && artifactStartedAtMs > 0
    ? `&startedAtMs=${encodeURIComponent(String(Math.floor(artifactStartedAtMs)))}`
    : "";
  fs.writeFileSync(summaryPath, `${JSON.stringify(remoteInputs.summary, null, 2)}\n`, "utf8");
  fs.writeFileSync(indexPath, `${JSON.stringify(remoteInputs.index, null, 2)}\n`, "utf8");

  const artifactBase = runAuditSourceBaseUrl(safeHost);
  const eventsUrl = new URL(
    `/api/compare/run-artifact?hostPort=${encodeURIComponent(safeHost)}&runNum=${encodeURIComponent(String(runNum))}&kind=events${runIdParam}${startedAtParam}`,
    artifactBase,
  ).toString();
  try {
    fs.writeFileSync(eventsPath, await fetchTextDirect(eventsUrl), "utf8");
  } catch {
    if (!fs.existsSync(eventsPath)) fs.writeFileSync(eventsPath, "", "utf8");
  }

  const telemetryUrl = new URL(
    `/api/compare/run-artifact?hostPort=${encodeURIComponent(safeHost)}&runNum=${encodeURIComponent(String(runNum))}&kind=telemetry${runIdParam}${startedAtParam}`,
    artifactBase,
  ).toString();
  try {
    fs.writeFileSync(telemetryPath, await fetchTextDirect(telemetryUrl), "utf8");
  } catch {
    if (!fs.existsSync(telemetryPath)) fs.writeFileSync(telemetryPath, "", "utf8");
  }
  const sessionTraceUrl = new URL(
    `/api/compare/run-artifact?hostPort=${encodeURIComponent(safeHost)}&runNum=${encodeURIComponent(String(runNum))}&kind=session-trace-log${runIdParam}${startedAtParam}`,
    artifactBase,
  ).toString();
  const sessionTracePath = path.join(hostRoot, "05_session_trace_run_1.jsonl");
  try {
    fs.writeFileSync(sessionTracePath, await fetchTextDirect(sessionTraceUrl), "utf8");
  } catch {
    if (!fs.existsSync(sessionTracePath)) fs.writeFileSync(sessionTracePath, "", "utf8");
  }
  const indexedSessions = Array.isArray(remoteInputs?.index?.sessions) ? remoteInputs.index.sessions : [];
  const detailedSessions = indexedSessions
    .slice()
    .sort((a, b) => Number(b?.startMs || 0) - Number(a?.startMs || 0))
    .slice(0, RUN_AUDIT_MIRROR_DETAILED_SESSION_LIMIT);
  const mirroredSessionAuditDir = path.join(runDir, "session_audits");
  fs.mkdirSync(mirroredSessionAuditDir, { recursive: true });
  for (const session of detailedSessions) {
    const slug = String(session?.slug || "").trim();
    if (!slug) continue;
    try {
      const canonicalPayload = await readRemoteCanonicalSessionJson(slug, true, safeHost);
      if (!canonicalPayload || typeof canonicalPayload !== "object") continue;
      const sessionDir = path.join(hostRoot, "sessions", slug);
      fs.mkdirSync(sessionDir, { recursive: true });
      fs.writeFileSync(
        path.join(sessionDir, "high_fidelity.json"),
        `${JSON.stringify(canonicalPayload, null, 2)}\n`,
        "utf8",
      );
    } catch {
      // Leave the run-level trace log as the fallback when canonical session
      // artifacts are unavailable for a specific slug.
    }
  }
  const compactSessions = indexedSessions;
  let mirroredCompactCount = 0;
  try {
    const compactBundleUrl = new URL(
      `/api/compare/run-artifact?hostPort=${encodeURIComponent(safeHost)}&runNum=${encodeURIComponent(String(runNum))}&kind=session-audit-compacts${runIdParam}${startedAtParam}`,
      artifactBase,
    ).toString();
    const bundleText = await fetchTextDirect(compactBundleUrl);
    for (const line of String(bundleText || "").split(/\r?\n/)) {
      const trimmed = String(line || "").trim();
      if (!trimmed) continue;
      try {
        const compactPayload = JSON.parse(trimmed);
        const slug = String(compactPayload?.slug || "").trim();
        if (!slug || !(compactPayload && typeof compactPayload === "object")) continue;
        fs.writeFileSync(
          path.join(mirroredSessionAuditDir, `${slug}.compact.json`),
          `${JSON.stringify(compactPayload, null, 2)}\n`,
          "utf8",
        );
        mirroredCompactCount += 1;
      } catch {}
    }
  } catch {}
  if (mirroredCompactCount === 0) {
    for (const session of compactSessions) {
      const slug = String(session?.slug || "").trim();
      if (!slug) continue;
      try {
        const compactPayload = await readRemoteSessionAuditCompact(runNum, slug, safeHost);
        if (compactPayload && typeof compactPayload === "object") {
          fs.writeFileSync(
            path.join(mirroredSessionAuditDir, `${slug}.compact.json`),
            `${JSON.stringify(compactPayload, null, 2)}\n`,
            "utf8",
          );
        }
      } catch {
        // Run audit can still fall back to canonical session trace plus live events
        // when a standalone session audit compact is unavailable.
      }
    }
  }
  return { runDir, summaryPath, indexPath, eventsPath, telemetryPath, strategyId };
}

function parseSlugStartSec(slugLike) {
  const match = String(slugLike || "").trim().match(/-(\d{9,})$/);
  return match ? Number(match[1]) : 0;
}

function auditDocCompleteness(doc, runNum, requestedIdentity, remoteInputs) {
  if (!doc || typeof doc !== "object") {
    return { ok: false, reason: "missing_doc" };
  }
  const cachedAuditCodeVersion = String(doc?.buildStats?.auditCodeVersion || doc?.auditCodeVersion || "").trim();
  if (cachedAuditCodeVersion && cachedAuditCodeVersion !== RUN_AUDIT_EXPECTED_CODE_VERSION) {
    return { ok: false, reason: "audit_code_version_mismatch", cachedAuditCodeVersion };
  }
  const docRunNum = Math.floor(Number(doc?.run?.runNum || 0));
  if (docRunNum !== Math.floor(Number(runNum) || 0)) {
    return { ok: false, reason: "run_num_mismatch", docRunNum };
  }
  if (
    requestedIdentity?.hostPort
    && String(doc?.source?.hostPort || "").trim()
    && String(doc.source.hostPort).trim() !== normalizeRunAuditHostPort(requestedIdentity.hostPort)
  ) {
    return {
      ok: false,
      reason: "host_port_mismatch",
      docHostPort: String(doc.source.hostPort),
      requestedHostPort: String(normalizeRunAuditHostPort(requestedIdentity.hostPort)),
    };
  }
  const docRunId = String(doc?.run?.runId || "").trim();
  if (
    requestedIdentity?.runIdText
    && docRunId
    && docRunId !== requestedIdentity.runIdText
  ) {
    return { ok: false, reason: "run_id_mismatch", docRunId, requestedRunId: requestedIdentity.runIdText };
  }
  const docStartedAtMs = Number(doc?.run?.startedAtMs || doc?.run?.runAuditIdentity?.startedAtMs || 0);
  if (
    requestedIdentity?.startedAtMs
    && Number.isFinite(docStartedAtMs)
    && docStartedAtMs > 0
    && requestedIdentity.startedAtMs !== docStartedAtMs
  ) {
    return { ok: false, reason: "started_at_ms_mismatch", docStartedAtMs, requestedStartedAtMs: requestedIdentity.startedAtMs };
  }
  const remoteSummary = remoteInputs?.summary && typeof remoteInputs.summary === "object" ? remoteInputs.summary : {};
  const remoteIndex = remoteInputs?.index && typeof remoteInputs.index === "object" ? remoteInputs.index : {};
  const indexedSessions = Array.isArray(remoteIndex.sessions) ? remoteIndex.sessions : [];
  const indexedSlugs = new Set(
    indexedSessions
      .map((session) => String(session?.slug || "").trim())
      .filter(Boolean),
  );
  const auditSessions = Array.isArray(doc.sessions) ? doc.sessions : [];
  const auditSlugs = new Set(
    auditSessions
      .map((session) => String(session?.slug || "").trim())
      .filter(Boolean),
  );
  const missingIndexedSlugs = [];
  for (const slug of indexedSlugs) {
    if (!auditSlugs.has(slug)) missingIndexedSlugs.push(slug);
  }
  if (missingIndexedSlugs.length > RUN_AUDIT_TOLERATED_MISSING_SESSIONS) {
    return {
      ok: false,
      reason: "missing_indexed_sessions",
      indexedSessionCount: indexedSlugs.size,
      auditSessionCount: auditSlugs.size,
      missingIndexedSessionCount: missingIndexedSlugs.length,
      sampleMissingIndexedSlugs: missingIndexedSlugs.slice(0, 5),
    };
  }
  const currentSlug = String(remoteSummary?.marketSlug || remoteIndex?.run?.marketSlug || "").trim();
  const latestIndexedStartSec = indexedSessions.reduce((best, session) => {
    const startSec = parseSlugStartSec(session?.slug);
    return startSec > best ? startSec : best;
  }, 0);
  const latestAuditStartSec = auditSessions.reduce((best, session) => {
    const startSec = parseSlugStartSec(session?.slug);
    return startSec > best ? startSec : best;
  }, 0);
  const toleratedLagSec = RUN_AUDIT_TOLERATED_MISSING_SESSIONS * 300;
  if (
    latestIndexedStartSec > 0
    && latestAuditStartSec > 0
    && latestAuditStartSec + toleratedLagSec < latestIndexedStartSec
  ) {
    return {
      ok: false,
      reason: "latest_session_behind_index",
      latestIndexedStartSec,
      latestAuditStartSec,
      indexedSessionCount: indexedSlugs.size,
      auditSessionCount: auditSlugs.size,
    };
  }
  return {
    ok: true,
    indexedSessionCount: indexedSlugs.size,
    auditSessionCount: auditSlugs.size,
    currentSlug,
    latestIndexedStartSec,
    latestAuditStartSec,
  };
}

async function ensureRunAuditArtifact(reqUrl) {
  const runNumRaw = Number(reqUrl.searchParams.get("runNum") || 0);
  const runNum = Number.isFinite(runNumRaw) && runNumRaw > 0 ? Math.floor(runNumRaw) : 0;
  if (!runNum) {
    return { statusCode: 400, contentType: "application/json", body: JSON.stringify({ ok: false, error: "Missing/invalid runNum", worker: WORKER_LABEL }, null, 2) };
  }
  if (!RUN_AUDIT_ENABLED) {
    return {
      statusCode: 503,
      contentType: "application/json",
      body: JSON.stringify({
        ok: false,
        error: "run audit worker mode is not configured",
        worker: WORKER_LABEL,
        runAuditEnabled: false,
      }, null, 2),
    };
  }
  const wantJson = String(reqUrl.searchParams.get("format") || "").trim().toLowerCase() === "json";
  const refresh = shouldBypassCache(reqUrl);
  const backgroundWarm = String(reqUrl.searchParams.get("backgroundWarm") || "").trim() === "1";
  const thresholdSig = ["t1", "t2", "t3", "t4"].map((key) => String(reqUrl.searchParams.get(key) || "")).join("|");
  const requestedIdentity = runAuditIdentityFromRequest(reqUrl);
  const cacheKey = `run_audit:${normalizeRunAuditHostPort(requestedIdentity.hostPort || RUN_AUDIT_HOST_PORT)}:${runNum}:${runAuditIdentityToken(runNum, reqUrl.searchParams.get("runId") || null, reqUrl.searchParams.get("startedAtMs") || null)}:${thresholdSig}`;
  const cachedPaths = runAuditCachePaths(runNum, requestedIdentity);
  const cachedOutPath = wantJson ? cachedPaths.jsonPath : cachedPaths.htmlPath;
  const hasCachedArtifact = fs.existsSync(cachedPaths.htmlPath) && fs.existsSync(cachedPaths.jsonPath);
  if (runAuditInflight.has(cacheKey) && hasCachedArtifact) {
    pushActivity(`SERVING LAST GOOD RUN AUDIT FOR RUN ${String(runNum)} WHILE REBUILD IS INFLIGHT`, {
      type: "run-audit-cache-serve-while-inflight",
      runNum,
      outPath: cachedOutPath,
    });
    return {
      statusCode: 200,
      contentType: wantJson ? "application/json" : "text/html",
      filePath: cachedOutPath,
    };
  }
  if (!runAuditInflight.has(cacheKey)) {
    const job = (async () => {
      const paths = runAuditCachePaths(runNum, requestedIdentity);
      fs.mkdirSync(paths.dir, { recursive: true });
      let remoteInputs = null;
      try {
        remoteInputs = await readRemoteRunAuditInputs(runNum, requestedIdentity);
      } catch (error) {
        pushActivity(`FAILED TO READ RUN AUDIT INPUTS FOR RUN ${String(runNum)}`, {
          type: "run-audit-inputs-error",
          runNum,
          error: String(error?.message || error),
        });
      }
      const htmlExists = fs.existsSync(paths.htmlPath);
      const jsonExists = fs.existsSync(paths.jsonPath);
      const cacheExists = htmlExists && jsonExists;
      const newestMtimeMs = Math.max(
        htmlExists ? Number(fs.statSync(paths.htmlPath).mtimeMs || 0) : 0,
        jsonExists ? Number(fs.statSync(paths.jsonPath).mtimeMs || 0) : 0
      );
      const ageMs = newestMtimeMs > 0 ? Math.max(0, Date.now() - newestMtimeMs) : Number.POSITIVE_INFINITY;
      let completeness = { ok: !cacheExists, reason: cacheExists ? "" : "cache_missing" };
      if (cacheExists) {
        try {
          completeness = auditDocCompleteness(
            JSON.parse(fs.readFileSync(paths.jsonPath, "utf8")),
            runNum,
            runAuditIdentityFromRequest(reqUrl, remoteInputs?.summary),
            remoteInputs,
          );
        } catch (error) {
          completeness = { ok: false, reason: "invalid_cached_json", error: String(error?.message || error) };
        }
      }
        const completenessRequiresImmediateRebuild =
        !completeness.ok &&
        (
          !cacheExists ||
          String(completeness.reason || "") === "missing_doc" ||
          String(completeness.reason || "") === "run_num_mismatch" ||
          String(completeness.reason || "") === "run_id_mismatch" ||
          String(completeness.reason || "") === "started_at_ms_mismatch" ||
          String(completeness.reason || "") === "host_port_mismatch" ||
          String(completeness.reason || "") === "audit_code_version_mismatch" ||
          String(completeness.reason || "") === "invalid_cached_json" ||
          String(completeness.reason || "") === "missing_indexed_sessions" ||
          String(completeness.reason || "") === "latest_session_behind_index"
        );
      if (cacheExists && !completeness.ok && !completenessRequiresImmediateRebuild) {
        pushActivity(`SERVING STALE CACHED RUN AUDIT FOR RUN ${String(runNum)}`, {
          type: "run-audit-cache-serve-stale",
          runNum,
          cacheAgeMs: Number.isFinite(ageMs) ? String(Math.round(ageMs)) : "inf",
          completenessReason: String(completeness.reason || "unknown"),
          indexedSessionCount: Number.isFinite(completeness.indexedSessionCount) ? String(completeness.indexedSessionCount) : "",
          cachedSessionCount: Number.isFinite(completeness.auditSessionCount) ? String(completeness.auditSessionCount) : "",
        });
      }
      const needsBuild = refresh || !cacheExists || ageMs > RUN_AUDIT_MAX_AGE_MS || completenessRequiresImmediateRebuild;
      const canServeStaleWhileBackgroundWarmCatchesUp = cacheExists && !refresh && !backgroundWarm;
      if (needsBuild && canServeStaleWhileBackgroundWarmCatchesUp) {
        pushActivity(`SERVING CACHED RUN AUDIT FOR RUN ${String(runNum)} WHILE BACKGROUND REBUILD CATCHES UP`, {
          type: "run-audit-cache-serve-while-rebuild-deferred",
          runNum,
          cacheAgeMs: Number.isFinite(ageMs) ? String(Math.round(ageMs)) : "inf",
          rebuildReason: !cacheExists ? "cache_missing" : (ageMs > RUN_AUDIT_MAX_AGE_MS ? "cache_stale" : String(completeness.reason || "unknown")),
          indexedSessionCount: remoteInputs ? String((Array.isArray(remoteInputs.index?.sessions) ? remoteInputs.index.sessions.length : 0)) : "",
          cachedSessionCount: completeness && Number.isFinite(completeness.auditSessionCount) ? String(completeness.auditSessionCount) : "",
        });
        return paths;
      }
      if (needsBuild) {
        pushActivity(`BUILDING RUN AUDIT FOR RUN ${String(runNum)}`, {
          type: "run-audit-build-start",
          runNum,
          refresh: refresh ? "1" : "0",
          cacheExists: cacheExists ? "1" : "0",
          cacheAgeMs: Number.isFinite(ageMs) ? String(Math.round(ageMs)) : "inf",
          rebuildReason: !cacheExists ? "cache_missing" : (ageMs > RUN_AUDIT_MAX_AGE_MS ? "cache_stale" : String(completeness.reason || "unknown")),
          indexedSessionCount: remoteInputs ? String((Array.isArray(remoteInputs.index?.sessions) ? remoteInputs.index.sessions.length : 0)) : "",
          cachedSessionCount: completeness && Number.isFinite(completeness.auditSessionCount) ? String(completeness.auditSessionCount) : "",
        });
        let sourceMirror = null;
        if (remoteInputs) {
          sourceMirror = await syncRunAuditSourceMirror(runNum, remoteInputs, requestedIdentity.hostPort, {
            coldBuild: !cacheExists,
          });
        } else {
          sourceMirror = existingRunAuditSourceMirror(runNum, requestedIdentity.hostPort);
          if (sourceMirror) {
            pushActivity(`USING LOCAL RUN AUDIT SOURCE MIRROR FOR RUN ${String(runNum)}`, {
              type: "run-audit-source-mirror-cache-hit",
              runNum,
              runDir: sourceMirror.runDir,
            });
          }
        }
        if (!sourceMirror) {
          if (!(RUN_AUDIT_REMOTE_HOST && RUN_AUDIT_REMOTE_KEY && RUN_AUDIT_REMOTE_ROOT)) {
            throw new Error("run audit worker requires remote canonical source configuration");
          }
          sourceMirror = await syncRunAuditSourceMirror(
            runNum,
            await readRemoteRunAuditInputs(runNum, requestedIdentity),
            requestedIdentity.hostPort,
            {
              coldBuild: !cacheExists,
            },
          );
        }
        const args = [
          RUN_AUDIT_SCRIPT_PATH,
          "--run", String(runNum),
          "--host-port", normalizeRunAuditHostPort(requestedIdentity.hostPort || RUN_AUDIT_HOST_PORT),
          "--run-dir", sourceMirror.runDir,
          "--public-base", UPSTREAM_ORIGIN || RUN_AUDIT_PUBLIC_BASE || "http://127.0.0.1",
          "--out-html", paths.htmlPath,
          "--out-json", paths.jsonPath,
        ];
        if (!sourceMirror?.runDir) {
          args.splice(2, 0,
            "--host", String(RUN_AUDIT_REMOTE_HOST || ""),
            "--key", String(RUN_AUDIT_REMOTE_KEY || ""),
            "--remote-root", String(RUN_AUDIT_REMOTE_ROOT || ""),
          );
        }
        await new Promise((resolve, reject) => {
          const child = spawn(RUN_AUDIT_PYTHON_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
          let stderr = "";
          let stdout = "";
          const killTimer = setTimeout(() => {
            try { child.kill("SIGTERM"); } catch {}
            setTimeout(() => {
              try { child.kill("SIGKILL"); } catch {}
            }, 1000).unref?.();
            reject(new Error(`run audit build timeout after ${RUN_AUDIT_SYNC_BUILD_TIMEOUT_MS}ms`));
          }, RUN_AUDIT_SYNC_BUILD_TIMEOUT_MS);
          killTimer.unref?.();
          child.stdout.on("data", (chunk) => { stdout += String(chunk || ""); });
          child.stderr.on("data", (chunk) => { stderr += String(chunk || ""); });
          child.on("error", (error) => {
            clearTimeout(killTimer);
            reject(error);
          });
          child.on("exit", (code) => {
            clearTimeout(killTimer);
            if (code === 0) resolve();
            else reject(new Error(stderr || stdout || `run audit exit=${code}`));
          });
        });
        pushActivity(`FINISHED RUN AUDIT FOR RUN ${String(runNum)}`, {
          type: "run-audit-build-finished",
          runNum,
        });
      } else {
        pushActivity(`USING CACHED RUN AUDIT FOR RUN ${String(runNum)}`, {
          type: "run-audit-cache-hit",
          runNum,
          indexedSessionCount: completeness && Number.isFinite(completeness.indexedSessionCount) ? String(completeness.indexedSessionCount) : "",
          cachedSessionCount: completeness && Number.isFinite(completeness.auditSessionCount) ? String(completeness.auditSessionCount) : "",
        });
      }
      return paths;
    })().finally(() => {
      runAuditInflight.delete(cacheKey);
    });
    runAuditInflight.set(cacheKey, job);
  }
  try {
    const paths = await runAuditInflight.get(cacheKey);
    const outPath = wantJson ? paths.jsonPath : paths.htmlPath;
    if (!fs.existsSync(outPath)) {
      return {
        statusCode: 404,
        contentType: "application/json",
        body: JSON.stringify({ ok: false, error: "run audit artifact missing", worker: WORKER_LABEL, runNum }, null, 2),
      };
    }
    return {
      statusCode: 200,
      contentType: wantJson ? "application/json" : "text/html",
      filePath: outPath,
    };
  } catch (error) {
    pushActivity(`FAILED RUN AUDIT FOR RUN ${String(runNum)}`, {
      type: "run-audit-build-error",
      runNum,
      error: String(error?.message || error),
    });
    if (fs.existsSync(cachedOutPath)) {
      pushActivity(`SERVING LAST GOOD RUN AUDIT FOR RUN ${String(runNum)} AFTER REFRESH BUILD FAILURE`, {
        type: "run-audit-cache-serve-after-build-error",
        runNum,
        refresh: refresh ? "1" : "0",
        outPath: cachedOutPath,
        error: String(error?.message || error),
      });
      return {
        statusCode: 200,
        contentType: wantJson ? "application/json" : "text/html",
        filePath: cachedOutPath,
      };
    }
    return {
      statusCode: 500,
      contentType: "application/json",
      body: JSON.stringify({ ok: false, error: String(error?.message || error), worker: WORKER_LABEL, runNum }, null, 2),
    };
  }
}

function workerHealth() {
  const processCpuPct = sampleProcessCpuPct();
  const mem = process.memoryUsage();
  const rssMb = Number(mem?.rss) / (1024 * 1024);
  const heapUsedMb = Number(mem?.heapUsed) / (1024 * 1024);
  return {
    ok: true,
    worker: WORKER_LABEL,
    workerLabel: WORKER_LABEL,
    port: PORT,
    upstreamOrigin: UPSTREAM_ORIGIN || null,
    cacheRoot: CACHE_ROOT,
    cacheEntries: cacheMem.size,
    processStats: {
      cpuPct: Number.isFinite(processCpuPct) ? Number(processCpuPct.toFixed(1)) : null,
      rssMb: Number.isFinite(rssMb) ? Number(rssMb.toFixed(1)) : null,
      heapUsedMb: Number.isFinite(heapUsedMb) ? Number(heapUsedMb.toFixed(1)) : null,
      loadAvg1m: Number.isFinite(Number(os.loadavg?.()[0])) ? Number(Number(os.loadavg()[0]).toFixed(2)) : null,
    },
    parityEnabled: PARITY_ENABLED,
    parityOutDir: PARITY_ENABLED ? PARITY_OUT_DIR : null,
    runAuditEnabled: RUN_AUDIT_ENABLED,
    runAuditExpectedCodeVersion: RUN_AUDIT_EXPECTED_CODE_VERSION,
    runAuditBackgroundWarmEnabled: RUN_AUDIT_BACKGROUND_WARM_ENABLED,
    runAuditBackgroundWarmIntervalMs: RUN_AUDIT_BACKGROUND_WARM_INTERVAL_MS,
    runAuditRemoteHost: RUN_AUDIT_ENABLED ? RUN_AUDIT_REMOTE_HOST : null,
    routing: {
      allowHotQuery: ALLOW_HOT_QUERY,
    },
    activityCount: activityLog.length,
    activityStreamPath: "/api/activity/stream",
    activityPage: "/activity",
    t: Date.now(),
  };
}

async function warmActiveRunAuditsInBackground() {
  if (!RUN_AUDIT_ENABLED || !RUN_AUDIT_BACKGROUND_WARM_ENABLED) return;
  if (runAuditBackgroundWarmInflight) return;
  runAuditBackgroundWarmInflight = true;
  try {
    const bots = await listActivePaperBotsForRunAuditWarm();
    const activeKeys = new Set();
    for (const bot of bots) {
      const key = `${String(bot.instanceId)}:${String(bot.runId)}:${String(bot.runNum)}`;
      activeKeys.add(key);
      const previous = runAuditBackgroundWarmState.get(key) || null;
      const slugChanged = String(previous?.marketSlug || "") !== String(bot.marketSlug || "");
      const firstSeen = !previous;
      const staleCheckDue = !previous || (Date.now() - Number(previous.lastAttemptAtMs || 0)) >= RUN_AUDIT_BACKGROUND_WARM_INTERVAL_MS;
      if (!(firstSeen || slugChanged || staleCheckDue)) continue;
      runAuditBackgroundWarmState.set(key, {
        ...previous,
        marketSlug: String(bot.marketSlug || ""),
        lastAttemptAtMs: Date.now(),
        runNum: Number(bot.runNum || 0),
        runId: String(bot.runId || ""),
        instanceId: String(bot.instanceId || ""),
      });
      pushActivity(
        `${firstSeen ? "WARMING" : (slugChanged ? "UPDATING" : "RECHECKING")} RUN AUDIT IN BACKGROUND FOR RUN ${String(bot.runNum)}${slugChanged ? ` AFTER ROLLOVER TO ${String(bot.marketSlug || "").toUpperCase()}` : ""}`,
        {
          type: "run-audit-background-warm",
          runNum: String(bot.runNum),
          runId: String(bot.runId),
          instanceId: String(bot.instanceId),
          marketSlug: String(bot.marketSlug),
          trigger: firstSeen ? "first_seen" : (slugChanged ? "session_rollover" : "periodic_recheck"),
        },
      );
      const result = await ensureRunAuditArtifact(buildRunAuditWarmUrl(bot));
      runAuditBackgroundWarmState.set(key, {
        ...runAuditBackgroundWarmState.get(key),
        marketSlug: String(bot.marketSlug || ""),
        lastAttemptAtMs: Date.now(),
        lastStatusCode: Number(result?.statusCode || 0),
        lastOkAtMs: Number(result?.statusCode || 0) < 400 ? Date.now() : Number(previous?.lastOkAtMs || 0),
      });
    }
    for (const key of Array.from(runAuditBackgroundWarmState.keys())) {
      if (!activeKeys.has(key)) runAuditBackgroundWarmState.delete(key);
    }
  } catch (error) {
    pushActivity("FAILED BACKGROUND RUN AUDIT WARM", {
      type: "run-audit-background-warm-error",
      error: String(error?.message || error),
    });
  } finally {
    runAuditBackgroundWarmInflight = false;
  }
}

const server = http.createServer(async (req, res) => {
  try {
    if (!req.url) return sendJson(res, 400, { ok: false, error: "missing url" });
    const reqUrl = new URL(req.url, `http://${req.headers.host || "127.0.0.1"}`);
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,OPTIONS",
        "access-control-allow-headers": "content-type",
      });
      return res.end();
    }
    if (req.method !== "GET") return sendJson(res, 405, { ok: false, error: "method not allowed" });
    if (reqUrl.pathname === "/" || reqUrl.pathname === "/activity") {
      return sendText(res, 200, renderActivityDashboard(), "text/html");
    }
    if (reqUrl.pathname === "/api/activity") return sendJson(res, 200, activitySnapshot());
    if (reqUrl.pathname === "/api/activity/stream") {
      res.writeHead(200, {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-store, no-transform",
        "connection": "keep-alive",
        "access-control-allow-origin": "*",
      });
      res.write(`data: ${JSON.stringify({ ok: true, connected: true, worker: WORKER_LABEL, t: Date.now() })}\n\n`);
      for (const item of activityLog) res.write(`data: ${JSON.stringify(item)}\n\n`);
      activityClients.add(res);
      req.on("close", () => activityClients.delete(res));
      return;
    }
    if (reqUrl.pathname === "/api/health") return sendJson(res, 200, workerHealth());
    if (
      reqUrl.pathname === "/api/session-history" &&
      String(reqUrl.searchParams.get("engine") || "paper").trim().toLowerCase() === "paper" &&
      String(reqUrl.searchParams.get("instanceId") || "").trim()
    ) {
      const proxied = await localJsonWithCache(reqUrl, buildCompactSessionHistoryPayload, "local:session_history_run_index");
      const body = Buffer.from(String(proxied.bodyBase64 || ""), "base64");
      return sendText(res, Number(proxied.statusCode || 200), body.toString("utf8"), "application/json", {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": proxied.cacheStatus || "pass",
        "x-mmx-upstream": String(proxied.upstreamUrl || ""),
      });
    }
    if (reqUrl.pathname === "/api/session-card-image") {
      const image = await ensureSessionCardImageResponse(reqUrl);
      return sendBinary(res, Number(image.statusCode || 200), image.body, image.contentType, {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": image.cacheStatus || "pass",
        "x-mmx-upstream": String(image.upstreamUrl || ""),
      });
    }
    if (/^\/api\/v2\/bots\/[^/]+\/latest-session-card-image\b/i.test(reqUrl.pathname)) {
      const image = await ensureLatestSessionCardImageResponse(reqUrl);
      return sendBinary(res, Number(image.statusCode || 200), image.body, image.contentType, {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": image.cacheStatus || "pass",
        "x-mmx-upstream": String(image.upstreamUrl || ""),
      });
    }
    if (reqUrl.pathname === "/api/run-audits/review") {
      const focusKeys = reqUrl.searchParams.getAll("focus").map((v) => String(v || "").trim()).filter(Boolean);
      const injectFocusBlock = (html) => {
        const questions = [];
        if (focusKeys.includes("run_audit_default_selected_gold_matches_actual")) {
          questions.push("On first load, do the default selected gold intervals keep Modeled equal to Actual?");
        }
        if (focusKeys.includes("run_audit_gold25_equivalence")) {
          questions.push("If all base bets are $25 and all gold bet inputs are $25, does no-gold selection produce the same Modeled P/L as all default gold intervals selected?");
        }
        if (!html || !questions.length) return html;
        const block = `
<section style="margin:16px 0;padding:14px 16px;border:1px solid rgba(232,185,49,.35);border-radius:12px;background:#15120a;color:#f3e6b0;">
  <div style="font-weight:700;margin-bottom:8px;">Diagnostic Agent Focus</div>
  <ul style="margin:0;padding-left:18px;">
    ${questions.map((q) => `<li style="margin:4px 0;">${escapeHtmlLite(q)}</li>`).join("")}
  </ul>
</section>`;
        if (/<body[^>]*>/i.test(html)) return html.replace(/<body[^>]*>/i, (m) => `${m}${block}`);
        return `${block}${html}`;
      };
      const local = await ensureRunAuditArtifact(reqUrl);
      if (local.filePath && Number(local.statusCode || 200) < 400) {
        if (focusKeys.length) {
          try {
            const html = fs.readFileSync(local.filePath, "utf8");
            return sendText(res, Number(local.statusCode || 200), injectFocusBlock(html), local.contentType || "text/html; charset=utf-8", {
              "x-mmx-worker": WORKER_LABEL,
              "x-mmx-cache": shouldBypassCache(reqUrl) ? "refresh" : "local",
              "x-mmx-upstream": "local:run_audit_worker",
            });
          } catch {}
        }
        res.writeHead(Number(local.statusCode || 200), {
          "Content-Type": local.contentType || "application/json",
          "Cache-Control": "no-store, max-age=0",
          "Access-Control-Allow-Origin": "*",
          "x-mmx-worker": WORKER_LABEL,
          "x-mmx-cache": shouldBypassCache(reqUrl) ? "refresh" : "local",
          "x-mmx-upstream": "local:run_audit_worker",
        });
        fs.createReadStream(local.filePath).pipe(res);
        return;
      }
      return sendText(res, Number(local.statusCode || 200), injectFocusBlock(String(local.body || "")), local.contentType || "application/json", {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": shouldBypassCache(reqUrl) ? "refresh" : "local",
        "x-mmx-upstream": "local:run_audit_worker",
      });
    }
    if (reqUrl.pathname === "/api/v2/markets/volume-5m-24h") {
      const proxied = await localJsonWithCache(reqUrl, buildVolumeSeriesPayload, "local:polymarket_gamma");
      const body = Buffer.from(String(proxied.bodyBase64 || ""), "base64");
      return sendText(res, Number(proxied.statusCode || 200), body.toString("utf8"), "application/json", {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": proxied.cacheStatus || "pass",
        "x-mmx-upstream": String(proxied.upstreamUrl || ""),
      });
    }
    if (String(reqUrl.pathname || "").toLowerCase().includes("/latest-session-card")) {
      const proxied = await localJsonWithCache(reqUrl, buildLatestSessionCardPayload, "local:latest_session_card");
      const body = Buffer.from(String(proxied.bodyBase64 || ""), "base64");
      return sendText(res, Number(proxied.statusCode || 200), body.toString("utf8"), "application/json", {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": proxied.cacheStatus || "pass",
        "x-mmx-upstream": String(proxied.upstreamUrl || ""),
      });
    }
    if (String(reqUrl.pathname || "").toLowerCase().includes("/last-session-history")) {
      const wantsHtml = String(reqUrl.searchParams.get("format") || "").trim().toLowerCase() === "html";
      const limitRaw = Number(reqUrl.searchParams.get("limit") || 100);
      const requestedLimit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(5000, Math.floor(limitRaw))) : 100;
      if (wantsHtml && !shouldBypassCache(reqUrl)) {
        const parts = String(reqUrl.pathname || "").split("/").filter(Boolean);
        const instanceId = (
          parts.length >= 5 &&
          String(parts[0] || "").toLowerCase() === "api" &&
          String(parts[1] || "").toLowerCase() === "v2" &&
          String(parts[2] || "").toLowerCase() === "bots" &&
          String(parts[4] || "").toLowerCase() === "last-session-history"
        )
          ? decodeURIComponent(String(parts[3] || "").trim())
          : "";
        const marketPrefix = String(reqUrl.searchParams.get("marketPrefix") || "").trim().toLowerCase();
        const cachedPayload = readLastSessionHistoryCache(instanceId, marketPrefix);
        if (cachedPayload && Array.isArray(cachedPayload?.sessions) && cachedPayload.sessions.length) {
          return sendText(res, 200, renderLastSessionHistoryHtml(limitLastSessionHistoryPayload(cachedPayload, requestedLimit)), "text/html", {
            "x-mmx-worker": WORKER_LABEL,
            "x-mmx-cache": "local",
            "x-mmx-upstream": "local:last_session_history_cache",
          });
        }
      }
      const proxied = await localJsonWithCache(reqUrl, buildLastSessionHistoryPayload, "local:last_session_history");
      const body = Buffer.from(String(proxied.bodyBase64 || ""), "base64").toString("utf8");
      if (wantsHtml) {
        let parsed = null;
        try { parsed = JSON.parse(body); } catch {}
        return sendText(res, Number(proxied.statusCode || 200), renderLastSessionHistoryHtml(limitLastSessionHistoryPayload(parsed, requestedLimit)), "text/html", {
          "x-mmx-worker": WORKER_LABEL,
          "x-mmx-cache": proxied.cacheStatus || "pass",
          "x-mmx-upstream": String(proxied.upstreamUrl || ""),
        });
      }
      return sendText(res, Number(proxied.statusCode || 200), body, "application/json", {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": proxied.cacheStatus || "pass",
        "x-mmx-upstream": String(proxied.upstreamUrl || ""),
      });
    }
    if (String(reqUrl.pathname || "").toLowerCase().includes("/session-artifacts-summary")) {
      const proxied = await localJsonWithCache(reqUrl, buildSessionArtifactsSummaryPayload, "local:session_artifacts_summary");
      const body = Buffer.from(String(proxied.bodyBase64 || ""), "base64");
      return sendText(res, Number(proxied.statusCode || 200), body.toString("utf8"), "application/json", {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-cache": proxied.cacheStatus || "pass",
        "x-mmx-upstream": String(proxied.upstreamUrl || ""),
      });
    }
    if (reqUrl.pathname === "/api/session-parity/latest") {
      await ensureParityArtifact(shouldBypassCache(reqUrl));
      const wantJson = String(reqUrl.searchParams.get("format") || "").trim().toLowerCase() === "json";
      const outPath = path.join(PARITY_OUT_DIR, wantJson ? "latest.json" : "latest.html");
      if (!fs.existsSync(outPath)) return sendJson(res, 404, { ok: false, error: "parity artifact missing" });
      const contentType = wantJson ? "application/json" : "text/html";
      return sendText(res, 200, fs.readFileSync(outPath, "utf8"), contentType, {
        "x-mmx-worker": WORKER_LABEL,
        "x-mmx-parity-local": "1",
      });
    }
    const proxied = await proxyWithCache(reqUrl);
    const body = Buffer.from(String(proxied.bodyBase64 || ""), "base64");
    return sendText(res, Number(proxied.statusCode || 200), body.toString("utf8"), proxied.contentType || "application/json", {
      "x-mmx-worker": WORKER_LABEL,
      "x-mmx-cache": proxied.cacheStatus || "pass",
      "x-mmx-upstream": String(proxied.upstreamUrl || ""),
    });
  } catch (error) {
    return sendJson(res, 500, { ok: false, error: String(error?.message || error), worker: WORKER_LABEL });
  }
});

server.listen(PORT, HOST, () => {
  const addr = server.address();
  const actualPort = addr && typeof addr === "object" ? addr.port : PORT;
  pushActivity(`NON-ESSENTIAL WORKER LISTENING ON http://127.0.0.1:${actualPort}/activity`, {
    type: "worker-listening",
    host: HOST,
    port: actualPort,
    upstream: UPSTREAM_ORIGIN || "-",
    parity: PARITY_ENABLED ? "on" : "off",
  });
  if (RUN_AUDIT_ENABLED && RUN_AUDIT_BACKGROUND_WARM_ENABLED) {
    setTimeout(() => {
      void warmActiveRunAuditsInBackground();
    }, 2000);
    setInterval(() => {
      void warmActiveRunAuditsInBackground();
    }, RUN_AUDIT_BACKGROUND_WARM_INTERVAL_MS);
  }
});
