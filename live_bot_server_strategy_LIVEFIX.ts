// src/live_bot_server_strategy.ts
//
// SECOND SERVER (run alongside src/live_bot_server.ts)
// Same codebase, but adds **external strategy loading** (Node + browser-style fallback)
// and uses that strategy to drive enter/exit decisions per engine.
//
// - DOES NOT change your websocket schema / endpoints
// - DOES NOT change your price feed (still bid-only)
// - Strategy runs per-engine (paper + live) with params derived from uiPaper/uiLive
//
// IMPORTANT:
// - Default PORT is 8788 so you can run both servers at once.
// - Set STRATEGY_PATH to point at your strategy file.
//   Example:
//     STRATEGY_PATH=TRADE_STRATEGY_v1.0.js PORT=8788 npm run dev
//
// Strategy file supported shapes:
//  A) Node module: module.exports.makeStrategy = (params) => ({ onTick, snapshot })
//  B) Browser-style: window.TradeStrategy.makeStrategy = (params) => ({ onTick, snapshot })
//     (your uploaded TRADE_STRATEGY_v1.0.js is likely this form)

import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
import fs from "fs";
import path from "path";
import vm from "vm";
import { DatabaseSync } from "node:sqlite";
import {
  archiveRunFolder,
  bootstrapParaFolders,
  getParaTree,
  moveFileToParaBucket,
  syncRunArtifactsToPara,
  type ParaBucket,
} from "./drive_para";
import { ClobClient, Side as ClobSide, OrderType } from "@polymarket/clob-client";

import { Wallet } from "ethers";
import * as dotenv from "dotenv";
// Load env from both current working directory and project root so
// running from either `/polymarket-bot` or `/polymarket-bot/src` works.
dotenv.config({ path: path.resolve(__dirname, "../.env") });
dotenv.config({ path: path.resolve(process.cwd(), ".env") });

// ===== LOGGING =====
const SELL_ONLY_LOGS = process.env.SELL_ONLY_LOGS === "1";
const LOG_HUMAN = process.env.LOG_HUMAN !== "0";
const LOG_HEARTBEAT_EVERY = Math.max(1, Number(process.env.LOG_HEARTBEAT_EVERY || 5));
const LOG_STRAT_OUT_EVERY = Math.max(1, Number(process.env.LOG_STRAT_OUT_EVERY || 3));

const _rawConsole = {
  log: console.log.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
};
const _suppressedByKey = new Map<string, number>();
const _seenByKey = new Map<string, number>();

function _argToText(a: any): string {
  if (a instanceof Error) return String(a.stack || a.message || a);
  if (typeof a === "string") return a;
  if (typeof a === "number" || typeof a === "boolean" || a == null) return String(a);
  try {
    return JSON.stringify(a);
  } catch {
    return String(a);
  }
}

function _sellOnlyAllowed(msg: string): boolean {
  const m = msg.toUpperCase();
  return m.includes("SELL") || m.includes("EXIT") || m.includes("STOP");
}

function _normalizeTag(rawTag: string): string {
  const t = rawTag.trim();
  if (t.startsWith("HEARTBEAT")) return "HEARTBEAT";
  if (t.startsWith("STRAT OUT")) return "STRAT OUT";
  return t;
}

function _extractTag(msg: string): { tag: string; rest: string } {
  const trimmed = msg.trim();
  if (trimmed.startsWith("[")) {
    const i = trimmed.indexOf("]");
    if (i > 1) {
      const rawTag = trimmed.slice(1, i);
      return { tag: _normalizeTag(rawTag), rest: trimmed.slice(i + 1).trim() };
    }
  }
  return { tag: "LOG", rest: trimmed };
}

function _throttleEveryForTag(tag: string): number {
  if (tag === "HEARTBEAT") return LOG_HEARTBEAT_EVERY;
  if (tag === "STRAT OUT") return LOG_STRAT_OUT_EVERY;
  return 1;
}

function _formatLine(level: "log" | "warn" | "error", msg: string): string {
  if (!LOG_HUMAN) return msg;
  const { tag, rest } = _extractTag(msg);
  const ts = new Date().toISOString().slice(11, 19);
  const lv = level === "error" ? "ERR" : level === "warn" ? "WRN" : "INF";
  return `${ts} ${lv} ${tag.padEnd(12)} | ${rest}`;
}

function _emitLog(level: "log" | "warn" | "error", args: any[]) {
  const rawMsg = args.map(_argToText).join(" ");
  if (SELL_ONLY_LOGS && !_sellOnlyAllowed(rawMsg)) return;

  const { tag } = _extractTag(rawMsg);
  const every = _throttleEveryForTag(tag);
  const key = `${level}:${tag}`;
  const seen = (_seenByKey.get(key) || 0) + 1;
  _seenByKey.set(key, seen);
  if (every > 1 && seen % every !== 1) {
    _suppressedByKey.set(key, (_suppressedByKey.get(key) || 0) + 1);
    return;
  }

  const suppressed = _suppressedByKey.get(key) || 0;
  if (suppressed > 0) {
    _suppressedByKey.set(key, 0);
    _rawConsole[level](_formatLine(level, `[${tag}] suppressed ${suppressed} similar lines`));
  }
  _rawConsole[level](_formatLine(level, rawMsg));
}

console.log = (...args: any[]) => _emitLog("log", args);
console.warn = (...args: any[]) => _emitLog("warn", args);
console.error = (...args: any[]) => _emitLog("error", args);

function logInfo(msg: string) {
  console.log(msg);
}

function logWarn(msg: string) {
  console.warn(msg);
}

function logError(msg: string) {
  console.error(msg);
}

function fmt2(v: any): string {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(2) : "—";
}


// eslint-disable-next-line @typescript-eslint/no-var-requires
const TradeStrategy = require("./strategy_external.js"); // adjust path
console.log("Strategy version:", TradeStrategy.VERSION);

type OutcomeSide = "UP" | "DOWN";
type Mode = "paper" | "live";
type Engine = "paper" | "live";
type StrategyLane = "BASE" | "NO_CROSS_WINNER";
type StrategyId =
  | "momentum_hc"
  | "confidence_b_phase1"
  | "confidence_c_pmodel"
  | "profit_locker"
  | "kalman"
  | "profit_locker_inflection";
type StrategyFamily = "momentum" | "locker";
type StrategyProfile = {
  family: StrategyFamily;
  resetRuntimeOnSessionRolloverExit: boolean;
  resetRuntimeOnFlatExitSignal: boolean;
  requiresTpRearm: boolean;
  resetRuntimeOnTradeExit: boolean;
};
type BotStatus = "starting" | "running" | "watching" | "stopped" | "error";
const PAPER_LANES: StrategyLane[] = ["BASE", "NO_CROSS_WINNER"];

const STRATEGY_PROFILES: Record<StrategyId, StrategyProfile> = {
  momentum_hc: {
    family: "momentum",
    resetRuntimeOnSessionRolloverExit: true,
    resetRuntimeOnFlatExitSignal: true,
    requiresTpRearm: true,
    resetRuntimeOnTradeExit: true,
  },
  confidence_b_phase1: {
    family: "momentum",
    resetRuntimeOnSessionRolloverExit: true,
    resetRuntimeOnFlatExitSignal: true,
    requiresTpRearm: true,
    resetRuntimeOnTradeExit: true,
  },
  confidence_c_pmodel: {
    family: "momentum",
    resetRuntimeOnSessionRolloverExit: true,
    resetRuntimeOnFlatExitSignal: true,
    requiresTpRearm: true,
    resetRuntimeOnTradeExit: true,
  },
  profit_locker: {
    family: "locker",
    resetRuntimeOnSessionRolloverExit: false,
    resetRuntimeOnFlatExitSignal: false,
    requiresTpRearm: false,
    resetRuntimeOnTradeExit: false,
  },
  kalman: {
    family: "locker",
    resetRuntimeOnSessionRolloverExit: false,
    resetRuntimeOnFlatExitSignal: false,
    requiresTpRearm: false,
    resetRuntimeOnTradeExit: false,
  },
  profit_locker_inflection: {
    family: "locker",
    resetRuntimeOnSessionRolloverExit: false,
    resetRuntimeOnFlatExitSignal: false,
    requiresTpRearm: false,
    resetRuntimeOnTradeExit: false,
  },
};

function normalizeStrategyId(raw: any): StrategyId {
  const v = String(raw || "momentum_hc").trim().toLowerCase();
  if (v === "profit_locker") return "profit_locker";
  if (v === "kalman") return "kalman";
  if (v === "profit_locker_inflection") return "profit_locker_inflection";
  if (v === "confidence_b_phase1") return "confidence_b_phase1";
  if (v === "confidence_c_pmodel") return "confidence_c_pmodel";
  return "momentum_hc";
}

function strategyProfileOf(strategyIdRaw: any): StrategyProfile {
  return STRATEGY_PROFILES[normalizeStrategyId(strategyIdRaw)];
}

const PORT = Number(process.env.PORT || 8788);
const CONFIG_WRITE_KEY = String(process.env.CONFIG_WRITE_KEY || "").trim();
const GAMMA_BASE = process.env.GAMMA_BASE || "https://gamma-api.polymarket.com";
const CLOB_BASE = process.env.CLOB_BASE || "https://clob.polymarket.com";
const BASE_SLUG = "btc-updown-5m";
const BTC_INTERVAL_SLUGS = {
  "5m": [String(process.env.BTC_BASE_SLUG_5M || BASE_SLUG || "btc-updown-5m").trim()],
  "15m": [String(process.env.BTC_BASE_SLUG_15M || "btc-updown-15m").trim(), "btc-updown-15min"],
  "hourly": [String(process.env.BTC_BASE_SLUG_HOURLY || "btc-updown-hourly").trim(), "btc-updown-1h"],
  "daily": [String(process.env.BTC_BASE_SLUG_DAILY || "btc-updown-daily").trim(), "btc-updown-1d"],
} as const;
const MIN_LIVE_ORDER_SHARES = 5.3;
const MAX_SHARES_PER_TRADE = 6;
const PRODUCTION_PRESET = {
  name: "MOM_HC_055_096_SL045_V1",
  ui: {
    entry: 0.55,
    exit: 0.96,
    stop: 0.45,
    useStop: true,
    useStopTimeGate: false,
    stopStartSec: 60,
    minEntrySec: 0,
    kellyOn: true,
    kellyMult: 1.0,
    kellyCap: 0.25,
    betUsd: 25,
    maxBetUsd: 100,
  },
  execution: {
    liveEntryStyle: "limit", // limit | immediate
    liveBuyTtlMs: 12_000,
    entryOffsetPx: 0.03,
    immediateEntryTtlMs: 2_500,
  },
  fees: {
    entryBuy: "taker_effective_curve",
    takeProfitExit: "maker_zero",
    stopLossExit: "taker_effective_curve",
  },
  reporting: {
    drawdownOverall: "peak_to_trough_pct",
    drawdownDaily: "open_to_low_pct",
  },
} as const;
const IMMEDIATE_TP_SELL_PX_FALLBACK = Number(process.env.IMMEDIATE_TP_SELL_PX_FALLBACK || 0.83);
const BUY_FILL_POLL_MS = Math.max(25, Number(process.env.BUY_FILL_POLL_MS || 75));
const BUY_TRADE_CHECK_MS = Math.max(50, Number(process.env.BUY_TRADE_CHECK_MS || 100));
const LIVE_BUY_TTL_MS = Math.max(1000, Number(process.env.LIVE_BUY_TTL_MS || PRODUCTION_PRESET.execution.liveBuyTtlMs));
const LIVE_ENTRY_STYLE = String(process.env.LIVE_ENTRY_STYLE || PRODUCTION_PRESET.execution.liveEntryStyle).toLowerCase(); // "immediate" | "limit"
const LIVE_ENTRY_IMMEDIATE_TTL_MS = Math.max(
  500,
  Number(process.env.LIVE_ENTRY_IMMEDIATE_TTL_MS || PRODUCTION_PRESET.execution.immediateEntryTtlMs)
);
const LIVE_ENTRY_AGGRESSIVE_BUFFER = Math.max(
  0,
  Number(process.env.LIVE_ENTRY_AGGRESSIVE_BUFFER || PRODUCTION_PRESET.execution.entryOffsetPx)
);
const STOP_SKIP_LOG_INTERVAL_MS = Math.max(500, Number(process.env.STOP_SKIP_LOG_INTERVAL_MS || 2000));
const STOP_TIME_GATE_DEFAULT_SEC = Number(process.env.STOP_TIME_GATE_DEFAULT_SEC || 240);
const HC_SERVER_STOP_THR = clamp01(Number(process.env.HC_SERVER_STOP_THR || 0.5));
const HC_SERVER_STOP_CONFIRM_TICKS = Math.max(1, Math.floor(Number(process.env.HC_SERVER_STOP_CONFIRM_TICKS || 3)));
const HC_OPP_STOP_THR = clamp01(Number(process.env.HC_OPP_STOP_THR || HC_SERVER_STOP_THR || 0.45));
const HC_OPP_STOP_HARD_FAILSAFE_PX = clamp01(Number(process.env.HC_OPP_STOP_HARD_FAILSAFE_PX || 0.40));
const HC_OPP_STOP_RAW_CONFIRM_TICKS = Math.max(1, Math.floor(Number(process.env.HC_OPP_STOP_RAW_CONFIRM_TICKS || HC_SERVER_STOP_CONFIRM_TICKS || 3)));
const HC_OPP_STOP_KALMAN_CONFIRM_TICKS = Math.max(1, Math.floor(Number(process.env.HC_OPP_STOP_KALMAN_CONFIRM_TICKS || 3)));
const HC_OPP_STOP_EARLY_SEC = Math.max(0, Number(process.env.HC_OPP_STOP_EARLY_SEC || 180));
const HC_OPP_STOP_EARLY_EXTRA_TICKS = Math.max(0, Math.floor(Number(process.env.HC_OPP_STOP_EARLY_EXTRA_TICKS || 2)));
const HC_OPP_STOP_HYST_ARM_PX = clamp01(Number(process.env.HC_OPP_STOP_HYST_ARM_PX || HC_OPP_STOP_THR));
const HC_OPP_STOP_HYST_DISARM_PX = clamp01(Number(process.env.HC_OPP_STOP_HYST_DISARM_PX || 0.48));
const HC_OPP_STOP_KALMAN_Q = Math.max(0, Number(process.env.HC_OPP_STOP_KALMAN_Q || 0.00005));
const HC_OPP_STOP_KALMAN_R = Math.max(1e-12, Number(process.env.HC_OPP_STOP_KALMAN_R || 0.0008));
const HC_ENTRY_REPRICE_OFFSETS = String(process.env.HC_ENTRY_REPRICE_OFFSETS || "0.02,0.04")
  .split(",")
  .map((s) => clamp01(Number(s.trim())))
  .filter((n) => Number.isFinite(n) && n > 0)
  .sort((a, b) => a - b);
const HC_ENTRY_REPRICE_CAP_PX = clamp01(Number(process.env.HC_ENTRY_REPRICE_CAP_PX || 0.9));
const HC_SAME_SIDE_MIN_BID = clamp01(Number(process.env.HC_SAME_SIDE_MIN_BID || 0.84));
const HC_SAME_SIDE_PERSISTENCE_TICKS = Math.max(1, Math.floor(Number(process.env.HC_SAME_SIDE_PERSISTENCE_TICKS || 4)));
const HC_SAME_SIDE_MIN_SEC_AFTER_STOP = Math.max(0, Number(process.env.HC_SAME_SIDE_MIN_SEC_AFTER_STOP || 10));
const HC_BLOCK_LAST_SEC = Math.max(0, Number(process.env.HC_BLOCK_LAST_SEC || 2));
const RESUME_LATEST_RUN_ON_RESTART = ["1", "true", "yes", "on"].includes(
  String(process.env.RESUME_LATEST_RUN_ON_RESTART || "0").trim().toLowerCase()
);
const LIVEFIX_RUNTIME_VERSION = "LIVEFIX-2026-02-20-v3";
// Always start paper mode from a clean balance/state on process restart.
const PAPER_RESET_ON_START = true;
const SERVER_RUN_START_MS = Date.now();
const SERVER_RUN_START_ISO = new Date(SERVER_RUN_START_MS).toISOString();

// ===== Helpers =====
function floorTo5mEpoch(tsSec: number) {
  return tsSec - (tsSec % 300);
}
function nowMs() {
  return Date.now();
}
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function clamp(x: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, x));
}
function shouldRearmLiveEntryAfterError(errMsg: string): boolean {
  const m = String(errMsg || "").toLowerCase();
  return (
    m.includes("below exchange minimum") ||
    m.includes("buy limit failed") ||
    m.includes("not filled within ttlms") ||
    m.includes("invalid order payload") ||
    m.includes("did not return orderid") ||
    m.includes("invalid buy price") ||
    m.includes("invalid notionalusd") ||
    m.includes("invalid calculated size")
  );
}
function isoNow() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

async function getJson(url: string, params?: Record<string, any>) {
  const u = new URL(url);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
    }
  }
  const r = await fetch(u.toString(), { method: "GET" });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new Error(`GET ${u} -> ${r.status} ${r.statusText} ${text}`.slice(0, 600));
  }
  return r.json();
}

function clamp01(px: number) {
  return Math.max(0.01, Math.min(0.99, px));
}
function inferSideBid(
  side: OutcomeSide,
  upBA: { bid: number | null },
  dnBA: { bid: number | null }
): number | null {
  const up = Number(upBA?.bid);
  const dn = Number(dnBA?.bid);
  if (side === "UP") {
    if (Number.isFinite(up)) return up;
    if (Number.isFinite(dn)) return clamp01(1 - dn);
    return null;
  }
  if (Number.isFinite(dn)) return dn;
  if (Number.isFinite(up)) return clamp01(1 - up);
  return null;
}
function shouldTriggerStop(
  ui: UiConfig,
  side: OutcomeSide,
  px: number,
  elapsedSec: number,
  engine: Engine,
  source: "generic" | "strategy"
): boolean {
  if (!ui.useStop) return false;
  if (!Number.isFinite(px) || px > ui.stop) return false;
  if (!ui.useStopTimeGate) return true;
  if (elapsedSec >= ui.stopStartSec) return true;

  console.log(
    `[STOP BLOCKED by gate] engine=${engine} source=${source} side=${side} ` +
    `elapsed=${elapsedSec.toFixed(2)}s stopStartSec=${ui.stopStartSec} px=${px} stop=${ui.stop}`
  );
  return false;
}

function buyPxForSide(
  side: OutcomeSide,
  upBA: { bid: number | null; ask?: number | null },
  dnBA: { bid: number | null; ask?: number | null },
  limitPx: number
) {
  const ask = side === "UP" ? upBA.ask : dnBA.ask;
  const bid = side === "UP" ? upBA.bid : dnBA.bid;
  const limit = clamp01(limitPx);
  const px = Number.isFinite(ask as any) ? Math.min(Number(ask), limit) : limit;
  // Loud debug if we’re about to place a non-marketable order
  console.log(`[BUY PX] side=${side} bid=${bid} ask=${ask} chosen=${px} limit=${limit}`);
  return clamp01(px);
}

function configuredEntryLimitPx(ui: UiConfig): number {
  return clamp01(Number(ui.entry) + LIVE_ENTRY_AGGRESSIVE_BUFFER);
}

function clearPendingEntry(st: TradeState) {
  st.enterInFlight = false;
  st.pendingEntrySide = null;
  st.pendingEntrySignalPx = null;
  st.pendingEntryLimitPx = null;
  st.pendingEntryBaseLimitPx = null;
  st.pendingEntryRepriceCount = 0;
  st.pendingEntryMode = null;
  st.pendingEntrySubtype = null;
  st.pendingEntryPlacedAtMs = null;
  st.pendingEntryExpiresAtMs = null;
  st.pendingEntryNotionalUsd = null;
}

function clearPendingTp(st: TradeState) {
  st.tpOrderId = null;
  st.pendingTpLimitPx = null;
  st.pendingTpPlacedAtMs = null;
}

function isNewEntryAllowed(engine: Engine, st: TradeState): boolean {
  if (!entryBlockActive) return true;
  if (!current.slug || (entryBlockedSessionSlug && current.slug !== entryBlockedSessionSlug)) return true;

  const k = `__lastEntryBlockedLogMs_${engine}`;
  const t = nowMs();
  if (!(st as any)[k] || t - (st as any)[k] > 2000) {
    console.log(
      `[ENTRY BLOCKED] engine=${engine} reason=startup_mid_session_guard session=${current.slug} ` +
        `blockUntilNextSession=true`
    );
    (st as any)[k] = t;
  }
  // If startup guard blocks an entry, resync external strategy state once for
  // this blocked session so it doesn't stay "entered" internally.
  if (entryGuardResyncSessionSlug !== current.slug) {
    entryGuardResyncSessionSlug = current.slug;
    reloadEngineStrategy(engine, "startup guard blocked entry resync");
    console.log(`[ENTRY GUARD] strategy resync engine=${engine} session=${current.slug}`);
  }
  return false;
}

function immediateBuyPxForSide(
  side: OutcomeSide,
  upBA: { bid: number | null; ask?: number | null },
  dnBA: { bid: number | null; ask?: number | null },
  entryLimitPx: number
) {
  const askRaw = side === "UP" ? upBA.ask : dnBA.ask;
  const bidRaw = side === "UP" ? upBA.bid : dnBA.bid;
  const ask = Number.isFinite(Number(askRaw)) ? Number(askRaw) : null;
  const bid = Number.isFinite(Number(bidRaw)) ? Number(bidRaw) : null;
  const floorPx = clamp01(entryLimitPx);
  // Use ask when available and add a small buffer to stay marketable as book moves.
  const base = ask ?? bid ?? floorPx;
  const px = clamp01(Math.max(floorPx, base + LIVE_ENTRY_AGGRESSIVE_BUFFER));
  console.log(
    `[BUY PX IMMEDIATE] side=${side} bid=${bid} ask=${ask} base=${base} floor=${floorPx} ` +
      `buffer=${LIVE_ENTRY_AGGRESSIVE_BUFFER} chosen=${px}`
  );
  return px;
}

function computeImmediateTpSellPx(ui: UiConfig | null | undefined, entryFillPx?: number | null): number {
  const fallbackPx = clamp01(IMMEDIATE_TP_SELL_PX_FALLBACK);
  if (!ui) return fallbackPx;

  const uiEntry = Number(ui.entry);
  const uiExit = Number(ui.exit);
  if (!Number.isFinite(uiExit)) return fallbackPx;
  // User-requested behavior: TP is always the intended configured limit price.
  return clamp01(uiExit);
}


function parseJsonArrayMaybe(x: any): any[] | null {
  if (x == null) return null;
  if (Array.isArray(x)) return x;
  if (typeof x === "string") {
    const s = x.trim();
    if (s.startsWith("[") && s.endsWith("]")) {
      try {
        const v = JSON.parse(s);
        return Array.isArray(v) ? v : null;
      } catch {
        return null;
      }
    }
  }
  return null;
}

function extractUpDownTokensFromMarket(mkt: any): { up: string; down: string } {
  const outcomes = parseJsonArrayMaybe(mkt?.outcomes);
  const tokenIds = parseJsonArrayMaybe(mkt?.clobTokenIds ?? mkt?.clobTokenIDs);

  if (!outcomes || !tokenIds || outcomes.length !== tokenIds.length) {
    throw new Error("Market missing aligned outcomes + clobTokenIds.");
  }

  const mapping: Record<string, string> = {};
  for (let i = 0; i < outcomes.length; i++) {
    const oc = String(outcomes[i]).trim().toUpperCase();
    const tid = String(tokenIds[i]);
    if (oc === "UP" || oc === "DOWN") mapping[oc] = tid;
  }
  if (!mapping.UP || !mapping.DOWN) throw new Error("Could not locate UP/DOWN token ids.");
  return { up: mapping.UP, down: mapping.DOWN };
}

function bestBidAskFromBook(book: any): { bid: number | null; ask: number | null } {
  const bids: number[] = [];
  const asks: number[] = [];
  const b = book?.bids;
  const a = book?.asks;

  if (Array.isArray(b)) {
    for (const row of b) {
      const p = row?.price;
      const pv = p != null ? Number(p) : NaN;
      if (Number.isFinite(pv)) bids.push(pv);
    }
  }
  if (Array.isArray(a)) {
    for (const row of a) {
      const p = row?.price;
      const pv = p != null ? Number(p) : NaN;
      if (Number.isFinite(pv)) asks.push(pv);
    }
  }

  const bid = bids.length ? Math.max(...bids) : null;
  const ask = asks.length ? Math.min(...asks) : null;
  return { bid, ask };
}

async function getBook(tokenId: string) {
  return getJson(`${CLOB_BASE}/book`, { token_id: tokenId });
}

type FillEstimate = {
  wouldFill: boolean | null;
  scorePct: number | null;
  requiredShares: number | null;
  fillableShares: number | null;
  fillableNotionalUsd: number | null;
  estVwapPx: number | null;
};

function toFiniteNum(v: any): number | null {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function clampPct01(v: number): number {
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, v));
}

function normalizeFillEstimate(raw: any): FillEstimate | null {
  if (!raw || typeof raw !== "object") return null;
  const wouldFillRaw = raw.wouldFill;
  const wouldFill = typeof wouldFillRaw === "boolean" ? wouldFillRaw : null;
  return {
    wouldFill,
    scorePct: toFiniteNum(raw.scorePct),
    requiredShares: toFiniteNum(raw.requiredShares),
    fillableShares: toFiniteNum(raw.fillableShares),
    fillableNotionalUsd: toFiniteNum(raw.fillableNotionalUsd),
    estVwapPx: toFiniteNum(raw.estVwapPx),
  };
}

async function estimateBuyLimitFillability(tokenId: string, limitPx: number, requiredShares: number): Promise<FillEstimate> {
  const lim = toFiniteNum(limitPx);
  const req = toFiniteNum(requiredShares);
  if (!Number.isFinite(lim) || lim <= 0 || !Number.isFinite(req) || req <= 0) {
    return {
      wouldFill: null,
      scorePct: null,
      requiredShares: toFiniteNum(requiredShares),
      fillableShares: null,
      fillableNotionalUsd: null,
      estVwapPx: null,
    };
  }

  try {
    const book = await getBook(tokenId);
    const asks = Array.isArray(book?.asks) ? book.asks : [];
    const eligible = asks
      .map((row: any) => ({
        px: toFiniteNum(row?.price),
        sz: toFiniteNum(row?.size ?? row?.quantity ?? row?.amount),
      }))
      .filter((r: any) => Number.isFinite(r.px) && Number.isFinite(r.sz) && r.sz > 0 && r.px <= lim)
      .sort((a: any, b: any) => Number(a.px) - Number(b.px));

    let remaining = req;
    let filledShares = 0;
    let filledNotional = 0;
    for (const lvl of eligible) {
      if (remaining <= 0) break;
      const take = Math.min(remaining, Number(lvl.sz));
      filledShares += take;
      filledNotional += take * Number(lvl.px);
      remaining -= take;
    }

    const estVwap = filledShares > 0 ? filledNotional / filledShares : null;
    const coverage = clampPct01(filledShares / req);
    return {
      wouldFill: coverage >= 0.999,
      scorePct: Math.round(coverage * 1000) / 10,
      requiredShares: req,
      fillableShares: filledShares,
      fillableNotionalUsd: filledNotional,
      estVwapPx: estVwap,
    };
  } catch {
    return {
      wouldFill: null,
      scorePct: null,
      requiredShares: req,
      fillableShares: null,
      fillableNotionalUsd: null,
      estVwapPx: null,
    };
  }
}

async function estimateSellLimitFillability(tokenId: string, limitPx: number, requiredShares: number): Promise<FillEstimate> {
  const lim = toFiniteNum(limitPx);
  const req = toFiniteNum(requiredShares);
  if (!Number.isFinite(lim) || lim <= 0 || !Number.isFinite(req) || req <= 0) {
    return {
      wouldFill: null,
      scorePct: null,
      requiredShares: toFiniteNum(requiredShares),
      fillableShares: null,
      fillableNotionalUsd: null,
      estVwapPx: null,
    };
  }

  try {
    const book = await getBook(tokenId);
    const bids = Array.isArray(book?.bids) ? book.bids : [];
    const eligible = bids
      .map((row: any) => ({
        px: toFiniteNum(row?.price),
        sz: toFiniteNum(row?.size ?? row?.quantity ?? row?.amount),
      }))
      .filter((r: any) => Number.isFinite(r.px) && Number.isFinite(r.sz) && r.sz > 0 && r.px >= lim)
      .sort((a: any, b: any) => Number(b.px) - Number(a.px));

    let remaining = req;
    let filledShares = 0;
    let filledNotional = 0;
    for (const lvl of eligible) {
      if (remaining <= 0) break;
      const take = Math.min(remaining, Number(lvl.sz));
      filledShares += take;
      filledNotional += take * Number(lvl.px);
      remaining -= take;
    }

    const estVwap = filledShares > 0 ? filledNotional / filledShares : null;
    const coverage = clampPct01(filledShares / req);
    return {
      wouldFill: coverage >= 0.999,
      scorePct: Math.round(coverage * 1000) / 10,
      requiredShares: req,
      fillableShares: filledShares,
      fillableNotionalUsd: filledNotional,
      estVwapPx: estVwap,
    };
  } catch {
    return {
      wouldFill: null,
      scorePct: null,
      requiredShares: req,
      fillableShares: null,
      fillableNotionalUsd: null,
      estVwapPx: null,
    };
  }
}

async function getBestBidAsk(tokenId: string): Promise<{ bid: number | null; ask: number | null }> {
  const book = await getBook(tokenId);
  return bestBidAskFromBook(book);
}

function isTransientBookFetchError(err: any): boolean {
  const msg = String(err?.message ?? err ?? "").toLowerCase();
  return (
    msg.includes(" 502 ") ||
    msg.includes(" 503 ") ||
    msg.includes(" 504 ") ||
    msg.includes("bad gateway") ||
    msg.includes("gateway timeout") ||
    msg.includes("fetch failed") ||
    msg.includes("etimedout") ||
    msg.includes("econnreset")
  );
}

async function getBestBidAskSafe(tokenId: string, sideLabel: "UP" | "DOWN"): Promise<{ bid: number | null; ask: number | null }> {
  const maxAttempts = 2;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await getBestBidAsk(tokenId);
    } catch (e: any) {
      const transient = isTransientBookFetchError(e);
      if (attempt < maxAttempts && transient) {
        await sleep(120);
        continue;
      }
      const key = `book:${sideLabel}:${tokenId}`;
      const now = nowMs();
      const last = ((getBestBidAskSafe as any).__lastWarnMs?.get(key) ?? 0) as number;
      if (now - last > 2500) {
        if (!(getBestBidAskSafe as any).__lastWarnMs) (getBestBidAskSafe as any).__lastWarnMs = new Map<string, number>();
        (getBestBidAskSafe as any).__lastWarnMs.set(key, now);
        const tag = transient ? "BOOK FETCH TRANSIENT" : "BOOK FETCH ERROR";
        console.warn(`[${tag}] side=${sideLabel} token=${tokenId} err=${String(e?.message ?? e)}`);
        broadcast({ type: "status", t: nowMs(), status: `${tag.toLowerCase()}: side=${sideLabel} (continuing)` });
      }
      return { bid: null, ask: null };
    }
  }
  return { bid: null, ask: null };
}

function reconcileBinaryBidPair(
  upLike: { bid: number | null; ask: number | null } | null | undefined,
  downLike: { bid: number | null; ask: number | null } | null | undefined
): {
  up: { bid: number | null; ask: number | null };
  down: { bid: number | null; ask: number | null };
  synthetic: boolean;
} {
  const upBidRaw = Number(upLike?.bid);
  const dnBidRaw = Number(downLike?.bid);
  let upBid = Number.isFinite(upBidRaw) ? clamp01(upBidRaw) : null;
  let dnBid = Number.isFinite(dnBidRaw) ? clamp01(dnBidRaw) : null;
  let synthetic = false;
  if (upBid == null && dnBid != null) {
    upBid = clamp01(1 - dnBid);
    synthetic = true;
  } else if (dnBid == null && upBid != null) {
    dnBid = clamp01(1 - upBid);
    synthetic = true;
  }
  return {
    up: { bid: upBid, ask: Number.isFinite(Number(upLike?.ask)) ? Number(upLike!.ask) : null },
    down: { bid: dnBid, ask: Number.isFinite(Number(downLike?.ask)) ? Number(downLike!.ask) : null },
    synthetic,
  };
}

function parseIsoZ(s: string): Date {
  const t = s.endsWith("Z") ? s.slice(0, -1) + "+00:00" : s;
  return new Date(t);
}

// ===== LIVE CLOB CLIENT (unified & fixed) =====
let __clobClient: ClobClient | null = null;
let __lastLivePosSyncMs = 0;
const LIVE_POS_SYNC_MS = Math.max(100, Number(process.env.LIVE_POS_SYNC_MS || 750));

async function getClobClient(): Promise<ClobClient> {
  if (__clobClient) return __clobClient;

  const chainId = Number(process.env.POLY_CHAIN_ID || 137);
  const priv = process.env.POLY_PRIVATE_KEY;
  if (!priv) throw new Error("POLY_PRIVATE_KEY is required for live trading");

  const signer = new Wallet(priv);

  const signatureType = Number(process.env.POLY_SIGNATURE_TYPE ?? "2");
  const funder = process.env.POLY_FUNDER_ADDRESS;

  if (signatureType === 2 && !funder) {
    throw new Error("POLY_FUNDER_ADDRESS is required when POLY_SIGNATURE_TYPE=2 (Gnosis Safe / proxy wallet)");
  }

  console.log(`[CLOB INIT]`.padEnd(20) + `signatureType = ${signatureType}`);
  console.log(`[CLOB INIT]`.padEnd(20) + `signer EOA    = ${signer.address}`);
  console.log(`[CLOB INIT]`.padEnd(20) + `funder/proxy  = ${funder || signer.address}`);
  console.log(
    `[CLOB INIT]`.padEnd(20) +
      `using provided API creds: ${!!(process.env.POLY_API_KEY && process.env.POLY_SECRET && process.env.POLY_PASSPHRASE)}`
  );

  let apiCreds;
  if (process.env.POLY_API_KEY && process.env.POLY_SECRET && process.env.POLY_PASSPHRASE) {
    apiCreds = {
      key: process.env.POLY_API_KEY,
      secret: process.env.POLY_SECRET,
      passphrase: process.env.POLY_PASSPHRASE,
    };
  } else {
    console.log("[CLOB] No API creds in env → deriving fresh ones...");
    const tempClient = new ClobClient(CLOB_BASE, chainId, signer);
    apiCreds = await tempClient.createOrDeriveApiKey();
  }

  const client = new ClobClient(CLOB_BASE, chainId, signer, apiCreds, signatureType, funder || undefined);
  __clobClient = client;
  return client;
}

function signedTradeDeltaForMe(tr: any, myAddrs: Set<string>): number {
  const side = String(tr?.side || "").toUpperCase();
  const sz = Number(tr?.size ?? 0);
  if (!Number.isFinite(sz) || sz <= 0) return 0;

  const owner = String(tr?.owner || tr?.taker || tr?.taker_address || tr?.trader || "").toLowerCase();
  const maker = String(tr?.maker_address || tr?.maker || "").toLowerCase();
  const traderSide = String(tr?.trader_side || "").toUpperCase();

  const isMyTaker = myAddrs.has(owner) && traderSide !== "MAKER";
  const isMyMaker = myAddrs.has(maker) && traderSide === "MAKER";
  if (!traderSide) {
    // If trader_side is missing, treat owner/taker as taker by default.
    if (myAddrs.has(owner)) return side === "BUY" ? sz : side === "SELL" ? -sz : 0;
    if (myAddrs.has(maker)) return side === "BUY" ? -sz : side === "SELL" ? sz : 0;
  }
  if (!isMyTaker && !isMyMaker) return 0;

  if (isMyTaker) return side === "BUY" ? sz : side === "SELL" ? -sz : 0;
  return side === "BUY" ? -sz : side === "SELL" ? sz : 0;
}

function coerceTradesList(raw: any): any[] {
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.data)) return raw.data;
  if (Array.isArray(raw?.items)) return raw.items;
  if (Array.isArray(raw?.trades)) return raw.trades;
  return [];
}

function tradeTsMs(tr: any): number | null {
  const raw = Number(tr?.timestamp ?? tr?.created_at ?? tr?.createdAt ?? tr?.time ?? 0);
  if (!Number.isFinite(raw) || raw <= 0) return null;
  // Some endpoints return epoch seconds, others milliseconds.
  return raw < 1e12 ? raw * 1000 : raw;
}

async function reconcileLivePositionMaybe(force = false, reason = "periodic") {
  const now = Date.now();
  if (!force && now - __lastLivePosSyncMs < LIVE_POS_SYNC_MS) return;
  __lastLivePosSyncMs = now;
  // Never touch live exchange state while live mode is off.
  if (!uiLive.enabled) return;
  // If live mode is on but key is missing, skip quietly (no noisy error loop in paper runs).
  if (!process.env.POLY_PRIVATE_KEY) return;
  if (!current.upToken || !current.downToken) return;

  try {
    const client = await getClobClient();
    const signerAddr = String((client as any)?.signer?.address || "").toLowerCase();
    const funderAddr = String((client as any)?.funder || process.env.POLY_FUNDER_ADDRESS || "").toLowerCase();
    const myAddrs = new Set<string>([signerAddr, funderAddr].filter(Boolean));
    if (!myAddrs.size) return;

    // Prefer direct position endpoints when available.
    try {
      const positions =
        (await (client as any).getPositions?.()) ??
        (await (client as any).getOpenPositions?.()) ??
        null;
      const posList = Array.isArray(positions) ? positions : [];
      if (posList.length) {
        const findSize = (assetId: string) => {
          const p = posList.find(
            (x: any) =>
              String(x?.asset_id ?? x?.assetId ?? x?.token_id ?? x?.tokenId ?? "") === String(assetId)
          );
          if (!p) return 0;
          const v = Number(p?.size ?? p?.position ?? p?.shares ?? p?.quantity ?? p?.balance ?? 0);
          return Number.isFinite(v) ? Math.max(0, v) : 0;
        };
        const upPos = findSize(current.upToken);
        const dnPos = findSize(current.downToken);
        const epsPos = 1e-6;
        if (upPos > epsPos || dnPos > epsPos) {
          const side: OutcomeSide = upPos >= dnPos ? "UP" : "DOWN";
          const shares = side === "UP" ? upPos : dnPos;
          stLive.entered = true;
          stLive.exited = false;
          stLive.side = side;
          stLive.shares = shares;
          stLive.entryTsMs = stLive.entryTsMs ?? now;
          stLive.marketSlug = current.slug;
          stLive.marketStartMs = current.startMs;
          stLive.marketEndMs = current.endMs;
          stLive.positionTokenId = side === "UP" ? current.upToken : current.downToken;
          stLive.buyFilledThisSession = true;
          stLive.buyAttemptedThisSession = true;
          stLive.buyAttemptedSessionSlug = current.slug;
          stLive.buyFilledAtMs = stLive.buyFilledAtMs ?? nowMs();
          stLive.firstSellAttemptAtMs = stLive.firstSellAttemptAtMs ?? null;
          console.warn(
            `[LIVE POS SYNC] reason=${reason} detected open position from positions side=${side} shares=${roundTo6(shares)} slug=${current.slug}`
          );
          broadcast({
            type: "status",
            t: nowMs(),
            status: `LIVE POS SYNC (${reason}) detected side=${side} shares=${roundTo6(shares)} from positions`,
          });
          return;
        }
      }
    } catch (e: any) {
      console.warn(`[LIVE POS SYNC POSITIONS ERROR] ${String(e?.message ?? e)}`);
    }

    const [upTrades, dnTrades] = await Promise.all([
      (client as any).getTrades?.({ asset_id: current.upToken }, true),
      (client as any).getTrades?.({ asset_id: current.downToken }, true),
    ]);

    const upList = coerceTradesList(upTrades);
    const dnList = coerceTradesList(dnTrades);
    let upNet = 0;
    let dnNet = 0;

    for (const tr of upList) upNet += signedTradeDeltaForMe(tr, myAddrs);
    for (const tr of dnList) dnNet += signedTradeDeltaForMe(tr, myAddrs);

    const eps = 1e-6;
    const hasUp = upNet > eps;
    const hasDn = dnNet > eps;

    if (!stLive.entered && !stLive.exited && (hasUp || hasDn)) {
      const side: OutcomeSide = hasUp && (!hasDn || upNet >= dnNet) ? "UP" : "DOWN";
      const shares = side === "UP" ? upNet : dnNet;
      stLive.entered = true;
      stLive.exited = false;
      stLive.side = side;
      stLive.shares = shares;
      stLive.entryTsMs = stLive.entryTsMs ?? now;
      stLive.marketSlug = current.slug;
      stLive.marketStartMs = current.startMs;
      stLive.marketEndMs = current.endMs;
      stLive.positionTokenId = side === "UP" ? current.upToken : current.downToken;
      stLive.buyFilledThisSession = true;
      stLive.buyAttemptedThisSession = true;
      stLive.buyAttemptedSessionSlug = current.slug;
      stLive.buyFilledAtMs = stLive.buyFilledAtMs ?? nowMs();
      stLive.firstSellAttemptAtMs = stLive.firstSellAttemptAtMs ?? null;
      console.warn(
        `[LIVE POS SYNC] reason=${reason} detected open position from trades side=${side} shares=${roundTo6(shares)} slug=${current.slug}`
      );
      broadcast({
        type: "status",
        t: nowMs(),
        status: `LIVE POS SYNC (${reason}) detected side=${side} shares=${roundTo6(shares)} from exchange trades`,
      });
    }
  } catch (e: any) {
    console.warn(`[LIVE POS SYNC ERROR] ${String(e?.message ?? e)}`);
  }
}

// ===== Trade Logging =====
const TRADE_LOG_DIR = process.env.TRADE_LOG_DIR
  ? path.resolve(process.env.TRADE_LOG_DIR)
  : path.resolve(__dirname, "trade_logs");
const LOG_PREFIX = Object.freeze({
  RUNS_META: "01_",
  RUNTIME_SNAPSHOT: "02_",
  TRADES: "03_",
  EXECUTION: "04_",
  SESSION_TRACE: "05_",
  SUMMARY: "06_",
  DAILY_LATEST: "07_",
  DAILY_STAMPED: "08_",
  HEDGE_BACKTEST: "09_",
  RUN_PATH_SUMMARY: "10_",
  SESSION_VOLATILITY: "11_",
  MISSED_HC_SAME_SIDE: "12_",
});
const RUNTIME_SNAPSHOT_PATH = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.RUNTIME_SNAPSHOT}runtime_state_latest.json`);
const RUNS_META_PATH = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.RUNS_META}server_runs.json`);
const LEGACY_RUNTIME_SNAPSHOT_PATH = path.join(TRADE_LOG_DIR, "runtime_state_latest.json");
const LEGACY_RUNS_META_PATH = path.join(TRADE_LOG_DIR, "server_runs.json");
let SERVER_RUN_ID = 1;
let CURRENT_RUN_START_MS = SERVER_RUN_START_MS;
let CURRENT_RUN_START_ISO = SERVER_RUN_START_ISO;
let RESUMED_EXISTING_RUN = false;
const sessionTraceMem = new Map<string, { xMs: number[]; up: number[]; down: number[]; sourceFile: string }>();
const lastSessionTraceWriteMsBySlug = new Map<string, number>();
const PARA_SYNC_ENABLED = String(process.env.PARA_SYNC_ENABLED ?? "1") !== "0";
const PARA_SYNC_INTERVAL_MS = Math.max(15_000, Number(process.env.PARA_SYNC_INTERVAL_MS || 60_000));
const PARA_SYNC_AREA_FOLDER = String(process.env.GDRIVE_RUN_AREA_FOLDER || "A_PaperTrading").trim() || "A_PaperTrading";
const BACKGROUND_REPORTS_ENABLED = String(process.env.BACKGROUND_REPORTS_ENABLED || "0") === "1";
const SNAPSHOT_CACHE_TTL_MS = Math.max(250, Number(process.env.SNAPSHOT_CACHE_TTL_MS || 1500));
const RUNTIME_STATE_SNAPSHOT_INTERVAL_MS = Math.max(5_000, Number(process.env.RUNTIME_STATE_SNAPSHOT_INTERVAL_MS || 15_000));
const MULTI_STATE_PERSIST_INTERVAL_MS = Math.max(5_000, Number(process.env.MULTI_STATE_PERSIST_INTERVAL_MS || 20_000));
let paraLastSyncAtMs: number | null = null;
let paraLastSyncReason: string | null = null;
let paraLastSyncError: string | null = null;
let paraLastSyncSummary: any = null;
let paraSyncInFlight = false;

type BotInstance = {
  instanceId: string;
  runId: string;
  runNum: number;
  marketBucket: "5m" | "15m" | "hourly" | "daily" | null;
  marketId: string;
  marketSlug: string;
  marketTitle: string;
  strategyId: StrategyId;
  mode: Mode;
  watchOnly: boolean;
  status: BotStatus;
  launchedAtMs: number;
  stoppedAtMs: number | null;
  latestPnlUsd: number | null;
  latestBalanceUsd: number | null;
  lastError: string | null;
  entry: number;
  exit: number;
  stop: number;
  useStop: boolean;
  minEntrySec: number;
  stopStartSec: number;
  betUsd: number;
  maxBetUsd: number;
  startBalanceUsd: number;
  strategyPath: string;
};

const botInstances = new Map<string, BotInstance>();
type BotRuntime = {
  instanceId: string;
  marketBucket: "5m" | "15m" | "hourly" | "daily" | null;
  marketSlug: string;
  sessionSlug: string | null;
  sessionClosedTrades: number;
  sessionLosses: number;
  upToken: string | null;
  downToken: string | null;
  marketStartMs: number | null;
  marketEndMs: number | null;
  lastTickMs: number | null;
  upBid: number | null;
  downBid: number | null;
  ticks: number;
  errorCount: number;
  lastError: string | null;
  entered: boolean;
  side: OutcomeSide | null;
  entryPx: number | null;
  entryTsMs: number | null;
  shares: number | null;
  notionalUsd: number | null;
  realizedPnlUsd: number;
  balanceUsd: number;
  closedTrades: number;
  wins: number;
  losses: number;
  sessionCapReconciledSlug?: string | null;
  sessionLossReconciledSlug?: string | null;
  momentumTpRearmNeeded?: boolean;
  momentumTpRearmSide?: OutcomeSide | null;
  lastAction: string | null;
  lastExitType: "tp" | "stop" | "settle" | null;
};
const botRuntimes = new Map<string, BotRuntime>();
const BOT_RUNTIME_POLL_MS = Math.max(250, Number(process.env.BOT_RUNTIME_POLL_MS || 250));
const BOT_RUNTIME_CONCURRENCY = Math.max(1, Number(process.env.BOT_RUNTIME_CONCURRENCY || 4));
const BOT_MAX_MOMENTUM_TRADES_PER_SESSION = Math.max(1, Number(process.env.BOT_MAX_MOMENTUM_TRADES_PER_SESSION || 2));
const BOT_MAX_PROFIT_LOCKER_TRADES_PER_SESSION = Math.max(1, Number(process.env.BOT_MAX_PROFIT_LOCKER_TRADES_PER_SESSION || 4));
const BOT_MAX_PROFIT_LOCKER_SESSION_LOSSES = Math.max(1, Number(process.env.BOT_MAX_PROFIT_LOCKER_SESSION_LOSSES || 3));
let botRuntimeTickInFlight = false;
const BOT_MAX_INSTANCES = Math.max(1, Number(process.env.BOT_MAX_INSTANCES || 100));
const BOT_MAX_PER_STRATEGY = Math.max(1, Number(process.env.BOT_MAX_PER_STRATEGY || 60));
const BOT_GLOBAL_MAX_NOTIONAL_USD = Math.max(10, Number(process.env.BOT_GLOBAL_MAX_NOTIONAL_USD || 5000));
const BOT_MAX_BET_USD = Math.max(1, Number(process.env.BOT_MAX_BET_USD || 250));
const MULTI_STATE_PATH = path.join(TRADE_LOG_DIR, "20_multi_market_state.json");
const MULTI_STATE_DB_PATH = String(process.env.MULTI_STATE_DB_PATH || path.join(TRADE_LOG_DIR, "20_multi_market_state.sqlite")).trim();
const REMAINING_IMPL_STRATEGY_PATH = path.resolve(__dirname, "docs", "remaining_implementation_strategy.md");
let BOT_RUN_SEQ = 0;
let multiDb: DatabaseSync | null = null;
type BotStrategyRuntime = {
  instanceId: string;
  strategyPath: string;
  strategy: Strategy | null;
  params: any;
  loadedAtMs: number | null;
  lastSnapshot: any;
  lastError: string | null;
};
const botStrategyRuntimes = new Map<string, BotStrategyRuntime>();

function makeId(prefix: string): string {
  const t = Date.now().toString(36);
  const r = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${t}_${r}`;
}

function strategyCatalog() {
  return [
    {
      strategyId: "momentum_hc",
      name: "Momentum/HC",
      version: LIVEFIX_RUNTIME_VERSION,
      enabled: true,
      params: {
        entry: uiPaper.entry,
        exit: uiPaper.exit,
        stop: uiPaper.stop,
        highConfidenceSpread: uiPaper.highConfidenceSpread,
        highConfidenceLead: uiPaper.highConfidenceLead,
      },
    },
    {
      strategyId: "confidence_b_phase1",
      name: "Confidence Bet B (Phase 1)",
      version: LIVEFIX_RUNTIME_VERSION,
      enabled: true,
      params: {
        entry: uiPaper.entry,
        exit: uiPaper.exit,
        stop: uiPaper.stop,
        note: "Locked confidence strategy (phase-1 baseline)",
      },
    },
    {
      strategyId: "confidence_c_pmodel",
      name: "Confidence Bet C (pModel)",
      version: LIVEFIX_RUNTIME_VERSION,
      enabled: true,
      params: {
        entry: uiPaper.entry,
        exit: uiPaper.exit,
        stop: uiPaper.stop,
        note: "pModel-gated confidence strategy",
      },
    },
    {
      strategyId: "profit_locker",
      name: "Profit Locker",
      version: LIVEFIX_RUNTIME_VERSION,
      enabled: true,
      params: {
        useStop: uiPaper.useStop,
        stopStartSec: uiPaper.stopStartSec,
        minEntrySec: uiPaper.minEntrySec,
      },
    },
    {
      strategyId: "kalman",
      name: "Kalman",
      version: LIVEFIX_RUNTIME_VERSION,
      enabled: true,
      params: {
        entry: uiPaper.entry,
        exit: uiPaper.exit,
        stop: uiPaper.stop,
        minEntrySec: uiPaper.minEntrySec,
      },
    },
    {
      strategyId: "profit_locker_inflection",
      name: "Profit Locker Inflection",
      version: LIVEFIX_RUNTIME_VERSION,
      enabled: true,
      params: {
        entry: 0.61,
        tp: 0.97,
        stop: 0.48,
        note: "Kalman inflection + profit lock profile",
      },
    },
  ];
}

function isBotActive(status: BotStatus): boolean {
  return status === "starting" || status === "running" || status === "watching" || status === "error";
}

function ensureRuntime(instance: BotInstance): BotRuntime {
  const prev = botRuntimes.get(instance.instanceId);
  if (prev) return prev;
  const rt: BotRuntime = {
    instanceId: instance.instanceId,
    marketBucket: instance.marketBucket ?? null,
    marketSlug: instance.marketSlug,
    sessionSlug: instance.marketSlug || null,
    sessionClosedTrades: 0,
    sessionLosses: 0,
    sessionCapReconciledSlug: null,
    sessionLossReconciledSlug: null,
    momentumTpRearmNeeded: false,
    momentumTpRearmSide: null,
    upToken: null,
    downToken: null,
    marketStartMs: null,
    marketEndMs: null,
    lastTickMs: null,
    upBid: null,
    downBid: null,
    ticks: 0,
    errorCount: 0,
    lastError: null,
    entered: false,
    side: null,
    entryPx: null,
    entryTsMs: null,
    shares: null,
    notionalUsd: null,
    realizedPnlUsd: 0,
    balanceUsd: Number.isFinite(Number(instance.startBalanceUsd)) ? Number(instance.startBalanceUsd) : 100,
    closedTrades: 0,
    wins: 0,
    losses: 0,
    lastAction: null,
    lastExitType: null,
  };
  botRuntimes.set(instance.instanceId, rt);
  return rt;
}

function resolveBotStrategyPath(strategyId: StrategyId, overridePath?: string | null): string {
  // Multi-market strategy file routing is intentionally fixed:
  // - momentum_hc -> strategy_external.js
  // - confidence_b_phase1 -> strategy_external_confidence_bet_phase1_locked.js
  // - confidence_c_pmodel -> strategy_external_confidence_bet_pmodel.js
  // - profit_locker/kalman -> strategy_external_run38_kalman.js
  // - profit_locker_inflection -> strategy_external_profit_locker_inflection.js
  // Ignore per-launch overrides to keep behavior deterministic across bots.
  void overridePath;
  const fromCandidates = (cands: string[]): string | null => {
    for (const c of cands) {
      if (fs.existsSync(c)) return c;
    }
    return null;
  };
  const momentumPath = fromCandidates([
    path.resolve(process.cwd(), "strategy_external.js"),
    path.resolve(process.cwd(), "src", "strategy_external.js"),
  ]);
  const confidenceBPath = fromCandidates([
    path.resolve(process.cwd(), "strategy_external_confidence_bet_phase1_locked.js"),
    path.resolve(process.cwd(), "src", "strategy_external_confidence_bet_phase1_locked.js"),
  ]);
  const confidenceCPath = fromCandidates([
    path.resolve(process.cwd(), "strategy_external_confidence_bet_pmodel.js"),
    path.resolve(process.cwd(), "src", "strategy_external_confidence_bet_pmodel.js"),
  ]);
  const kalmanPath = fromCandidates([
    path.resolve(process.cwd(), "strategy_external_run38_kalman.js"),
    path.resolve(process.cwd(), "src", "strategy_external_run38_kalman.js"),
  ]);
  const inflectPath = fromCandidates([
    path.resolve(process.cwd(), "strategy_external_profit_locker_inflection.js"),
    path.resolve(process.cwd(), "src", "strategy_external_profit_locker_inflection.js"),
  ]);
  if (strategyId === "profit_locker_inflection") {
    if (inflectPath) return inflectPath;
    return path.resolve(process.cwd(), "src", "strategy_external_profit_locker_inflection.js");
  }
  if (strategyId === "profit_locker" || strategyId === "kalman") {
    if (kalmanPath) return kalmanPath;
    return path.resolve(process.cwd(), "src", "strategy_external_run38_kalman.js");
  }
  if (strategyId === "confidence_b_phase1") {
    if (confidenceBPath) return confidenceBPath;
    return path.resolve(process.cwd(), "src", "strategy_external_confidence_bet_phase1_locked.js");
  }
  if (strategyId === "confidence_c_pmodel") {
    if (confidenceCPath) return confidenceCPath;
    return path.resolve(process.cwd(), "src", "strategy_external_confidence_bet_pmodel.js");
  }
  if (strategyId === "momentum_hc") {
    if (momentumPath) return momentumPath;
    return path.resolve(process.cwd(), "src", "strategy_external.js");
  }
  return path.resolve(process.cwd(), "src", "strategy_external.js");
}

function buildBotStrategyParams(instance: BotInstance): any {
  const profile = strategyProfileOf(instance.strategyId);
  // Strategy-specific param maps prevent accidental cross-pollination.
  if (profile.family === "locker") {
    return {
      // Profit Locker strategy file consumes Kalman-style keys.
      entryBreakoutThr: instance.entry,
      crossReentryThr: instance.entry,
      // Optional tuning hooks kept explicit for this strategy family.
      hardTpKalman: instance.exit,
      // Sizing
      bet: instance.betUsd,
      maxBetUsd: instance.maxBetUsd,
      strategyId: instance.strategyId,
      mode: instance.mode,
    };
  }
  // Momentum/HC strategy file consumes the noCross + threshold keys.
  return {
    buyThr: instance.entry,
    sellThr: instance.exit,
    stopThr: instance.stop,
    useStop: instance.useStop,
    minEntrySec: instance.minEntrySec,
    stopStartSec: instance.stopStartSec,
    bet: instance.betUsd,
    maxBetUsd: instance.maxBetUsd,
    noCrossMinSpread: uiPaper.highConfidenceSpread,
    noCrossMinBid: uiPaper.highConfidenceLead,
    strategyId: instance.strategyId,
    mode: instance.mode,
  };
}

function ensureBotStrategyRuntime(instance: BotInstance): BotStrategyRuntime {
  const existing = botStrategyRuntimes.get(instance.instanceId);
  if (existing && existing.strategy && existing.strategyPath === instance.strategyPath) return existing;
  const params = buildBotStrategyParams(instance);
  const out: BotStrategyRuntime = {
    instanceId: instance.instanceId,
    strategyPath: instance.strategyPath,
    strategy: null,
    params,
    loadedAtMs: null,
    lastSnapshot: null,
    lastError: null,
  };
  try {
    const factory = loadStrategyFactoryFromFile(instance.strategyPath);
    const strategy = factory(params);
    if (!strategy || typeof strategy.onTick !== "function") throw new Error("makeStrategy() must return { onTick() }");
    out.strategy = strategy;
    out.loadedAtMs = nowMs();
    out.lastError = null;
  } catch (e: any) {
    out.strategy = null;
    out.loadedAtMs = nowMs();
    out.lastError = String(e?.message || e || "strategy load error");
  }
  botStrategyRuntimes.set(instance.instanceId, out);
  return out;
}

function activeBotInstances(): BotInstance[] {
  return Array.from(botInstances.values()).filter((b) => b.status !== "stopped");
}

function countByStrategy(strategyId: StrategyId): number {
  return activeBotInstances().filter((b) => b.strategyId === strategyId).length;
}

function currentGlobalOpenNotionalUsd(): number {
  let total = 0;
  for (const rt of botRuntimes.values()) {
    if (rt.entered && Number.isFinite(Number(rt.notionalUsd))) total += Number(rt.notionalUsd);
  }
  return total;
}

function botRunDir(runNum: number): string {
  return path.join(TRADE_LOG_DIR, "multi_runs", `run_${Math.floor(runNum)}`);
}

function botRunTelemetryPath(runNum: number): string {
  return path.join(botRunDir(runNum), "telemetry.jsonl");
}

function sanitizeFileSegment(v: any): string {
  return String(v ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "") || "unknown";
}

function botRunIndexPath(strategyId: string, runNum: number): string {
  const sid = sanitizeFileSegment(strategyId);
  const rn = Math.floor(Number(runNum));
  return path.join(botRunDir(rn), `index_${sid}_run_${rn}.json`);
}

function pairRunSessionsFromEvents(rows: any[]): any[] {
  const openBySlug = new Map<string, any[]>();
  const outBySlug = new Map<string, any>();
  const sorted = (Array.isArray(rows) ? rows.slice() : [])
    .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
  const pushOpen = (slug: string, row: any) => {
    const arr = openBySlug.get(slug) || [];
    arr.push(row);
    openBySlug.set(slug, arr);
  };
  const shiftOpen = (slug: string): any | null => {
    const arr = openBySlug.get(slug) || [];
    if (!arr.length) return null;
    const v = arr.shift() || null;
    openBySlug.set(slug, arr);
    return v;
  };
  for (const r of sorted) {
    const ev = String(r?.event || "").toLowerCase();
    const slug = String(r?.marketSlug || "").trim();
    if (!slug) continue;
    if (ev === "enter") {
      pushOpen(slug, r);
      continue;
    }
    if (ev !== "exit") continue;
    const en = shiftOpen(slug);
    const entryTsMs = Number(en?.t);
    const exitTsMs = Number(r?.t);
    const pnlUsd = Number(r?.pnlUsd);
    const lane = {
      entryTsMs: Number.isFinite(entryTsMs) ? entryTsMs : null,
      exitTsMs: Number.isFinite(exitTsMs) ? exitTsMs : null,
      side: String(r?.side || en?.side || "").toUpperCase() || null,
      entryPx: Number.isFinite(Number(en?.entryPx)) ? Number(en.entryPx) : null,
      exitPx: Number.isFinite(Number(r?.exitPx)) ? Number(r.exitPx) : null,
      shares: Number.isFinite(Number(en?.shares)) ? Number(en.shares) : null,
      notionalUsd: Number.isFinite(Number(en?.notionalUsd)) ? Number(en.notionalUsd) : null,
      pnlUsd: Number.isFinite(pnlUsd) ? pnlUsd : null,
      exitType: String(r?.exitType || "") || null,
      exitReasonRaw: String(r?.exitReasonRaw || "") || null,
      balanceUsd: Number.isFinite(Number(r?.balanceUsd)) ? Number(r.balanceUsd) : null,
    };
    const prev = outBySlug.get(slug);
    if (!prev) {
      outBySlug.set(slug, {
        slug,
        firstTsMs: Number.isFinite(entryTsMs) ? entryTsMs : Number.isFinite(exitTsMs) ? exitTsMs : null,
        lastTsMs: Number.isFinite(exitTsMs) ? exitTsMs : Number.isFinite(entryTsMs) ? entryTsMs : null,
        pnlUsd: Number.isFinite(pnlUsd) ? pnlUsd : 0,
        balanceUsd: Number.isFinite(Number(r?.balanceUsd)) ? Number(r.balanceUsd) : null,
        lanes: [lane],
        laneCount: 1,
      });
      continue;
    }
    prev.firstTsMs = Number.isFinite(entryTsMs)
      ? (!Number.isFinite(Number(prev.firstTsMs)) ? entryTsMs : Math.min(Number(prev.firstTsMs), entryTsMs))
      : prev.firstTsMs;
    prev.lastTsMs = Number.isFinite(exitTsMs)
      ? (!Number.isFinite(Number(prev.lastTsMs)) ? exitTsMs : Math.max(Number(prev.lastTsMs), exitTsMs))
      : prev.lastTsMs;
    prev.pnlUsd = Number(prev.pnlUsd || 0) + (Number.isFinite(pnlUsd) ? pnlUsd : 0);
    if (Number.isFinite(Number(r?.balanceUsd))) prev.balanceUsd = Number(r.balanceUsd);
    prev.lanes = Array.isArray(prev.lanes) ? prev.lanes : [];
    prev.lanes.push(lane);
    prev.laneCount = prev.lanes.length;
    outBySlug.set(slug, prev);
  }
  return Array.from(outBySlug.values()).sort((a, b) => Number(b?.lastTsMs || 0) - Number(a?.lastTsMs || 0));
}

function writeBotRunIndex(instance: BotInstance, rt: BotRuntime | null) {
  try {
    const runNum = Math.floor(Number(instance.runNum));
    if (!Number.isFinite(runNum) || runNum <= 0) return;
    const dir = botRunDir(runNum);
    fs.mkdirSync(dir, { recursive: true });
    const summary = readBotRunSummary(runNum);
    const events = readBotRunEventsCached(runNum);
    const sessions = pairRunSessionsFromEvents(events);
    const slugSet = new Set<string>();
    for (const s of sessions) slugSet.add(String(s?.slug || ""));
    if (instance.marketSlug) slugSet.add(String(instance.marketSlug));
    const traceSinceMs = Math.max(0, Number(instance.launchedAtMs || 0) - 60_000);
    const traceMap = readSessionTracesBySlug(traceSinceMs, null);
    const tracesBySlug: Record<string, any> = {};
    for (const slug of slugSet) {
      if (!slug) continue;
      const tr: any = traceMap.get(slug);
      if (!tr) continue;
      tracesBySlug[slug] = {
        xMs: Array.isArray(tr?.xMs) ? tr.xMs : [],
        up: Array.isArray(tr?.up) ? tr.up : [],
        down: Array.isArray(tr?.down) ? tr.down : [],
        source: String(tr?.sourceFile || "session_trace_runlog"),
      };
    }
    const srt = botStrategyRuntimes.get(instance.instanceId) || null;
    const payload = {
      schemaVersion: "run-index.v1",
      generatedAtMs: nowMs(),
      generatedAtIso: new Date().toISOString(),
      run: {
        runId: instance.runId,
        runNum: runNum,
        instanceId: instance.instanceId,
        strategyId: instance.strategyId,
        strategyPath: instance.strategyPath,
        marketSlug: instance.marketSlug,
        marketTitle: instance.marketTitle,
        mode: instance.mode,
        launchedAtMs: instance.launchedAtMs,
        stoppedAtMs: instance.stoppedAtMs,
      },
      summary: summary || null,
      latestRuntime: rt
        ? {
            entered: rt.entered,
            side: rt.side,
            entryPx: rt.entryPx,
            upBid: rt.upBid,
            downBid: rt.downBid,
            balanceUsd: rt.balanceUsd,
            realizedPnlUsd: rt.realizedPnlUsd,
            lastAction: rt.lastAction,
            lastExitType: rt.lastExitType,
            sessionSlug: rt.sessionSlug,
          }
        : null,
      latestStrategySnapshot: srt?.lastSnapshot ?? null,
      files: {
        eventsJsonl: path.join(dir, "events.jsonl"),
        telemetryJsonl: botRunTelemetryPath(runNum),
        summaryJson: path.join(dir, "summary.json"),
      },
      sessions,
      tracesBySlug,
    };
    const outPath = botRunIndexPath(instance.strategyId, runNum);
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2) + "\n", "utf8");
  } catch {}
}

function appendBotRunTelemetry(instance: BotInstance, rt: BotRuntime, strategySnapshot: any) {
  try {
    const runNum = Math.floor(Number(instance.runNum));
    if (!Number.isFinite(runNum) || runNum <= 0) return;
    const dir = botRunDir(runNum);
    fs.mkdirSync(dir, { recursive: true });
    const p = botRunTelemetryPath(runNum);
    const row = {
      t: nowMs(),
      iso: new Date().toISOString(),
      instanceId: instance.instanceId,
      runId: instance.runId,
      runNum: runNum,
      strategyId: instance.strategyId,
      marketSlug: instance.marketSlug,
      sessionSlug: rt.sessionSlug,
      upBid: rt.upBid,
      downBid: rt.downBid,
      entered: rt.entered,
      side: rt.side,
      entryPx: rt.entryPx,
      balanceUsd: rt.balanceUsd,
      realizedPnlUsd: rt.realizedPnlUsd,
      strategySnapshot: strategySnapshot ?? null,
    };
    fs.appendFileSync(p, JSON.stringify(row) + "\n", "utf8");
  } catch {}
}

function readJsonlSafe(p: string): any[] {
  try {
    if (!fs.existsSync(p)) return [];
    const txt = fs.readFileSync(p, "utf8");
    return txt
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean)
      .map((ln) => {
        try { return JSON.parse(ln); } catch { return null; }
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

function readBotRunEvents(runNum: number): any[] {
  const p = path.join(botRunDir(runNum), "events.jsonl");
  return readJsonlSafe(p);
}

const botRunEventsCache = new Map<number, { mtimeMs: number; size: number; rows: any[] }>();
function readBotRunEventsCached(runNum: number): any[] {
  const rn = Math.floor(Number(runNum));
  if (!Number.isFinite(rn) || rn <= 0) return [];
  const p = path.join(botRunDir(rn), "events.jsonl");
  try {
    const st = fs.statSync(p);
    const mtimeMs = Number(st.mtimeMs || 0);
    const size = Number(st.size || 0);
    const prev = botRunEventsCache.get(rn);
    if (prev && Number(prev.mtimeMs) === mtimeMs && Number(prev.size) === size && Array.isArray(prev.rows)) {
      return prev.rows;
    }
    const rows = readJsonlSafe(p);
    botRunEventsCache.set(rn, { mtimeMs, size, rows });
    return rows;
  } catch {
    botRunEventsCache.delete(rn);
    return [];
  }
}

function countBotRunSessionExits(runNum: number, instanceId: string, marketSlug: string): number {
  const rn = Math.floor(Number(runNum));
  const iid = String(instanceId || "").trim();
  const slug = String(marketSlug || "").trim();
  if (!Number.isFinite(rn) || rn <= 0 || !iid || !slug) return 0;
  let n = 0;
  try {
    const rows = readBotRunEventsCached(rn);
    for (const r of rows) {
      if (String(r?.instanceId || "").trim() !== iid) continue;
      if (String(r?.marketSlug || "").trim() !== slug) continue;
      if (String(r?.event || "").toLowerCase() !== "exit") continue;
      n += 1;
    }
  } catch {
    return 0;
  }
  return n;
}

function countBotRunSessionLossExits(runNum: number, instanceId: string, marketSlug: string): number {
  const rn = Math.floor(Number(runNum));
  const iid = String(instanceId || "").trim();
  const slug = String(marketSlug || "").trim();
  if (!Number.isFinite(rn) || rn <= 0 || !iid || !slug) return 0;
  let n = 0;
  try {
    const rows = readBotRunEventsCached(rn);
    for (const r of rows) {
      if (String(r?.instanceId || "").trim() !== iid) continue;
      if (String(r?.marketSlug || "").trim() !== slug) continue;
      if (String(r?.event || "").toLowerCase() !== "exit") continue;
      const pnlUsd = Number(r?.pnlUsd);
      if (Number.isFinite(pnlUsd) && pnlUsd < 0) n += 1;
    }
  } catch {
    return 0;
  }
  return n;
}

function botSessionTradeCap(strategyIdRaw: any): number | null {
  const sid = String(strategyIdRaw || "").trim().toLowerCase();
  if (sid === "momentum_hc" || sid === "confidence_b_phase1" || sid === "confidence_c_pmodel") {
    return BOT_MAX_MOMENTUM_TRADES_PER_SESSION;
  }
  if (sid === "profit_locker" || sid === "profit_locker_inflection") return BOT_MAX_PROFIT_LOCKER_TRADES_PER_SESSION;
  return null;
}

function botSessionLossCap(strategyIdRaw: any): number | null {
  const sid = String(strategyIdRaw || "").trim().toLowerCase();
  if (sid === "profit_locker" || sid === "profit_locker_inflection") return BOT_MAX_PROFIT_LOCKER_SESSION_LOSSES;
  return null;
}

function readBotRunSummary(runNum: number): any | null {
  try {
    const p = path.join(botRunDir(runNum), "summary.json");
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function appendBotRunEvent(instance: BotInstance, rt: BotRuntime, event: Record<string, any>) {
  try {
    const dir = botRunDir(instance.runNum);
    fs.mkdirSync(dir, { recursive: true });
    const p = path.join(dir, "events.jsonl");
    const row = {
      t: nowMs(),
      iso: new Date().toISOString(),
      instanceId: instance.instanceId,
      runId: instance.runId,
      runNum: instance.runNum,
      marketSlug: instance.marketSlug,
      strategyId: instance.strategyId,
      mode: instance.mode,
      runtime: {
        entered: rt.entered,
        side: rt.side,
        upBid: rt.upBid,
        downBid: rt.downBid,
        realizedPnlUsd: rt.realizedPnlUsd,
        balanceUsd: rt.balanceUsd,
      },
      ...event,
    };
    fs.appendFileSync(p, JSON.stringify(row) + "\n", "utf8");
    // Invalidate cached parsed rows for this run so next read sees fresh events.
    botRunEventsCache.delete(Math.floor(Number(instance.runNum)));
    writeBotRunIndex(instance, rt);
  } catch {}
}

function writeBotRunSummary(instance: BotInstance, rt: BotRuntime) {
  try {
    const dir = botRunDir(instance.runNum);
    fs.mkdirSync(dir, { recursive: true });
    const summary = {
      schemaVersion: "1.0",
      runId: instance.runId,
      runNum: instance.runNum,
      instanceId: instance.instanceId,
      marketId: instance.marketId,
      marketSlug: instance.marketSlug,
      marketTitle: instance.marketTitle,
      strategyId: instance.strategyId,
      strategyPath: instance.strategyPath,
      mode: instance.mode,
      status: instance.status,
      startedAtMs: instance.launchedAtMs,
      endedAtMs: instance.stoppedAtMs,
      startedAtIso: new Date(instance.launchedAtMs).toISOString(),
      endedAtIso: Number.isFinite(Number(instance.stoppedAtMs)) ? new Date(Number(instance.stoppedAtMs)).toISOString() : null,
      startBalanceUsd: instance.startBalanceUsd,
      endBalanceUsd: rt.balanceUsd,
      pnlUsd: rt.realizedPnlUsd,
      tradesClosed: rt.closedTrades,
      wins: rt.wins,
      losses: rt.losses,
      lastExitType: rt.lastExitType,
      generatedAtIso: new Date().toISOString(),
    };
    fs.writeFileSync(path.join(dir, "summary.json"), JSON.stringify(summary, null, 2) + "\n", "utf8");
    writeBotRunIndex(instance, rt);
  } catch {}
}

function purgeInactiveAndOrphanBotState(reason: string = "unknown"): number {
  let removed = 0;
  // Remove stopped instances and all associated runtime/strategy state.
  for (const [instanceId, inst] of Array.from(botInstances.entries())) {
    const status = String(inst?.status || "").toLowerCase();
    if (status === "stopped") {
      botInstances.delete(instanceId);
      botRuntimes.delete(instanceId);
      botStrategyRuntimes.delete(instanceId);
      removed += 1;
    }
  }
  // Remove runtimes with no instance owner.
  for (const instanceId of Array.from(botRuntimes.keys())) {
    if (!botInstances.has(instanceId)) {
      botRuntimes.delete(instanceId);
      botStrategyRuntimes.delete(instanceId);
      removed += 1;
    }
  }
  // Remove strategy runtimes with no instance owner.
  for (const instanceId of Array.from(botStrategyRuntimes.keys())) {
    if (!botInstances.has(instanceId)) {
      botStrategyRuntimes.delete(instanceId);
      removed += 1;
    }
  }
  if (removed > 0) {
    console.log(`[MULTI STATE] purged stale state count=${removed} reason=${reason}`);
  }
  return removed;
}

function persistMultiMarketState() {
  try {
    const db = getMultiStateDb();
    const now = Date.now();
    db.exec("BEGIN IMMEDIATE");
    try {
      const upsertKv = db.prepare("INSERT INTO kv(key,value,updated_ms) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ms=excluded.updated_ms");
      upsertKv.run("run_seq", String(BOT_RUN_SEQ), now);
      upsertKv.run("generated_at_ms", String(now), now);

      db.exec("DELETE FROM bot_instances");
      db.exec("DELETE FROM bot_runtimes");
      const insInst = db.prepare("INSERT INTO bot_instances(instance_id,payload_json,updated_ms) VALUES(?,?,?)");
      const insRt = db.prepare("INSERT INTO bot_runtimes(instance_id,payload_json,updated_ms) VALUES(?,?,?)");
      for (const b of botInstances.values()) {
        insInst.run(b.instanceId, JSON.stringify(b), now);
      }
      for (const r of botRuntimes.values()) {
        insRt.run(r.instanceId, JSON.stringify(r), now);
      }
      db.exec("COMMIT");
    } catch (inner: any) {
      try { db.exec("ROLLBACK"); } catch {}
      throw inner;
    }
  } catch (e: any) {
    console.warn(`[MULTI STATE SAVE ERROR] ${String(e?.message || e)}`);
  }
}

function loadMultiMarketState() {
  let loadedFromDb = false;
  try {
    const db = getMultiStateDb();
    const runSeqRow: any = db.prepare("SELECT value FROM kv WHERE key='run_seq' LIMIT 1").get();
    const runSeq = Number(runSeqRow?.value);
    if (Number.isFinite(runSeq) && runSeq >= 0) BOT_RUN_SEQ = Math.floor(runSeq);
    const instRows = db.prepare("SELECT payload_json FROM bot_instances").all() as Array<{ payload_json: string }>;
    const rtRows = db.prepare("SELECT payload_json FROM bot_runtimes").all() as Array<{ payload_json: string }>;
    if (instRows.length || rtRows.length) {
      for (const rr of rtRows) {
        let r: any = null;
        try { r = JSON.parse(String(rr.payload_json || "{}")); } catch {}
        if (!r || typeof r !== "object") continue;
        const instanceId = String(r.instanceId || "").trim();
        if (!instanceId) continue;
        botRuntimes.set(instanceId, r as BotRuntime);
      }
      for (const br of instRows) {
        let b: any = null;
        try { b = JSON.parse(String(br.payload_json || "{}")); } catch {}
        if (!b || typeof b !== "object") continue;
        const instanceId = String(b.instanceId || "").trim();
        if (!instanceId) continue;
        const runNumRaw = Number(b.runNum);
        const runNum = Number.isFinite(runNumRaw) ? Math.floor(runNumRaw) : ++BOT_RUN_SEQ;
        const merged: BotInstance = {
          ...(b as BotInstance),
          marketBucket: normalizeBotMarketBucket((b as any).marketBucket),
          runNum,
          strategyPath: resolveBotStrategyPath(
            normalizeStrategyId((b as any).strategyId),
            String((b as any).strategyPath || "").trim() || null
          ),
        };
        botInstances.set(instanceId, merged);
        if (merged.runNum > BOT_RUN_SEQ) BOT_RUN_SEQ = merged.runNum;
        if (!botRuntimes.has(instanceId)) ensureRuntime(merged);
      }
      loadedFromDb = true;
      const purged = purgeInactiveAndOrphanBotState("load:sqlite");
      if (purged > 0) persistMultiMarketState();
      console.log(`[MULTI STATE] restored from sqlite instances=${botInstances.size} runtimes=${botRuntimes.size} runSeq=${BOT_RUN_SEQ}`);
    }
  } catch (e: any) {
    console.warn(`[MULTI STATE LOAD ERROR] ${String(e?.message || e)}`);
  }
  if (!loadedFromDb) {
    const migrated = loadMultiMarketStateFromJsonFile();
    if (migrated) {
      purgeInactiveAndOrphanBotState("load:json");
      persistMultiMarketState();
      console.log(`[MULTI STATE] migrated JSON state -> sqlite path=${MULTI_STATE_DB_PATH}`);
    }
  }
}

function loadMultiMarketStateFromJsonFile(): boolean {
  try {
    if (!fs.existsSync(MULTI_STATE_PATH)) return false;
    const raw = fs.readFileSync(MULTI_STATE_PATH, "utf8");
    const j = JSON.parse(raw || "{}");
    const runSeq = Number(j?.runSeq);
    if (Number.isFinite(runSeq) && runSeq >= 0) BOT_RUN_SEQ = Math.floor(runSeq);
    const instances = Array.isArray(j?.instances) ? j.instances : [];
    const runtimes = Array.isArray(j?.runtimes) ? j.runtimes : [];
    for (const r of runtimes) {
      if (!r || typeof r !== "object") continue;
      const instanceId = String((r as any).instanceId || "").trim();
      if (!instanceId) continue;
      botRuntimes.set(instanceId, r as BotRuntime);
    }
    for (const b of instances) {
      if (!b || typeof b !== "object") continue;
      const instanceId = String((b as any).instanceId || "").trim();
      if (!instanceId) continue;
      const runNumRaw = Number((b as any).runNum);
      const runNum = Number.isFinite(runNumRaw) ? Math.floor(runNumRaw) : ++BOT_RUN_SEQ;
      const merged: BotInstance = {
        ...(b as BotInstance),
        marketBucket: normalizeBotMarketBucket((b as any).marketBucket),
        runNum,
        strategyPath: resolveBotStrategyPath(
          normalizeStrategyId((b as any).strategyId),
          String((b as any).strategyPath || "").trim() || null
        ),
      };
      botInstances.set(instanceId, merged);
      if (merged.runNum > BOT_RUN_SEQ) BOT_RUN_SEQ = merged.runNum;
      if (!botRuntimes.has(instanceId)) {
        ensureRuntime(merged);
      }
    }
    console.log(`[MULTI STATE] restored from JSON instances=${botInstances.size} runtimes=${botRuntimes.size} runSeq=${BOT_RUN_SEQ}`);
    return true;
  } catch (e: any) {
    console.warn(`[MULTI STATE LOAD JSON ERROR] ${String(e?.message || e)}`);
    return false;
  }
}

function getMultiStateDb(): DatabaseSync {
  if (multiDb) return multiDb;
  ensureTradeLogDir();
  multiDb = new DatabaseSync(MULTI_STATE_DB_PATH);
  multiDb.exec("PRAGMA journal_mode=WAL");
  multiDb.exec("PRAGMA synchronous=NORMAL");
  multiDb.exec(`
    CREATE TABLE IF NOT EXISTS kv (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_ms INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bot_instances (
      instance_id TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      updated_ms INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bot_runtimes (
      instance_id TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      updated_ms INTEGER NOT NULL
    );
  `);
  return multiDb;
}

async function resolveRuntimeMarketMeta(rt: BotRuntime): Promise<void> {
  if (rt.upToken && rt.downToken && Number.isFinite(Number(rt.marketEndMs))) return;
  const evt = await gammaEventBySlug(rt.marketSlug);
  const mkt = Array.isArray(evt?.markets) && evt.markets.length ? evt.markets[0] : null;
  if (!mkt) throw new Error(`No market found by slug: ${rt.marketSlug}`);
  const tokens = extractUpDownTokensFromMarket(mkt);
  const endDate = mkt?.endDate ?? evt?.endDate;
  const startDate = mkt?.startDate ?? evt?.startDate;
  rt.upToken = tokens.up;
  rt.downToken = tokens.down;
  rt.marketStartMs = startDate ? parseIsoZ(String(startDate)).getTime() : rt.marketStartMs;
  rt.marketEndMs = endDate ? parseIsoZ(String(endDate)).getTime() : rt.marketEndMs;
}

function normalizeBotMarketBucket(raw: any): "5m" | "15m" | "hourly" | "daily" | null {
  const v = String(raw || "").trim().toLowerCase();
  if (v === "5m" || v === "15m" || v === "hourly" || v === "daily") return v;
  return null;
}

async function resolveCurrentBucketMarket(
  bucket: "5m" | "15m" | "hourly" | "daily"
): Promise<{ slug: string; startMs: number; endMs: number; upToken: string; downToken: string } | null> {
  try {
    const rows = await getCurrentBtcIntervalMarkets();
    const one = rows.find((r) => String((r as any)?.bucket || "").toLowerCase() === String(bucket).toLowerCase()) || null;
    if (!one) return null;
    const slug = String(one.slug || "").trim();
    if (!slug) return null;
    return {
      slug,
      startMs: Number(one.startMs),
      endMs: Number(one.endMs),
      upToken: String(one.upToken || ""),
      downToken: String(one.downToken || ""),
    };
  } catch {
    return null;
  }
}

async function tickBotRuntime(instance: BotInstance): Promise<void> {
  const rt = ensureRuntime(instance);
  let forceStrategyResyncReason: string | null = null;
  if (!Number.isFinite(Number((rt as any).sessionClosedTrades))) (rt as any).sessionClosedTrades = 0;
  if (!Number.isFinite(Number((rt as any).sessionLosses))) (rt as any).sessionLosses = 0;
  if (typeof (rt as any).momentumTpRearmNeeded !== "boolean") (rt as any).momentumTpRearmNeeded = false;
  const rtRearmSide = String((rt as any).momentumTpRearmSide || "").toUpperCase();
  if (rtRearmSide !== "UP" && rtRearmSide !== "DOWN") (rt as any).momentumTpRearmSide = null;
  if (typeof (rt as any).sessionSlug !== "string" || !(rt as any).sessionSlug) {
    (rt as any).sessionSlug = String(rt.marketSlug || "");
  }
  const runtimeSessionSlug = String((rt as any).sessionSlug || "").trim();
  const runtimeMarketSlug = String(rt.marketSlug || "").trim();
  if (runtimeMarketSlug && runtimeSessionSlug !== runtimeMarketSlug && !rt.entered) {
    (rt as any).sessionSlug = runtimeMarketSlug;
    (rt as any).sessionClosedTrades = 0;
    (rt as any).sessionLosses = 0;
    (rt as any).sessionCapReconciledSlug = null;
    (rt as any).sessionLossReconciledSlug = null;
    (rt as any).momentumTpRearmNeeded = false;
    (rt as any).momentumTpRearmSide = null;
  }
  let mutated = false;
  try {
    if (instance.marketBucket) {
      const cur = await resolveCurrentBucketMarket(instance.marketBucket);
      if (cur && String(cur.slug) !== String(rt.marketSlug || "")) {
        const prevSlug = String(rt.marketSlug || "");
        // Never carry an open paper position across bucket session slugs.
        // If runtime still shows entered at rollover, force-flat it first.
        if (rt.entered && rt.side && Number.isFinite(Number(rt.entryPx)) && Number.isFinite(Number(rt.shares))) {
          const sideNowBid = rt.side === "UP" ? Number(rt.upBid) : Number(rt.downBid);
          const settlePx =
            Number.isFinite(sideNowBid)
              ? (Number(sideNowBid) > 0.5 ? 1 : 0)
              : Number(rt.entryPx);
          const pnlUsd = (Number(settlePx) - Number(rt.entryPx)) * Number(rt.shares);
          rt.realizedPnlUsd += pnlUsd;
          rt.balanceUsd += pnlUsd;
          rt.closedTrades += 1;
          rt.sessionClosedTrades = Number(rt.sessionClosedTrades || 0) + 1;
          if (pnlUsd > 0) rt.wins += 1;
          else if (pnlUsd < 0) {
            rt.losses += 1;
            rt.sessionLosses = Number(rt.sessionLosses || 0) + 1;
          }
          rt.lastExitType = "settle";
          rt.lastAction = "session_rollover_force_flat";
          appendBotRunEvent(instance, rt, {
            event: "exit",
            side: rt.side,
            exitType: "settle",
            via: "session_rollover_force_flat",
            reason: Number.isFinite(sideNowBid) ? "bucket_slug_changed_last_bid" : "bucket_slug_changed_no_bid_entry_px",
            exitPx: settlePx,
            pnlUsd,
            realizedPnlUsd: rt.realizedPnlUsd,
            balanceUsd: rt.balanceUsd,
          });
          rt.entered = false;
          rt.side = null;
          rt.entryPx = null;
          rt.entryTsMs = null;
          rt.shares = null;
          rt.notionalUsd = null;
          if (strategyProfileOf(instance.strategyId).resetRuntimeOnSessionRolloverExit) {
            botStrategyRuntimes.delete(instance.instanceId);
          }
          mutated = true;
        }
      }
      if (cur && !rt.entered && String(cur.slug) !== String(rt.marketSlug || "")) {
        const prevSlug = String(rt.marketSlug || "");
        rt.marketBucket = instance.marketBucket;
        rt.marketSlug = String(cur.slug);
        rt.upToken = String(cur.upToken || "") || null;
        rt.downToken = String(cur.downToken || "") || null;
        rt.marketStartMs = Number.isFinite(Number(cur.startMs)) ? Number(cur.startMs) : null;
        rt.marketEndMs = Number.isFinite(Number(cur.endMs)) ? Number(cur.endMs) : null;
        rt.sessionSlug = String(cur.slug);
        rt.sessionClosedTrades = 0;
        rt.sessionLosses = 0;
        rt.sessionCapReconciledSlug = null;
        rt.sessionLossReconciledSlug = null;
        (rt as any).momentumTpRearmNeeded = false;
        (rt as any).momentumTpRearmSide = null;
        instance.marketSlug = String(cur.slug);
        instance.marketId = String(cur.slug);
        instance.marketTitle = `BTC ${instance.marketBucket} Markets`;
        mutated = true;
        forceStrategyResyncReason = `session_rollover:${prevSlug}->${String(cur.slug)}`;
      }
    }
    await resolveRuntimeMarketMeta(rt);
    if (!rt.upToken || !rt.downToken) throw new Error("UP/DOWN tokens unavailable");
    const [upBAraw, downBAraw] = await Promise.all([
      getBestBidAsk(rt.upToken),
      getBestBidAsk(rt.downToken),
    ]);
    const reconciled = reconcileBinaryBidPair(upBAraw, downBAraw);
    const upBA = reconciled.up;
    const downBA = reconciled.down;
    rt.upBid = Number.isFinite(Number(upBA?.bid)) ? Number(upBA.bid) : null;
    rt.downBid = Number.isFinite(Number(downBA?.bid)) ? Number(downBA.bid) : null;
    rt.lastTickMs = nowMs();
    // Persist per-session trace for multi-bot runtime so focused history refresh
    // can rebuild UP/DOWN lines from recorded data.
    recordSessionTracePoint(String(rt.marketSlug || instance.marketSlug || ""), rt.lastTickMs, rt.upBid, rt.downBid);
    rt.ticks += 1;
    rt.lastError = null;
    const now = nowMs();
    const elapsedSec = Number.isFinite(Number(rt.marketStartMs)) ? Math.max(0, (now - Number(rt.marketStartMs)) / 1000) : 0;
    if (forceStrategyResyncReason) {
      botStrategyRuntimes.delete(instance.instanceId);
      logInfo(
        `BOT STRATEGY RESYNC | inst=${instance.instanceId} run=${instance.runNum} ` +
        `reason=${forceStrategyResyncReason}`
      );
    }
    const srt = ensureBotStrategyRuntime(instance);
    const hasBotStrategy = !!srt?.strategy;
    let strategyAction: StrategyAction = null;
    if (hasBotStrategy && Number.isFinite(Number(rt.upBid)) && Number.isFinite(Number(rt.downBid))) {
      try {
        const raw = srt.strategy!.onTick({
          elapsedSec,
          upBid: Number(rt.upBid),
          downBid: Number(rt.downBid),
          isFinal: Number.isFinite(Number(rt.marketEndMs)) ? (now >= Number(rt.marketEndMs)) : false,
        });
        strategyAction = normalizeStrategyAction(raw);
        if (!rt.entered && !!strategyAction?.exit && !strategyAction?.enter) {
          // Desync guard: strategy thinks it has an open lane while runtime is flat.
          const key = `${String(rt.marketSlug || "")}|flat_exit_only`;
          const last = Number((rt as any)._lastEntryBlockLogByReason?.[key] || 0);
          if (!Number.isFinite(last) || (now - last) >= 3000) {
            logWarn(
              `BOT STRATEGY DESYNC | inst=${instance.instanceId} run=${instance.runNum} ` +
              `slug=${rt.marketSlug} reason=flat_runtime_but_exit_signal action=${JSON.stringify(strategyAction)}`
            );
            (rt as any)._lastEntryBlockLogByReason = (rt as any)._lastEntryBlockLogByReason || {};
            (rt as any)._lastEntryBlockLogByReason[key] = now;
          }
          // Keep locker-family runtimes stateful across a session; only force
          // re-arm momentum-family runtimes on flat-runtime exit-only signals.
          if (strategyProfileOf(instance.strategyId).resetRuntimeOnFlatExitSignal) {
            botStrategyRuntimes.delete(instance.instanceId);
            const rsrt = ensureBotStrategyRuntime(instance);
            const raw2 = rsrt?.strategy?.onTick?.({
              elapsedSec,
              upBid: Number(rt.upBid),
              downBid: Number(rt.downBid),
              isFinal: Number.isFinite(Number(rt.marketEndMs)) ? (now >= Number(rt.marketEndMs)) : false,
            });
            strategyAction = normalizeStrategyAction(raw2);
            botStrategyRuntimes.set(instance.instanceId, rsrt);
          } else {
            strategyAction = null;
          }
        }
        try {
          srt.lastSnapshot = typeof srt.strategy?.snapshot === "function" ? srt.strategy.snapshot() : null;
        } catch {
          srt.lastSnapshot = null;
        }
        srt.lastError = null;
      } catch (e: any) {
        srt.lastError = String(e?.message || e || "strategy onTick error");
      }
      botStrategyRuntimes.set(instance.instanceId, srt);
      const lastTelMs = Number((rt as any)._lastRunTelemetryMs || 0);
      if (!Number.isFinite(lastTelMs) || (now - lastTelMs) >= 250) {
        appendBotRunTelemetry(instance, rt, srt?.lastSnapshot ?? null);
        (rt as any)._lastRunTelemetryMs = now;
      }
    }
    const minEntrySec = Math.max(0, Number(instance.minEntrySec || 0));
    const betUsd = Math.max(1, Number(instance.betUsd || 25));
    const maxBetUsd = Math.max(betUsd, Number(instance.maxBetUsd || betUsd));

    if (!instance.watchOnly) {
      if (!hasBotStrategy) {
        rt.lastAction = "strategy_not_loaded";
        rt.lastError = srt?.lastError || "strategy runtime unavailable";
        botRuntimes.set(rt.instanceId, rt);
        const next = botInstances.get(instance.instanceId);
        if (next && next.status !== "stopped") {
          next.status = "error";
          next.lastError = rt.lastError;
          botInstances.set(next.instanceId, next);
        }
        return;
      }
      if (!rt.entered) {
        // Momentum guard: after a TP on a side, require that side to dip below
        // the entry threshold once before allowing another entry on that side.
        if (strategyProfileOf(instance.strategyId).requiresTpRearm && !!(rt as any).momentumTpRearmNeeded) {
          const rearmSide = String((rt as any).momentumTpRearmSide || "").toUpperCase();
          const rearmPx =
            rearmSide === "UP" ? Number(rt.upBid) :
            rearmSide === "DOWN" ? Number(rt.downBid) :
            NaN;
          const rearmEntryThr = Number(instance.entry);
          if (Number.isFinite(rearmPx) && Number.isFinite(rearmEntryThr) && rearmPx < rearmEntryThr) {
            (rt as any).momentumTpRearmNeeded = false;
            (rt as any).momentumTpRearmSide = null;
            mutated = true;
          }
        }
        if (elapsedSec >= minEntrySec) {
          const capPerSession = botSessionTradeCap(instance.strategyId);
          const lossCapPerSession = botSessionLossCap(instance.strategyId);
          const sessionSlug = String(rt.marketSlug || "");
          if (Number.isFinite(Number(capPerSession)) && Number(capPerSession) > 0) {
            // Reconcile once per session slug; avoids expensive file scans on every tick.
            if (String(rt.sessionCapReconciledSlug || "") !== sessionSlug) {
              const persistedExits = countBotRunSessionExits(
                Number(instance.runNum),
                String(instance.instanceId || ""),
                sessionSlug
              );
              if (Number.isFinite(persistedExits) && persistedExits > Number(rt.sessionClosedTrades || 0)) {
                rt.sessionClosedTrades = Number(persistedExits);
              }
              rt.sessionCapReconciledSlug = sessionSlug;
            }
          }
          if (Number.isFinite(Number(lossCapPerSession)) && Number(lossCapPerSession) > 0) {
            // Reconcile once per session slug; avoids expensive file scans on every tick.
            if (String(rt.sessionLossReconciledSlug || "") !== sessionSlug) {
              const persistedLosses = countBotRunSessionLossExits(
                Number(instance.runNum),
                String(instance.instanceId || ""),
                sessionSlug
              );
              if (Number.isFinite(persistedLosses) && persistedLosses > Number(rt.sessionLosses || 0)) {
                rt.sessionLosses = Number(persistedLosses);
              }
              rt.sessionLossReconciledSlug = sessionSlug;
            }
          }
          const sessCapReached =
            Number.isFinite(Number(capPerSession)) &&
            Number(capPerSession) > 0 &&
            Number.isFinite(Number(rt.sessionClosedTrades)) &&
            Number(rt.sessionClosedTrades) >= Number(capPerSession);
          if (sessCapReached) {
            const key = `${String(rt.marketSlug || "")}|entry_blocked_session_cap`;
            const last = Number((rt as any)._lastEntryBlockLogByReason?.[key] || 0);
            if (!Number.isFinite(last) || (now - last) >= 5000) {
              logWarn(
                `BOT ENTRY BLOCKED | reason=session_cap inst=${instance.instanceId} run=${instance.runNum} ` +
                `slug=${rt.marketSlug} closed=${Number(rt.sessionClosedTrades || 0)} cap=${Number(capPerSession)}`
              );
              (rt as any)._lastEntryBlockLogByReason = (rt as any)._lastEntryBlockLogByReason || {};
              (rt as any)._lastEntryBlockLogByReason[key] = now;
            }
            rt.lastAction = "entry_blocked_session_cap";
            botRuntimes.set(rt.instanceId, rt);
            return;
          }
          const sessionLossCapReached =
            Number.isFinite(Number(lossCapPerSession)) &&
            Number(lossCapPerSession) > 0 &&
            Number.isFinite(Number(rt.sessionLosses)) &&
            Number(rt.sessionLosses) >= Number(lossCapPerSession);
          if (sessionLossCapReached) {
            const key = `${String(rt.marketSlug || "")}|entry_blocked_session_loss_cap`;
            const last = Number((rt as any)._lastEntryBlockLogByReason?.[key] || 0);
            if (!Number.isFinite(last) || (now - last) >= 5000) {
              logWarn(
                `BOT ENTRY BLOCKED | reason=session_loss_cap inst=${instance.instanceId} run=${instance.runNum} ` +
                `slug=${rt.marketSlug} losses=${Number(rt.sessionLosses || 0)} cap=${Number(lossCapPerSession)}`
              );
              (rt as any)._lastEntryBlockLogByReason = (rt as any)._lastEntryBlockLogByReason || {};
              (rt as any)._lastEntryBlockLogByReason[key] = now;
            }
            rt.lastAction = "entry_blocked_session_loss_cap";
            botRuntimes.set(rt.instanceId, rt);
            return;
          }
          let side: OutcomeSide | null = null;
          const strategySide = String(strategyAction?.enter?.side || "").toUpperCase();
          if (strategySide === "UP" || strategySide === "DOWN") {
            side = strategySide as OutcomeSide;
          }
          if (!side) {
            const entryThr = Number(instance.entry);
            const upBidNow = Number(rt.upBid);
            const downBidNow = Number(rt.downBid);
            const thresholdHit =
              Number.isFinite(entryThr) &&
              ((Number.isFinite(upBidNow) && upBidNow >= entryThr) ||
                (Number.isFinite(downBidNow) && downBidNow >= entryThr));
            if (thresholdHit) {
              const key = `${String(rt.marketSlug || "")}|no_strategy_enter_side`;
              const last = Number((rt as any)._lastEntryBlockLogByReason?.[key] || 0);
              if (!Number.isFinite(last) || (now - last) >= 3000) {
                logWarn(
                  `BOT ENTRY SKIP | reason=no_strategy_enter_side inst=${instance.instanceId} run=${instance.runNum} ` +
                  `slug=${rt.marketSlug} up=${fmt2(upBidNow)} down=${fmt2(downBidNow)} entry=${fmt2(entryThr)} ` +
                  `action=${JSON.stringify(strategyAction || null)}`
                );
                (rt as any)._lastEntryBlockLogByReason = (rt as any)._lastEntryBlockLogByReason || {};
                (rt as any)._lastEntryBlockLogByReason[key] = now;
              }
            }
          }
          if (side) {
            if (strategyProfileOf(instance.strategyId).requiresTpRearm) {
              const needRearm = !!(rt as any).momentumTpRearmNeeded;
              const rearmSide = String((rt as any).momentumTpRearmSide || "").toUpperCase();
              if (needRearm && rearmSide && rearmSide === String(side)) {
                const key = `${String(rt.marketSlug || "")}|entry_blocked_tp_rearm_${rearmSide.toLowerCase()}`;
                const last = Number((rt as any)._lastEntryBlockLogByReason?.[key] || 0);
                if (!Number.isFinite(last) || (now - last) >= 3000) {
                  const entryThrNum = Number(instance.entry);
                  const sideBidNum = side === "UP" ? Number(rt.upBid) : Number(rt.downBid);
                  const entryThrTxt = Number.isFinite(entryThrNum) ? entryThrNum.toFixed(2) : "—";
                  const sideBidTxt = Number.isFinite(sideBidNum) ? sideBidNum.toFixed(2) : "—";
                  logWarn(
                    `BOT ENTRY BLOCKED | reason=tp_rearm_required inst=${instance.instanceId} run=${instance.runNum} ` +
                    `slug=${rt.marketSlug} side=${side} entryThr=${entryThrTxt} ` +
                    `sideBid=${sideBidTxt}`
                  );
                  (rt as any)._lastEntryBlockLogByReason = (rt as any)._lastEntryBlockLogByReason || {};
                  (rt as any)._lastEntryBlockLogByReason[key] = now;
                }
                side = null;
                rt.lastAction = "entry_blocked_tp_rearm_required";
                botRuntimes.set(rt.instanceId, rt);
              }
            }
          }
          if (side) {
            const sideBid = side === "UP" ? Number(rt.upBid) : Number(rt.downBid);
            if (Number.isFinite(sideBid) && sideBid > 0) {
              const actionNotionalUsd = Number(strategyAction?.enter?.notionalUsd);
              const notionalUsd = Number.isFinite(actionNotionalUsd) && actionNotionalUsd > 0
                ? Math.max(1, Math.min(maxBetUsd, actionNotionalUsd))
                : Math.max(1, Math.min(maxBetUsd, betUsd));
              const openNow = currentGlobalOpenNotionalUsd();
              if (openNow + notionalUsd <= BOT_GLOBAL_MAX_NOTIONAL_USD) {
                const shares = notionalUsd / sideBid;
                rt.entered = true;
                rt.side = side;
                rt.entryPx = sideBid;
                rt.entryTsMs = now;
                rt.notionalUsd = notionalUsd;
                rt.shares = shares;
                rt.lastAction = `enter_${side.toLowerCase()}`;
                mutated = true;
                appendBotRunEvent(instance, rt, { event: "enter", side, entryPx: sideBid, notionalUsd, shares });
              }
            }
          }
        }
      } else if (rt.side && Number.isFinite(Number(rt.entryPx)) && Number.isFinite(Number(rt.shares))) {
        const sideBid = rt.side === "UP" ? Number(rt.upBid) : Number(rt.downBid);
        if (Number.isFinite(sideBid)) {
          const stratExitSideRaw = String(strategyAction?.exit?.side || "").toUpperCase();
          const stratExitSideOk =
            !stratExitSideRaw ||
            stratExitSideRaw === String(rt.side || "").toUpperCase();
          const strategyExitRequested = !!strategyAction?.exit && stratExitSideOk;
          const strategyExitType = String(strategyAction?.exit?.type || "").toLowerCase();
          const strategyExitPx = Number(strategyAction?.exit?.exitPx);
          const expired = Number.isFinite(Number(rt.marketEndMs)) && now >= Number(rt.marketEndMs);
          if (strategyExitRequested || expired) {
            const exitPx = expired
              ? (Number.isFinite(sideBid) ? (sideBid > 0.5 ? 1 : 0) : 0)
              : (Number.isFinite(strategyExitPx) ? Number(strategyExitPx) : sideBid);
            const pnlUsd = (exitPx - Number(rt.entryPx)) * Number(rt.shares);
            rt.realizedPnlUsd += pnlUsd;
            rt.balanceUsd += pnlUsd;
            rt.closedTrades += 1;
            rt.sessionClosedTrades = Number(rt.sessionClosedTrades || 0) + 1;
            if (pnlUsd > 0) rt.wins += 1;
            else if (pnlUsd < 0) {
              rt.losses += 1;
              rt.sessionLosses = Number(rt.sessionLosses || 0) + 1;
            }
            rt.lastExitType = expired
              ? "settle"
              : (strategyExitRequested
                ? ((() => {
                    const t = String(strategyExitType || "").toLowerCase();
                    const stopLike =
                      t.includes("stop") ||
                      t.includes("cross") ||
                      t.includes("fade") ||
                      t.includes("inflect") ||
                      t.includes("loss");
                    return stopLike ? "stop" : "tp";
                  })())
                : "settle");
            if (strategyProfileOf(instance.strategyId).requiresTpRearm) {
              if (String(rt.lastExitType) === "tp") {
                (rt as any).momentumTpRearmNeeded = true;
                (rt as any).momentumTpRearmSide = rt.side;
              } else {
                (rt as any).momentumTpRearmNeeded = false;
                (rt as any).momentumTpRearmSide = null;
              }
            }
            rt.lastAction = `exit_${String(rt.lastExitType)}`;
            mutated = true;
            appendBotRunEvent(instance, rt, {
              event: "exit",
              side: rt.side,
              exitType: rt.lastExitType,
              exitReasonRaw: expired
                ? "session_expired"
                : (strategyExitRequested ? String(strategyAction?.exit?.type || "") : ""),
              exitPx,
              pnlUsd,
              realizedPnlUsd: rt.realizedPnlUsd,
              balanceUsd: rt.balanceUsd,
            });
            rt.entered = false;
            rt.side = null;
            rt.entryPx = null;
            rt.entryTsMs = null;
            rt.shares = null;
            rt.notionalUsd = null;
            // Re-arm per-bot strategy state for momentum-family only.
            // Locker-family is intentionally stateful across trades in-session.
            if (strategyProfileOf(instance.strategyId).resetRuntimeOnTradeExit) {
              botStrategyRuntimes.delete(instance.instanceId);
            }
          }
        }
      }
    }

    const next = botInstances.get(instance.instanceId);
    if (next) {
      next.status = next.watchOnly ? "watching" : "running";
      next.lastError = null;
      next.latestPnlUsd = Number(rt.realizedPnlUsd.toFixed(6));
      next.latestBalanceUsd = Number(rt.balanceUsd.toFixed(6));
      botInstances.set(next.instanceId, next);
    }
    botRuntimes.set(rt.instanceId, rt);
    broadcast({
      type: "bot_runtime",
      t: nowMs(),
      instanceId: instance.instanceId,
      runId: instance.runId,
      strategyId: instance.strategyId,
      marketSlug: instance.marketSlug,
      status: botInstances.get(instance.instanceId)?.status || instance.status,
      runtime: {
        marketSlug: rt.marketSlug,
        lastTickMs: rt.lastTickMs,
        upBid: rt.upBid,
        downBid: rt.downBid,
        entered: rt.entered,
        side: rt.side,
        entryPx: rt.entryPx,
        realizedPnlUsd: rt.realizedPnlUsd,
        balanceUsd: rt.balanceUsd,
        closedTrades: rt.closedTrades,
        sessionClosedTrades: Number(rt.sessionClosedTrades || 0),
        sessionLosses: Number(rt.sessionLosses || 0),
        wins: rt.wins,
        losses: rt.losses,
        lastAction: rt.lastAction,
        lastExitType: rt.lastExitType,
        errorCount: rt.errorCount,
      },
    });
    if (mutated) {
      writeBotRunSummary(instance, rt);
      persistMultiMarketState();
    }
  } catch (err: any) {
    rt.errorCount += 1;
    rt.lastTickMs = nowMs();
    rt.lastError = String(err?.message || err || "runtime error");
    botRuntimes.set(rt.instanceId, rt);
    const next = botInstances.get(instance.instanceId);
    if (next && next.status !== "stopped") {
      next.status = "error";
      next.lastError = rt.lastError;
      botInstances.set(next.instanceId, next);
    }
    persistMultiMarketState();
  }
}

async function tickAllBotRuntimes(): Promise<void> {
  if (botRuntimeTickInFlight) return;
  botRuntimeTickInFlight = true;
  try {
    const active = Array.from(botInstances.values()).filter((b) => isBotActive(b.status) && b.status !== "stopped");
    for (let i = 0; i < active.length; i += BOT_RUNTIME_CONCURRENCY) {
      const batch = active.slice(i, i + BOT_RUNTIME_CONCURRENCY);
      await Promise.all(batch.map(async (b) => {
        try {
          await tickBotRuntime(b);
        } catch {}
      }));
    }
  } finally {
    botRuntimeTickInFlight = false;
  }
}

function ensureTradeLogDir() {
  try {
    fs.mkdirSync(TRADE_LOG_DIR, { recursive: true });
  } catch {}
}
function tradeLogPathForNow() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return path.join(TRADE_LOG_DIR, `${LOG_PREFIX.TRADES}${yyyy}-${mm}-${dd}_trades.jsonl`);
}
function executionLogPathForNow() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return path.join(TRADE_LOG_DIR, `${LOG_PREFIX.EXECUTION}${yyyy}-${mm}-${dd}_execution.jsonl`);
}
function executionLogPathForDate(dateStr: string) {
  return path.join(TRADE_LOG_DIR, `${LOG_PREFIX.EXECUTION}${dateStr}_execution.jsonl`);
}
function executionLogPathLegacyForDate(dateStr: string) {
  return path.join(TRADE_LOG_DIR, `${dateStr}_execution.jsonl`);
}
function sessionTraceLogPathForRun(runId: number) {
  return path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SESSION_TRACE}session_trace_run_${runId}.jsonl`);
}
function appendTradeLog(obj: any) {
  ensureTradeLogDir();
  const line = JSON.stringify(obj) + "\n";
  fs.appendFile(tradeLogPathForNow(), line, (err) => {
    if (err) console.error(`[${isoNow()}] trade log append error:`, err);
  });
}
function appendExecutionLog(obj: any) {
  ensureTradeLogDir();
  const line = JSON.stringify(obj) + "\n";
  fs.appendFile(executionLogPathForNow(), line, (err) => {
    if (err) console.error(`[${isoNow()}] execution log append error:`, err);
  });
}

function appendSessionTraceLogPoint(obj: any) {
  ensureTradeLogDir();
  const line = JSON.stringify(obj) + "\n";
  fs.appendFile(sessionTraceLogPathForRun(SERVER_RUN_ID), line, (err) => {
    if (err) console.error(`[${isoNow()}] session trace append error:`, err);
  });
}

function recordSessionTracePoint(slug: string, tMs: number, upBid: number | null, downBid: number | null) {
  const s = String(slug || "");
  if (!s) return;
  const up = toFiniteOrNull(upBid);
  const down = toFiniteOrNull(downBid);
  if (!Number.isFinite(up) && !Number.isFinite(down)) return;
  const t = Number(tMs);
  if (!Number.isFinite(t)) return;

  const mem = sessionTraceMem.get(s) || { xMs: [], up: [], down: [], sourceFile: "session_trace_runlog" };
  const lastIdx = mem.xMs.length - 1;
  const lastT = lastIdx >= 0 ? Number(mem.xMs[lastIdx]) : NaN;
  if (Number.isFinite(lastT) && t <= lastT) {
    mem.up[lastIdx] = Number.isFinite(up) ? Number(up) : NaN;
    mem.down[lastIdx] = Number.isFinite(down) ? Number(down) : NaN;
    sessionTraceMem.set(s, mem);
    return;
  }
  mem.xMs.push(t);
  mem.up.push(Number.isFinite(up) ? Number(up) : NaN);
  mem.down.push(Number.isFinite(down) ? Number(down) : NaN);
  if (mem.xMs.length > 1200) {
    mem.xMs.splice(0, mem.xMs.length - 1200);
    mem.up.splice(0, mem.up.length - 1200);
    mem.down.splice(0, mem.down.length - 1200);
  }
  sessionTraceMem.set(s, mem);

  const lastWrite = lastSessionTraceWriteMsBySlug.get(s) || 0;
  if (t - lastWrite < 900) return;
  lastSessionTraceWriteMsBySlug.set(s, t);
  appendSessionTraceLogPoint({
    t,
    iso: new Date(t).toISOString(),
    runId: SERVER_RUN_ID,
    slug: s,
    upBid: Number.isFinite(up) ? Number(up) : null,
    downBid: Number.isFinite(down) ? Number(down) : null,
  });
}

function listSessionTraceLogFiles(): string[] {
  ensureTradeLogDir();
  return fs
    .readdirSync(TRADE_LOG_DIR)
    .filter((f) => /^(?:05_)?session_trace_run_\d+\.jsonl$/.test(f))
    .map((f) => path.join(TRADE_LOG_DIR, f))
    .sort();
}

function readSessionTracesBySlug(
  sinceMs: number,
  runId?: number | null
): Map<string, { xMs: number[]; up: number[]; down: number[]; sourceFile: string }> {
  const out = new Map<string, { xMs: number[]; up: number[]; down: number[]; sourceFile: string }>();
  const runIdFilter =
    runId == null
      ? null
      : (Number.isFinite(Number(runId)) && Number(runId) > 0 ? Number(runId) : null);
  const files = listSessionTraceLogFiles();
  for (const fp of files) {
    let raw = "";
    try {
      raw = fs.readFileSync(fp, "utf8");
    } catch {
      continue;
    }
    const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    for (const ln of lines) {
      let row: any = null;
      try {
        row = JSON.parse(ln);
      } catch {
        continue;
      }
      const t = Number(row?.t);
      if (!Number.isFinite(t) || t < sinceMs) continue;
      if (runIdFilter != null && Number(row?.runId) !== Number(runIdFilter)) continue;
      const slug = String(row?.slug || "");
      if (!slug) continue;
      const up = toFiniteOrNull(row?.upBid);
      const down = toFiniteOrNull(row?.downBid);
      if (!Number.isFinite(up) && !Number.isFinite(down)) continue;
      const cur = out.get(slug) || { xMs: [], up: [], down: [], sourceFile: "session_trace_runlog" };
      cur.xMs.push(t);
      cur.up.push(Number.isFinite(up) ? Number(up) : NaN);
      cur.down.push(Number.isFinite(down) ? Number(down) : NaN);
      out.set(slug, cur);
    }
  }

  // Use in-memory trace ONLY for the currently active open session.
  // All historical sessions must be sourced from persisted session_trace_run_*.jsonl files.
  const activeSlug = String((current as any)?.slug || "").trim();
  const activeEndMs = Number((current as any)?.endMs);
  const activeOpen = !!activeSlug && Number.isFinite(activeEndMs) && Date.now() < activeEndMs;
  const allowMemForRun = runIdFilter == null || Number(runIdFilter) === Number(SERVER_RUN_ID);
  for (const [slug, mem] of sessionTraceMem.entries()) {
    if (!allowMemForRun) continue;
    if (!activeOpen) continue;
    if (String(slug || "") !== activeSlug) continue;
    const cur = out.get(slug) || { xMs: [], up: [], down: [], sourceFile: "session_trace_runlog" };
    for (let i = 0; i < mem.xMs.length; i++) {
      const t = Number(mem.xMs[i]);
      if (!Number.isFinite(t) || t < sinceMs) continue;
      cur.xMs.push(t);
      cur.up.push(Number.isFinite(mem.up[i]) ? Number(mem.up[i]) : NaN);
      cur.down.push(Number.isFinite(mem.down[i]) ? Number(mem.down[i]) : NaN);
    }
    out.set(slug, cur);
  }

  for (const [slug, tr] of out.entries()) {
    const zipped = tr.xMs.map((t, i) => ({ t, up: tr.up[i], down: tr.down[i] })).filter((p) => Number.isFinite(p.t));
    zipped.sort((a, b) => a.t - b.t);
    const dedup: Array<{ t: number; up: number; down: number }> = [];
    for (const p of zipped) {
      const last = dedup[dedup.length - 1];
      if (last && Math.abs(last.t - p.t) < 1) {
        last.up = p.up;
        last.down = p.down;
        continue;
      }
      dedup.push(p);
    }
    out.set(slug, {
      xMs: dedup.map((p) => p.t),
      up: dedup.map((p) => p.up),
      down: dedup.map((p) => p.down),
      sourceFile: "session_trace_runlog",
    });
  }

  return out;
}

function listTradeLogFiles(): string[] {
  ensureTradeLogDir();
  return fs
    .readdirSync(TRADE_LOG_DIR)
    .filter((f) => /^(?:03_)?\d{4}-\d{2}-\d{2}_trades\.jsonl$/.test(f))
    .map((f) => path.join(TRADE_LOG_DIR, f))
    .sort();
}

type RunPathName = "MOMENTUM_FIRST" | "HC_SOLO" | "HC_SECOND_SAME" | "HC_SECOND_OPPOSITE";
type RunPathTrade = {
  slug: string;
  side: OutcomeSide | null;
  entryPx: number | null;
  exitPx: number | null;
  pnlUsd: number | null;
  cumulativeBalanceUsd: number | null;
  entryIso: string | null;
  exitIso: string | null;
};
type RunPathSummary = {
  runId: number;
  tradeCount: number;
  totalPnlUsd: number;
  byPath: Record<RunPathName, { count: number; totalPnlUsd: number; trades: RunPathTrade[] }>;
};

function summarizeRunPathsFromTradeRows(rows: any[], runId: number): RunPathSummary {
  type Sess = {
    slug: string;
    base: {
      side: OutcomeSide | null;
      entryPx: number | null;
      exitPx: number | null;
      pnlUsd: number | null;
      balanceUsd: number | null;
      entryTs: number | null;
      exitTs: number | null;
      entryIso: string | null;
      exitIso: string | null;
    };
    hc: {
      side: OutcomeSide | null;
      entryPx: number | null;
      exitPx: number | null;
      pnlUsd: number | null;
      balanceUsd: number | null;
      entryTs: number | null;
      exitTs: number | null;
      entryIso: string | null;
      exitIso: string | null;
    };
  };

  const bySession = new Map<string, Sess>();
  for (const r of rows) {
    const rr = Number(r?.runId);
    if (!Number.isFinite(rr) || rr !== runId) continue;
    const slug = String(r?.market?.slug || r?.st?.marketSlug || "");
    if (!slug) continue;
    const exec = r?.execution;
    const mode = String(exec?.mode || "").toUpperCase();
    const phase = String(exec?.phase || "").toLowerCase();
    if (mode !== "BASE" && mode !== "NO_CROSS_WINNER") continue;
    if (phase !== "entry_fill" && phase !== "exit_fill" && phase !== "stop_fill") continue;

    const s =
      bySession.get(slug) ||
      {
        slug,
        base: { side: null, entryPx: null, exitPx: null, pnlUsd: null, balanceUsd: null, entryTs: null, exitTs: null, entryIso: null, exitIso: null },
        hc: { side: null, entryPx: null, exitPx: null, pnlUsd: null, balanceUsd: null, entryTs: null, exitTs: null, entryIso: null, exitIso: null },
      };
    const lane = mode === "BASE" ? s.base : s.hc;
    const t = Number(r?.t);
    const iso = String(r?.iso || "");
    const sideRaw = String(exec?.side || r?.st?.side || "").toUpperCase();
    const side = sideRaw === "UP" || sideRaw === "DOWN" ? (sideRaw as OutcomeSide) : null;

    if (phase === "entry_fill") {
      lane.entryTs = Number.isFinite(t) ? t : lane.entryTs;
      lane.entryIso = iso || lane.entryIso;
      lane.entryPx = toFiniteOrNull(exec?.actualPx ?? exec?.actualFillPx ?? r?.st?.entryPx);
      lane.side = side ?? lane.side;
    } else {
      lane.exitTs = Number.isFinite(t) ? t : lane.exitTs;
      lane.exitIso = iso || lane.exitIso;
      lane.exitPx = toFiniteOrNull(exec?.actualPx ?? exec?.actualFillPx ?? r?.st?.exitPx);
      lane.pnlUsd = toFiniteOrNull(r?.st?.pnlUsd);
      lane.balanceUsd = toFiniteOrNull(r?.st?.balanceUsd);
      lane.side = side ?? lane.side;
    }
    bySession.set(slug, s);
  }

  const emptyBucket = () => ({ count: 0, totalPnlUsd: 0, trades: [] as RunPathTrade[] });
  const byPath: Record<RunPathName, { count: number; totalPnlUsd: number; trades: RunPathTrade[] }> = {
    MOMENTUM_FIRST: emptyBucket(),
    HC_SOLO: emptyBucket(),
    HC_SECOND_SAME: emptyBucket(),
    HC_SECOND_OPPOSITE: emptyBucket(),
  };

  const pushTrade = (path: RunPathName, t: RunPathTrade) => {
    byPath[path].count += 1;
    if (Number.isFinite(Number(t.pnlUsd))) byPath[path].totalPnlUsd += Number(t.pnlUsd);
    byPath[path].trades.push(t);
  };

  for (const s of bySession.values()) {
    const hasBase = Number.isFinite(Number(s.base.entryTs)) && Number.isFinite(Number(s.base.exitTs));
    const hasHc = Number.isFinite(Number(s.hc.entryTs)) && Number.isFinite(Number(s.hc.exitTs));

    if (hasBase) {
      pushTrade("MOMENTUM_FIRST", {
        slug: s.slug,
        side: s.base.side,
        entryPx: s.base.entryPx,
        exitPx: s.base.exitPx,
        pnlUsd: s.base.pnlUsd,
        cumulativeBalanceUsd: s.base.balanceUsd,
        entryIso: s.base.entryIso,
        exitIso: s.base.exitIso,
      });
    }

    if (!hasHc) continue;
    if (!hasBase) {
      pushTrade("HC_SOLO", {
        slug: s.slug,
        side: s.hc.side,
        entryPx: s.hc.entryPx,
        exitPx: s.hc.exitPx,
        pnlUsd: s.hc.pnlUsd,
        cumulativeBalanceUsd: s.hc.balanceUsd,
        entryIso: s.hc.entryIso,
        exitIso: s.hc.exitIso,
      });
      continue;
    }

    const same = s.hc.side != null && s.base.side != null && s.hc.side === s.base.side;
    pushTrade(same ? "HC_SECOND_SAME" : "HC_SECOND_OPPOSITE", {
      slug: s.slug,
      side: s.hc.side,
      entryPx: s.hc.entryPx,
      exitPx: s.hc.exitPx,
      pnlUsd: s.hc.pnlUsd,
      cumulativeBalanceUsd: s.hc.balanceUsd,
      entryIso: s.hc.entryIso,
      exitIso: s.hc.exitIso,
    });
  }

  const tradeCount =
    byPath.MOMENTUM_FIRST.count + byPath.HC_SOLO.count + byPath.HC_SECOND_SAME.count + byPath.HC_SECOND_OPPOSITE.count;
  const totalPnlUsd =
    byPath.MOMENTUM_FIRST.totalPnlUsd +
    byPath.HC_SOLO.totalPnlUsd +
    byPath.HC_SECOND_SAME.totalPnlUsd +
    byPath.HC_SECOND_OPPOSITE.totalPnlUsd;

  // Keep trades chronologically ordered inside each path.
  for (const k of Object.keys(byPath) as RunPathName[]) {
    byPath[k].trades.sort((a, b) => Number(new Date(a.entryIso || 0)) - Number(new Date(b.entryIso || 0)));
    byPath[k].totalPnlUsd = round4(byPath[k].totalPnlUsd);
  }

  return {
    runId,
    tradeCount,
    totalPnlUsd: round4(totalPnlUsd),
    byPath,
  };
}

function readJsonlFiles(files: string[]): any[] {
  const rows: any[] = [];
  for (const fp of files) {
    let raw = "";
    try {
      raw = fs.readFileSync(fp, "utf8");
    } catch {
      continue;
    }
    const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    for (const ln of lines) {
      try {
        rows.push(JSON.parse(ln));
      } catch {}
    }
  }
  return rows;
}

function buildEtSessionLabel(tsSec: number): string {
  const d = new Date(tsSec * 1000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  }).formatToParts(d);
  const hour = parts.find((p) => p.type === "hour")?.value || "12";
  const minute = parts.find((p) => p.type === "minute")?.value || "00";
  const dayPeriod = (parts.find((p) => p.type === "dayPeriod")?.value || "AM").toUpperCase();
  return `${hour}_${minute}${dayPeriod}`;
}

function resolveSessionCsvPathFromSlug(slug: string): string | null {
  const m = String(slug || "").match(/-(\d{10})$/);
  if (!m) return null;
  const startSec = Number(m[1]);
  if (!Number.isFinite(startSec)) return null;
  const endSec = startSec + 300;

  const d = new Date(startSec * 1000);
  const month = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "long",
  }).format(d);
  const day = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    day: "numeric",
  }).format(d);
  const fromLabel = buildEtSessionLabel(startSec);
  const toLabel = buildEtSessionLabel(endSec);
  const fileName = `Bitcoin Up or Down - ${month} ${day}, ${fromLabel}-${toLabel} ET.csv`;

  const envDir = String(process.env.BTC_5M_DATA_DIR || "").trim();
  const candidates = [
    envDir ? path.resolve(envDir, fileName) : null,
    path.resolve("/Users/aliathar/btc_5m_data", fileName),
    path.resolve(process.cwd(), "btc_5m_data", fileName),
    path.resolve(__dirname, "../btc_5m_data", fileName),
  ].filter((p): p is string => !!p);
  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }
  return null;
}

function readSessionTraceFromCsv(slug: string) {
  const filePath = resolveSessionCsvPathFromSlug(slug);
  if (!filePath) return null;
  const m = String(slug || "").match(/-(\d{10})$/);
  const startSec = m ? Number(m[1]) : NaN;
  if (!Number.isFinite(startSec)) return null;

  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    if (lines.length < 2) return null;
    const xMs: number[] = [];
    const up: number[] = [];
    const down: number[] = [];
    const parsePx = (raw: any) => {
      const s = String(raw ?? "").trim();
      if (!s.length) return NaN;
      const n = Number(s);
      return Number.isFinite(n) ? n : NaN;
    };
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",");
      if (cols.length < 3) continue;
      const sec = Number(cols[0]);
      const upPx = parsePx(cols[1]);
      const downPx = parsePx(cols[2]);
      if (!Number.isFinite(sec) || (!Number.isFinite(upPx) && !Number.isFinite(downPx))) continue;
      xMs.push(Math.round((startSec + sec) * 1000));
      up.push(Number.isFinite(upPx) ? upPx : NaN);
      down.push(Number.isFinite(downPx) ? downPx : NaN);
    }
    if (!xMs.length) return null;
    return { xMs, up, down, sourceFile: filePath };
  } catch {
    return null;
  }
}

function isTraceUsable(
  tr: { xMs: number[]; up: number[]; down: number[] } | null | undefined,
  minPoints = 10,
  minSpanMs = 30_000
) {
  if (!tr || !Array.isArray(tr.xMs) || !Array.isArray(tr.up) || !Array.isArray(tr.down)) return false;
  if (tr.xMs.length < minPoints || tr.up.length !== tr.xMs.length || tr.down.length !== tr.xMs.length) return false;
  const t0 = Number(tr.xMs[0]);
  const t1 = Number(tr.xMs[tr.xMs.length - 1]);
  if (!Number.isFinite(t0) || !Number.isFinite(t1)) return false;
  return (t1 - t0) >= minSpanMs;
}

function buildSessionTraceFromRecordedLogs(rows: any[], slug: string, engine: Engine, runId?: number) {
  let entryPoint: { t: number; side: OutcomeSide; px: number } | null = null;
  let exitPoint: { t: number; side: OutcomeSide; px: number } | null = null;
  for (const r of rows) {
    if (r?.engine !== engine) continue;
    if (Number.isFinite(Number(runId)) && Number(r?.runId) !== Number(runId)) continue;
    if (String(r?.market?.slug || "") !== slug) continue;
    const t = Number(r?.t);
    if (!Number.isFinite(t)) continue;
    const phase = String(r?.execution?.phase || "").toLowerCase();
    if (phase !== "entry_fill" && phase !== "exit_fill" && phase !== "stop_fill") continue;

    const action = String(r?.action || "").toUpperCase();
    const sideRaw =
      String(r?.execution?.side || "").toUpperCase() ||
      (action.includes("_UP_") ? "UP" : action.includes("_DOWN_") ? "DOWN" : "");
    const side = sideRaw === "UP" || sideRaw === "DOWN" ? (sideRaw as OutcomeSide) : null;
    if (!side) continue;

    const actualPx = toFiniteOrNull(r?.execution?.actualPx ?? r?.execution?.actualFillPx ?? r?.actualFillPx ?? r?.fillPx);
    const stPx =
      side === "UP"
        ? toFiniteOrNull(r?.st?.entryPx ?? r?.st?.exitPx)
        : toFiniteOrNull(r?.st?.entryPx ?? r?.st?.exitPx);
    const sidePx = actualPx ?? stPx;
    if (!Number.isFinite(sidePx)) continue;
    const p = clamp01(sidePx);
    if (phase === "entry_fill") {
      if (!entryPoint || t < entryPoint.t) entryPoint = { t, side, px: p };
      continue;
    }
    if (phase === "exit_fill" || phase === "stop_fill") {
      if (!exitPoint || t > exitPoint.t) exitPoint = { t, side, px: p };
      continue;
    }
  }

  if (!entryPoint) return null;
  const endPoint = exitPoint ?? { t: entryPoint.t + 1000, side: entryPoint.side, px: entryPoint.px };

  const entryUp = entryPoint.side === "UP" ? entryPoint.px : clamp01(1 - entryPoint.px);
  const entryDn = entryPoint.side === "DOWN" ? entryPoint.px : clamp01(1 - entryPoint.px);
  const exitUp = endPoint.side === "UP" ? endPoint.px : clamp01(1 - endPoint.px);
  const exitDn = endPoint.side === "DOWN" ? endPoint.px : clamp01(1 - endPoint.px);

  const xMs = [entryPoint.t, endPoint.t];
  const up = [entryUp, exitUp];
  const down = [entryDn, exitDn];
  return { xMs, up, down, sourceFile: "trade_logs_fills" };
}

function buildTraceFromSessionEndpoints(sessionLike: any) {
  const sideRaw = String(sessionLike?.side || "").toUpperCase();
  const side = sideRaw === "UP" || sideRaw === "DOWN" ? (sideRaw as OutcomeSide) : null;
  const t0Raw = Number(sessionLike?.entryTsMs ?? sessionLike?.attemptTsMs);
  if (!Number.isFinite(t0Raw)) return null;
  const t0 = Number(t0Raw);
  const t1Raw = Number(sessionLike?.exitTsMs);
  const t1 = Number.isFinite(t1Raw) && t1Raw > t0 ? Number(t1Raw) : (t0 + 1000);
  const entryRaw = toFiniteOrNull(sessionLike?.entryPx ?? sessionLike?.signalPx ?? sessionLike?.intendedPx);
  const exitRaw = toFiniteOrNull(sessionLike?.exitPx ?? entryRaw);
  if (!Number.isFinite(Number(entryRaw)) || !Number.isFinite(Number(exitRaw))) return null;
  const entry = clamp01(Number(entryRaw));
  const exit = clamp01(Number(exitRaw));
  if (side === "UP") {
    return {
      xMs: [t0, t1],
      up: [entry, exit],
      down: [clamp01(1 - entry), clamp01(1 - exit)],
      sourceFile: "session_endpoints_fallback",
    };
  }
  if (side === "DOWN") {
    return {
      xMs: [t0, t1],
      up: [clamp01(1 - entry), clamp01(1 - exit)],
      down: [entry, exit],
      sourceFile: "session_endpoints_fallback",
    };
  }
  return {
    xMs: [t0, t1],
    up: [entry, exit],
    down: [clamp01(1 - entry), clamp01(1 - exit)],
    sourceFile: "session_endpoints_fallback",
  };
}

function inferSessionStartMsFromSlug(slugLike: any): number | null {
  const m = String(slugLike || "").match(/-(\d{10})$/);
  if (!m) return null;
  const sec = Number(m[1]);
  if (!Number.isFinite(sec)) return null;
  const ms = sec * 1000;
  return Number.isFinite(ms) ? ms : null;
}

function inferSessionDurationMsFromSlug(slugLike: any): number {
  const s = String(slugLike || "").toLowerCase();
  if (s.includes("updown-15m")) return 15 * 60_000;
  if (s.includes("updown-hourly")) return 60 * 60_000;
  if (s.includes("updown-daily")) return 24 * 60 * 60_000;
  return 5 * 60_000;
}

function buildNeutralTraceFromSlug(sessionLike: any) {
  const slug = String(sessionLike?.slug || "");
  const startMs =
    inferSessionStartMsFromSlug(slug) ??
    toFiniteOrNull(sessionLike?.attemptTsMs ?? sessionLike?.entryTsMs ?? sessionLike?.exitTsMs);
  if (!Number.isFinite(Number(startMs))) return null;
  const t0 = Number(startMs);
  const durationMs = inferSessionDurationMsFromSlug(slug);
  const t1 = Math.max(t0 + 1000, t0 + durationMs - 1000);
  return {
    xMs: [t0, t1],
    up: [0.5, 0.5],
    down: [0.5, 0.5],
    sourceFile: "neutral_slug_fallback",
  };
}

type ServerRunRow = { runId: number; startMs: number; startIso: string };
type ServerRunsMeta = { lastRunId: number; runs: ServerRunRow[] };

function readRunsMeta(): ServerRunsMeta {
  ensureTradeLogDir();
  try {
    const pathToRead = fs.existsSync(RUNS_META_PATH)
      ? RUNS_META_PATH
      : fs.existsSync(LEGACY_RUNS_META_PATH)
      ? LEGACY_RUNS_META_PATH
      : "";
    if (!pathToRead) return { lastRunId: 0, runs: [] };
    const raw = fs.readFileSync(pathToRead, "utf8");
    const j = JSON.parse(raw || "{}");
    const runs = Array.isArray(j?.runs)
      ? j.runs
          .map((r: any) => ({
            runId: Number(r?.runId),
            startMs: Number(r?.startMs),
            startIso: String(r?.startIso || ""),
          }))
          .filter((r: ServerRunRow) => Number.isFinite(r.runId) && Number.isFinite(r.startMs) && !!r.startIso)
          .sort((a: ServerRunRow, b: ServerRunRow) => a.startMs - b.startMs)
      : [];
    const lastRunId = Number.isFinite(Number(j?.lastRunId)) ? Number(j.lastRunId) : (runs.length ? runs[runs.length - 1].runId : 0);
    return { lastRunId, runs };
  } catch {
    return { lastRunId: 0, runs: [] };
  }
}

function writeRunsMeta(meta: ServerRunsMeta) {
  ensureTradeLogDir();
  try {
    fs.writeFileSync(RUNS_META_PATH, JSON.stringify(meta, null, 2) + "\n");
  } catch (e: any) {
    console.warn(`[RUN META WRITE ERROR] ${String(e?.message ?? e)}`);
  }
}

function registerServerRun() {
  const meta = readRunsMeta();
  if (RESUME_LATEST_RUN_ON_RESTART && Array.isArray(meta.runs) && meta.runs.length > 0) {
    const latest = meta.runs
      .slice()
      .sort((a, b) => (Number(a.startMs) - Number(b.startMs)) || (Number(a.runId) - Number(b.runId)))
      .pop() as ServerRunRow | undefined;
    if (latest && Number.isFinite(Number(latest.runId)) && Number(latest.runId) > 0) {
      SERVER_RUN_ID = Number(latest.runId);
      CURRENT_RUN_START_MS = Number(latest.startMs) > 0 ? Number(latest.startMs) : SERVER_RUN_START_MS;
      CURRENT_RUN_START_ISO = String(latest.startIso || new Date(CURRENT_RUN_START_MS).toISOString());
      RESUMED_EXISTING_RUN = true;
      console.log(`[RUN META] resume latest run enabled -> reusing runId=${SERVER_RUN_ID}`);
      return;
    }
  }
  const nextRunId = Math.max(0, Number(meta.lastRunId || 0)) + 1;
  SERVER_RUN_ID = nextRunId;
  CURRENT_RUN_START_MS = SERVER_RUN_START_MS;
  CURRENT_RUN_START_ISO = SERVER_RUN_START_ISO;
  RESUMED_EXISTING_RUN = false;
  const runs = [...meta.runs, { runId: nextRunId, startMs: SERVER_RUN_START_MS, startIso: SERVER_RUN_START_ISO }];
  const trimmed = runs.slice(-300);
  writeRunsMeta({ lastRunId: nextRunId, runs: trimmed });
}

function restorePaperBalanceFromTradeLogs(runId: number): number | null {
  if (!Number.isFinite(Number(runId))) return null;
  try {
    const rows = readJsonlFiles(listTradeLogFiles())
      .filter((r) => Number(r?.runId) === Number(runId) && String(r?.engine || "") === "paper")
      .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
    let lastBal: number | null = null;
    for (const r of rows) {
      const b = toFiniteOrNull(r?.st?.balanceUsd);
      if (Number.isFinite(b)) lastBal = Number(b);
    }
    return Number.isFinite(Number(lastBal)) ? Number(lastBal) : null;
  } catch {
    return null;
  }
}

function resolveSinceMs(scopeRaw: string, sinceMsRaw: number, runsRaw: number): number {
  const scope = String(scopeRaw || "run").toLowerCase();
  const sinceMsInput = Number.isFinite(sinceMsRaw) && sinceMsRaw > 0 ? sinceMsRaw : 0;
  if (scope === "day") return sinceMsInput;
  if (scope === "runs") {
    const n = Math.max(1, Math.min(100, Number.isFinite(runsRaw) ? Math.floor(runsRaw) : 10));
    const meta = readRunsMeta();
    const list = Array.isArray(meta.runs) ? meta.runs.slice().sort((a, b) => a.startMs - b.startMs) : [];
    const idx = Math.max(0, list.length - n);
    const base = list.length ? Number(list[idx].startMs) : CURRENT_RUN_START_MS;
    return Math.max(base, sinceMsInput);
  }
  const base = CURRENT_RUN_START_MS;
  return Math.max(base, sinceMsInput || base);
}

function getRunMetaById(runId: number): { runId: number; startMs: number; startIso: string } | null {
  const rid = Number(runId);
  if (!Number.isFinite(rid)) return null;
  const meta = readRunsMeta();
  const list = Array.isArray(meta.runs) ? meta.runs : [];
  for (const r of list) {
    if (Number(r?.runId) !== rid) continue;
    const startMs = Number(r?.startMs);
    const startIso = String(r?.startIso || "");
    if (!Number.isFinite(startMs) || startMs <= 0 || !startIso) continue;
    return { runId: rid, startMs, startIso };
  }
  return null;
}

function resolveRunScope(req: any, scopeRaw: string, sinceMsRaw: number, runsRaw: number) {
  const scope = String(scopeRaw || "run").toLowerCase();
  const sinceMsInput = Number.isFinite(Number(sinceMsRaw)) && Number(sinceMsRaw) > 0 ? Number(sinceMsRaw) : 0;
  if (scope !== "run") {
    return {
      scope,
      runId: null as number | null,
      runStartMs: null as number | null,
      runStartIso: null as string | null,
      sinceMs: resolveSinceMs(scope, sinceMsRaw, runsRaw),
    };
  }
  const runIdReqRaw = Number(req?.query?.runId);
  const selectedRunId = Number.isFinite(runIdReqRaw) ? Math.floor(runIdReqRaw) : Number(SERVER_RUN_ID);
  const selectedMeta = getRunMetaById(selectedRunId);
  if (selectedMeta) {
    const effectiveSinceMs = Math.max(Number(selectedMeta.startMs), sinceMsInput || 0);
    return {
      scope,
      runId: selectedMeta.runId,
      runStartMs: selectedMeta.startMs,
      runStartIso: selectedMeta.startIso,
      sinceMs: effectiveSinceMs,
    };
  }
  return {
    scope,
    runId: Number(SERVER_RUN_ID),
    runStartMs: Number(CURRENT_RUN_START_MS),
    runStartIso: String(CURRENT_RUN_START_ISO),
    sinceMs: resolveSinceMs(scope, sinceMsRaw, runsRaw),
  };
}

function filterTradeRowsByScope(
  rows: any[],
  engine: Engine,
  scopeRaw: string,
  sinceMs: number,
  runIdForScope?: number | null
): any[] {
  const scope = String(scopeRaw || "run").toLowerCase();
  const runId = Number.isFinite(Number(runIdForScope)) ? Number(runIdForScope) : Number(SERVER_RUN_ID);
  return rows.filter((r) => {
    if (r?.engine !== engine) return false;
    const t = Number(r?.t);
    if (!Number.isFinite(t) || t < sinceMs) return false;
    if (scope === "run") {
      const rr = Number(r?.runId);
      if (!Number.isFinite(rr) || rr !== runId) return false;
    }
    return true;
  });
}

function buildHealthSnapshot() {
  return {
    runtimeVersion: LIVEFIX_RUNTIME_VERSION,
    runId: SERVER_RUN_ID,
    runStartMs: CURRENT_RUN_START_MS,
    runStartIso: CURRENT_RUN_START_ISO,
    strategyPath: STRATEGY_PATH,
    strategyLoaded: {
      paper: !!strategyPaperByLane.BASE && !!strategyPaperByLane.NO_CROSS_WINNER,
      live: !!strategyLive,
    },
    entryGuard: {
      active: entryBlockActive,
      blockedSessionSlug: entryBlockedSessionSlug,
    },
    liveEnabled: !!uiLive.enabled,
  };
}

function writeRuntimeStateSnapshot() {
  try {
    ensureTradeLogDir();
    const payload = {
      savedAtMs: nowMs(),
      savedAtIso: isoNow(),
      runtimeVersion: LIVEFIX_RUNTIME_VERSION,
      current,
      uiPaper,
      uiLive,
      pendingUiLive,
      paperAccount,
      liveAccount,
      stPaper,
      stLive,
      entryGuard: {
        entryBlockActive,
        entryBlockedSessionSlug,
      },
    };
    fs.writeFileSync(RUNTIME_SNAPSHOT_PATH, JSON.stringify(payload, null, 2) + "\n");
  } catch (e: any) {
    console.warn(`[SNAPSHOT WRITE ERROR] ${String(e?.message ?? e)}`);
  }
}

function loadRuntimeStateSnapshot() {
  try {
    const snapshotPath = fs.existsSync(RUNTIME_SNAPSHOT_PATH)
      ? RUNTIME_SNAPSHOT_PATH
      : fs.existsSync(LEGACY_RUNTIME_SNAPSHOT_PATH)
      ? LEGACY_RUNTIME_SNAPSHOT_PATH
      : "";
    if (!snapshotPath) return false;
    const raw = fs.readFileSync(snapshotPath, "utf8");
    const parsed = JSON.parse(raw || "{}");
    if (!parsed || typeof parsed !== "object") return false;

    // On fresh-paper starts, keep strategy/UI defaults from code instead of
    // reviving older per-run UI values from snapshot.
    if (!PAPER_RESET_ON_START) {
      if (parsed.uiPaper && typeof parsed.uiPaper === "object") uiPaper = { ...uiPaper, ...parsed.uiPaper, mode: "paper", enabled: true };
      if (parsed.uiLive && typeof parsed.uiLive === "object") uiLive = { ...uiLive, ...parsed.uiLive, mode: "live" };
      if (parsed.pendingUiLive && typeof parsed.pendingUiLive === "object") pendingUiLive = { ...uiLive, ...parsed.pendingUiLive, mode: "live" };
    }

    if (!PAPER_RESET_ON_START && parsed.paperAccount && Number.isFinite(Number(parsed.paperAccount.balanceUsd))) {
      paperAccount.balanceUsd = Number(parsed.paperAccount.balanceUsd);
    }
    if (parsed.liveAccount && Number.isFinite(Number(parsed.liveAccount.balanceUsd))) {
      liveAccount.balanceUsd = Number(parsed.liveAccount.balanceUsd);
    }

    if (!PAPER_RESET_ON_START && parsed.stPaper && typeof parsed.stPaper === "object") stPaper = { ...stPaper, ...parsed.stPaper };
    if (parsed.stLive && typeof parsed.stLive === "object") stLive = { ...stLive, ...parsed.stLive };

    if (parsed.current && typeof parsed.current === "object") {
      current = {
        slug: String(parsed.current.slug || current.slug),
        startMs: Number(parsed.current.startMs || current.startMs),
        endMs: Number(parsed.current.endMs || current.endMs),
        upToken: String(parsed.current.upToken || current.upToken),
        downToken: String(parsed.current.downToken || current.downToken),
        volumeUsd: Number.isFinite(Number(parsed.current.volumeUsd)) ? Number(parsed.current.volumeUsd) : current.volumeUsd,
      };
    }

    // Do not restore entry-guard from snapshot; each process start must re-arm
    // against the first observed session to prevent mid-session fresh entries.
    entryBlockActive = true;
    entryBlockedSessionSlug = null;
    // Never resume pending entry orders across process restarts.
    clearPendingEntry(stPaper);
    clearPendingEntry(stLive);
    stPaper.buyAttemptedThisSession = false;
    stPaper.buyAttemptedSessionSlug = null;
    stLive.buyAttemptedThisSession = false;
    stLive.buyAttemptedSessionSlug = null;

    if (PAPER_RESET_ON_START) {
      if (RESUMED_EXISTING_RUN) {
        const restoredBalance = restorePaperBalanceFromTradeLogs(SERVER_RUN_ID);
        resetPaperForNewRun();
        if (Number.isFinite(Number(restoredBalance))) {
          paperAccount.balanceUsd = Number(restoredBalance);
          stPaper.balanceUsd = Number(restoredBalance);
          console.log(
            `[PAPER RESTORE] resumed runId=${SERVER_RUN_ID} restored balance=${Number(restoredBalance).toFixed(4)} from trade logs`
          );
        } else {
          console.log(
            `[PAPER RESTORE] resumed runId=${SERVER_RUN_ID} had no recoverable balance; using fresh balance=100.00`
          );
        }
      } else {
        resetPaperForNewRun();
        console.log(`[PAPER RESET] fresh run: balance=100.00 (snapshot paper state ignored)`);
      }
    }

    uiPaper.enabled = true;
    stPaper.balanceUsd = paperAccount.balanceUsd;
    const baseLane = { ...stPaper };
    const hcLane: TradeState = {
      ...stPaper,
      entered: false,
      exited: false,
      side: null,
      entryTsMs: null,
      exitTsMs: null,
      entryPx: null,
      exitPx: null,
      shares: null,
      notionalUsd: null,
      grossPnlUsd: null,
      entryFeeUsd: null,
      exitFeeUsd: null,
      totalFeesUsd: null,
      pnlUsd: null,
      roiPct: null,
      holdSec: null,
      stopFallbackTriggered: false,
      positionTokenId: null,
      buyFilledThisSession: false,
      pendingEntryMode: null,
      pendingEntrySubtype: null,
      entryMode: null,
      entrySubtype: null,
    };
    paperLaneStates.BASE = baseLane;
    paperLaneStates.NO_CROSS_WINNER = hcLane;
    clearPendingEntry(paperLaneStates.BASE);
    clearPendingEntry(paperLaneStates.NO_CROSS_WINNER);
    syncPaperAggregateState();
    if (liveAccount.balanceUsd != null) stLive.balanceUsd = liveAccount.balanceUsd;
    console.log(
      `[SNAPSHOT LOADED] path=${snapshotPath} paperBal=${paperAccount.balanceUsd.toFixed(2)} ` +
        `liveBal=${liveAccount.balanceUsd ?? "—"}`
    );
    return true;
  } catch (e: any) {
    console.warn(`[SNAPSHOT LOAD ERROR] ${String(e?.message ?? e)}`);
    return false;
  }
}

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

function round4(n: number): number {
  return Math.round(n * 10000) / 10000;
}

const TAKER_EFFECTIVE_RATE_TABLE: Array<{ px: number; rate: number }> = [
  { px: 0.01, rate: 0.0 },
  { px: 0.05, rate: 0.0006 },
  { px: 0.1, rate: 0.0020 },
  { px: 0.15, rate: 0.0041 },
  { px: 0.2, rate: 0.0064 },
  { px: 0.25, rate: 0.0088 },
  { px: 0.3, rate: 0.0110 },
  { px: 0.35, rate: 0.0129 },
  { px: 0.4, rate: 0.0144 },
  { px: 0.45, rate: 0.0153 },
  { px: 0.5, rate: 0.0156 },
  { px: 0.55, rate: 0.0153 },
  { px: 0.6, rate: 0.0144 },
  { px: 0.65, rate: 0.0129 },
  { px: 0.7, rate: 0.0110 },
  { px: 0.75, rate: 0.0088 },
  { px: 0.8, rate: 0.0064 },
  { px: 0.85, rate: 0.0041 },
  { px: 0.9, rate: 0.0020 },
  { px: 0.95, rate: 0.0006 },
  { px: 0.99, rate: 0.0 },
];

type ExitFeeMode = "maker" | "taker" | "none";

function takerEffectiveRateAtPrice(pxRaw: number): number {
  const px = clamp01(Number(pxRaw));
  if (!Number.isFinite(px)) return 0;
  const rows = TAKER_EFFECTIVE_RATE_TABLE;
  if (px <= rows[0].px) return rows[0].rate;
  if (px >= rows[rows.length - 1].px) return rows[rows.length - 1].rate;
  for (let i = 1; i < rows.length; i++) {
    const a = rows[i - 1];
    const b = rows[i];
    if (px <= b.px) {
      const span = b.px - a.px;
      if (span <= 0) return b.rate;
      const t = (px - a.px) / span;
      return a.rate + (b.rate - a.rate) * t;
    }
  }
  return 0;
}

function calcTakerFeeUsd(pxRaw: number, sharesRaw: number): number {
  const px = Number(pxRaw);
  const shares = Number(sharesRaw);
  if (!Number.isFinite(px) || !Number.isFinite(shares) || px <= 0 || shares <= 0) return 0;
  const notional = px * shares;
  const fee = notional * takerEffectiveRateAtPrice(px);
  return Math.max(0, round4(fee));
}

function applyEntryFeeAccounting(st: TradeState, entryPxRaw: number, sharesRaw: number) {
  const entryFeeUsd = calcTakerFeeUsd(entryPxRaw, sharesRaw);
  st.entryFeeUsd = entryFeeUsd;
  st.exitFeeUsd = null;
  st.totalFeesUsd = entryFeeUsd;
}

function applyExitAccounting(st: TradeState, exitPxRaw: number, exitFeeMode: ExitFeeMode) {
  const entryPx = Number(st.entryPx);
  const exitPx = Number(exitPxRaw);
  const shares = Number(st.shares);
  const entryFeeUsd = Number.isFinite(Number(st.entryFeeUsd))
    ? Number(st.entryFeeUsd)
    : calcTakerFeeUsd(entryPx, shares);

  const grossPnlUsd =
    Number.isFinite(entryPx) && Number.isFinite(exitPx) && Number.isFinite(shares)
      ? (exitPx - entryPx) * shares
      : 0;
  const exitFeeUsd =
    exitFeeMode === "taker" && Number.isFinite(exitPx) && Number.isFinite(shares)
      ? calcTakerFeeUsd(exitPx, shares)
      : 0;
  const totalFeesUsd = entryFeeUsd + exitFeeUsd;
  const pnlUsd = grossPnlUsd - totalFeesUsd;

  st.grossPnlUsd = grossPnlUsd;
  st.entryFeeUsd = entryFeeUsd;
  st.exitFeeUsd = exitFeeUsd;
  st.totalFeesUsd = totalFeesUsd;
  st.pnlUsd = pnlUsd;
  st.roiPct = st.notionalUsd && st.notionalUsd > 0 ? (pnlUsd / st.notionalUsd) * 100 : null;

  return { grossPnlUsd, entryFeeUsd, exitFeeUsd, totalFeesUsd, pnlUsd };
}

function pctOf(numerator: number, denominator: number): number | null {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) return null;
  return numerator / denominator;
}

function computeBalanceDrawdownMetrics(engineRows: any[]) {
  const points = engineRows
    .map((r) => ({
      iso: String(r?.iso || ""),
      balanceUsd: Number(r?.st?.balanceUsd),
    }))
    .filter((p) => p.iso && Number.isFinite(p.balanceUsd)) as Array<{ iso: string; balanceUsd: number }>;

  if (!points.length) return null;

  let peakBal = points[0].balanceUsd;
  let peakIso = points[0].iso;
  let maxDdUsd = 0;
  let ddPeakBal = peakBal;
  let ddPeakIso = peakIso;
  let ddTroughBal = peakBal;
  let ddTroughIso = peakIso;

  for (const p of points) {
    if (p.balanceUsd > peakBal) {
      peakBal = p.balanceUsd;
      peakIso = p.iso;
    }
    const dd = peakBal - p.balanceUsd;
    if (dd > maxDdUsd) {
      maxDdUsd = dd;
      ddPeakBal = peakBal;
      ddPeakIso = peakIso;
      ddTroughBal = p.balanceUsd;
      ddTroughIso = p.iso;
    }
  }

  const dayMap = new Map<string, { open: number; low: number }>();
  for (const p of points) {
    const day = p.iso.slice(0, 10);
    if (!dayMap.has(day)) {
      dayMap.set(day, { open: p.balanceUsd, low: p.balanceUsd });
    } else {
      const v = dayMap.get(day)!;
      if (p.balanceUsd < v.low) v.low = p.balanceUsd;
    }
  }
  let maxDailyOpenToLowUsd = 0;
  let maxDailyOpenToLowPct: number | null = null;
  let maxDailyOpenToLowDay: string | null = null;
  for (const [day, v] of dayMap.entries()) {
    const dd = Math.max(0, v.open - v.low);
    const ddPct = pctOf(dd, v.open);
    if (dd > maxDailyOpenToLowUsd) {
      maxDailyOpenToLowUsd = dd;
      maxDailyOpenToLowPct = ddPct;
      maxDailyOpenToLowDay = day;
    }
  }

  return {
    overall: {
      maxDrawdownUsd: round2(maxDdUsd),
      maxDrawdownPct: maxDdUsd > 0 ? round4(pctOf(maxDdUsd, ddPeakBal) ?? 0) : 0,
      peakBalanceUsd: round2(ddPeakBal),
      peakIso: ddPeakIso,
      troughBalanceUsd: round2(ddTroughBal),
      troughIso: ddTroughIso,
    },
    daily: {
      maxOpenToLowUsd: round2(maxDailyOpenToLowUsd),
      maxOpenToLowPct: maxDailyOpenToLowPct == null ? null : round4(maxDailyOpenToLowPct),
      day: maxDailyOpenToLowDay,
    },
  };
}

function writeTradeSummarySnapshot() {
  ensureTradeLogDir();
  const sourceFile = tradeLogPathForNow();
  const outJson = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SUMMARY}summary_latest.json`);
  const outTxt = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SUMMARY}summary_latest.txt`);

  try {
    if (!fs.existsSync(sourceFile)) {
      const empty = {
        generatedAt: isoNow(),
        sourceFile,
        exists: false,
        message: "No trade log file for current date yet.",
      };
      fs.writeFileSync(outJson, JSON.stringify(empty, null, 2) + "\n");
      fs.writeFileSync(outTxt, `[${empty.generatedAt}] ${empty.message}\n`);
      return;
    }

    const raw = fs.readFileSync(sourceFile, "utf8");
    const rows = raw
      .split(/\r?\n/)
      .filter((x) => x.trim().length > 0)
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter((x) => !!x) as any[];

    const paperRows = rows.filter((r) => r.engine === "paper");
    const liveRows = rows.filter((r) => r.engine === "live");
    const enterActions = new Set(["ENTER_UP_PAPER", "ENTER_DOWN_PAPER", "ENTER_UP_LIVE", "ENTER_DOWN_LIVE"]);
    const exitActions = new Set([
      "EXIT_UP_PAPER",
      "EXIT_DOWN_PAPER",
      "STOP_UP_PAPER",
      "STOP_DOWN_PAPER",
      "EXIT_UP_LIVE",
      "EXIT_DOWN_LIVE",
      "STOP_UP_LIVE",
      "STOP_DOWN_LIVE",
    ]);

    const paperEnters = paperRows.filter((r) => enterActions.has(String(r.action)));
    const paperExits = paperRows.filter((r) => exitActions.has(String(r.action)));
    const liveEnters = liveRows.filter((r) => enterActions.has(String(r.action)));
    const liveExits = liveRows.filter((r) => exitActions.has(String(r.action)));

    const paperRealizedPnl = paperExits.reduce((acc, r) => acc + toNum(r?.st?.pnlUsd), 0);
    const liveRealizedPnl = liveExits.reduce((acc, r) => acc + toNum(r?.st?.pnlUsd), 0);
    const paperWins = paperExits.filter((r) => toNum(r?.st?.pnlUsd) > 0).length;
    const paperLosses = paperExits.filter((r) => toNum(r?.st?.pnlUsd) < 0).length;
    const liveWins = liveExits.filter((r) => toNum(r?.st?.pnlUsd) > 0).length;
    const liveLosses = liveExits.filter((r) => toNum(r?.st?.pnlUsd) < 0).length;

    const lastRow = rows.length ? rows[rows.length - 1] : null;
    const lastPaperExit = paperExits.length ? paperExits[paperExits.length - 1] : null;
    const lastLiveExit = liveExits.length ? liveExits[liveExits.length - 1] : null;
    const lastPaperCfg = (() => {
      for (let i = rows.length - 1; i >= 0; i--) {
        if (rows[i]?.engine === "paper" && rows[i]?.ui) return rows[i].ui;
      }
      return null;
    })();
    const lastLiveCfg = (() => {
      for (let i = rows.length - 1; i >= 0; i--) {
        if (rows[i]?.engine === "live" && rows[i]?.ui) return rows[i].ui;
      }
      return null;
    })();
    const paperDrawdown = computeBalanceDrawdownMetrics(paperRows);
    const liveDrawdown = computeBalanceDrawdownMetrics(liveRows);

    const summary = {
      generatedAt: isoNow(),
      sourceFile,
      preset: PRODUCTION_PRESET,
      totals: {
        events: rows.length,
        paperEvents: paperRows.length,
        liveEvents: liveRows.length,
      },
      paper: {
        enters: paperEnters.length,
        exits: paperExits.length,
        openPosition: paperEnters.length > paperExits.length,
        wins: paperWins,
        losses: paperLosses,
        winRate: paperExits.length ? paperWins / paperExits.length : null,
        realizedPnlUsd: round2(paperRealizedPnl),
        lastExit: lastPaperExit
          ? {
              iso: lastPaperExit.iso ?? null,
              action: lastPaperExit.action ?? null,
              pnlUsd: round2(toNum(lastPaperExit?.st?.pnlUsd)),
              balanceUsd: round2(toNum(lastPaperExit?.st?.balanceUsd)),
            }
          : null,
        config: lastPaperCfg
          ? {
              enabled: !!lastPaperCfg.enabled,
              entry: toNum(lastPaperCfg.entry),
              exit: toNum(lastPaperCfg.exit),
              stop: toNum(lastPaperCfg.stop),
              useStop: !!lastPaperCfg.useStop,
              minEntrySec: toNum(lastPaperCfg.minEntrySec),
              betUsd: toNum(lastPaperCfg.betUsd),
              maxBetUsd: toNum(lastPaperCfg.maxBetUsd),
            }
          : null,
        drawdown: paperDrawdown,
      },
      live: {
        enters: liveEnters.length,
        exits: liveExits.length,
        openPosition: liveEnters.length > liveExits.length,
        wins: liveWins,
        losses: liveLosses,
        winRate: liveExits.length ? liveWins / liveExits.length : null,
        realizedPnlUsd: round2(liveRealizedPnl),
        lastExit: lastLiveExit
          ? {
              iso: lastLiveExit.iso ?? null,
              action: lastLiveExit.action ?? null,
              pnlUsd: round2(toNum(lastLiveExit?.st?.pnlUsd)),
              balanceUsd: round2(toNum(lastLiveExit?.st?.balanceUsd)),
            }
          : null,
        config: lastLiveCfg
          ? {
              enabled: !!lastLiveCfg.enabled,
              entry: toNum(lastLiveCfg.entry),
              exit: toNum(lastLiveCfg.exit),
              stop: toNum(lastLiveCfg.stop),
              useStop: !!lastLiveCfg.useStop,
              minEntrySec: toNum(lastLiveCfg.minEntrySec),
              betUsd: toNum(lastLiveCfg.betUsd),
              maxBetUsd: toNum(lastLiveCfg.maxBetUsd),
            }
          : null,
        drawdown: liveDrawdown,
      },
      lastEvent: lastRow
        ? {
            iso: lastRow.iso ?? null,
            engine: lastRow.engine ?? null,
            action: lastRow.action ?? null,
          }
        : null,
    };

    fs.writeFileSync(outJson, JSON.stringify(summary, null, 2) + "\n");

    const txt =
      `Updated: ${summary.generatedAt}\n` +
      `Source: ${summary.sourceFile}\n` +
      `Events: total=${summary.totals.events} paper=${summary.totals.paperEvents} live=${summary.totals.liveEvents}\n` +
      `Paper: open=${summary.paper.openPosition} enters=${summary.paper.enters} exits=${summary.paper.exits} ` +
      `winRate=${summary.paper.winRate == null ? "n/a" : (summary.paper.winRate * 100).toFixed(2) + "%"} ` +
      `realizedPnl=${summary.paper.realizedPnlUsd.toFixed(2)}\n` +
      `Paper DD: overall=${summary.paper.drawdown ? `${(summary.paper.drawdown.overall.maxDrawdownPct * 100).toFixed(2)}%` : "n/a"} ` +
      `daily=${summary.paper.drawdown && summary.paper.drawdown.daily.maxOpenToLowPct != null ? `${(summary.paper.drawdown.daily.maxOpenToLowPct * 100).toFixed(2)}%` : "n/a"}\n` +
      `Paper last exit: ${summary.paper.lastExit ? `${summary.paper.lastExit.iso} ${summary.paper.lastExit.action} pnl=${summary.paper.lastExit.pnlUsd.toFixed(2)} bal=${summary.paper.lastExit.balanceUsd.toFixed(2)}` : "none"}\n` +
      `Live: open=${summary.live.openPosition} enters=${summary.live.enters} exits=${summary.live.exits} ` +
      `winRate=${summary.live.winRate == null ? "n/a" : (summary.live.winRate * 100).toFixed(2) + "%"} ` +
      `realizedPnl=${summary.live.realizedPnlUsd.toFixed(2)}\n` +
      `Live DD: overall=${summary.live.drawdown ? `${(summary.live.drawdown.overall.maxDrawdownPct * 100).toFixed(2)}%` : "n/a"} ` +
      `daily=${summary.live.drawdown && summary.live.drawdown.daily.maxOpenToLowPct != null ? `${(summary.live.drawdown.daily.maxOpenToLowPct * 100).toFixed(2)}%` : "n/a"}\n` +
      `Live last exit: ${summary.live.lastExit ? `${summary.live.lastExit.iso} ${summary.live.lastExit.action} pnl=${summary.live.lastExit.pnlUsd.toFixed(2)} bal=${summary.live.lastExit.balanceUsd.toFixed(2)}` : "none"}\n` +
      `Last event: ${summary.lastEvent ? `${summary.lastEvent.iso} ${summary.lastEvent.engine} ${summary.lastEvent.action}` : "none"}\n`;
    fs.writeFileSync(outTxt, txt);
  } catch (e: any) {
    const msg = `[${isoNow()}] summary write error: ${String(e?.message ?? e)}\n`;
    try {
      fs.writeFileSync(outTxt, msg);
    } catch {}
    console.error(msg.trim());
  }
}

function writeRunPathSummarySnapshot() {
  ensureTradeLogDir();
  const outLatest = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.RUN_PATH_SUMMARY}run_path_summary_latest.json`);
  try {
    const tradeRows = readJsonlFiles(listTradeLogFiles()).sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
    const runIds = Array.from(
      new Set(
        tradeRows
          .map((r) => Number(r?.runId))
          .filter((n) => Number.isFinite(n))
          .map((n) => Number(n))
      )
    ).sort((a, b) => a - b);

    const runs = runIds.map((rid) => summarizeRunPathsFromTradeRows(tradeRows, rid));
    const payload = {
      generatedAt: isoNow(),
      currentRunId: SERVER_RUN_ID,
      source: "trade_logs_real_fills",
      runs,
    };
    fs.writeFileSync(outLatest, JSON.stringify(payload, null, 2) + "\n");
    const current = runs.find((r) => r.runId === SERVER_RUN_ID);
    if (current) {
      const outCurrent = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.RUN_PATH_SUMMARY}run_${SERVER_RUN_ID}_path_summary.json`);
      fs.writeFileSync(
        outCurrent,
        JSON.stringify(
          {
            generatedAt: payload.generatedAt,
            source: payload.source,
            currentRunId: SERVER_RUN_ID,
            run: current,
          },
          null,
          2
        ) + "\n"
      );
    }
  } catch (e: any) {
    console.warn(`[RUN PATH SUMMARY ERROR] ${String(e?.message ?? e)}`);
  }
}

function avg(nums: number[]): number | null {
  if (!nums.length) return null;
  const s = nums.reduce((a, b) => a + b, 0);
  return s / nums.length;
}

function stdev(nums: number[]): number | null {
  if (!nums.length) return null;
  const m = avg(nums);
  if (!Number.isFinite(Number(m))) return null;
  const variance = nums.reduce((acc, x) => acc + Math.pow(x - Number(m), 2), 0) / nums.length;
  return Math.sqrt(variance);
}

function computeSeriesVolatility(points: number[]) {
  const clean = points.map((x) => Number(x)).filter((x) => Number.isFinite(x));
  if (clean.length < 2) {
    return {
      points: clean.length,
      start: clean.length ? clean[0] : null,
      end: clean.length ? clean[clean.length - 1] : null,
      min: clean.length ? Math.min(...clean) : null,
      max: clean.length ? Math.max(...clean) : null,
      range: null,
      meanAbsStep: null,
      stdevStep: null,
      realizedVar: null,
    };
  }
  const diffs: number[] = [];
  for (let i = 1; i < clean.length; i++) diffs.push(clean[i] - clean[i - 1]);
  const absSteps = diffs.map((d) => Math.abs(d));
  const realizedVar = diffs.reduce((acc, d) => acc + d * d, 0);
  const mn = Math.min(...clean);
  const mx = Math.max(...clean);
  return {
    points: clean.length,
    start: clean[0],
    end: clean[clean.length - 1],
    min: mn,
    max: mx,
    range: mx - mn,
    meanAbsStep: avg(absSteps),
    stdevStep: stdev(diffs),
    realizedVar,
  };
}

function writeSessionVolatilitySnapshot() {
  ensureTradeLogDir();
  const outLatest = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SESSION_VOLATILITY}session_volatility_latest.json`);
  const outCurrent = path.join(
    TRADE_LOG_DIR,
    `${LOG_PREFIX.SESSION_VOLATILITY}session_volatility_run_${SERVER_RUN_ID}.json`
  );
  try {
    const traces = readSessionTracesBySlug(CURRENT_RUN_START_MS - 5 * 60_000);
    const sessions: any[] = [];
    for (const [slug, tr] of traces.entries()) {
      const x = tr.xMs.map((t) => Number(t)).filter((t) => Number.isFinite(t));
      if (!x.length) continue;
      const upVol = computeSeriesVolatility(tr.up);
      const downVol = computeSeriesVolatility(tr.down);
      sessions.push({
        slug,
        startIso: new Date(Math.min(...x)).toISOString(),
        endIso: new Date(Math.max(...x)).toISOString(),
        durationSec: (Math.max(...x) - Math.min(...x)) / 1000,
        up: upVol,
        down: downVol,
      });
    }
    sessions.sort((a, b) => String(a.slug).localeCompare(String(b.slug)));
    const byUpRange = [...sessions]
      .filter((s) => Number.isFinite(Number(s?.up?.range)))
      .sort((a, b) => Number(b.up.range) - Number(a.up.range))
      .slice(0, 20)
      .map((s) => ({ slug: s.slug, upRange: s.up.range, downRange: s.down.range }));
    const byDownRange = [...sessions]
      .filter((s) => Number.isFinite(Number(s?.down?.range)))
      .sort((a, b) => Number(b.down.range) - Number(a.down.range))
      .slice(0, 20)
      .map((s) => ({ slug: s.slug, upRange: s.up.range, downRange: s.down.range }));

    const payload = {
      generatedAt: isoNow(),
      runId: SERVER_RUN_ID,
      source: "session_trace_runlog",
      sessions: sessions.length,
      summary: {
        avgUpRange: avg(
          sessions.map((s) => Number(s?.up?.range)).filter((x) => Number.isFinite(x))
        ),
        avgDownRange: avg(
          sessions.map((s) => Number(s?.down?.range)).filter((x) => Number.isFinite(x))
        ),
        avgUpStdevStep: avg(
          sessions.map((s) => Number(s?.up?.stdevStep)).filter((x) => Number.isFinite(x))
        ),
        avgDownStdevStep: avg(
          sessions.map((s) => Number(s?.down?.stdevStep)).filter((x) => Number.isFinite(x))
        ),
      },
      topVolatileByUpRange: byUpRange,
      topVolatileByDownRange: byDownRange,
      rows: sessions,
    };
    fs.writeFileSync(outLatest, JSON.stringify(payload, null, 2) + "\n");
    fs.writeFileSync(outCurrent, JSON.stringify(payload, null, 2) + "\n");
  } catch (e: any) {
    console.warn(`[SESSION VOL SNAPSHOT ERROR] ${String(e?.message ?? e)}`);
  }
}

function writeMissedSameSideHcSnapshot() {
  ensureTradeLogDir();
  const outLatestJson = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.MISSED_HC_SAME_SIDE}missed_same_side_hc_latest.json`);
  const outRunJson = path.join(
    TRADE_LOG_DIR,
    `${LOG_PREFIX.MISSED_HC_SAME_SIDE}run_${SERVER_RUN_ID}_missed_same_side_hc.json`
  );
  const outRunTsv = path.join(
    TRADE_LOG_DIR,
    `${LOG_PREFIX.MISSED_HC_SAME_SIDE}run_${SERVER_RUN_ID}_missed_same_side_hc.tsv`
  );
  try {
    const tradeRows = readJsonlFiles(listTradeLogFiles())
      .filter((r) => Number(r?.runId) === SERVER_RUN_ID && String(r?.engine || "") === "paper")
      .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
    const traceRows = readJsonlFiles(listSessionTraceLogFiles())
      .filter((r) => Number(r?.runId) === SERVER_RUN_ID)
      .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));

    const traceBySlug = new Map<string, Array<{ t: number; iso: string; upBid: number | null; downBid: number | null }>>();
    for (const r of traceRows) {
      const slug = String(r?.slug || "");
      if (!slug) continue;
      const t = Number(r?.t);
      if (!Number.isFinite(t)) continue;
      const arr = traceBySlug.get(slug) || [];
      arr.push({
        t,
        iso: String(r?.iso || new Date(t).toISOString()),
        upBid: toFiniteOrNull(r?.upBid),
        downBid: toFiniteOrNull(r?.downBid),
      });
      traceBySlug.set(slug, arr);
    }
    for (const [slug, arr] of traceBySlug.entries()) {
      arr.sort((a, b) => a.t - b.t);
      traceBySlug.set(slug, arr);
    }

    const baseEntries = tradeRows.filter(
      (r) => String(r?.strategyLane || "") === "BASE" && /^ENTER_(UP|DOWN)_PAPER$/.test(String(r?.action || ""))
    );
    const baseStops = tradeRows.filter(
      (r) => String(r?.strategyLane || "") === "BASE" && /^STOP_(UP|DOWN)_PAPER$/.test(String(r?.action || ""))
    );
    const hcEntries = tradeRows.filter(
      (r) => String(r?.strategyLane || "") === "NO_CROSS_WINNER" && /^ENTER_(UP|DOWN)_PAPER$/.test(String(r?.action || ""))
    );

    const rows: Array<{
      row: number;
      session: string;
      side: OutcomeSide;
      momentumStopIso: string;
      entryIso: string;
      entryPx: number;
      exitIso: string;
      exitPx: number;
      exitType: string;
      pnlUsd: number;
      cumulativePnlUsd: number;
    }> = [];

    const entryThr = Number(uiPaper.highConfidenceLead);
    const stopThr = Number(HC_SERVER_STOP_THR);
    const stopConfirmTicks = Number(HC_SERVER_STOP_CONFIRM_TICKS);
    const tpPx = 1.0;
    const betUsd = Number(uiPaper.betUsd);

    for (const be of baseEntries) {
      const slug = String(be?.market?.slug || "");
      const side = String(be?.st?.side || "") as OutcomeSide;
      if (!slug || (side !== "UP" && side !== "DOWN")) continue;

      const entryTsMs = Number(be?.st?.entryTsMs ?? be?.t);
      const stop = baseStops.find(
        (r) =>
          String(r?.market?.slug || "") === slug &&
          String(r?.st?.side || "") === side &&
          Number(r?.st?.entryTsMs ?? NaN) === entryTsMs
      );
      if (!stop) continue;

      const stopTs = Number(stop?.t);
      if (!Number.isFinite(stopTs)) continue;

      const points = (traceBySlug.get(slug) || []).filter((p) => p.t >= stopTs);
      if (!points.length) continue;

      const sameSidePx = (p: { upBid: number | null; downBid: number | null }) =>
        side === "UP" ? Number(p.upBid) : Number(p.downBid);
      const trigIdx = points.findIndex((p) => Number.isFinite(sameSidePx(p)) && sameSidePx(p) >= entryThr);
      if (trigIdx < 0) continue;

      const hcSameSideTaken = hcEntries.some(
        (h) => String(h?.market?.slug || "") === slug && String(h?.st?.side || "") === side && Number(h?.t) >= stopTs
      );
      if (hcSameSideTaken) continue;

      const entryPt = points[trigIdx];
      const entryPx = sameSidePx(entryPt);
      if (!(Number.isFinite(entryPx) && entryPx > 0)) continue;
      const shares = betUsd / entryPx;
      const entryFeeUsd = calcTakerFeeUsd(entryPx, shares);

      let stopStreak = 0;
      let exitPx = NaN;
      let exitIso = "";
      let exitType = "";
      for (let i = trigIdx + 1; i < points.length; i++) {
        const p = sameSidePx(points[i]);
        if (!Number.isFinite(p)) continue;
        if (p <= stopThr) stopStreak += 1;
        else stopStreak = 0;

        if (stopStreak >= stopConfirmTicks) {
          exitPx = stopThr;
          exitIso = points[i].iso;
          exitType = `STOP_C${stopConfirmTicks}`;
          break;
        }
        if (p >= tpPx) {
          exitPx = tpPx;
          exitIso = points[i].iso;
          exitType = "TP_1.0";
          break;
        }
      }

      if (!Number.isFinite(exitPx)) {
        const last = points[points.length - 1];
        const lastPx = sameSidePx(last);
        exitPx = Number.isFinite(lastPx) && lastPx > 0.5 ? 1 : 0;
        exitIso = String(last?.iso || "");
        exitType = "SETTLE_ROUND";
      }

      const grossPnlUsd = (exitPx - entryPx) * shares;
      const exitFeeUsd = exitType.startsWith("STOP") ? calcTakerFeeUsd(exitPx, shares) : 0;
      const pnlUsd = grossPnlUsd - entryFeeUsd - exitFeeUsd;

      rows.push({
        row: 0,
        session: slug,
        side,
        momentumStopIso: String(stop?.iso || ""),
        entryIso: String(entryPt.iso || ""),
        entryPx,
        exitIso,
        exitPx,
        exitType,
        pnlUsd,
        cumulativePnlUsd: 0,
      });
    }

    rows.sort((a, b) => a.entryIso.localeCompare(b.entryIso));
    let cum = 0;
    for (let i = 0; i < rows.length; i++) {
      cum += Number(rows[i].pnlUsd || 0);
      rows[i].row = i + 1;
      rows[i].cumulativePnlUsd = cum;
    }

    const payload = {
      generatedAt: isoNow(),
      runId: SERVER_RUN_ID,
      assumptions: {
        entryThresholdPx: entryThr,
        betUsd,
        stopPx: stopThr,
        stopConfirmTicks,
        tpPx,
      },
      totals: {
        count: rows.length,
        pnlUsd: rows.reduce((acc, r) => acc + Number(r.pnlUsd || 0), 0),
      },
      rows,
    };

    fs.writeFileSync(outLatestJson, JSON.stringify(payload, null, 2) + "\n");
    fs.writeFileSync(outRunJson, JSON.stringify(payload, null, 2) + "\n");

    const tsv = [
      [
        "row",
        "session",
        "side",
        "momentum_stop_iso",
        "entry_iso",
        "entry_px",
        "exit_iso",
        "exit_px",
        "exit_type",
        "pnl_usd",
        "cumulative_pnl_usd",
      ].join("\t"),
      ...rows.map((r) =>
        [
          r.row,
          r.session,
          r.side,
          r.momentumStopIso,
          r.entryIso,
          roundTo6(r.entryPx),
          r.exitIso,
          roundTo6(r.exitPx),
          r.exitType,
          roundTo6(r.pnlUsd),
          roundTo6(r.cumulativePnlUsd),
        ].join("\t")
      ),
    ].join("\n");
    fs.writeFileSync(outRunTsv, tsv + "\n");
  } catch (e: any) {
    console.warn(`[MISSED SAME-SIDE HC SNAPSHOT ERROR] ${String(e?.message ?? e)}`);
  }
}

function trailing24hDailyReport() {
  ensureTradeLogDir();
  const now = Date.now();
  const sinceMs = now - 24 * 60 * 60 * 1000;
  const outJson = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.DAILY_LATEST}daily_report_latest.json`);
  const outTxt = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.DAILY_LATEST}daily_report_latest.txt`);
  const stampedName = `${LOG_PREFIX.DAILY_STAMPED}${new Date().toISOString().slice(0, 10)}_daily_report.json`;
  const stampedPath = path.join(TRADE_LOG_DIR, stampedName);

  try {
    const tradeFiles = fs
      .readdirSync(TRADE_LOG_DIR)
      .filter((f) => /^(?:03_)?\d{4}-\d{2}-\d{2}_trades\.jsonl$/.test(f))
      .map((f) => path.join(TRADE_LOG_DIR, f))
      .sort();
    const execFiles = fs
      .readdirSync(TRADE_LOG_DIR)
      .filter((f) => /^(?:04_)?\d{4}-\d{2}-\d{2}_execution\.jsonl$/.test(f))
      .map((f) => path.join(TRADE_LOG_DIR, f))
      .sort();

    const readJsonl = (files: string[]) => {
      const rows: any[] = [];
      for (const fp of files) {
        const raw = fs.readFileSync(fp, "utf8");
        const lines = raw.split(/\r?\n/).filter((x) => x.trim().length > 0);
        for (const ln of lines) {
          try {
            rows.push(JSON.parse(ln));
          } catch {}
        }
      }
      return rows;
    };

    const tradeRowsAll = readJsonl(tradeFiles);
    const execRowsAll = readJsonl(execFiles);

    const tradeRows = tradeRowsAll.filter((r) => Number(r?.t) >= sinceMs && Number(r?.t) <= now);
    const execRows = execRowsAll.filter((r) => Number(r?.t) >= sinceMs && Number(r?.t) <= now);

    const byEngine = (engine: Engine) => tradeRows.filter((r) => r?.engine === engine);
    const paperRows = byEngine("paper");
    const liveRows = byEngine("live");
    const allRows = tradeRows;

    const exitSet = new Set([
      "EXIT_UP_PAPER",
      "EXIT_DOWN_PAPER",
      "STOP_UP_PAPER",
      "STOP_DOWN_PAPER",
      "EXIT_UP_LIVE",
      "EXIT_DOWN_LIVE",
      "STOP_UP_LIVE",
      "STOP_DOWN_LIVE",
    ]);
    const exitsAll = allRows.filter((r) => exitSet.has(String(r?.action || "")));
    const exitsPaper = paperRows.filter((r) => exitSet.has(String(r?.action || "")));
    const exitsLive = liveRows.filter((r) => exitSet.has(String(r?.action || "")));

    const pnlAll = exitsAll.reduce((acc, r) => acc + toNum(r?.st?.pnlUsd), 0);
    const pnlPaper = exitsPaper.reduce((acc, r) => acc + toNum(r?.st?.pnlUsd), 0);
    const pnlLive = exitsLive.reduce((acc, r) => acc + toNum(r?.st?.pnlUsd), 0);

    const holdAll = exitsAll.map((r) => Number(r?.st?.holdSec)).filter(Number.isFinite) as number[];
    const holdPaper = exitsPaper.map((r) => Number(r?.st?.holdSec)).filter(Number.isFinite) as number[];
    const holdLive = exitsLive.map((r) => Number(r?.st?.holdSec)).filter(Number.isFinite) as number[];

    const winsAll = exitsAll.filter((r) => toNum(r?.st?.pnlUsd) > 0).length;
    const winsPaper = exitsPaper.filter((r) => toNum(r?.st?.pnlUsd) > 0).length;
    const winsLive = exitsLive.filter((r) => toNum(r?.st?.pnlUsd) > 0).length;

    const ddPaper = computeBalanceDrawdownMetrics(paperRows);
    const ddLive = computeBalanceDrawdownMetrics(liveRows);
    const ddAll = computeBalanceDrawdownMetrics(allRows);

    const entryExec = execRows
      .map((r) => r?.execution)
      .filter((e) => !!e && String(e?.phase || "").startsWith("entry_"));
    const entryFillExec = execRows
      .map((r) => r?.execution)
      .filter((e) => !!e && String(e?.phase || "") === "entry_fill");

    const signalToIntended = entryExec
      .map((e) => Number(e?.assumedSlippagePx))
      .filter(Number.isFinite) as number[];
    const intendedToActual = entryFillExec
      .map((e) => Number(e?.intendedVsActualPx))
      .filter(Number.isFinite) as number[];
    const signalToActual = entryFillExec
      .map((e) => Number(e?.realizedSlippagePx))
      .filter(Number.isFinite) as number[];

    const report = {
      generatedAt: isoNow(),
      window: {
        kind: "trailing_24h",
        sinceMs,
        untilMs: now,
        sinceIso: new Date(sinceMs).toISOString(),
        untilIso: new Date(now).toISOString(),
      },
      totals: {
        tradeEvents: allRows.length,
        executionEvents: execRows.length,
      },
      pnlUsd: {
        total: round2(pnlAll),
        paper: round2(pnlPaper),
        live: round2(pnlLive),
      },
      drawdownPct: {
        overall: ddAll ? round4((ddAll.overall.maxDrawdownPct || 0) * 100) : null,
        paper: ddPaper ? round4((ddPaper.overall.maxDrawdownPct || 0) * 100) : null,
        live: ddLive ? round4((ddLive.overall.maxDrawdownPct || 0) * 100) : null,
        dailyOpenToLowOverall: ddAll?.daily?.maxOpenToLowPct == null ? null : round4(ddAll.daily.maxOpenToLowPct * 100),
      },
      fillQualityPx: {
        avgSignalToIntended: avg(signalToIntended) == null ? null : round4(avg(signalToIntended)!),
        avgIntendedToActual: avg(intendedToActual) == null ? null : round4(avg(intendedToActual)!),
        avgSignalToActual: avg(signalToActual) == null ? null : round4(avg(signalToActual)!),
        samples: {
          signalToIntended: signalToIntended.length,
          intendedToActual: intendedToActual.length,
          signalToActual: signalToActual.length,
        },
      },
      performance: {
        winRatePct: exitsAll.length ? round2((winsAll / exitsAll.length) * 100) : null,
        winRatePctPaper: exitsPaper.length ? round2((winsPaper / exitsPaper.length) * 100) : null,
        winRatePctLive: exitsLive.length ? round2((winsLive / exitsLive.length) * 100) : null,
        avgHoldSec: avg(holdAll) == null ? null : round2(avg(holdAll)!),
        avgHoldSecPaper: avg(holdPaper) == null ? null : round2(avg(holdPaper)!),
        avgHoldSecLive: avg(holdLive) == null ? null : round2(avg(holdLive)!),
        exitsCount: exitsAll.length,
      },
    };

    fs.writeFileSync(outJson, JSON.stringify(report, null, 2) + "\n");
    fs.writeFileSync(stampedPath, JSON.stringify(report, null, 2) + "\n");

    const txt =
      `Updated: ${report.generatedAt}\n` +
      `Window: ${report.window.sinceIso} -> ${report.window.untilIso}\n` +
      `P/L USD (24h): total=${report.pnlUsd.total.toFixed(2)} paper=${report.pnlUsd.paper.toFixed(2)} live=${report.pnlUsd.live.toFixed(2)}\n` +
      `Max DD %: overall=${report.drawdownPct.overall == null ? "n/a" : report.drawdownPct.overall.toFixed(2) + "%"} ` +
      `daily=${report.drawdownPct.dailyOpenToLowOverall == null ? "n/a" : report.drawdownPct.dailyOpenToLowOverall.toFixed(2) + "%"}\n` +
      `Fill quality (px): signal->intended=${report.fillQualityPx.avgSignalToIntended == null ? "n/a" : report.fillQualityPx.avgSignalToIntended.toFixed(4)} ` +
      `intended->actual=${report.fillQualityPx.avgIntendedToActual == null ? "n/a" : report.fillQualityPx.avgIntendedToActual.toFixed(4)} ` +
      `signal->actual=${report.fillQualityPx.avgSignalToActual == null ? "n/a" : report.fillQualityPx.avgSignalToActual.toFixed(4)}\n` +
      `Win rate: ${report.performance.winRatePct == null ? "n/a" : report.performance.winRatePct.toFixed(2) + "%"} ` +
      `| Avg hold: ${report.performance.avgHoldSec == null ? "n/a" : report.performance.avgHoldSec.toFixed(2) + "s"} ` +
      `| Exits: ${report.performance.exitsCount}\n`;
    fs.writeFileSync(outTxt, txt);

    return report;
  } catch (e: any) {
    const msg = `[${isoNow()}] daily report write error: ${String(e?.message ?? e)}\n`;
    try {
      fs.writeFileSync(outTxt, msg);
    } catch {}
    console.error(msg.trim());
    return null;
  }
}

function collectCurrentRunArtifactPaths(): string[] {
  const runId = Number(SERVER_RUN_ID);
  const paths = [
    tradeLogPathForNow(),
    executionLogPathForNow(),
    sessionTraceLogPathForRun(runId),
    RUNTIME_SNAPSHOT_PATH,
    RUNS_META_PATH,
    MULTI_STATE_DB_PATH,
    MULTI_STATE_PATH,
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SUMMARY}summary_latest.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SUMMARY}summary_latest.txt`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.DAILY_LATEST}daily_report_latest.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.DAILY_LATEST}daily_report_latest.txt`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.RUN_PATH_SUMMARY}run_path_summary_latest.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.RUN_PATH_SUMMARY}run_${runId}_path_summary.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SESSION_VOLATILITY}session_volatility_latest.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.SESSION_VOLATILITY}session_volatility_run_${runId}.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.MISSED_HC_SAME_SIDE}missed_same_side_hc_latest.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.MISSED_HC_SAME_SIDE}run_${runId}_missed_same_side_hc.json`),
    path.join(TRADE_LOG_DIR, `${LOG_PREFIX.MISSED_HC_SAME_SIDE}run_${runId}_missed_same_side_hc.tsv`),
    REMAINING_IMPL_STRATEGY_PATH,
  ];
  return Array.from(new Set(paths.map((p) => path.resolve(p))));
}

function collectBotRunArtifactPaths(instance: BotInstance): string[] {
  const dir = botRunDir(instance.runNum);
  const paths = [
    path.join(dir, "summary.json"),
    path.join(dir, "events.jsonl"),
  ];
  return Array.from(new Set(paths.map((p) => path.resolve(p))));
}

async function syncArtifactsToPara(reason: string) {
  if (!PARA_SYNC_ENABLED) return null;
  if (paraSyncInFlight) return null;
  paraSyncInFlight = true;
  try {
    const coreResult = await syncRunArtifactsToPara({
      runId: Number(SERVER_RUN_ID),
      areaFolderName: PARA_SYNC_AREA_FOLDER,
      localPaths: collectCurrentRunArtifactPaths(),
    });
    const botResults: Array<{ instanceId: string; runId: string; runNum: number; uploaded: number; skipped: number; runFolderLink: string | null }> = [];
    for (const b of botInstances.values()) {
      const localPaths = collectBotRunArtifactPaths(b);
      const r = await syncRunArtifactsToPara({
        runId: Number(b.runNum),
        areaFolderName: PARA_SYNC_AREA_FOLDER,
        localPaths,
      });
      botResults.push({
        instanceId: b.instanceId,
        runId: b.runId,
        runNum: b.runNum,
        uploaded: Array.isArray(r.uploaded) ? r.uploaded.length : 0,
        skipped: Array.isArray(r.skipped) ? r.skipped.length : 0,
        runFolderLink: r.runFolderLink || null,
      });
    }
    paraLastSyncAtMs = Date.now();
    paraLastSyncReason = reason;
    paraLastSyncError = null;
    paraLastSyncSummary = {
      runId: coreResult.runId,
      uploaded: Array.isArray(coreResult.uploaded) ? coreResult.uploaded.length : 0,
      skipped: Array.isArray(coreResult.skipped) ? coreResult.skipped.length : 0,
      runFolderId: coreResult.runFolderId,
      runFolderLink: coreResult.runFolderLink,
      botRuns: botResults,
    };
    return {
      ok: true,
      reason,
      core: coreResult,
      botRuns: botResults,
    };
  } catch (err: any) {
    paraLastSyncAtMs = Date.now();
    paraLastSyncReason = reason;
    paraLastSyncError = String(err?.message || err || "unknown error");
    console.warn(`[PARA SYNC ERROR] reason=${reason} ${paraLastSyncError}`);
    return null;
  } finally {
    paraSyncInFlight = false;
  }
}

// ===== Sizing (paper + live) =====
function calcNotionalUsd(params: {
  balanceUsd: number;
  betUsd: number;
  maxBetUsd: number;
  kellyOn: boolean;
  kellyMult: number;
  kellyCap: number;
  kellyFraction?: number;
}) {
  const { balanceUsd, betUsd, maxBetUsd, kellyOn, kellyMult, kellyCap } = params;

  if (!Number.isFinite(balanceUsd) || balanceUsd <= 0) return 0;

  // Kelly is considered OFF whenever multiplier <= 0, even if toggle is true.
  const kellyEnabled = !!kellyOn && Number.isFinite(kellyMult) && kellyMult > 0;
  if (!kellyEnabled) {
    return clamp(betUsd, 0, Math.min(maxBetUsd, balanceUsd));
  }

  const kf = clamp(params.kellyFraction ?? 0.10, 0, 1);
  const capFrac = Number.isFinite(kellyCap) && kellyCap > 0 ? kellyCap : 1;
  const frac = clamp(kellyMult * kf, 0, capFrac);
  const usd = balanceUsd * frac;
  return clamp(usd, 0, Math.min(maxBetUsd, balanceUsd));
}

function withDerivedUi(ui: UiConfig, balanceUsd: number | null | undefined) {
  const bal = Number.isFinite(Number(balanceUsd)) ? Number(balanceUsd) : 0;
  // Paper mode is fixed-notional sizing: use bet range directly and ignore Kelly math.
  const paperFixedBet = clamp(ui.betUsd, 0, Math.min(ui.maxBetUsd, bal));
  if (ui.mode === "paper") {
    return {
      ...ui,
      kellyActive: false,
      effectiveBetUsd: paperFixedBet,
    };
  }
  const effectiveBetUsd = calcNotionalUsd({
    balanceUsd: bal,
    betUsd: ui.betUsd,
    maxBetUsd: ui.maxBetUsd,
    kellyOn: ui.kellyOn,
    kellyMult: ui.kellyMult,
    kellyCap: ui.kellyCap,
  });
  const kellyActive = !!ui.kellyOn && Number.isFinite(ui.kellyMult) && ui.kellyMult > 0;
  return {
    ...ui,
    kellyActive,
    effectiveBetUsd,
  };
}

// ===== State =====
type UiConfig = {
  mode: Mode;
  enabled: boolean;

  entry: number;
  exit: number;
  stop: number;
  useStop: boolean;
  useStopTimeGate: boolean;
  stopStartSec: number;
  minEntrySec: number;

  kellyOn: boolean;
  kellyMult: number;
  kellyCap: number;
  betUsd: number;
  maxBetUsd: number;
  highConfidenceSpread: number;
  highConfidenceLead: number;
};

type TradeState = {
  marketSlug: string | null;
  marketStartMs: number | null;
  marketEndMs: number | null;
    exitInFlight?: boolean;

  // live guards
  enterInFlight?: boolean;
  lastEnterAttemptMs?: number;
  buyAttemptedThisSession?: boolean;
  buyAttemptedSessionSlug?: string | null;
  buyFilledThisSession?: boolean;
  tpOrderInFlight?: boolean;
  lastTpArmAttemptMs?: number;
  tpOrderId?: string | null;
  buyFilledAtMs?: number | null;
  firstSellAttemptAtMs?: number | null;

  entered: boolean;
  exited: boolean;
  side: OutcomeSide | null;

  entryTsMs: number | null;
  exitTsMs: number | null;

  entryPx: number | null;
  exitPx: number | null;

  shares: number | null;
  notionalUsd: number | null;
  grossPnlUsd: number | null;
  entryFeeUsd: number | null;
  exitFeeUsd: number | null;
  totalFeesUsd: number | null;

  pnlUsd: number | null;
  roiPct: number | null;
  holdSec: number | null;

  balanceUsd: number | null;

  stopFallbackTriggered: boolean;
  positionTokenId: string | null;

  // Paper/live pending entry order simulation (used by paper for TTL behavior)
  pendingEntrySide?: OutcomeSide | null;
  pendingEntrySignalPx?: number | null;
  pendingEntryLimitPx?: number | null;
  pendingEntryBaseLimitPx?: number | null;
  pendingEntryRepriceCount?: number | null;
  pendingEntryMode?: string | null;
  pendingEntrySubtype?: string | null;
  pendingEntryPlacedAtMs?: number | null;
  pendingEntryExpiresAtMs?: number | null;
  pendingEntryNotionalUsd?: number | null;
  pendingTpLimitPx?: number | null;
  pendingTpPlacedAtMs?: number | null;
  entryMode?: string | null;
  entrySubtype?: string | null;
};

const DEFAULTS: UiConfig = {
  mode: "paper",
  enabled: false,

  entry: PRODUCTION_PRESET.ui.entry,
  exit: PRODUCTION_PRESET.ui.exit,
  stop: PRODUCTION_PRESET.ui.stop,
  useStop: PRODUCTION_PRESET.ui.useStop,
  useStopTimeGate: PRODUCTION_PRESET.ui.useStopTimeGate,
  stopStartSec: PRODUCTION_PRESET.ui.stopStartSec,
  minEntrySec: PRODUCTION_PRESET.ui.minEntrySec,

  kellyOn: PRODUCTION_PRESET.ui.kellyOn,
  kellyMult: PRODUCTION_PRESET.ui.kellyMult,
  kellyCap: PRODUCTION_PRESET.ui.kellyCap,
  betUsd: PRODUCTION_PRESET.ui.betUsd,
  maxBetUsd: PRODUCTION_PRESET.ui.maxBetUsd,
  highConfidenceSpread: 0.18,
  highConfidenceLead: 0.70,
};

let uiPaper: UiConfig = { ...DEFAULTS, mode: "paper", enabled: true };
let uiLive: UiConfig = { ...DEFAULTS, mode: "live" };
let pendingUiLive: UiConfig | null = null;

const paperAccount = { balanceUsd: 100 };
const liveAccount = { balanceUsd: null as number | null, lastFetchMs: 0 };

let current = {
  slug: "" as string,
  startMs: 0,
  endMs: 0,
  upToken: "" as string,
  downToken: "" as string,
  volumeUsd: null as number | null,
};
let lastObservedBids: { upBid: number | null; downBid: number | null; tsMs: number | null } = {
  upBid: null,
  downBid: null,
  tsMs: null,
};

// Startup safety: never open a new position in the first session observed after restart.
let entryBlockActive = true;
let entryBlockedSessionSlug: string | null = null;
let entryGuardResyncSessionSlug: string | null = null;

let stPaper: TradeState = {
  marketSlug: null,
  marketStartMs: null,
  marketEndMs: null,
  exitInFlight: false,

  enterInFlight: false,
  lastEnterAttemptMs: 0,
  buyAttemptedThisSession: false,
  buyAttemptedSessionSlug: null,
  buyFilledThisSession: false,
  tpOrderInFlight: false,
  lastTpArmAttemptMs: 0,
  tpOrderId: null,
  buyFilledAtMs: null,
  firstSellAttemptAtMs: null,

  entered: false,
  exited: false,
  side: null,

  entryTsMs: null,
  exitTsMs: null,

  entryPx: null,
  exitPx: null,

  shares: null,
  notionalUsd: null,
  grossPnlUsd: null,
  entryFeeUsd: null,
  exitFeeUsd: null,
  totalFeesUsd: null,

  pnlUsd: null,
  roiPct: null,
  holdSec: null,

  balanceUsd: paperAccount.balanceUsd,

  stopFallbackTriggered: false,
  positionTokenId: null,
  pendingEntrySide: null,
  pendingEntrySignalPx: null,
  pendingEntryLimitPx: null,
  pendingEntryBaseLimitPx: null,
  pendingEntryRepriceCount: 0,
  pendingEntryMode: null,
  pendingEntrySubtype: null,
  pendingEntryPlacedAtMs: null,
  pendingEntryExpiresAtMs: null,
  pendingEntryNotionalUsd: null,
  pendingTpLimitPx: null,
  pendingTpPlacedAtMs: null,
  entryMode: null,
  entrySubtype: null,
};

let stLive: TradeState = {
  marketSlug: null,
  marketStartMs: null,
  marketEndMs: null,

  enterInFlight: false,
  lastEnterAttemptMs: 0,
  buyAttemptedThisSession: false,
  buyAttemptedSessionSlug: null,
  buyFilledThisSession: false,
  tpOrderInFlight: false,
  lastTpArmAttemptMs: 0,
  tpOrderId: null,
  buyFilledAtMs: null,
  firstSellAttemptAtMs: null,
  exitInFlight: false,

  entered: false,
  exited: false,
  side: null,

  entryTsMs: null,
  exitTsMs: null,

  entryPx: null,
  exitPx: null,

  shares: null,
  notionalUsd: null,
  grossPnlUsd: null,
  entryFeeUsd: null,
  exitFeeUsd: null,
  totalFeesUsd: null,

  pnlUsd: null,
  roiPct: null,
  holdSec: null,

  balanceUsd: liveAccount.balanceUsd,

  stopFallbackTriggered: false,
  positionTokenId: null,
  pendingEntrySide: null,
  pendingEntrySignalPx: null,
  pendingEntryLimitPx: null,
  pendingEntryBaseLimitPx: null,
  pendingEntryRepriceCount: 0,
  pendingEntryMode: null,
  pendingEntrySubtype: null,
  pendingEntryPlacedAtMs: null,
  pendingEntryExpiresAtMs: null,
  pendingEntryNotionalUsd: null,
  pendingTpLimitPx: null,
  pendingTpPlacedAtMs: null,
  entryMode: null,
  entrySubtype: null,
};

let paperLaneStates: Record<StrategyLane, TradeState> = {
  BASE: { ...stPaper },
  NO_CROSS_WINNER: { ...stPaper },
};
let paperSessionLockLane: StrategyLane | null = null;

function laneHasPendingOrOpenPosition(s: TradeState): boolean {
  const hasPendingPlacedAt =
    s.pendingEntryPlacedAtMs != null &&
    Number.isFinite(Number(s.pendingEntryPlacedAtMs)) &&
    Number(s.pendingEntryPlacedAtMs) > 0;
  const hasPendingExpiresAt =
    s.pendingEntryExpiresAtMs != null &&
    Number.isFinite(Number(s.pendingEntryExpiresAtMs)) &&
    Number(s.pendingEntryExpiresAtMs) > 0;
  const hasPending =
    !!s.enterInFlight ||
    !!s.pendingEntrySide ||
    hasPendingPlacedAt ||
    hasPendingExpiresAt;
  const hasOpen = !!s.entered && !s.exited;
  return hasPending || hasOpen;
}

function hasOtherPaperLanePendingOrOpen(lane: StrategyLane): boolean {
  for (const other of PAPER_LANES) {
    if (other === lane) continue;
    if (laneHasPendingOrOpenPosition(paperLaneStates[other])) return true;
  }
  return false;
}

function refreshPaperSessionLock() {
  if (!paperSessionLockLane) return;
  const locked = paperLaneStates[paperSessionLockLane];
  if (!locked || !laneHasPendingOrOpenPosition(locked)) {
    paperSessionLockLane = null;
  }
}

function lockPaperSessionLane(lane: StrategyLane, _state: "pending" | "open") {
  paperSessionLockLane = lane;
}

function unlockPaperSessionLane(lane?: StrategyLane) {
  if (!lane || paperSessionLockLane === lane) {
    paperSessionLockLane = null;
  }
}

function isPaperSessionLockedForLane(lane: StrategyLane): boolean {
  refreshPaperSessionLock();
  return !!paperSessionLockLane && paperSessionLockLane !== lane;
}

function syncPaperAggregateState() {
  const candidates = PAPER_LANES.map((lane) => paperLaneStates[lane]);
  const active = candidates
    .filter((s) => s.entered && !s.exited)
    .sort((a, b) => Number(b.entryTsMs ?? 0) - Number(a.entryTsMs ?? 0));
  const ranked = (active.length ? active : candidates).slice().sort((a, b) => {
    const ta = Math.max(Number(a.exitTsMs ?? 0), Number(a.entryTsMs ?? 0));
    const tb = Math.max(Number(b.exitTsMs ?? 0), Number(b.entryTsMs ?? 0));
    return tb - ta;
  });
  const src = ranked[0] ?? stPaper;
  stPaper = { ...src, balanceUsd: paperAccount.balanceUsd };
}

function resetPaperForNewRun() {
  paperAccount.balanceUsd = 100;
  stPaper.marketSlug = null;
  stPaper.marketStartMs = null;
  stPaper.marketEndMs = null;
  stPaper.exitInFlight = false;

  stPaper.enterInFlight = false;
  stPaper.lastEnterAttemptMs = 0;
  stPaper.buyAttemptedThisSession = false;
  stPaper.buyAttemptedSessionSlug = null;
  stPaper.buyFilledThisSession = false;
  stPaper.tpOrderInFlight = false;
  stPaper.lastTpArmAttemptMs = 0;
  stPaper.tpOrderId = null;
  stPaper.buyFilledAtMs = null;
  stPaper.firstSellAttemptAtMs = null;

  stPaper.entered = false;
  stPaper.exited = false;
  stPaper.side = null;
  stPaper.entryTsMs = null;
  stPaper.exitTsMs = null;
  stPaper.entryPx = null;
  stPaper.exitPx = null;
  stPaper.shares = null;
  stPaper.notionalUsd = null;
  stPaper.grossPnlUsd = null;
  stPaper.entryFeeUsd = null;
  stPaper.exitFeeUsd = null;
  stPaper.totalFeesUsd = null;
  stPaper.pnlUsd = null;
  stPaper.roiPct = null;
  stPaper.holdSec = null;
  stPaper.balanceUsd = paperAccount.balanceUsd;
  stPaper.stopFallbackTriggered = false;
  stPaper.positionTokenId = null;
  stPaper.pendingEntrySide = null;
  stPaper.pendingEntrySignalPx = null;
  stPaper.pendingEntryLimitPx = null;
  stPaper.pendingEntryBaseLimitPx = null;
  stPaper.pendingEntryRepriceCount = 0;
  stPaper.pendingEntryMode = null;
  stPaper.pendingEntrySubtype = null;
  stPaper.pendingEntryPlacedAtMs = null;
  stPaper.pendingEntryExpiresAtMs = null;
  stPaper.pendingEntryNotionalUsd = null;
  stPaper.pendingTpLimitPx = null;
  stPaper.pendingTpPlacedAtMs = null;
  stPaper.entryMode = null;
  stPaper.entrySubtype = null;
  paperLaneStates.BASE = { ...stPaper };
  paperLaneStates.NO_CROSS_WINNER = { ...stPaper };
  paperSessionLockLane = null;
  syncPaperAggregateState();
}

// ===== External Strategy Loading =====
type StrategyTick = {
  elapsedSec: number;
  upBid: number;
  downBid: number;
  isFinal?: boolean;
};

type StrategyAction =
  | null
  | {
      enter?: {
        side: OutcomeSide;
        entrySec?: number;
        entryPx?: number;
        limitPx?: number;
        ttlMs?: number;
        mode?: string;
        hcSubtype?: string;
        shares?: number;
        notionalUsd?: number;
      };
      exit?: {
        type: string;
        side: OutcomeSide;
        exitSec?: number;
        exitPx?: number;
        stopSource?: string;
        stopReason?: string;
        stopMeta?: Record<string, any>;
      };
    };

type Strategy = {
  onTick: (t: StrategyTick) => StrategyAction;
  snapshot?: () => any;
};

type StrategyFactory = (params: any) => Strategy;

function resolveStrategyPath(): string {
  const envPath = process.env.STRATEGY_PATH;
  if (envPath) {
    const direct = path.resolve(process.cwd(), envPath);
    if (fs.existsSync(direct)) return direct;

    const srcFallback = path.resolve(process.cwd(), "src", path.basename(envPath));
    if (fs.existsSync(srcFallback)) {
      console.warn(`[STRATEGY PATH FALLBACK] STRATEGY_PATH=${envPath} not found at ${direct}; using ${srcFallback}`);
      return srcFallback;
    }
    return direct; // keep original for clear error logging
  }

  const cwdFile = path.resolve(process.cwd(), "strategy_external.js");
  if (fs.existsSync(cwdFile)) return cwdFile;
  return path.resolve(process.cwd(), "src/strategy_external.js");
}

const STRATEGY_PATH = resolveStrategyPath();

function normalizeStrategyAction(raw: any): StrategyAction {
  if (!raw || typeof raw !== "object") return null;
  if (raw.enter || raw.exit) return raw as StrategyAction;

  const side = raw.side === "UP" || raw.side === "DOWN" ? raw.side : undefined;
  const rawType = String(raw.type ?? raw.action ?? raw.signal ?? "").toUpperCase();
  const pxNum = Number(raw.exitPx ?? raw.entryPx ?? raw.price ?? raw.px);

  const looksLikeEnter =
    raw.buy === true ||
    raw.shouldEnter === true ||
    raw.open === true ||
    raw.enterNow === true ||
    rawType.includes("BUY") ||
    rawType.includes("ENTER") ||
    rawType.includes("OPEN");

  const looksLikeExit =
    raw.sell === true ||
    raw.shouldExit === true ||
    raw.close === true ||
    raw.exitNow === true ||
    rawType.includes("SELL") ||
    rawType.includes("EXIT") ||
    rawType.includes("STOP") ||
    rawType.includes("LOSS") ||
    rawType.includes("REDEEM");

  if (!looksLikeEnter && !looksLikeExit) return null;

  const out: NonNullable<StrategyAction> = {};

  if (looksLikeEnter && side) {
    out.enter = {
      side: side as OutcomeSide,
      ...(Number.isFinite(pxNum) ? { entryPx: pxNum } : {}),
    };
  }

  if (looksLikeExit && side) {
    out.exit = {
      type: rawType || "EXIT",
      side: side as OutcomeSide,
      ...(Number.isFinite(pxNum) ? { exitPx: pxNum } : {}),
    };
  }

  return out.enter || out.exit ? out : null;
}

function tokenIdForSide(side: OutcomeSide): string {
  return side === "UP" ? current.upToken : current.downToken;
}
function roundTo6(n: number) {
  return Math.round(n * 1e6) / 1e6;
}
function floorTo6(n: number) {
  return Math.floor(n * 1e6) / 1e6;
}

function loadStrategyFactoryFromFile(filePath: string): StrategyFactory {
  const code = fs.readFileSync(filePath, "utf8");

  const sandbox: any = {
    console,
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    Buffer,
    process,
    require,
    module: { exports: {} },
    exports: {},
    window: {},
  };
  sandbox.exports = sandbox.module.exports;

  const context = vm.createContext(sandbox);
  const script = new vm.Script(code, { filename: filePath });
  script.runInContext(context);

  const modExp = sandbox.module.exports;

  const factory =
    (modExp && typeof modExp.makeStrategy === "function" && modExp.makeStrategy) ||
    (modExp && typeof modExp.default?.makeStrategy === "function" && modExp.default.makeStrategy) ||
    (sandbox.window?.TradeStrategy &&
      typeof sandbox.window.TradeStrategy.makeStrategy === "function" &&
      sandbox.window.TradeStrategy.makeStrategy) ||
    null;

  if (!factory) {
    throw new Error(
      `Strategy file did not expose makeStrategy(). Expected module.exports.makeStrategy or window.TradeStrategy.makeStrategy. path=${filePath}`
    );
  }

  return factory as StrategyFactory;
}

let strategyFactory: StrategyFactory | null = null;
let strategyPaper: Strategy | null = null;
let strategyLive: Strategy | null = null;
let strategyHotReloadArmed = false;
let strategyPaperByLane: Record<StrategyLane, Strategy | null> = {
  BASE: null,
  NO_CROSS_WINNER: null,
};

function buildStrategyParams(engine: Engine, lane?: StrategyLane) {
  const ui = engine === "paper" ? uiPaper : uiLive;
  const baseEnabled = lane === "NO_CROSS_WINNER" ? false : true;
  const noCrossWinnerEnabled = lane === "BASE" ? false : true;
  const laneMinEntrySec =
    lane === "NO_CROSS_WINNER"
      ? 0
      : ui.minEntrySec;
  return {
    buyThr: ui.entry,
    sellThr: ui.exit,
    bet: ui.betUsd,
    useStop: ui.useStop,
    stopThr: ui.stop,
    useStopTimeGate: ui.useStopTimeGate,
    stopStartSec: ui.stopStartSec,
    minEntrySec: laneMinEntrySec,
    noCrossWinnerEnabled,
    baseEnabled,
    noCrossMinSpread: ui.highConfidenceSpread,
    noCrossMinBid: ui.highConfidenceLead,
    noCrossEntryOffsetPx: 0.03,
    noCrossMaxLimitPx: 0.90,
    noCrossSellThr: 1.0,
    noCrossExitOnCross: true,
    noCrossSameMinBid: HC_SAME_SIDE_MIN_BID,
    noCrossSamePersistenceTicks: HC_SAME_SIDE_PERSISTENCE_TICKS,
    noCrossAfterStopMinSec: HC_SAME_SIDE_MIN_SEC_AFTER_STOP,
  };
}

function broadcast(obj: any) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    // @ts-ignore
    if (client.readyState === 1) {
      // @ts-ignore
      client.send(msg);
    }
  }
}

function reloadStrategyFactorySafe(reason: string) {
  try {
    strategyFactory = loadStrategyFactoryFromFile(STRATEGY_PATH);
    console.log(`[STRATEGY LOAD OK] reason=${reason} path=${STRATEGY_PATH}`);
    broadcast({ type: "status", t: nowMs(), status: `strategy factory loaded (${reason}) path=${path.basename(STRATEGY_PATH)}` });
  } catch (e: any) {
    strategyFactory = null;
    console.error(`[STRATEGY LOAD FAIL] reason=${reason} path=${STRATEGY_PATH} err=${String(e?.stack || e?.message || e)}`);
    broadcast({ type: "status", t: nowMs(), status: `strategy factory load FAILED: ${String(e?.message ?? e)}` });
  }
}

function reloadEngineStrategy(engine: Engine, reason: string) {
  try {
    // Load a fresh strategy module context per engine so paper/live never share
    // module-level mutable state from external strategy files.
    const factory = loadStrategyFactoryFromFile(STRATEGY_PATH);
    if (engine === "paper") {
      for (const lane of PAPER_LANES) {
        const params = buildStrategyParams(engine, lane);
        const strat = factory(params);
        if (!strat || typeof strat.onTick !== "function") throw new Error("makeStrategy() must return { onTick() }");
        strategyPaperByLane[lane] = strat;
        console.log(`[STRATEGY ENGINE OK] engine=${engine} lane=${lane} reason=${reason}`);
        broadcast({ type: "status", t: nowMs(), status: `strategy reloaded (${engine}:${lane}) (${reason})`, engine, lane, params });
      }
      strategyPaper = strategyPaperByLane.BASE;
    } else {
      const params = buildStrategyParams(engine);
      const strat = factory(params);
      if (!strat || typeof strat.onTick !== "function") throw new Error("makeStrategy() must return { onTick() }");
      strategyLive = strat;
      console.log(`[STRATEGY ENGINE OK] engine=${engine} reason=${reason}`);
      broadcast({ type: "status", t: nowMs(), status: `strategy reloaded (${engine}) (${reason})`, engine, params });
    }
  } catch (e: any) {
    if (engine === "paper") {
      strategyPaper = null;
      strategyPaperByLane.BASE = null;
      strategyPaperByLane.NO_CROSS_WINNER = null;
    } else strategyLive = null;

    console.error(`[STRATEGY ENGINE FAIL] engine=${engine} reason=${reason} err=${String(e?.stack || e?.message || e)}`);
    broadcast({ type: "status", t: nowMs(), status: `strategy reload FAILED (${engine}): ${String(e?.message ?? e)}`, engine });
  }
}

function rearmPaperLaneStrategy(lane: StrategyLane, reason: string) {
  try {
    if (!strategyFactory) strategyFactory = loadStrategyFactoryFromFile(STRATEGY_PATH);
    const params = buildStrategyParams("paper", lane);
    const strat = strategyFactory(params);
    if (!strat || typeof strat.onTick !== "function") throw new Error("makeStrategy() must return { onTick() }");
    strategyPaperByLane[lane] = strat;
    console.log(`[STRATEGY LANE REARM] lane=${lane} reason=${reason}`);
  } catch (e: any) {
    console.error(`[STRATEGY LANE REARM FAIL] lane=${lane} reason=${reason} err=${String(e?.message ?? e)}`);
  }
}

function setupStrategyHotReloadWatcher() {
  if (strategyHotReloadArmed) return;
  strategyHotReloadArmed = true;

  const strategyDir = path.dirname(STRATEGY_PATH);
  const strategyFile = path.basename(STRATEGY_PATH);
  let dirWatcher: fs.FSWatcher | null = null;
  let reloadTimer: NodeJS.Timeout | null = null;
  let lastSig = "";

  const readSig = (): string => {
    try {
      const st = fs.statSync(STRATEGY_PATH);
      if (!Number.isFinite(st.mtimeMs)) return "";
      return `${Math.round(st.mtimeMs)}:${st.size}`;
    } catch {
      return "";
    }
  };

  const scheduleReload = (source: string) => {
    if (reloadTimer) clearTimeout(reloadTimer);
    reloadTimer = setTimeout(() => {
      const sigNow = readSig();
      if (!sigNow || sigNow === lastSig) return;
      lastSig = sigNow;
      const reason = `hot-reload:${source}`;
      console.log(`[STRATEGY HOT RELOAD] reason=${reason} path=${STRATEGY_PATH}`);
      reloadStrategyFactorySafe(reason);
      reloadEngineStrategy("paper", reason);
      reloadEngineStrategy("live", reason);
    }, 220);
  };

  lastSig = readSig();
  fs.watchFile(STRATEGY_PATH, { interval: 500 }, (curr, prev) => {
    if (!curr || !prev) return;
    if (curr.mtimeMs === 0) return; // file temporarily missing during atomic save
    if (curr.mtimeMs !== prev.mtimeMs || curr.size !== prev.size) {
      scheduleReload("watchFile");
    }
  });

  try {
    dirWatcher = fs.watch(strategyDir, (_eventType, filename) => {
      if (!filename) return;
      if (String(filename) !== strategyFile) return;
      scheduleReload("watch");
    });
  } catch (e: any) {
    console.warn(`[STRATEGY HOT RELOAD] dir watch unavailable err=${String(e?.message ?? e)}`);
  }

  const cleanup = () => {
    try { fs.unwatchFile(STRATEGY_PATH); } catch {}
    if (dirWatcher) {
      try { dirWatcher.close(); } catch {}
      dirWatcher = null;
    }
    if (reloadTimer) {
      clearTimeout(reloadTimer);
      reloadTimer = null;
    }
  };

  process.once("SIGINT", cleanup);
  process.once("SIGTERM", cleanup);
  process.once("exit", cleanup);

  console.log(`[STRATEGY HOT RELOAD] watching file=${STRATEGY_PATH}`);
  broadcast({ type: "status", t: nowMs(), status: `strategy hot-reload watcher active (${strategyFile})` });
}

// ===== Web + WS Server =====
const app = express();
app.use(express.json());
const PUBLIC_DIR_CANDIDATES = [
  path.resolve(process.cwd(), "public"),
  path.resolve(__dirname, "../public"),
];
const PUBLIC_DIR = PUBLIC_DIR_CANDIDATES.find((p) => fs.existsSync(path.join(p, "index.html"))) || PUBLIC_DIR_CANDIDATES[0];
app.use(express.static(PUBLIC_DIR));
app.get("/", (_req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, "index.html"));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

function activeEngine(): Engine {
  return uiLive.enabled ? "live" : "paper";
}
function activeUi(): UiConfig {
  return activeEngine() === "live" ? uiLive : uiPaper;
}
function activeSt(): TradeState {
  return activeEngine() === "live" ? stLive : stPaper;
}
function activeBalanceUsd(): number | null {
  return activeEngine() === "live" ? (liveAccount.balanceUsd ?? stLive.balanceUsd ?? null) : paperAccount.balanceUsd;
}

function toFiniteOrNull(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function inferSideFromActionOrState(action: string, st: TradeState): OutcomeSide | null {
  const a = String(action || "").toUpperCase();
  if (a.includes("_UP_") || a.endsWith("_UP")) return "UP";
  if (a.includes("_DOWN_") || a.endsWith("_DOWN")) return "DOWN";
  return (st.side as OutcomeSide | null) ?? null;
}

function buildExecutionSnapshot(action: string, st: TradeState, ui: UiConfig, extra?: Record<string, any>) {
  const a = String(action || "");
  const tracked =
    /^ENTER(_ORDER|_CANCEL)?_/i.test(a) || /^SIGNAL_/i.test(a) || /^EXIT_/i.test(a) || /^STOP_/i.test(a);
  if (!tracked) return null;

  const x = extra || {};
  const side = inferSideFromActionOrState(a, st);
  const signalPx = toFiniteOrNull(x.signalPx);
  const signalThresholdPx = toFiniteOrNull(x.signalThresholdPx);
  const explicitOrderPx = toFiniteOrNull(x.buyOrderPx ?? x.orderPx ?? x.limitPx ?? x.targetPx ?? x.expectedPx);
  const explicitFillPx = toFiniteOrNull(x.actualFillPx ?? x.fillPx ?? x.executedPx);
  let phase = "trade_event";
  if (/^ENTER_ORDER_/i.test(a)) phase = "entry_order";
  else if (/^ENTER_CANCEL_/i.test(a)) phase = "entry_cancel";
  else if (/^ENTER_/i.test(a)) phase = "entry_fill";
  else if (/^SIGNAL_STOP_/i.test(a)) phase = "stop_signal";
  else if (/^SIGNAL_EXIT_/i.test(a)) phase = "exit_signal";
  else if (/^SIGNAL_/i.test(a)) phase = "signal";
  else if (/^STOP_/i.test(a)) phase = "stop_fill";
  else if (/^EXIT_/i.test(a)) phase = "exit_fill";

  let intendedPx: number | null = explicitOrderPx;
  if (intendedPx == null) {
    if (/^ENTER_/i.test(a)) intendedPx = toFiniteOrNull(st.entryPx);
    else if (/STOP/i.test(a)) intendedPx = toFiniteOrNull(ui.stop);
    else if (/EXIT/i.test(a)) intendedPx = toFiniteOrNull(ui.exit);
  }

  let actualPx: number | null = explicitFillPx;
  if (actualPx == null) {
    if (/^ENTER_/i.test(a)) actualPx = toFiniteOrNull(st.entryPx);
    else if (/STOP|EXIT/i.test(a)) actualPx = toFiniteOrNull(st.exitPx);
  }

  const buyOrderPx = /^ENTER_/i.test(a) ? intendedPx : null;
  const actualFillPx = /^ENTER_/i.test(a) ? actualPx : null;
  const ttlMs = toFiniteOrNull(x.ttlMs);
  const mode = x.mode != null ? String(x.mode) : null;
  const fillEstimate = normalizeFillEstimate(x.fillEstimate);
  const stopReason = x.stopReason != null ? String(x.stopReason) : null;
  const stopSource = x.stopSource != null ? String(x.stopSource) : null;
  const stopMeta = x.stopMeta && typeof x.stopMeta === "object" ? x.stopMeta : null;

  const assumedSlippagePx =
    toFiniteOrNull(x.assumedSlippagePx) ??
    (signalPx != null && intendedPx != null ? round4(intendedPx - signalPx) : null);
  const realizedSlippagePx =
    toFiniteOrNull(x.realizedSlippagePx) ??
    (signalPx != null && actualPx != null ? round4(actualPx - signalPx) : null);
  const intendedVsActualPx =
    toFiniteOrNull(x.intendedVsActualPx) ??
    (intendedPx != null && actualPx != null ? round4(actualPx - intendedPx) : null);

  return {
    action: a,
    phase,
    side,
    signalPx,
    signalThresholdPx,
    intendedPx,
    actualPx,
    buyOrderPx,
    actualFillPx,
    assumedSlippagePx,
    realizedSlippagePx,
    intendedVsActualPx,
    ttlMs,
    mode,
    stopReason,
    stopSource,
    stopArmed: typeof stopMeta?.armed === "boolean" ? stopMeta.armed : null,
    stopRawStreak: toFiniteNum(stopMeta?.rawStreak),
    stopRawNeed: toFiniteNum(stopMeta?.rawNeed),
    stopKalmanStreak: toFiniteNum(stopMeta?.kalStreak),
    stopKalmanNeed: toFiniteNum(stopMeta?.kalNeed),
    stopKalmanPx: toFiniteNum(stopMeta?.kalmanPx),
    stopKalmanSlope: toFiniteNum(stopMeta?.kalmanSlope),
    fillEstWouldFill: fillEstimate?.wouldFill ?? null,
    fillEstScorePct: fillEstimate?.scorePct ?? null,
    fillEstRequiredShares: fillEstimate?.requiredShares ?? null,
    fillEstFillableShares: fillEstimate?.fillableShares ?? null,
    fillEstVwapPx: fillEstimate?.estVwapPx ?? null,
  };
}

function emitTrade(
  engine: Engine,
  action: string,
  extra?: Record<string, any>,
  stOverride?: TradeState,
  uiOverride?: UiConfig
) {
  const ui = uiOverride ?? (engine === "paper" ? uiPaper : uiLive);
  const st = stOverride ?? (engine === "paper" ? stPaper : stLive);
  const bal = engine === "paper" ? paperAccount.balanceUsd : liveAccount.balanceUsd ?? stLive.balanceUsd ?? null;
  const execution = buildExecutionSnapshot(action, st, ui, extra);

  const payload = {
    type: "trade",
    runId: SERVER_RUN_ID,
    runStartMs: CURRENT_RUN_START_MS,
    runStartIso: CURRENT_RUN_START_ISO,
    t: nowMs(),
    iso: isoNow(),
    engine,
    action,
    strategyLane: extra?.strategyLane ?? null,
    market: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd },
    st: {
      marketSlug: st.marketSlug,
      entered: st.entered,
      exited: st.exited,
      side: st.side,

      entryTsMs: st.entryTsMs,
      exitTsMs: st.exitTsMs,
      entryPx: st.entryPx,
      exitPx: st.exitPx,

      shares: st.shares,
      notionalUsd: st.notionalUsd,
      grossPnlUsd: st.grossPnlUsd,
      entryFeeUsd: st.entryFeeUsd,
      exitFeeUsd: st.exitFeeUsd,
      totalFeesUsd: st.totalFeesUsd,

      pnlUsd: st.pnlUsd,
      roiPct: st.roiPct,
      holdSec: st.holdSec,

      balanceUsd: st.balanceUsd,
      stopFallbackTriggered: st.stopFallbackTriggered,
    },
    ui: {
      ...withDerivedUi(ui, bal),
    },
    ...(execution ? { execution } : {}),
    ...(extra || {}),
  };

  appendTradeLog(payload);
  if (execution) {
    appendExecutionLog({
      type: "execution",
      t: payload.t,
      iso: payload.iso,
      engine: payload.engine,
      action: payload.action,
      market: payload.market,
      execution: payload.execution,
      st: {
        side: payload?.st?.side ?? null,
        entryPx: payload?.st?.entryPx ?? null,
        exitPx: payload?.st?.exitPx ?? null,
        shares: payload?.st?.shares ?? null,
        notionalUsd: payload?.st?.notionalUsd ?? null,
        grossPnlUsd: payload?.st?.grossPnlUsd ?? null,
        entryFeeUsd: payload?.st?.entryFeeUsd ?? null,
        exitFeeUsd: payload?.st?.exitFeeUsd ?? null,
        totalFeesUsd: payload?.st?.totalFeesUsd ?? null,
        pnlUsd: payload?.st?.pnlUsd ?? null,
      },
      ui: {
        entry: payload?.ui?.entry ?? null,
        exit: payload?.ui?.exit ?? null,
        stop: payload?.ui?.stop ?? null,
      },
    });
  }
  broadcast(payload);
}

function broadcastState() {
  const activeBal = activeBalanceUsd();
  const paperBal = paperAccount.balanceUsd;
  const liveBal = liveAccount.balanceUsd ?? stLive.balanceUsd ?? null;
  broadcast({
    type: "state",
    t: nowMs(),
    current: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd },
    ui: withDerivedUi(activeUi(), activeBal),
    paper: { ui: withDerivedUi(uiPaper, paperBal), st: stPaper, balanceUsd: paperBal },
    live: { ui: withDerivedUi(uiLive, liveBal), st: stLive, balanceUsd: liveBal },
    pendingUiLive,
    health: buildHealthSnapshot(),
  });
}

wss.on("connection", (ws) => {
  // @ts-ignore
  ws.send(JSON.stringify({ type: "status", status: "connected", t: nowMs() }));
  // @ts-ignore
  ws.send(JSON.stringify({ type: "market", t: nowMs(), current: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd } }));
  // @ts-ignore
  ws.send(
    JSON.stringify({
      type: "state",
      t: nowMs(),
      current: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd },
      ui: withDerivedUi(activeUi(), activeBalanceUsd()),
      paper: { ui: withDerivedUi(uiPaper, paperAccount.balanceUsd), st: stPaper, balanceUsd: paperAccount.balanceUsd },
      live: {
        ui: withDerivedUi(uiLive, liveAccount.balanceUsd ?? stLive.balanceUsd ?? null),
        st: stLive,
        balanceUsd: liveAccount.balanceUsd,
      },
      pendingUiLive,
      health: buildHealthSnapshot(),
    })
  );
  const botSnapshot = Array.from(botInstances.values()).map((b) => ({
    ...b,
    runtime: botRuntimes.get(b.instanceId) || null,
  }));
  // @ts-ignore
  ws.send(JSON.stringify({ type: "bots_snapshot", t: nowMs(), items: botSnapshot }));
});

app.get("/api/state", (_req, res) => {
  const ui = activeUi();
  const st = activeSt();
  const bal = activeBalanceUsd();
  res.json({
    ui: withDerivedUi(ui, bal),
    st: { ...st, balanceUsd: bal },
    current: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd },
    paper: {
      ui: withDerivedUi(uiPaper, paperAccount.balanceUsd),
      st: { ...stPaper, balanceUsd: paperAccount.balanceUsd },
      balanceUsd: paperAccount.balanceUsd,
    },
    live: {
      ui: withDerivedUi(uiLive, liveAccount.balanceUsd ?? stLive.balanceUsd ?? null),
      st: { ...stLive, balanceUsd: liveAccount.balanceUsd ?? stLive.balanceUsd ?? null },
      balanceUsd: liveAccount.balanceUsd ?? null,
    },
    pendingUiLive,
    health: buildHealthSnapshot(),
    bots: Array.from(botInstances.values()).map((b) => ({
      ...b,
      runtime: botRuntimes.get(b.instanceId) || null,
    })),
  });
});

app.get("/api/chart-history", (req, res) => {
  try {
    const engineRaw = String(req.query.engine ?? "paper").toLowerCase();
    const engine: Engine = engineRaw === "live" ? "live" : "paper";
    const marketSlugFilter = String(req.query.marketSlug ?? req.query.slug ?? "").trim().toLowerCase();
    const marketPrefixFilter = String(req.query.marketPrefix ?? req.query.prefix ?? "").trim().toLowerCase();
    const instanceIdFilter = String(req.query.instanceId ?? "").trim();
    const sinceMsRaw = Number(req.query.sinceMs ?? 0);
    const maxSessionsRaw = Number(req.query.maxSessions ?? 0);
    const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0
      ? Math.max(1, Math.min(5000, Math.floor(maxSessionsRaw)))
      : null;
    const runsRaw = Number(req.query.runs ?? 10);
    const scopeRaw = String(req.query.scope ?? "run").toLowerCase();
    const scopeInfo = resolveRunScope(req, scopeRaw, sinceMsRaw, runsRaw);
    const scope = scopeInfo.scope;
    const sinceMs = scopeInfo.sinceMs;
    const runScopeId = scopeInfo.runId;
    const runStartMs = scopeInfo.runStartMs;
    const runStartIso = scopeInfo.runStartIso;

    if (instanceIdFilter) {
      const inst = botInstances.get(instanceIdFilter) || null;
      const runNumRaw = Number(req.query.runNum ?? inst?.runNum ?? 0);
      const runNum = Number.isFinite(runNumRaw) && runNumRaw > 0 ? Math.floor(runNumRaw) : 0;
      const sinceMsInst = Number.isFinite(Number(sinceMsRaw)) && Number(sinceMsRaw) > 0 ? Number(sinceMsRaw) : 0;
      if (!runNum) {
        return res.json({
          ok: true,
          engine,
          runId: null,
          runStartMs: null,
          runStartIso: null,
          runStartsMs: [],
          points: [],
          count: 0,
          sinceMs: sinceMsInst,
          source: "bot_run_events",
          instanceId: instanceIdFilter,
        });
      }
      const summary = readBotRunSummary(runNum) || null;
      const startBal = Number(summary?.startBalanceUsd ?? inst?.startBalanceUsd ?? 100);
      const startMsInstRaw = Number(summary?.startedAtMs ?? inst?.launchedAtMs ?? 0);
      const startMsInst = Number.isFinite(startMsInstRaw) && startMsInstRaw > 0 ? startMsInstRaw : Date.now();
      const sinceStart = sinceMsInst > 0 ? Math.max(sinceMsInst, startMsInst) : startMsInst;
      const events = readBotRunEvents(runNum)
        .filter((e) => String(e?.instanceId || "").trim() === instanceIdFilter)
        .filter((e) => {
          const slug = String(e?.marketSlug || "").trim().toLowerCase();
          if (!slug) return false;
          if (marketSlugFilter) return slug === marketSlugFilter;
          if (marketPrefixFilter) return slug.startsWith(marketPrefixFilter);
          return true;
        })
        .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
      const points: Array<{ t: number; v: number }> = [{ t: sinceStart, v: Number.isFinite(startBal) ? startBal : 100 }];
      let running = Number.isFinite(startBal) ? startBal : 100;
      for (const e of events) {
        if (String(e?.event || "") !== "exit") continue;
        const t = Number(e?.t);
        if (!Number.isFinite(t) || t < sinceStart) continue;
        const bal = Number(e?.balanceUsd ?? e?.runtime?.balanceUsd);
        const pnl = Number(e?.pnlUsd);
        if (Number.isFinite(bal)) {
          running = bal;
          points.push({ t, v: running });
        } else if (Number.isFinite(pnl)) {
          running += pnl;
          points.push({ t, v: running });
        }
      }
      // Keep focused test equity continuous to "now" while instance is active.
      // This appends a live mark-to-market tail point after the realized curve.
      try {
        const runtimeLive = botRuntimes.get(instanceIdFilter) || null;
        const instRunNumLive = Number(inst?.runNum);
        const runtimeRunMatches =
          Number.isFinite(instRunNumLive) &&
          Number.isFinite(runNum) &&
          Number(instRunNumLive) === Number(runNum);
        if (runtimeLive && runtimeRunMatches) {
          const rtStartBalRaw = Number(inst?.startBalanceUsd ?? summary?.startBalanceUsd ?? startBal);
          const rtStartBal = Number.isFinite(rtStartBalRaw) ? rtStartBalRaw : 100;
          const rtBalRaw = Number(runtimeLive.balanceUsd);
          const rtRealizedRaw = Number(runtimeLive.realizedPnlUsd);
          let liveBal = Number.isFinite(rtBalRaw)
            ? rtBalRaw
            : (Number.isFinite(rtRealizedRaw) ? (rtStartBal + rtRealizedRaw) : NaN);
          const sideNow = String(runtimeLive.side || "").toUpperCase();
          const enteredNow = !!runtimeLive.entered;
          const entryPxNow = Number(runtimeLive.entryPx);
          const sharesNow = Number(runtimeLive.shares);
          const sideBidNow =
            sideNow === "UP" ? Number(runtimeLive.upBid)
            : (sideNow === "DOWN" ? Number(runtimeLive.downBid) : NaN);
          if (
            enteredNow &&
            (sideNow === "UP" || sideNow === "DOWN") &&
            Number.isFinite(entryPxNow) &&
            Number.isFinite(sharesNow) &&
            sharesNow > 0 &&
            Number.isFinite(sideBidNow)
          ) {
            liveBal += (sideBidNow - entryPxNow) * sharesNow;
          }
          const tailTsRaw = Number(runtimeLive.lastTickMs);
          const tailTs = Number.isFinite(tailTsRaw) && tailTsRaw > 0 ? tailTsRaw : Date.now();
          if (Number.isFinite(liveBal) && Number.isFinite(tailTs) && tailTs >= sinceStart) {
            const lastPt = points.length ? points[points.length - 1] : null;
            if (
              !lastPt ||
              Math.abs(Number(lastPt.v) - Number(liveBal)) > 1e-9 ||
              Math.abs(Number(lastPt.t) - Number(tailTs)) > 250
            ) {
              points.push({ t: Number(tailTs), v: Number(liveBal) });
            }
          }
        }
      } catch {}
      points.sort((a, b) => Number(a.t) - Number(b.t));
      return res.json({
        ok: true,
        engine,
        runId: runNum,
        runStartMs: startMsInst,
        runStartIso: new Date(startMsInst).toISOString(),
        runStartsMs: [startMsInst],
        points,
        count: points.length,
        sinceMs: sinceStart,
        source: "bot_run_events",
        instanceId: instanceIdFilter,
      });
    }

    const tradeRows = filterTradeRowsByScope(readJsonlFiles(listTradeLogFiles()), engine, scope, sinceMs, runScopeId)
      .filter((r) => {
        if (!marketSlugFilter && !marketPrefixFilter) return true;
        const slug = String(r?.st?.marketSlug || r?.market?.slug || "").trim().toLowerCase();
        if (!slug) return false;
        if (marketSlugFilter) return slug === marketSlugFilter;
        return slug.startsWith(marketPrefixFilter);
      });
    let points: Array<{ t: number; v: number }> = [];

    if (engine === "paper") {
      // Source of truth for paper equity: cumulative realized P/L from closed exits.
      const exitRe = /^(EXIT|STOP)_(UP|DOWN)_(PAPER|LIVE)$/;
      const exits = tradeRows
        .filter((r) => exitRe.test(String(r?.action || "")))
        .map((r) => ({
          t: Number(r?.t),
          pnl: toFiniteOrNull(r?.st?.pnlUsd),
        }))
        .filter((x) => Number.isFinite(x.t) && Number.isFinite(x.pnl))
        .sort((a, b) => a.t - b.t);

      let bal = 100;
      points.push({ t: sinceMs > 0 ? sinceMs : Date.now(), v: bal });
      for (const ex of exits) {
        bal += ex.pnl;
        points.push({ t: ex.t, v: bal });
      }
    } else {
      points = tradeRows
        .map((r) => ({
          t: Number(r?.t),
          v: toFiniteOrNull(r?.st?.balanceUsd),
        }))
        .filter((p) => Number.isFinite(p.t) && Number.isFinite(p.v) && p.t >= sinceMs)
        .sort((a, b) => a.t - b.t);
    }

    // Deduplicate consecutive identical points and identical timestamps.
    const deduped: Array<{ t: number; v: number }> = [];
    for (const p of points) {
      const last = deduped[deduped.length - 1];
      if (last && Math.abs(last.v - p.v) < 1e-9 && Math.abs(last.t - p.t) < 1) continue;
      deduped.push(p);
    }

    // Anchor curve at initial paper starting balance.
    if (engine === "paper" && !deduped.length) deduped.push({ t: Date.now(), v: 100 });

    const runStartsMs =
      scope === "run"
        ? [CURRENT_RUN_START_MS]
        : readRunsMeta().runs
            .map((r) => Number(r.startMs))
            .filter((ms) => Number.isFinite(ms) && ms >= sinceMs)
            .sort((a, b) => a - b);

    res.json({
      ok: true,
      engine,
      runId: Number.isFinite(Number(runScopeId)) ? Number(runScopeId) : SERVER_RUN_ID,
      runStartMs: Number.isFinite(Number(runStartMs)) ? Number(runStartMs) : CURRENT_RUN_START_MS,
      runStartIso: runStartIso || CURRENT_RUN_START_ISO,
      runStartsMs,
      points: deduped,
      count: deduped.length,
      sinceMs,
      source: "trade_logs",
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.get("/api/stats/summary", (req, res) => {
  try {
    const engineRaw = String(req.query.engine ?? "paper").toLowerCase();
    const engine: Engine = engineRaw === "live" ? "live" : "paper";
    const marketSlugFilter = String(req.query.marketSlug ?? req.query.slug ?? "").trim().toLowerCase();
    const marketPrefixFilter = String(req.query.marketPrefix ?? req.query.prefix ?? "").trim().toLowerCase();
    const instanceIdFilter = String(req.query.instanceId ?? "").trim();
    const sinceMsRaw = Number(req.query.sinceMs ?? 0);
    const maxSessionsRaw = Number(req.query.maxSessions ?? 0);
    const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0
      ? Math.max(1, Math.min(5000, Math.floor(maxSessionsRaw)))
      : null;
    const runsRaw = Number(req.query.runs ?? 10);
    const scopeRaw = String(req.query.scope ?? "run").toLowerCase();
    const scopeInfo = resolveRunScope(req, scopeRaw, sinceMsRaw, runsRaw);
    const scope = scopeInfo.scope;
    const sinceMs = scopeInfo.sinceMs;
    const runScopeId = scopeInfo.runId;

    if (instanceIdFilter) {
      const inst = botInstances.get(instanceIdFilter) || null;
      const runNumRaw = Number(req.query.runNum ?? inst?.runNum ?? 0);
      const runNum = Number.isFinite(runNumRaw) && runNumRaw > 0 ? Math.floor(runNumRaw) : 0;
      const sinceMsInst = Number.isFinite(Number(sinceMsRaw)) && Number(sinceMsRaw) > 0 ? Number(sinceMsRaw) : 0;
      if (!runNum) {
        return res.json({
          ok: true,
          engine,
          sinceMs: sinceMsInst,
          scope,
          totalFeesUsd: 0,
          totalEntryFeesUsd: 0,
          totalExitFeesUsd: 0,
          estimatedSlippageUsd: 0,
          slippageFillCount: 0,
          source: "bot_run_events",
          instanceId: instanceIdFilter,
        });
      }
      const summary = readBotRunSummary(runNum) || null;
      const runStartMsInstRaw = Number(summary?.startedAtMs ?? inst?.launchedAtMs ?? 0);
      const runStartMsInst = Number.isFinite(runStartMsInstRaw) && runStartMsInstRaw > 0 ? runStartMsInstRaw : Date.now();
      const sinceStart = sinceMsInst > 0 ? Math.max(sinceMsInst, runStartMsInst) : runStartMsInst;
      const rows = readBotRunEvents(runNum)
        .filter((e) => String(e?.instanceId || "").trim() === instanceIdFilter)
        .filter((e) => Number(e?.t) >= sinceStart)
        .filter((e) => {
          const slug = String(e?.marketSlug || "").trim().toLowerCase();
          if (!slug) return false;
          if (marketSlugFilter) return slug === marketSlugFilter;
          if (marketPrefixFilter) return slug.startsWith(marketPrefixFilter);
          return true;
        });
      let closed = 0;
      let pnlTotal = 0;
      for (const r of rows) {
        if (String(r?.event || "").toLowerCase() !== "exit") continue;
        const p = Number(r?.pnlUsd);
        if (Number.isFinite(p)) pnlTotal += p;
        closed += 1;
      }
      return res.json({
        ok: true,
        engine,
        sinceMs: sinceStart,
        scope,
        totalFeesUsd: 0,
        totalEntryFeesUsd: 0,
        totalExitFeesUsd: 0,
        estimatedSlippageUsd: 0,
        slippageFillCount: 0,
        source: "bot_run_events",
        instanceId: instanceIdFilter,
        closedTrades: closed,
        pnlUsd: round4(pnlTotal),
      });
    }

    const tradeRows = filterTradeRowsByScope(readJsonlFiles(listTradeLogFiles()), engine, scope, sinceMs, runScopeId)
      .filter((r) => {
        if (!marketSlugFilter && !marketPrefixFilter) return true;
        const slug = String(r?.st?.marketSlug || r?.market?.slug || "").trim().toLowerCase();
        if (!slug) return false;
        if (marketSlugFilter) return slug === marketSlugFilter;
        return slug.startsWith(marketPrefixFilter);
      })
      .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));

    let totalEntryFeesUsd = 0;
    let totalExitFeesUsd = 0;
    let estimatedSlippageUsd = 0;
    let slippageFillCount = 0;

    const enterRe = /^ENTER_(UP|DOWN)_(PAPER|LIVE)$/;
    const exitRe = /^(EXIT|STOP)_(UP|DOWN)_(PAPER|LIVE)$/;

    for (const r of tradeRows) {
      const action = String(r?.action || "");
      const entryFeeUsd = toNum(r?.st?.entryFeeUsd);
      const exitFeeUsd = toNum(r?.st?.exitFeeUsd);

      if (enterRe.test(action)) totalEntryFeesUsd += entryFeeUsd;
      if (exitRe.test(action)) totalExitFeesUsd += exitFeeUsd;

      const shares = toNum(r?.st?.shares);
      const intendedPx = toNum(r?.execution?.intendedPx);
      const actualPx = toNum(r?.execution?.actualPx);
      if (Number.isFinite(shares) && shares > 0 && Number.isFinite(intendedPx) && Number.isFinite(actualPx)) {
        estimatedSlippageUsd += Math.abs(actualPx - intendedPx) * shares;
        slippageFillCount += 1;
      }
    }

    const totalFeesUsd = totalEntryFeesUsd + totalExitFeesUsd;
    res.json({
      ok: true,
      engine,
      sinceMs,
      scope,
      totalFeesUsd,
      totalEntryFeesUsd,
      totalExitFeesUsd,
      estimatedSlippageUsd,
      slippageFillCount,
      source: "trade_logs",
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.get("/api/session-history", (req, res) => {
  try {
    const engineRaw = String(req.query.engine ?? "paper").toLowerCase();
    const engine: Engine = engineRaw === "live" ? "live" : "paper";
    const marketSlugFilter = String(req.query.marketSlug ?? req.query.slug ?? "").trim().toLowerCase();
    const marketPrefixFilter = String(req.query.marketPrefix ?? req.query.prefix ?? "").trim().toLowerCase();
    const instanceIdFilter = String(req.query.instanceId ?? "").trim();
    const sinceMsRaw = Number(req.query.sinceMs ?? 0);
    const maxSessionsRaw = Number(req.query.maxSessions ?? 0);
    const maxSessions = Number.isFinite(maxSessionsRaw) && maxSessionsRaw > 0
      ? Math.max(1, Math.min(5000, Math.floor(maxSessionsRaw)))
      : null;
    const runsRaw = Number(req.query.runs ?? 10);
    const scopeRaw = String(req.query.scope ?? "run").toLowerCase();
    const includeTrace = String(req.query.includeTrace ?? "0") === "1";
    const scopeInfo = resolveRunScope(req, scopeRaw, sinceMsRaw, runsRaw);
    const scope = scopeInfo.scope;
    const sinceMs = scopeInfo.sinceMs;
    const runScopeId = scopeInfo.runId;
    const runStartMs = scopeInfo.runStartMs;
    const runStartIso = scopeInfo.runStartIso;

    if (instanceIdFilter) {
      const inst = botInstances.get(instanceIdFilter) || null;
      const runNumRaw = Number(req.query.runNum ?? inst?.runNum ?? 0);
      const runNum = Number.isFinite(runNumRaw) && runNumRaw > 0 ? Math.floor(runNumRaw) : 0;
      const sinceMsInst = Number.isFinite(Number(sinceMsRaw)) && Number(sinceMsRaw) > 0 ? Number(sinceMsRaw) : 0;
      if (!runNum) {
        return res.json({
          ok: true,
          engine,
          runId: null,
          runStartMs: null,
          runStartIso: null,
          sessions: [],
          count: 0,
          sinceMs: sinceMsInst,
          source: "bot_run_events",
          instanceId: instanceIdFilter,
        });
      }
      const summary = readBotRunSummary(runNum) || null;
      const runStartMsInstRaw = Number(summary?.startedAtMs ?? inst?.launchedAtMs ?? 0);
      const runStartMsInst = Number.isFinite(runStartMsInstRaw) && runStartMsInstRaw > 0 ? runStartMsInstRaw : Date.now();
      const sinceStart = sinceMsInst > 0 ? Math.max(sinceMsInst, runStartMsInst) : runStartMsInst;
      const rows = readBotRunEvents(runNum)
        .filter((e) => String(e?.instanceId || "").trim() === instanceIdFilter)
        .filter((e) => Number(e?.t) >= sinceStart)
        .filter((e) => {
          const slug = String(e?.marketSlug || "").trim().toLowerCase();
          if (!slug) return false;
          if (marketSlugFilter) return slug === marketSlugFilter;
          if (marketPrefixFilter) return slug.startsWith(marketPrefixFilter);
          return true;
        })
        .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
      const openBySlug = new Map<string, any[]>();
      const perSlug = new Map<string, any>();
      const pushOpen = (slug: string, row: any) => {
        const arr = openBySlug.get(slug) || [];
        arr.push(row);
        openBySlug.set(slug, arr);
      };
      const shiftOpen = (slug: string): any | null => {
        const arr = openBySlug.get(slug) || [];
        if (!arr.length) return null;
        const v = arr.shift() || null;
        openBySlug.set(slug, arr);
        return v;
      };
      for (const r of rows) {
        const ev = String(r?.event || "").toLowerCase();
        const slug = String(r?.marketSlug || "");
        if (!slug) continue;
        if (ev === "enter") {
          pushOpen(slug, r);
          continue;
        }
        if (ev !== "exit") continue;
        const en = shiftOpen(slug);
        const entryTsMs = Number(en?.t);
        const exitTsMs = Number(r?.t);
        const entryPx = Number(en?.entryPx);
        const exitPx = Number(r?.exitPx);
        const pnlUsd = Number(r?.pnlUsd);
        const balanceUsd = Number(r?.balanceUsd ?? r?.runtime?.balanceUsd);
        const holdSec =
          Number.isFinite(entryTsMs) && Number.isFinite(exitTsMs)
            ? Math.max(0, (exitTsMs - entryTsMs) / 1000)
            : null;
        const side = String(r?.side || en?.side || "").toUpperCase();
        const lane = {
          lane: `trade_${Math.max(1, ((perSlug.get(slug)?.laneCount ?? 0) + 1))}`,
          strategyMode: String(inst?.strategyId || "bot_runtime"),
          side: side === "UP" || side === "DOWN" ? side : null,
          attemptTsMs: Number.isFinite(entryTsMs) ? entryTsMs : null,
          entryTsMs: Number.isFinite(entryTsMs) ? entryTsMs : null,
          exitTsMs: Number.isFinite(exitTsMs) ? exitTsMs : null,
          signalPx: null,
          intendedPx: null,
          entryPx: Number.isFinite(entryPx) ? entryPx : null,
          exitPx: Number.isFinite(exitPx) ? exitPx : null,
          pnlUsd: Number.isFinite(pnlUsd) ? pnlUsd : null,
          roiPct:
            Number.isFinite(pnlUsd) && Number.isFinite(Number(en?.notionalUsd)) && Number(en.notionalUsd) > 0
              ? (Number(pnlUsd) / Number(en.notionalUsd)) * 100
              : null,
          holdSec,
          closed: true,
          attempted: true,
          canceled: false,
          via: "bot_runtime",
          reason: String(r?.exitReasonRaw || r?.exitType || ""),
          exitType: String(r?.exitType || ""),
          exitReasonRaw: String(r?.exitReasonRaw || ""),
          shares: Number.isFinite(Number(en?.shares)) ? Number(en.shares) : null,
          notionalUsd: Number.isFinite(Number(en?.notionalUsd)) ? Number(en.notionalUsd) : null,
          balanceUsd: Number.isFinite(balanceUsd) ? balanceUsd : null,
          feesUsd: 0,
          estSlippageUsd: 0,
          sessionVolumeUsd: null,
          fillEstScorePct: null,
          fillAccuracyPct: null,
        };
        const prev = perSlug.get(slug);
        const slugSecMatch = slug.match(/-(\d{10})$/);
        const slugStartMs =
          slugSecMatch && Number.isFinite(Number(slugSecMatch[1])) ? Number(slugSecMatch[1]) * 1000 : null;
        const slugEndMs = Number.isFinite(Number(slugStartMs)) ? Number(slugStartMs) + 299_000 : null;
        const baseLabel = (() => {
          if (Number.isFinite(Number(slugStartMs))) {
            const base = slug.replace(/-\d{10}$/, "");
            const d = new Date(Number(slugStartMs));
            return `${base} @ ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
          }
          if (Number.isFinite(Number(entryTsMs))) {
            const d = new Date(Number(entryTsMs));
            return `${slug} @ ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
          }
          return slug;
        })();
        if (!prev) {
          perSlug.set(slug, {
            key: `${runNum}:${instanceIdFilter}:${slug}`,
            runId: runNum,
            instanceId: instanceIdFilter,
            slug,
            humanLabel: baseLabel,
            endMs: Number.isFinite(Number(slugEndMs)) ? Number(slugEndMs) : null,
            strategyMode: String(inst?.strategyId || "bot_runtime"),
            side: side === "UP" || side === "DOWN" ? side : null,
            attemptTsMs: Number.isFinite(entryTsMs) ? entryTsMs : null,
            entryTsMs: Number.isFinite(entryTsMs) ? entryTsMs : null,
            exitTsMs: Number.isFinite(exitTsMs) ? exitTsMs : null,
            signalPx: null,
            intendedPx: null,
            entryPx: Number.isFinite(entryPx) ? entryPx : null,
            exitPx: Number.isFinite(exitPx) ? exitPx : null,
            pnlUsd: Number.isFinite(pnlUsd) ? pnlUsd : 0,
            roiPct: null,
            holdSec,
            closed: true,
            attempted: true,
            canceled: false,
            via: "bot_runtime",
            reason: String(r?.exitReasonRaw || r?.exitType || ""),
            exitType: String(r?.exitType || ""),
            exitReasonRaw: String(r?.exitReasonRaw || ""),
            shares: Number.isFinite(Number(en?.shares)) ? Number(en.shares) : null,
            notionalUsd: Number.isFinite(Number(en?.notionalUsd)) ? Number(en.notionalUsd) : null,
            balanceUsd: Number.isFinite(balanceUsd) ? balanceUsd : null,
            feesUsd: 0,
            estSlippageUsd: 0,
            sessionVolumeUsd: null,
            fillEstScorePct: null,
            fillAccuracyPct: null,
            trace: null,
            traceSource: null,
            lanes: [lane],
            laneCount: 1,
          });
          continue;
        }
        const nextEntryTs = Number(prev?.entryTsMs);
        const nextExitTs = Number(prev?.exitTsMs);
        const nextPnl = Number(prev?.pnlUsd);
        const nextNotional = Number(prev?.notionalUsd);
        const nextShares = Number(prev?.shares);
        const lanes = Array.isArray(prev?.lanes) ? prev.lanes.slice() : [];
        lanes.push(lane);
        prev.humanLabel = baseLabel;
        prev.entryTsMs =
          Number.isFinite(entryTsMs) && (!Number.isFinite(nextEntryTs) || entryTsMs < nextEntryTs)
            ? entryTsMs
            : (Number.isFinite(nextEntryTs) ? nextEntryTs : null);
        prev.exitTsMs =
          Number.isFinite(exitTsMs) && (!Number.isFinite(nextExitTs) || exitTsMs > nextExitTs)
            ? exitTsMs
            : (Number.isFinite(nextExitTs) ? nextExitTs : null);
        if (Number.isFinite(entryTsMs) && Number.isFinite(entryPx) && (!Number.isFinite(nextEntryTs) || entryTsMs <= nextEntryTs)) {
          prev.entryPx = entryPx;
        }
        if (Number.isFinite(exitTsMs) && Number.isFinite(exitPx) && (!Number.isFinite(nextExitTs) || exitTsMs >= nextExitTs)) {
          prev.exitPx = exitPx;
          prev.reason = String(r?.exitReasonRaw || r?.exitType || prev.reason || "");
          prev.exitType = String(r?.exitType || prev.exitType || "");
          prev.exitReasonRaw = String(r?.exitReasonRaw || prev.exitReasonRaw || "");
          prev.balanceUsd = Number.isFinite(balanceUsd) ? balanceUsd : prev.balanceUsd;
        }
        prev.side = prev.side || (side === "UP" || side === "DOWN" ? side : null);
        prev.pnlUsd = Number.isFinite(nextPnl)
          ? (nextPnl + (Number.isFinite(pnlUsd) ? pnlUsd : 0))
          : (Number.isFinite(pnlUsd) ? pnlUsd : null);
        prev.notionalUsd = Number.isFinite(nextNotional)
          ? (nextNotional + (Number.isFinite(Number(en?.notionalUsd)) ? Number(en.notionalUsd) : 0))
          : (Number.isFinite(Number(en?.notionalUsd)) ? Number(en.notionalUsd) : null);
        prev.shares = Number.isFinite(nextShares)
          ? (nextShares + (Number.isFinite(Number(en?.shares)) ? Number(en.shares) : 0))
          : (Number.isFinite(Number(en?.shares)) ? Number(en.shares) : null);
        prev.holdSec =
          Number.isFinite(Number(prev.entryTsMs)) && Number.isFinite(Number(prev.exitTsMs))
            ? Math.max(0, (Number(prev.exitTsMs) - Number(prev.entryTsMs)) / 1000)
            : null;
        prev.lanes = lanes;
        prev.laneCount = lanes.length;
        if (Number.isFinite(Number(prev.pnlUsd)) && Number.isFinite(Number(prev.notionalUsd)) && Number(prev.notionalUsd) > 0) {
          prev.roiPct = (Number(prev.pnlUsd) / Number(prev.notionalUsd)) * 100;
        }
        perSlug.set(slug, prev);
      }
      let sessions: any[] = Array.from(perSlug.values());
      if (includeTrace && sessions.length) {
        // Trace rows are written with SERVER_RUN_ID, which is different from
        // per-bot runNum. For instance scoped history, match by slug/time.
        const inferSlugStartMs = (slugLike: any): number | null => {
          const m = String(slugLike || "").match(/-(\d{10})$/);
          if (!m) return null;
          const sec = Number(m[1]);
          if (!Number.isFinite(sec)) return null;
          const ms = sec * 1000;
          return Number.isFinite(ms) ? ms : null;
        };
        let traceSinceMs = Number(sinceStart);
        for (const s of sessions) {
          const slugStartMs = inferSlugStartMs(s?.slug);
          if (Number.isFinite(Number(slugStartMs)) && Number(slugStartMs) > 946684800000) {
            traceSinceMs = Math.min(Number(traceSinceMs), Number(slugStartMs));
          }
        }
        if (!Number.isFinite(Number(traceSinceMs)) || Number(traceSinceMs) <= 0) {
          traceSinceMs = Number(sinceStart);
        }
        const runTraceBySlug = readSessionTracesBySlug(traceSinceMs, null);
        let traceRowsCache: any[] | null = null;
        const getTraceRows = () => {
          if (traceRowsCache) return traceRowsCache;
          traceRowsCache = readJsonlFiles(listTradeLogFiles())
            .filter((r) => String(r?.engine || "").toLowerCase() === engine)
            .filter((r) => Number(r.t) >= traceSinceMs)
            .filter((r) => {
              const slug = String(r?.market?.slug || "").trim().toLowerCase();
              if (!slug) return false;
              if (marketSlugFilter) return slug === marketSlugFilter;
              if (!marketSlugFilter && marketPrefixFilter) return slug.startsWith(marketPrefixFilter);
              return true;
            });
          return traceRowsCache;
        };
        for (const s of sessions) {
          const slug = String(s?.slug || "");
          if (!slug) continue;
          const trCsv = readSessionTraceFromCsv(slug);
          const trRun = runTraceBySlug.get(slug) || null;
          const trFill = buildSessionTraceFromRecordedLogs(
            getTraceRows(),
            slug,
            engine,
            Number.isFinite(Number(s?.runId)) ? Number(s.runId) : undefined
          );
          const trFallback = buildTraceFromSessionEndpoints(s);
          const trNeutral = buildNeutralTraceFromSlug(s);
          const tr =
            (isTraceUsable(trCsv, 10, 30_000) ? trCsv : null) ||
            (isTraceUsable(trRun, 10, 30_000) ? trRun : null) ||
            trFill ||
            trFallback ||
            trNeutral;
          if (!tr) continue;
          s.trace = {
            xMs: Array.isArray(tr.xMs) ? tr.xMs : [],
            up: Array.isArray(tr.up) ? tr.up.map((v: any) => (Number.isFinite(Number(v)) ? Number(v) : null)) : [],
            down: Array.isArray(tr.down) ? tr.down.map((v: any) => (Number.isFinite(Number(v)) ? Number(v) : null)) : [],
            source: String((tr as any)?.sourceFile || "session_trace_runlog"),
          };
          s.traceSource = String((tr as any)?.sourceFile || "session_trace_runlog");
        }
      }
      if (includeTrace) {
        // Ensure open sessions still hydrate their live chart even if no trade
        // entry/exit has been recorded yet for this instance.
        const inferSlugStartMs = (slugLike: any): number | null => {
          const m = String(slugLike || "").match(/-(\d{10})$/);
          if (!m) return null;
          const sec = Number(m[1]);
          if (!Number.isFinite(sec)) return null;
          const ms = sec * 1000;
          return Number.isFinite(ms) ? ms : null;
        };
        const inferSlugDurationMs = (slugLike: any): number => {
          const s = String(slugLike || "").toLowerCase();
          if (s.includes("updown-15m")) return 15 * 60_000;
          if (s.includes("updown-hourly")) return 60 * 60_000;
          if (s.includes("updown-daily")) return 24 * 60 * 60_000;
          return 5 * 60_000;
        };
        let traceSinceMs = Number(sinceStart);
        for (const s of sessions) {
          const slugStartMs = inferSlugStartMs(s?.slug);
          if (Number.isFinite(Number(slugStartMs)) && Number(slugStartMs) > 946684800000) {
            traceSinceMs = Math.min(Number(traceSinceMs), Number(slugStartMs));
          }
        }
        if (!Number.isFinite(Number(traceSinceMs)) || Number(traceSinceMs) <= 0) {
          traceSinceMs = Number(sinceStart);
        }
        const runTraceBySlug = readSessionTracesBySlug(traceSinceMs, null);
        const existingSlugs = new Set<string>(sessions.map((s) => String(s?.slug || "")));
        for (const [slug, tr] of runTraceBySlug.entries()) {
          if (!slug) continue;
          if (existingSlugs.has(slug)) continue;
          const slugLower = String(slug).toLowerCase();
          if (marketSlugFilter && slugLower !== marketSlugFilter) continue;
          if (!marketSlugFilter && marketPrefixFilter && !slugLower.startsWith(marketPrefixFilter)) continue;
          const xMs = Array.isArray(tr?.xMs) ? tr.xMs : [];
          if (xMs.length < 2) continue;
          const firstTs = Number(xMs[0]);
          const slugStartMs = inferSlugStartMs(slug);
          const durationMs = inferSlugDurationMs(slug);
          const endMs = Number.isFinite(Number(slugStartMs))
            ? Number(slugStartMs) + Number(durationMs) - 1000
            : (Number.isFinite(firstTs) ? firstTs + Number(durationMs) - 1000 : null);
          sessions.push({
            key: `${runNum}:${instanceIdFilter}:${slug}`,
            runId: runNum,
            instanceId: instanceIdFilter,
            slug,
            humanLabel: (() => {
              if (Number.isFinite(Number(slugStartMs))) {
                const base = slug.replace(/-\d{10}$/, "");
                const d = new Date(Number(slugStartMs));
                return `${base} @ ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
              }
              return slug;
            })(),
            endMs: Number.isFinite(Number(endMs)) ? Number(endMs) : null,
            strategyMode: String(inst?.strategyId || "bot_runtime"),
            side: null,
            attemptTsMs: Number.isFinite(firstTs) ? firstTs : null,
            entryTsMs: null,
            exitTsMs: null,
            signalPx: null,
            intendedPx: null,
            entryPx: null,
            exitPx: null,
            pnlUsd: 0,
            roiPct: 0,
            holdSec: 0,
            closed: false,
            attempted: false,
            canceled: false,
            via: "trace_only",
            reason: "no_trade",
            exitType: null,
            exitReasonRaw: null,
            shares: null,
            notionalUsd: null,
            balanceUsd: null,
            feesUsd: 0,
            estSlippageUsd: 0,
            sessionVolumeUsd: null,
            fillEstScorePct: null,
            fillAccuracyPct: null,
            trace: {
              xMs: Array.isArray(tr.xMs) ? tr.xMs : [],
              up: Array.isArray(tr.up) ? tr.up.map((v: any) => (Number.isFinite(Number(v)) ? Number(v) : null)) : [],
              down: Array.isArray(tr.down) ? tr.down.map((v: any) => (Number.isFinite(Number(v)) ? Number(v) : null)) : [],
              source: String((tr as any)?.sourceFile || "session_trace_runlog"),
            },
            traceSource: String((tr as any)?.sourceFile || "session_trace_runlog"),
            lanes: [],
            laneCount: 0,
          });
        }
      }
      // Densify instance history: ensure every session bucket from launch is represented,
      // even when there were no trades in that bucket.
      {
        const inferIntervalMs = (): number => {
          const b = String((inst as any)?.marketBucket || "").toLowerCase();
          if (b === "15m") return 15 * 60_000;
          if (b === "hourly") return 60 * 60_000;
          if (b === "daily") return 24 * 60 * 60_000;
          const sample = String((inst as any)?.marketSlug || sessions[0]?.slug || "").toLowerCase();
          if (sample.includes("updown-15m")) return 15 * 60_000;
          if (sample.includes("updown-hourly")) return 60 * 60_000;
          if (sample.includes("updown-daily")) return 24 * 60 * 60_000;
          return 5 * 60_000;
        };
        const intervalMs = inferIntervalMs();
        const baseSlugRaw = String((inst as any)?.marketSlug || sessions[0]?.slug || "").trim();
        const baseSlug = baseSlugRaw.replace(/-\d{10}$/, "");
        if (baseSlug && Number.isFinite(intervalMs) && intervalMs > 0) {
          const traceSlugSet = new Set<string>();
          if (includeTrace) {
            try {
              const traceBySlug = readSessionTracesBySlug(Number(sinceStart), null);
              for (const slug of traceBySlug.keys()) {
                if (slug && String(slug).startsWith(`${baseSlug}-`)) {
                  traceSlugSet.add(String(slug));
                }
              }
            } catch {}
          }
          const startBal = Number(summary?.startBalanceUsd ?? (inst as any)?.startBalanceUsd ?? 100);
          const existingBySlug = new Map<string, any>();
          for (const s of sessions) {
            const slug = String(s?.slug || "").trim();
            if (!slug) continue;
            existingBySlug.set(slug, s);
          }
          const slotStartMs = Math.floor(Number(sinceStart) / intervalMs) * intervalMs;
          const slotEndMs = Math.floor(Date.now() / intervalMs) * intervalMs;
          let runningBal = Number.isFinite(startBal) ? startBal : 100;
          const dense: any[] = [];
          for (let slotMs = slotStartMs; slotMs <= slotEndMs; slotMs += intervalMs) {
            const slug = `${baseSlug}-${Math.floor(slotMs / 1000)}`;
            if (!existingBySlug.has(slug) && traceSlugSet.has(slug)) {
              continue;
            }
            const d = new Date(slotMs);
            const label = `${baseSlug} @ ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
            const endMs = slotMs + intervalMs - 1000;
            const existing = existingBySlug.get(slug) || null;
            const row = existing ? { ...existing } : {
              key: `${runNum}:${instanceIdFilter}:${slug}`,
              runId: runNum,
              instanceId: instanceIdFilter,
              slug,
              humanLabel: label,
              endMs,
              strategyMode: String(inst?.strategyId || "bot_runtime"),
              side: null,
              attemptTsMs: slotMs,
              entryTsMs: null,
              exitTsMs: null,
              signalPx: null,
              intendedPx: null,
              entryPx: null,
              exitPx: null,
              pnlUsd: 0,
              roiPct: 0,
              holdSec: 0,
              closed: Date.now() >= endMs,
              attempted: false,
              canceled: false,
              via: "no_trade_bucket",
              reason: "no_trade",
              exitType: null,
              exitReasonRaw: null,
              shares: null,
              notionalUsd: null,
              balanceUsd: null,
              feesUsd: 0,
              estSlippageUsd: 0,
              sessionVolumeUsd: null,
              fillEstScorePct: null,
              fillAccuracyPct: null,
              trace: null,
              traceSource: null,
              lanes: [],
              laneCount: 0,
            };
            row.humanLabel = String(row?.humanLabel || label);
            row.endMs = Number.isFinite(Number(row?.endMs)) ? Number(row.endMs) : endMs;
            const bal = Number(row?.balanceUsd);
            const pnl = Number(row?.pnlUsd);
            if (Number.isFinite(bal)) {
              runningBal = bal;
            } else if (Number.isFinite(pnl)) {
              runningBal += pnl;
              row.balanceUsd = runningBal;
            } else {
              row.balanceUsd = runningBal;
            }
            if (!Number.isFinite(Number(row?.attemptTsMs))) row.attemptTsMs = slotMs;
            dense.push(row);
          }
          sessions.splice(0, sessions.length, ...dense);
        }
      }
      const sortTs = (s: any): number => {
        const ex = Number(s?.exitTsMs);
        if (Number.isFinite(ex) && ex > 946684800000) return ex;
        const en = Number(s?.entryTsMs);
        if (Number.isFinite(en) && en > 946684800000) return en;
        const end = Number(s?.endMs);
        if (Number.isFinite(end) && end > 946684800000) return end;
        const at = Number(s?.attemptTsMs);
        if (Number.isFinite(at) && at > 946684800000) return at;
        const m = String(s?.slug || "").match(/-(\d{10})$/);
        if (m && Number.isFinite(Number(m[1]))) return Number(m[1]) * 1000;
        return 0;
      };
      sessions.sort((a, b) => sortTs(b) - sortTs(a));
      if (maxSessions != null && sessions.length > maxSessions) {
        sessions = sessions.slice(0, maxSessions);
      }
      return res.json({
        ok: true,
        engine,
        runId: runNum,
        runStartMs: runStartMsInst,
        runStartIso: new Date(runStartMsInst).toISOString(),
        sessions,
        count: sessions.length,
        sinceMs: sinceStart,
        source: "bot_run_events",
        instanceId: instanceIdFilter,
      });
    }

    const rows = filterTradeRowsByScope(readJsonlFiles(listTradeLogFiles()), engine, scope, sinceMs, runScopeId)
      .sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));

    type LaneSess = {
      lane: string;
      strategyMode: string | null;
      side: OutcomeSide | null;
      attemptTsMs: number | null;
      entryTsMs: number | null;
      exitTsMs: number | null;
      signalPx: number | null;
      intendedPx: number | null;
      entryPx: number | null;
      exitPx: number | null;
      pnlUsd: number | null;
      roiPct: number | null;
      holdSec: number | null;
      closed: boolean;
      attempted: boolean;
      canceled: boolean;
      via: string | null;
      reason: string | null;
      exitType: string | null;
      shares: number | null;
      notionalUsd: number | null;
      balanceUsd: number | null;
      feesUsd: number | null;
      estSlippageUsd: number | null;
      sessionVolumeUsd: number | null;
      fillEstScorePct: number | null;
      fillEstWouldFill: boolean | null;
      fillAccuracyPct: number | null;
    };

    type Sess = {
      key: string;
      runId: number | null;
      instanceId: string | null;
      slug: string;
      endMs: number | null;
      strategyMode: string | null;
      side: OutcomeSide | null;
      attemptTsMs: number | null;
      entryTsMs: number | null;
      exitTsMs: number | null;
      signalPx: number | null;
      intendedPx: number | null;
      entryPx: number | null;
      exitPx: number | null;
      pnlUsd: number | null;
      roiPct: number | null;
      holdSec: number | null;
      closed: boolean;
      attempted: boolean;
      canceled: boolean;
      via: string | null;
      reason: string | null;
      exitType: string | null;
      shares: number | null;
      notionalUsd: number | null;
      balanceUsd: number | null;
      feesUsd: number | null;
      estSlippageUsd: number | null;
      sessionVolumeUsd: number | null;
      fillEstScorePct: number | null;
      fillAccuracyPct: number | null;
      trace?: {
        xMs: number[];
        up: Array<number | null>;
        down: Array<number | null>;
        source?: string;
      } | null;
      traceSource?: string | null;
      lanes?: LaneSess[];
      laneCount?: number;
      // internal aggregation only
      _laneByKey?: Record<string, LaneSess>;
    };

    const bySession = new Map<string, Sess>();
    const inferEndMsFromSlug = (slug: string): number | null => {
      const m = String(slug || "").match(/-(\d{10})$/);
      if (!m) return null;
      const startMs = Number(m[1]) * 1000;
      return Number.isFinite(startMs) ? startMs + 300_000 : null;
    };
    const enterRe = /^ENTER_(UP|DOWN)_(PAPER|LIVE)$/;
    const enterOrderRe = /^ENTER_ORDER_(UP|DOWN)_(PAPER|LIVE)$/;
    const enterCancelRe = /^ENTER_CANCEL_(UP|DOWN)_(PAPER|LIVE)$/;
    const exitRe = /^(EXIT|STOP)_(UP|DOWN)_(PAPER|LIVE)$/;
    const normalizeLaneKey = (row: any): string => {
      const candidates = [row?.strategyLane, row?.execution?.mode, row?.mode]
        .map((v) => String(v ?? "").trim().toUpperCase())
        .filter(Boolean);
      for (const c of candidates) {
        if (c === "NO_CROSS_WINNER" || c === "HIGH_CONFIDENCE" || c === "HIGHCONFIDENCE") return "NO_CROSS_WINNER";
        if (c === "BASE" || c === "MOMENTUM") return "BASE";
      }
      return "BASE";
    };
    const laneModeFromKey = (k: string): string => {
      if (k === "NO_CROSS_WINNER") return "NO_CROSS_WINNER";
      if (k === "BASE") return "BASE";
      return "BASE";
    };
    const getLane = (sess: Sess, laneKey: string): LaneSess => {
      if (!sess._laneByKey) sess._laneByKey = {};
      const key = String(laneKey || "BASE").toUpperCase();
      if (!sess._laneByKey[key]) {
        sess._laneByKey[key] = {
          lane: key,
          strategyMode: laneModeFromKey(key),
          side: null,
          attemptTsMs: null,
          entryTsMs: null,
          exitTsMs: null,
          signalPx: null,
          intendedPx: null,
          entryPx: null,
          exitPx: null,
          pnlUsd: null,
          roiPct: null,
          holdSec: null,
          closed: false,
          attempted: false,
          canceled: false,
          via: null,
          reason: null,
          exitType: null,
          shares: null,
          notionalUsd: null,
          balanceUsd: null,
          feesUsd: 0,
          estSlippageUsd: 0,
          sessionVolumeUsd: null,
          fillEstScorePct: null,
          fillEstWouldFill: null,
          fillAccuracyPct: null,
        };
      }
      return sess._laneByKey[key];
    };

    for (const r of rows) {
      const slug = String(r?.st?.marketSlug || r?.market?.slug || "");
      if (!slug) continue;
      const rowInstanceId = String((r as any)?.instanceId || "").trim();
      const slugLower = String(slug).toLowerCase();
      if (marketSlugFilter && slugLower !== marketSlugFilter) continue;
      if (!marketSlugFilter && marketPrefixFilter && !slugLower.startsWith(marketPrefixFilter)) continue;
      const runIdRaw = Number(r?.runId);
      const runId = Number.isFinite(runIdRaw) ? runIdRaw : null;
      const sessionKey = `${runId ?? "na"}:${rowInstanceId || "na"}:${slug}`;
      const action = String(r?.action || "");
      const t = Number(r?.t);
      const side = (String(r?.st?.side || "").toUpperCase() as OutcomeSide) || null;
      const laneKey = normalizeLaneKey(r);
      const sess = bySession.get(sessionKey) || {
        key: sessionKey,
        runId,
        instanceId: rowInstanceId || null,
        slug,
        endMs: null,
        strategyMode: null,
        side: null,
        attemptTsMs: null,
        entryTsMs: null,
        exitTsMs: null,
        signalPx: null,
        intendedPx: null,
        entryPx: null,
        exitPx: null,
        pnlUsd: null,
        roiPct: null,
        holdSec: null,
        closed: false,
        attempted: false,
        canceled: false,
        via: null,
        reason: null,
        exitType: null,
        shares: null,
        notionalUsd: null,
        balanceUsd: null,
        feesUsd: 0,
        estSlippageUsd: 0,
        sessionVolumeUsd: null,
        fillEstScorePct: null,
        fillAccuracyPct: null,
        trace: null,
        traceSource: null,
        lanes: [],
        laneCount: 0,
        _laneByKey: {},
      };
      const marketEndMs = toFiniteOrNull(r?.market?.endMs);
      const inferredEndMs = inferEndMsFromSlug(slug);
      if (Number.isFinite(Number(marketEndMs))) sess.endMs = Number(marketEndMs);
      else if (!Number.isFinite(Number(sess.endMs)) && Number.isFinite(Number(inferredEndMs))) sess.endMs = Number(inferredEndMs);
      const lane = getLane(sess, laneKey);

      if (enterOrderRe.test(action)) {
        const sessVol = toFiniteOrNull(r?.market?.volumeUsd);
        if (Number.isFinite(sessVol)) {
          lane.sessionVolumeUsd = Math.max(Number(lane.sessionVolumeUsd ?? 0), Number(sessVol));
          sess.sessionVolumeUsd = Math.max(Number(sess.sessionVolumeUsd ?? 0), Number(sessVol));
        }
        if (!lane.attemptTsMs || t < lane.attemptTsMs) lane.attemptTsMs = Number.isFinite(t) ? t : lane.attemptTsMs;
        if (!sess.attemptTsMs || t < sess.attemptTsMs) sess.attemptTsMs = Number.isFinite(t) ? t : sess.attemptTsMs;
        const signalPx = toFiniteOrNull(r?.signalPx ?? r?.execution?.signalPx);
        const intendedPx = toFiniteOrNull(r?.buyOrderPx ?? r?.limitPx ?? r?.execution?.intendedPx);
        if (Number.isFinite(signalPx)) lane.signalPx = signalPx;
        if (Number.isFinite(intendedPx)) lane.intendedPx = intendedPx;
        if (Number.isFinite(signalPx)) sess.signalPx = signalPx;
        if (Number.isFinite(intendedPx)) sess.intendedPx = intendedPx;
        const fillEstScorePct = toFiniteOrNull(r?.execution?.fillEstScorePct);
        const fillEstWouldFillRaw = r?.execution?.fillEstWouldFill;
        const fillEstWouldFill = typeof fillEstWouldFillRaw === "boolean" ? fillEstWouldFillRaw : null;
        if (Number.isFinite(fillEstScorePct)) lane.fillEstScorePct = fillEstScorePct;
        if (fillEstWouldFill !== null) lane.fillEstWouldFill = fillEstWouldFill;
        lane.attempted = true;
        sess.attempted = true;
        const m = action.match(enterOrderRe);
        const sideFromAction = m && (m[1] === "UP" || m[1] === "DOWN") ? (m[1] as OutcomeSide) : null;
        if (!lane.side && sideFromAction) lane.side = sideFromAction;
        if (!sess.side && sideFromAction) sess.side = sideFromAction;
        const strategyMode =
          r?.execution?.mode != null
            ? String(r.execution.mode)
            : (r?.mode != null ? String(r.mode) : null);
        if (strategyMode && !lane.strategyMode) lane.strategyMode = strategyMode;
        if (strategyMode && !sess.strategyMode) sess.strategyMode = strategyMode;
      }

      if (enterCancelRe.test(action)) {
        lane.canceled = true;
        lane.attempted = true;
        if (lane.fillEstWouldFill !== null) lane.fillAccuracyPct = lane.fillEstWouldFill ? 0 : 100;
        sess.canceled = true;
        sess.attempted = true;
      }

      if (enterRe.test(action)) {
        const sessVol = toFiniteOrNull(r?.market?.volumeUsd);
        if (Number.isFinite(sessVol)) {
          lane.sessionVolumeUsd = Math.max(Number(lane.sessionVolumeUsd ?? 0), Number(sessVol));
          sess.sessionVolumeUsd = Math.max(Number(sess.sessionVolumeUsd ?? 0), Number(sessVol));
        }
        if (!lane.entryTsMs || t < lane.entryTsMs) lane.entryTsMs = Number.isFinite(t) ? t : lane.entryTsMs;
        if (!sess.entryTsMs || t < sess.entryTsMs) sess.entryTsMs = Number.isFinite(t) ? t : sess.entryTsMs;
        const px = toFiniteOrNull(r?.execution?.actualPx ?? r?.execution?.actualFillPx ?? r?.st?.entryPx);
        if (Number.isFinite(px)) lane.entryPx = px;
        if (Number.isFinite(px)) sess.entryPx = px;
        const shares = toFiniteOrNull(r?.st?.shares);
        const notional = toFiniteOrNull(r?.st?.notionalUsd);
        const intendedPx = toFiniteOrNull(r?.execution?.intendedPx);
        const actualPx = toFiniteOrNull(r?.execution?.actualPx ?? r?.execution?.actualFillPx ?? r?.st?.entryPx);
        const entryFeeUsd = toFiniteOrNull(r?.st?.entryFeeUsd);
        if (Number.isFinite(shares) && shares > 0) lane.shares = shares;
        if (Number.isFinite(notional) && notional > 0) lane.notionalUsd = notional;
        if (Number.isFinite(shares) && shares > 0) sess.shares = shares;
        if (Number.isFinite(notional) && notional > 0) sess.notionalUsd = notional;
        if (Number.isFinite(entryFeeUsd)) {
          lane.feesUsd = Number(lane.feesUsd ?? 0) + Number(entryFeeUsd);
          sess.feesUsd = Number(sess.feesUsd ?? 0) + Number(entryFeeUsd);
        }
        if (Number.isFinite(shares) && shares > 0 && Number.isFinite(intendedPx) && Number.isFinite(actualPx)) {
          const slipUsd = Math.abs(Number(actualPx) - Number(intendedPx)) * Number(shares);
          lane.estSlippageUsd = Number(lane.estSlippageUsd ?? 0) + slipUsd;
          sess.estSlippageUsd = Number(sess.estSlippageUsd ?? 0) + slipUsd;
        }
        if (!lane.side && (side === "UP" || side === "DOWN")) lane.side = side;
        if (!sess.side && (side === "UP" || side === "DOWN")) sess.side = side;
        if (lane.fillEstWouldFill !== null) lane.fillAccuracyPct = lane.fillEstWouldFill ? 100 : 0;
        const strategyMode =
          r?.execution?.mode != null
            ? String(r.execution.mode)
            : (r?.mode != null ? String(r.mode) : null);
        if (strategyMode && !lane.strategyMode) lane.strategyMode = strategyMode;
        if (strategyMode && !sess.strategyMode) sess.strategyMode = strategyMode;
      }

      if (exitRe.test(action)) {
        const sessVol = toFiniteOrNull(r?.market?.volumeUsd);
        if (Number.isFinite(sessVol)) {
          lane.sessionVolumeUsd = Math.max(Number(lane.sessionVolumeUsd ?? 0), Number(sessVol));
          sess.sessionVolumeUsd = Math.max(Number(sess.sessionVolumeUsd ?? 0), Number(sessVol));
        }
        const px = toFiniteOrNull(r?.execution?.actualPx ?? r?.execution?.actualFillPx ?? r?.st?.exitPx);
        lane.exitTsMs = Number.isFinite(t) ? t : lane.exitTsMs;
        lane.exitPx = Number.isFinite(px) ? px : lane.exitPx;
        sess.exitTsMs = Number.isFinite(t) ? t : sess.exitTsMs;
        sess.exitPx = Number.isFinite(px) ? px : sess.exitPx;
        const entryTsMs = toFiniteOrNull(r?.st?.entryTsMs);
        const entryPx = toFiniteOrNull(r?.execution?.entryPx ?? r?.st?.entryPx);
        if (!lane.entryTsMs && Number.isFinite(entryTsMs)) lane.entryTsMs = entryTsMs;
        if (!Number.isFinite(Number(lane.entryPx)) && Number.isFinite(entryPx)) lane.entryPx = entryPx;
        if (!sess.entryTsMs && Number.isFinite(entryTsMs)) sess.entryTsMs = entryTsMs;
        if (!Number.isFinite(Number(sess.entryPx)) && Number.isFinite(entryPx)) sess.entryPx = entryPx;
        const pnl = toFiniteOrNull(r?.st?.pnlUsd);
        const roi = toFiniteOrNull(r?.st?.roiPct);
        const hold = toFiniteOrNull(r?.st?.holdSec);
        const shares = toFiniteOrNull(r?.st?.shares);
        const notional = toFiniteOrNull(r?.st?.notionalUsd);
        const balance = toFiniteOrNull(r?.st?.balanceUsd);
        const intendedPx = toFiniteOrNull(r?.execution?.intendedPx);
        const actualPx = toFiniteOrNull(r?.execution?.actualPx ?? r?.execution?.actualFillPx ?? r?.st?.exitPx);
        const exitFeeUsd = toFiniteOrNull(r?.st?.exitFeeUsd);
        lane.pnlUsd = Number.isFinite(pnl) ? pnl : lane.pnlUsd;
        lane.roiPct = Number.isFinite(roi) ? roi : lane.roiPct;
        lane.holdSec = Number.isFinite(hold) ? hold : lane.holdSec;
        if (Number.isFinite(shares) && shares > 0) lane.shares = shares;
        if (Number.isFinite(notional) && notional > 0) lane.notionalUsd = notional;
        if (Number.isFinite(balance)) lane.balanceUsd = balance;
        sess.pnlUsd = Number.isFinite(pnl) ? pnl : sess.pnlUsd;
        sess.roiPct = Number.isFinite(roi) ? roi : sess.roiPct;
        sess.holdSec = Number.isFinite(hold) ? hold : sess.holdSec;
        if (Number.isFinite(shares) && shares > 0) sess.shares = shares;
        if (Number.isFinite(notional) && notional > 0) sess.notionalUsd = notional;
        if (Number.isFinite(balance)) sess.balanceUsd = balance;
        if (Number.isFinite(exitFeeUsd)) {
          lane.feesUsd = Number(lane.feesUsd ?? 0) + Number(exitFeeUsd);
          sess.feesUsd = Number(sess.feesUsd ?? 0) + Number(exitFeeUsd);
        }
        if (Number.isFinite(shares) && shares > 0 && Number.isFinite(intendedPx) && Number.isFinite(actualPx)) {
          const slipUsd = Math.abs(Number(actualPx) - Number(intendedPx)) * Number(shares);
          lane.estSlippageUsd = Number(lane.estSlippageUsd ?? 0) + slipUsd;
          sess.estSlippageUsd = Number(sess.estSlippageUsd ?? 0) + slipUsd;
        }
        if (side === "UP" || side === "DOWN") lane.side = side;
        lane.closed = true;
        lane.via = r?.via != null ? String(r.via) : lane.via;
        lane.reason = r?.reason != null ? String(r.reason) : lane.reason;
        lane.exitType = r?.exitType != null ? String(r.exitType) : lane.exitType;
        if (!lane.strategyMode && String(lane.exitType || "").toUpperCase().includes("CROSS")) {
          lane.strategyMode = "NO_CROSS_WINNER";
        }
        if ((!Number.isFinite(lane.holdSec) || lane.holdSec == null) && Number.isFinite(Number(lane.entryTsMs)) && Number.isFinite(Number(lane.exitTsMs))) {
          lane.holdSec = Math.max(0, (Number(lane.exitTsMs) - Number(lane.entryTsMs)) / 1000);
        }

        if (side === "UP" || side === "DOWN") sess.side = side;
        sess.closed = true;
        sess.via = r?.via != null ? String(r.via) : sess.via;
        sess.reason = r?.reason != null ? String(r.reason) : sess.reason;
        sess.exitType = r?.exitType != null ? String(r.exitType) : sess.exitType;
        if (!sess.strategyMode && String(sess.exitType || "").toUpperCase().includes("CROSS")) {
          sess.strategyMode = "NO_CROSS_WINNER";
        }
        if ((!Number.isFinite(sess.holdSec) || sess.holdSec == null) && Number.isFinite(Number(sess.entryTsMs)) && Number.isFinite(Number(sess.exitTsMs))) {
          sess.holdSec = Math.max(0, (Number(sess.exitTsMs) - Number(sess.entryTsMs)) / 1000);
        }
      }

      bySession.set(sessionKey, sess);
    }

    // Roll up per-lane details into one per-session summary.
    for (const sess of bySession.values()) {
      const lanesRaw = Object.values(sess._laneByKey || {});
      const lanes = lanesRaw
        .filter((l) => l.attempted || Number.isFinite(Number(l.entryTsMs)) || Number.isFinite(Number(l.exitTsMs)))
        .sort((a, b) => {
          const rank = (v: string) => (String(v || "").toUpperCase() === "BASE" ? 0 : (String(v || "").toUpperCase() === "NO_CROSS_WINNER" ? 1 : 2));
          return rank(a.lane) - rank(b.lane);
        });
      sess.lanes = lanes;
      sess.laneCount = lanes.length;

      const closedLanes = lanes.filter((l) => l.closed && Number.isFinite(Number(l.pnlUsd)));
      if (closedLanes.length) {
        sess.pnlUsd = closedLanes.reduce((acc, l) => acc + Number(l.pnlUsd), 0);
        const notionalSum = closedLanes.reduce((acc, l) => acc + (Number.isFinite(Number(l.notionalUsd)) ? Number(l.notionalUsd) : 0), 0);
        if (notionalSum > 0 && Number.isFinite(Number(sess.pnlUsd))) {
          sess.roiPct = (Number(sess.pnlUsd) / notionalSum) * 100;
        }
      }
      const feesSum = lanes.reduce((acc, l) => acc + (Number.isFinite(Number(l.feesUsd)) ? Number(l.feesUsd) : 0), 0);
      const slipSum = lanes.reduce((acc, l) => acc + (Number.isFinite(Number(l.estSlippageUsd)) ? Number(l.estSlippageUsd) : 0), 0);
      const fillEstVals = lanes.map((l) => Number(l.fillEstScorePct)).filter((v) => Number.isFinite(v));
      const fillAccVals = lanes.map((l) => Number(l.fillAccuracyPct)).filter((v) => Number.isFinite(v));
      const volVals = lanes.map((l) => Number(l.sessionVolumeUsd)).filter((v) => Number.isFinite(v));
      sess.feesUsd = Number.isFinite(feesSum) ? feesSum : 0;
      sess.estSlippageUsd = Number.isFinite(slipSum) ? slipSum : 0;
      sess.fillEstScorePct = fillEstVals.length ? fillEstVals.reduce((a, b) => a + b, 0) / fillEstVals.length : null;
      sess.fillAccuracyPct = fillAccVals.length ? fillAccVals.reduce((a, b) => a + b, 0) / fillAccVals.length : null;
      sess.sessionVolumeUsd = volVals.length ? Math.max(...volVals) : sess.sessionVolumeUsd;

      const entryTimes = lanes.map((l) => Number(l.entryTsMs)).filter((v) => Number.isFinite(v));
      const exitTimes = lanes.map((l) => Number(l.exitTsMs)).filter((v) => Number.isFinite(v));
      if (entryTimes.length) sess.entryTsMs = Math.min(...entryTimes);
      if (exitTimes.length) sess.exitTsMs = Math.max(...exitTimes);
      if (Number.isFinite(Number(sess.entryTsMs)) && Number.isFinite(Number(sess.exitTsMs))) {
        sess.holdSec = Math.max(0, (Number(sess.exitTsMs) - Number(sess.entryTsMs)) / 1000);
      }

      // Display a neutral side/price in dual-lane sessions to avoid implying one lane.
      if (lanes.length > 1) {
        sess.side = null;
        sess.entryPx = null;
        sess.exitPx = null;
        sess.strategyMode = "DUAL";
      } else if (lanes.length === 1) {
        const l = lanes[0];
        if (!sess.side) sess.side = l.side;
        if (!Number.isFinite(Number(sess.entryPx)) && Number.isFinite(Number(l.entryPx))) sess.entryPx = Number(l.entryPx);
        if (!Number.isFinite(Number(sess.exitPx)) && Number.isFinite(Number(l.exitPx))) sess.exitPx = Number(l.exitPx);
        if (!sess.strategyMode && l.strategyMode) sess.strategyMode = l.strategyMode;
      }

      // Balance after session: use latest closed lane's balance.
      const latestLaneBal = lanes
        .filter((l) => Number.isFinite(Number(l.balanceUsd)) && Number.isFinite(Number(l.exitTsMs)))
        .sort((a, b) => Number(b.exitTsMs) - Number(a.exitTsMs))[0];
      if (latestLaneBal && Number.isFinite(Number(latestLaneBal.balanceUsd))) {
        sess.balanceUsd = Number(latestLaneBal.balanceUsd);
      }

      delete sess._laneByKey;
    }

    let sessions = Array.from(bySession.values())
      .filter((s) => s.attemptTsMs != null || s.entryTsMs != null || s.exitTsMs != null)
      .sort((a, b) => Number(b.exitTsMs || b.entryTsMs || b.attemptTsMs || 0) - Number(a.exitTsMs || a.entryTsMs || a.attemptTsMs || 0));

    if (includeTrace) {
      const traceRunId = scope === "run" ? runScopeId : null;
      const runTraceBySlug = readSessionTracesBySlug(sinceMs, traceRunId);
      const seenSlugs = new Set<string>(sessions.map((s) => String(s.slug || "")));
      for (const [slug, tr] of runTraceBySlug.entries()) {
        if (!slug || seenSlugs.has(slug)) continue;
        const slugLower = String(slug).toLowerCase();
        if (marketSlugFilter && slugLower !== marketSlugFilter) continue;
        if (!marketSlugFilter && marketPrefixFilter && !slugLower.startsWith(marketPrefixFilter)) continue;
        const firstTs = Array.isArray(tr?.xMs) && tr.xMs.length ? Number(tr.xMs[0]) : null;
        sessions.push({
          key: `trace:${slug}`,
          runId: null,
          instanceId: null,
          slug,
          endMs: Number.isFinite(Number(firstTs)) ? Number(firstTs) + 300_000 : inferEndMsFromSlug(slug),
          strategyMode: null,
          side: null,
          attemptTsMs: Number.isFinite(Number(firstTs)) ? Number(firstTs) : null,
          entryTsMs: null,
          exitTsMs: null,
          signalPx: null,
          intendedPx: null,
          entryPx: null,
          exitPx: null,
          pnlUsd: 0,
          roiPct: 0,
          holdSec: 0,
          closed: false,
          attempted: false,
          canceled: false,
          via: "trace_only",
          reason: "no_trade",
          exitType: null,
          shares: null,
          notionalUsd: null,
          balanceUsd: null,
          feesUsd: 0,
          estSlippageUsd: 0,
          sessionVolumeUsd: null,
          fillEstScorePct: null,
          fillAccuracyPct: null,
          trace: {
            xMs: tr.xMs,
            up: tr.up.map((v) => (Number.isFinite(v) ? v : null)),
            down: tr.down.map((v) => (Number.isFinite(v) ? v : null)),
            source: String((tr as any)?.sourceFile || ""),
          },
          traceSource: String((tr as any)?.sourceFile || ""),
        });
      }

      sessions.sort((a, b) => Number(b.exitTsMs || b.entryTsMs || b.attemptTsMs || 0) - Number(a.exitTsMs || a.entryTsMs || a.attemptTsMs || 0));

      for (const s of sessions) {
        const trCsv = readSessionTraceFromCsv(s.slug);
        const trRun = runTraceBySlug.get(s.slug) || null;
        const trFill = buildSessionTraceFromRecordedLogs(rows, s.slug, engine, Number.isFinite(Number(s.runId)) ? Number(s.runId) : undefined);
        const trFallback = buildTraceFromSessionEndpoints(s);
        const trNeutral = buildNeutralTraceFromSlug(s);
        const tr =
          (isTraceUsable(trCsv, 10, 30_000) ? trCsv : null) ||
          (isTraceUsable(trRun, 10, 30_000) ? trRun : null) ||
          trFill ||
          trFallback ||
          trNeutral;
        if (!tr) continue;
        s.trace = {
          xMs: tr.xMs,
          up: tr.up.map((v) => (Number.isFinite(v) ? v : null)),
          down: tr.down.map((v) => (Number.isFinite(v) ? v : null)),
          source: String((tr as any)?.sourceFile || ""),
        };
        s.traceSource = String((tr as any)?.sourceFile || "");

        // Backfill unresolved/legacy rows:
        // 1) legacy rollover rows persisted as SETTLE0 even when final side bid implies 1
        // 2) rows with entry but no recorded exit (commonly from restart/interruption)
        const hasEntryNoExit =
          !s.closed &&
          (s.side === "UP" || s.side === "DOWN") &&
          Number.isFinite(Number(s.entryPx)) &&
          Number.isFinite(Number(s.entryTsMs));
        const legacyForcedSettle0 =
          s.closed &&
          (s.reason === "session_end_no_exit" || s.exitType === "SETTLE0") &&
          Number(s.exitPx) === 0 &&
          (s.side === "UP" || s.side === "DOWN") &&
          Number.isFinite(Number(s.entryPx));
        if (!legacyForcedSettle0 && !hasEntryNoExit) continue;

        const upArr = Array.isArray(s.trace?.up) ? s.trace.up : [];
        const dnArr = Array.isArray(s.trace?.down) ? s.trace.down : [];
        let lastUp: number | null = null;
        let lastDn: number | null = null;
        for (let i = upArr.length - 1; i >= 0; i--) {
          const n = Number(upArr[i]);
          if (Number.isFinite(n)) {
            lastUp = n;
            break;
          }
        }
        for (let i = dnArr.length - 1; i >= 0; i--) {
          const n = Number(dnArr[i]);
          if (Number.isFinite(n)) {
            lastDn = n;
            break;
          }
        }
        const lastSideBid = inferSideBid(s.side as OutcomeSide, { bid: lastUp }, { bid: lastDn });
        if (!Number.isFinite(Number(lastSideBid))) continue;
        const settledPx = Number(lastSideBid) > 0.5 ? 1 : 0;
        const entryPx = Number(s.entryPx);

        let shares = Number(s.shares);
        if (!Number.isFinite(shares) || shares <= 0) {
          const prevPnl = Number(s.pnlUsd);
          if (Number.isFinite(prevPnl) && entryPx > 0) {
            shares = prevPnl / (0 - entryPx);
          }
        }
        if ((!Number.isFinite(shares) || shares <= 0) && Number.isFinite(Number(s.notionalUsd)) && Number(s.notionalUsd) > 0 && entryPx > 0) {
          shares = Number(s.notionalUsd) / entryPx;
        }
        if (!Number.isFinite(shares) || shares <= 0) continue;

        const grossPnlUsd = (settledPx - entryPx) * shares;
        const entryFeeUsd = toNum((s as any).entryFeeUsd);
        const exitFeeUsd = 0;
        const totalFeesUsd = entryFeeUsd + exitFeeUsd;
        const pnlUsd = grossPnlUsd - totalFeesUsd;
        const notionalUsd = Number.isFinite(Number(s.notionalUsd)) && Number(s.notionalUsd) > 0
          ? Number(s.notionalUsd)
          : (entryPx * shares);

        const traceLastTs = Array.isArray(s.trace?.xMs) && s.trace.xMs.length
          ? Number(s.trace.xMs[s.trace.xMs.length - 1])
          : null;
        const inferredExitTs =
          Number.isFinite(traceLastTs) ? Number(traceLastTs)
            : (Number.isFinite(Number(s.exitTsMs)) ? Number(s.exitTsMs)
              : (Number.isFinite(Number((s as any).endMs)) ? Number((s as any).endMs) : null));
        s.closed = true;
        s.exitTsMs = Number.isFinite(inferredExitTs) ? inferredExitTs : s.exitTsMs;
        s.exitPx = settledPx;
        (s as any).grossPnlUsd = grossPnlUsd;
        (s as any).entryFeeUsd = entryFeeUsd;
        (s as any).exitFeeUsd = exitFeeUsd;
        (s as any).totalFeesUsd = totalFeesUsd;
        s.pnlUsd = pnlUsd;
        s.roiPct = notionalUsd > 0 ? (pnlUsd / notionalUsd) * 100 : s.roiPct;
        s.reason = hasEntryNoExit
          ? "session_end_round_last_bid_backfill_missing_exit"
          : "session_end_round_last_bid_backfill";
        s.exitType = "SETTLE_ROUND";
      }
    }

    // Normalize paper session cumulative balances from realized P/L so historical
    // corrections don't leave downstream stale st.balanceUsd snapshots.
    if (engine === "paper") {
      const byRun = new Map<number, Sess[]>();
      for (const s of sessions) {
        const rr = Number(s?.runId);
        if (!Number.isFinite(rr)) continue;
        const arr = byRun.get(rr) || [];
        arr.push(s);
        byRun.set(rr, arr);
      }
      byRun.forEach((runSessions) => {
        runSessions.sort(
          (a, b) =>
            Number(a?.exitTsMs || a?.entryTsMs || a?.attemptTsMs || 0) -
            Number(b?.exitTsMs || b?.entryTsMs || b?.attemptTsMs || 0)
        );

        let runningBalance: number | null = null;
        for (const s of runSessions) {
          const bal = toFiniteOrNull(s?.balanceUsd);
          const pnl = toFiniteOrNull(s?.pnlUsd);
          if (Number.isFinite(bal) && Number.isFinite(pnl)) {
            runningBalance = Number(bal) - Number(pnl);
            break;
          }
        }
        if (!Number.isFinite(runningBalance)) runningBalance = 100;

        for (const s of runSessions) {
          const pnl = toFiniteOrNull(s?.pnlUsd);
          if (!(s?.closed && Number.isFinite(pnl))) continue;
          runningBalance = Number(runningBalance) + Number(pnl);
          s.balanceUsd = round4(Number(runningBalance));
        }
      });
    }

    if (maxSessions != null && sessions.length > maxSessions) {
      sessions = sessions.slice(0, maxSessions);
    }
    res.json({
      ok: true,
      engine,
      runId: Number.isFinite(Number(runScopeId)) ? Number(runScopeId) : SERVER_RUN_ID,
      runStartMs: Number.isFinite(Number(runStartMs)) ? Number(runStartMs) : CURRENT_RUN_START_MS,
      runStartIso: runStartIso || CURRENT_RUN_START_ISO,
      sessions,
      count: sessions.length,
      sinceMs,
      source: "trade_logs",
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.get("/api/export/execution", (req, res) => {
  const formatRaw = String(req.query.format ?? "json").toLowerCase();
  const format = formatRaw === "csv" ? "csv" : "json";
  const dateRaw = String(req.query.date ?? new Date().toISOString().slice(0, 10));
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateRaw)) {
    return res.status(400).json({ ok: false, error: "Invalid date format. Use YYYY-MM-DD." });
  }
  const filePath = executionLogPathForDate(dateRaw);
  const legacyPath = executionLogPathLegacyForDate(dateRaw);
  const existingPath = fs.existsSync(filePath) ? filePath : fs.existsSync(legacyPath) ? legacyPath : "";
  if (!existingPath) {
    return res.status(404).json({ ok: false, error: `No execution log for ${dateRaw}` });
  }
  const lines = fs
    .readFileSync(existingPath, "utf8")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  const rows: any[] = [];
  for (const ln of lines) {
    try {
      rows.push(JSON.parse(ln));
    } catch {}
  }

  const mapped = rows.map((r) => ({
    iso: String(r?.iso ?? ""),
    engine: String(r?.engine ?? ""),
    action: String(r?.action ?? ""),
    marketSlug: String(r?.market?.slug ?? ""),
    phase: String(r?.execution?.phase ?? ""),
    side: String(r?.execution?.side ?? ""),
    signalPx: toFiniteOrNull(r?.execution?.signalPx),
    intendedPx: toFiniteOrNull(r?.execution?.intendedPx),
    actualPx: toFiniteOrNull(r?.execution?.actualPx),
    assumedSlippagePx: toFiniteOrNull(r?.execution?.assumedSlippagePx),
    realizedSlippagePx: toFiniteOrNull(r?.execution?.realizedSlippagePx),
    intendedVsActualPx: toFiniteOrNull(r?.execution?.intendedVsActualPx),
    ttlMs: toFiniteOrNull(r?.execution?.ttlMs),
    fillEstWouldFill:
      typeof r?.execution?.fillEstWouldFill === "boolean" ? r.execution.fillEstWouldFill : null,
    fillEstScorePct: toFiniteOrNull(r?.execution?.fillEstScorePct),
    fillEstRequiredShares: toFiniteOrNull(r?.execution?.fillEstRequiredShares),
    fillEstFillableShares: toFiniteOrNull(r?.execution?.fillEstFillableShares),
    fillEstVwapPx: toFiniteOrNull(r?.execution?.fillEstVwapPx),
  }));

  if (format === "json") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename=\"execution_${dateRaw}.json\"`);
    return res.send(JSON.stringify({ date: dateRaw, rows: mapped }, null, 2));
  }

  const cols = [
    "iso",
    "engine",
    "action",
    "marketSlug",
    "phase",
    "side",
    "signalPx",
    "intendedPx",
    "actualPx",
    "assumedSlippagePx",
    "realizedSlippagePx",
    "intendedVsActualPx",
    "ttlMs",
    "fillEstWouldFill",
    "fillEstScorePct",
    "fillEstRequiredShares",
    "fillEstFillableShares",
    "fillEstVwapPx",
  ];
  const esc = (v: any) => {
    if (v == null) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, "\"\"")}"` : s;
  };
  const csv = [cols.join(","), ...mapped.map((r) => cols.map((c) => esc((r as any)[c])).join(","))].join("\n") + "\n";
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename=\"execution_${dateRaw}.csv\"`);
  return res.send(csv);
});

app.get("/api/test-clob", async (_req, res) => {
  try {
    const client = await getClobClient();
    const balance = await (client as any).getBalanceAllowance?.({ asset_type: "COLLATERAL" });
    res.json({
      ok: true,
      signer: (client as any).signer?.address,
      funder: (client as any).funder,
      balance: balance?.balance,
    });
  } catch (err: any) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/report/daily", (_req, res) => {
  const outJson = path.join(TRADE_LOG_DIR, `${LOG_PREFIX.DAILY_LATEST}daily_report_latest.json`);
  const legacyOutJson = path.join(TRADE_LOG_DIR, "daily_report_latest.json");
  const reportPath = fs.existsSync(outJson) ? outJson : legacyOutJson;
  if (!fs.existsSync(reportPath)) {
    const built = trailing24hDailyReport();
    if (!built) return res.status(500).json({ ok: false, error: "Failed to build daily report." });
    return res.json({ ok: true, report: built });
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    return res.json({ ok: true, report: parsed });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.get("/api/report/run-path-summary", (req, res) => {
  try {
    const runIdReq = Number(req.query.runId ?? SERVER_RUN_ID);
    const runId = Number.isFinite(runIdReq) ? Math.floor(runIdReq) : SERVER_RUN_ID;
    const tradeRows = readJsonlFiles(listTradeLogFiles()).sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
    const summary = summarizeRunPathsFromTradeRows(tradeRows, runId);
    return res.json({
      ok: true,
      generatedAt: isoNow(),
      source: "trade_logs_real_fills",
      currentRunId: SERVER_RUN_ID,
      run: summary,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.post("/api/config", (req, res) => {
  if (CONFIG_WRITE_KEY) {
    const hdrKey = String(req.get("x-config-key") || "").trim();
    const bodyKey = String((req.body && req.body.configWriteKey) || "").trim();
    const queryKey = String((req.query && (req.query as any).configWriteKey) || "").trim();
    const provided = hdrKey || bodyKey || queryKey;
    if (!provided || provided !== CONFIG_WRITE_KEY) {
      return res.status(401).json({ ok: false, error: "Unauthorized config write" });
    }
  }

  const body = req.body ?? {};
  // Paper engine is always on by design.
  uiPaper.enabled = true;
  console.log(`[CFG] paper.enabled=${uiPaper.enabled} live.enabled=${uiLive.enabled} pendingUiLive=${!!pendingUiLive}`);

  const mode: Mode = body.mode === "paper" ? "paper" : "live";

  const normalized: UiConfig = {
    ...(mode === "paper" ? uiPaper : uiLive),
    mode,
    enabled: typeof body.enabled === "boolean" ? body.enabled : mode === "paper" ? uiPaper.enabled : uiLive.enabled,

    entry: Number.isFinite(Number(body.entry)) ? Number(body.entry) : mode === "paper" ? uiPaper.entry : uiLive.entry,
    exit: Number.isFinite(Number(body.exit)) ? Number(body.exit) : mode === "paper" ? uiPaper.exit : uiLive.exit,
    stop: Number.isFinite(Number(body.stop)) ? Number(body.stop) : mode === "paper" ? uiPaper.stop : uiLive.stop,
    useStop: typeof body.useStop === "boolean" ? body.useStop : mode === "paper" ? uiPaper.useStop : uiLive.useStop,
    useStopTimeGate:
      typeof body.useStopTimeGate === "boolean"
        ? body.useStopTimeGate
        : mode === "paper"
          ? uiPaper.useStopTimeGate
          : uiLive.useStopTimeGate,
    stopStartSec:
      Number.isFinite(Number(body.stopStartSec))
        ? Math.max(0, Number(body.stopStartSec))
        : mode === "paper"
          ? uiPaper.stopStartSec
          : uiLive.stopStartSec,
    minEntrySec: Number.isFinite(Number(body.minEntrySec)) ? Number(body.minEntrySec) : mode === "paper" ? uiPaper.minEntrySec : uiLive.minEntrySec,

    kellyOn: typeof body.kellyOn === "boolean" ? body.kellyOn : mode === "paper" ? uiPaper.kellyOn : uiLive.kellyOn,
    kellyMult: Number.isFinite(Number(body.kellyMult)) ? Number(body.kellyMult) : mode === "paper" ? uiPaper.kellyMult : uiLive.kellyMult,
    kellyCap: Number.isFinite(Number(body.kellyCap)) ? Number(body.kellyCap) : mode === "paper" ? uiPaper.kellyCap : uiLive.kellyCap,
    betUsd: Number.isFinite(Number(body.betUsd)) ? Number(body.betUsd) : mode === "paper" ? uiPaper.betUsd : uiLive.betUsd,
    maxBetUsd: Number.isFinite(Number(body.maxBetUsd)) ? Number(body.maxBetUsd) : mode === "paper" ? uiPaper.maxBetUsd : uiLive.maxBetUsd,
    highConfidenceSpread:
      Number.isFinite(Number(body.highConfidenceSpread))
        ? clamp01(Number(body.highConfidenceSpread))
        : mode === "paper"
          ? uiPaper.highConfidenceSpread
          : uiLive.highConfidenceSpread,
    highConfidenceLead:
      Number.isFinite(Number(body.highConfidenceLead))
        ? clamp01(Number(body.highConfidenceLead))
        : mode === "paper"
          ? uiPaper.highConfidenceLead
          : uiLive.highConfidenceLead,
  };
  console.log(
    `[CFG APPLY] mode=${mode} enabled=${normalized.enabled} entry=${normalized.entry} tp=${normalized.exit} ` +
    `sl=${normalized.stop} useStop=${normalized.useStop} stopGate=${normalized.useStopTimeGate} ` +
    `stopStartSec=${normalized.stopStartSec} minEntrySec=${normalized.minEntrySec} ` +
    `hcSpread=${normalized.highConfidenceSpread} hcLead=${normalized.highConfidenceLead}`
  );

  // Enabled toggle controls LIVE only; paper stays always enabled.
  if (typeof body.enabled === "boolean") {
    uiLive.enabled = body.enabled;
    if (pendingUiLive) pendingUiLive.enabled = body.enabled;
    if (body.enabled) {
      void reconcileLivePositionMaybe(true, "enabled-toggle");
    }
  }

  if (mode === "paper") {
    normalized.enabled = true;
    uiPaper = normalized;
    uiPaper.enabled = true;
    stPaper.balanceUsd = paperAccount.balanceUsd;
    reloadEngineStrategy("paper", "paper config apply");
    broadcast({ type: "status", t: nowMs(), status: "applied config immediately (paper engine)", paper: { ui: uiPaper } });
    broadcastState();
    return res.json({ ok: true, appliedNow: true, engine: "paper", ui: uiPaper });
  }

  normalized.enabled = typeof body.enabled === "boolean" ? body.enabled : uiLive.enabled;
  pendingUiLive = normalized;
  broadcast({ type: "status", t: nowMs(), status: "queued config (live engine) for next session", pendingUiLive });
  broadcastState();
  return res.json({ ok: true, queued: true, appliesNextSession: true, engine: "live", pendingUiLive });
});

app.get("/api/v2/drive/para/tree", async (req, res) => {
  try {
    const bucketRaw = String(req.query?.bucket || "").trim().toLowerCase();
    const bucket: ParaBucket | undefined = (
      bucketRaw === "project" ||
      bucketRaw === "area" ||
      bucketRaw === "resource" ||
      bucketRaw === "archive"
    ) ? (bucketRaw as ParaBucket) : undefined;
    const data = await getParaTree(bucket);
    return res.json(data);
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
      hint: "Ensure GOOGLE_APPLICATION_CREDENTIALS and GDRIVE_PARA_ROOT_FOLDER_ID are set and the root folder is shared with the service account.",
    });
  }
});

app.post("/api/v2/drive/para/bootstrap", async (_req, res) => {
  try {
    const data = await bootstrapParaFolders();
    return res.json(data);
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
      hint: "Ensure GOOGLE_APPLICATION_CREDENTIALS and GDRIVE_PARA_ROOT_FOLDER_ID are set and Drive API is enabled.",
    });
  }
});

app.post("/api/v2/drive/para/move", async (req, res) => {
  try {
    const body = req.body ?? {};
    const fileId = String(body.fileId || "").trim();
    const bucketRaw = String(body.bucket || "").trim().toLowerCase();
    const targetFolderId = String(body.targetFolderId || "").trim() || null;
    const bucket: ParaBucket | undefined = (
      bucketRaw === "project" ||
      bucketRaw === "area" ||
      bucketRaw === "resource" ||
      bucketRaw === "archive"
    ) ? (bucketRaw as ParaBucket) : undefined;
    if (!fileId) {
      return res.status(400).json({ ok: false, error: "fileId is required" });
    }
    if (!bucket) {
      return res.status(400).json({ ok: false, error: "bucket is required and must be one of: project|area|resource|archive" });
    }
    const data = await moveFileToParaBucket({ fileId, bucket, targetFolderId });
    return res.json(data);
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
      hint: "If bucket folder is missing, call /api/v2/drive/para/bootstrap first.",
    });
  }
});

app.post("/api/v2/drive/para/archive-run", async (req, res) => {
  try {
    const body = req.body ?? {};
    const fileId = String(body.fileId || "").trim();
    if (!fileId) {
      return res.status(400).json({ ok: false, error: "fileId is required" });
    }
    const data = await archiveRunFolder(fileId);
    return res.json(data);
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
      hint: "If archive bucket is missing, call /api/v2/drive/para/bootstrap first.",
    });
  }
});

app.get("/api/v2/drive/runs/sync-status", (_req, res) => {
  return res.json({
    ok: true,
    enabled: PARA_SYNC_ENABLED,
    intervalMs: PARA_SYNC_INTERVAL_MS,
    areaFolderName: PARA_SYNC_AREA_FOLDER,
    inFlight: paraSyncInFlight,
    runId: SERVER_RUN_ID,
    lastSyncAtMs: paraLastSyncAtMs,
    lastSyncReason: paraLastSyncReason,
    lastSyncError: paraLastSyncError,
    lastSyncSummary: paraLastSyncSummary,
  });
});

app.get("/api/v2/drive/audit", async (_req, res) => {
  try {
    const credPath = String(process.env.GOOGLE_APPLICATION_CREDENTIALS || "").trim();
    const rootFolderId = String(process.env.GDRIVE_PARA_ROOT_FOLDER_ID || "").trim();
    const checks: Record<string, boolean> = {
      credentialsPathSet: !!credPath,
      credentialsFileExists: !!credPath && fs.existsSync(path.resolve(credPath)),
      rootFolderIdSet: !!rootFolderId,
      paraSyncEnabled: !!PARA_SYNC_ENABLED,
      paraSyncHealthy: !!PARA_SYNC_ENABLED && !String(paraLastSyncError || "").trim(),
      paraTreeReachable: false,
      paraBucketsPresent: false,
    };
    let treeErr: string | null = null;
    let tree: any = null;
    try {
      tree = await getParaTree();
      checks.paraTreeReachable = !!tree?.ok;
      const b = tree?.buckets || {};
      checks.paraBucketsPresent = !!(b.project?.found && b.area?.found && b.resource?.found && b.archive?.found);
    } catch (e: any) {
      treeErr = String(e?.message || e || "unknown error");
    }
    return res.json({
      ok: Object.values(checks).every((v) => !!v),
      checks,
      env: {
        credentialsPath: credPath || null,
        rootFolderId: rootFolderId || null,
      },
      sync: {
        enabled: PARA_SYNC_ENABLED,
        intervalMs: PARA_SYNC_INTERVAL_MS,
        inFlight: paraSyncInFlight,
        lastSyncAtMs: paraLastSyncAtMs,
        lastSyncReason: paraLastSyncReason,
        lastSyncError: paraLastSyncError,
      },
      treeSummary: tree?.ok ? tree : null,
      treeError: treeErr,
    });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: String(err?.message || err || "unknown error") });
  }
});

app.post("/api/v2/drive/runs/sync-now", async (req, res) => {
  try {
    const reason = String(req.body?.reason || "manual_sync").trim() || "manual_sync";
    const data = await syncArtifactsToPara(reason);
    if (!data) {
      return res.json({
        ok: true,
        queued: false,
        skipped: true,
        reason: PARA_SYNC_ENABLED ? "sync_in_flight_or_noop" : "sync_disabled",
        runId: SERVER_RUN_ID,
      });
    }
    return res.json(data);
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
    });
  }
});

app.post("/api/v2/drive/runs/:instanceId/sync", async (req, res) => {
  try {
    const instanceId = String(req.params.instanceId || "").trim();
    const b = botInstances.get(instanceId);
    if (!b) return res.status(404).json({ ok: false, error: "instance not found" });
    const data = await syncRunArtifactsToPara({
      runId: Number(b.runNum),
      areaFolderName: PARA_SYNC_AREA_FOLDER,
      localPaths: collectBotRunArtifactPaths(b),
    });
    return res.json({ ok: true, instanceId, runId: b.runId, runNum: b.runNum, sync: data });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: String(err?.message || err || "unknown error") });
  }
});

app.get("/api/v2/strategies", (_req, res) => {
  return res.json({
    ok: true,
    asOfMs: Date.now(),
    items: strategyCatalog(),
  });
});

app.get("/api/v2/markets/hot", async (req, res) => {
  try {
    const limitRaw = Number(req.query?.limit ?? 25);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 25;
    const includeNew = String(req.query?.includeNew ?? "1") !== "0";
    const items = await discoverHotMarkets(limit, includeNew);
    return res.json({
      ok: true,
      asOfMs: Date.now(),
      count: items.length,
      items,
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
    });
  }
});

app.get("/api/v2/markets/audit", async (req, res) => {
  try {
    const limitRaw = Number(req.query?.limit ?? 80);
    const limit = Number.isFinite(limitRaw) ? Math.max(20, Math.min(200, Math.floor(limitRaw))) : 80;
    const data = await auditBtcIntervalMarkets(limit);
    return res.json(data);
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
    });
  }
});

app.post("/api/v2/bots", async (req, res) => {
  try {
    const body = req.body ?? {};
    const marketBucket = normalizeBotMarketBucket(body.marketBucket);
    const marketId = String(body.marketId || "").trim();
    const marketSlug = String(body.marketSlug || marketId || "").trim();
    const marketTitle = String(body.marketTitle || marketSlug || "").trim();
    const strategyIdRaw = String(body.strategyId || "momentum_hc").trim().toLowerCase();
    const strategyId: StrategyId = normalizeStrategyId(strategyIdRaw);
    const mode: Mode = String(body.mode || "paper").trim().toLowerCase() === "live" ? "live" : "paper";
    const watchOnly = body.watchOnly === true;
    const strategyPath = resolveBotStrategyPath(strategyId, String(body.strategyPath || "").trim() || null);
    if (!fs.existsSync(strategyPath)) {
      return res.status(400).json({
        ok: false,
        error: `strategy file missing for ${strategyId}: ${strategyPath}`,
      });
    }
    const entry = clamp01(Number.isFinite(Number(body.entry)) ? Number(body.entry) : Number(uiPaper.entry));
    const exitDefault = (strategyId === "profit_locker" || strategyId === "profit_locker_inflection") ? 0.9 : Number(uiPaper.exit);
    const stopDefault = (strategyId === "profit_locker" || strategyId === "profit_locker_inflection") ? 0.5 : Number(uiPaper.stop);
    const stopStartDefault = (strategyId === "profit_locker" || strategyId === "profit_locker_inflection") ? 20 : Number(uiPaper.stopStartSec || 0);
    const exit = clamp01(Number.isFinite(Number(body.exit)) ? Number(body.exit) : exitDefault);
    const stop = clamp01(Number.isFinite(Number(body.stop)) ? Number(body.stop) : stopDefault);
    const useStop = typeof body.useStop === "boolean" ? body.useStop : true;
    const minEntrySec = Math.max(0, Number.isFinite(Number(body.minEntrySec)) ? Number(body.minEntrySec) : Number(uiPaper.minEntrySec || 0));
    const stopStartSec = Math.max(0, Number.isFinite(Number(body.stopStartSec)) ? Number(body.stopStartSec) : stopStartDefault);
    const betUsd = Math.max(1, Number.isFinite(Number(body.betUsd)) ? Number(body.betUsd) : Number(uiPaper.betUsd || 25));
    const maxBetUsd = Math.max(betUsd, Number.isFinite(Number(body.maxBetUsd)) ? Number(body.maxBetUsd) : Number(uiPaper.maxBetUsd || betUsd));
    const requestedStartBalanceUsd = Math.max(1, Number.isFinite(Number(body.startBalanceUsd)) ? Number(body.startBalanceUsd) : 100);
    const startBalanceUsd = mode === "paper" ? 100 : requestedStartBalanceUsd;
    if (activeBotInstances().length >= BOT_MAX_INSTANCES) {
      return res.status(400).json({ ok: false, error: `bot limit reached (BOT_MAX_INSTANCES=${BOT_MAX_INSTANCES})` });
    }
    if (countByStrategy(strategyId) >= BOT_MAX_PER_STRATEGY) {
      return res.status(400).json({ ok: false, error: `strategy limit reached for ${strategyId} (BOT_MAX_PER_STRATEGY=${BOT_MAX_PER_STRATEGY})` });
    }
    if (betUsd > BOT_MAX_BET_USD || maxBetUsd > BOT_MAX_BET_USD) {
      return res.status(400).json({ ok: false, error: `bet exceeds BOT_MAX_BET_USD=${BOT_MAX_BET_USD}` });
    }
    if (!marketBucket && !marketId && !marketSlug) {
      return res.status(400).json({ ok: false, error: "marketBucket (or marketId/marketSlug) is required" });
    }
    let resolvedMarketId = marketId;
    let resolvedMarketSlug = marketSlug;
    let resolvedMarketTitle = marketTitle;
    if (marketBucket) {
      const cur = await resolveCurrentBucketMarket(marketBucket);
      if (!cur) return res.status(400).json({ ok: false, error: `No current market for bucket ${marketBucket}` });
      resolvedMarketId = String(cur.slug);
      resolvedMarketSlug = String(cur.slug);
      resolvedMarketTitle = `BTC ${marketBucket} Markets`;
    }
    const instanceId = makeId("inst");
    BOT_RUN_SEQ += 1;
    const runNum = BOT_RUN_SEQ;
    const runId = `run_${runNum}_${Date.now().toString(36)}`;
    const now = Date.now();
    const snap = mode === "live" ? computeLatestPnlSnapshot() : null;
    const key = resolvedMarketSlug || resolvedMarketId;
    const latest = snap ? latestSnapshotForBot(snap, { runNum, marketSlug: String(key), marketId: String(key) }) : null;
    const bot: BotInstance = {
      instanceId,
      runId,
      runNum,
      marketBucket: marketBucket ?? null,
      marketId: resolvedMarketId || resolvedMarketSlug,
      marketSlug: resolvedMarketSlug || resolvedMarketId,
      marketTitle: resolvedMarketTitle || resolvedMarketSlug || resolvedMarketId,
      strategyId,
      mode,
      watchOnly,
      status: watchOnly ? "watching" : "running",
      launchedAtMs: now,
      stoppedAtMs: null,
      latestPnlUsd: mode === "paper" ? 0 : (latest?.pnlUsd ?? null),
      latestBalanceUsd: mode === "paper" ? Number(startBalanceUsd) : (latest?.balanceUsd ?? null),
      lastError: null,
      entry,
      exit,
      stop,
      useStop,
      minEntrySec,
      stopStartSec,
      betUsd,
      maxBetUsd,
      startBalanceUsd,
      strategyPath,
    };
    botInstances.set(instanceId, bot);
    const rt = ensureRuntime(bot);
    ensureBotStrategyRuntime(bot);
    writeBotRunSummary(bot, rt);
    appendBotRunEvent(bot, rt, { event: "launch", watchOnly: bot.watchOnly, strategyId: bot.strategyId });
    persistMultiMarketState();
    await tickBotRuntime(bot);
    const rtAfter = botRuntimes.get(instanceId) || null;
    return res.json({ ok: true, ...bot, runtime: rtAfter });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "unknown error"),
    });
  }
});

app.get("/api/v2/bots", (_req, res) => {
  const purged = purgeInactiveAndOrphanBotState("api:v2/bots");
  if (purged > 0) persistMultiMarketState();
  const snap = computeLatestPnlSnapshot();
  const items = Array.from(botInstances.values()).map((b) => {
    const latest = latestSnapshotForBot(snap, b);
    const rt = botRuntimes.get(b.instanceId);
    const srt = botStrategyRuntimes.get(b.instanceId) || null;
    return {
      ...b,
      latestPnlUsd: b.latestPnlUsd ?? latest?.pnlUsd ?? null,
      latestBalanceUsd: b.latestBalanceUsd ?? latest?.balanceUsd ?? null,
      strategyRuntime: srt
        ? {
            loadedAtMs: srt.loadedAtMs,
            lastError: srt.lastError,
            strategyPath: srt.strategyPath,
          }
        : null,
      runtime: rt
        ? {
            lastTickMs: rt.lastTickMs,
            upBid: rt.upBid,
            downBid: rt.downBid,
            ticks: rt.ticks,
            errorCount: rt.errorCount,
            lastError: rt.lastError,
            entered: rt.entered,
            side: rt.side,
            entryPx: rt.entryPx,
            realizedPnlUsd: rt.realizedPnlUsd,
            balanceUsd: rt.balanceUsd,
            closedTrades: rt.closedTrades,
            wins: rt.wins,
            losses: rt.losses,
            lastAction: rt.lastAction,
            lastExitType: rt.lastExitType,
          }
        : null,
    };
  });
  return res.json({
    ok: true,
    asOfMs: Date.now(),
    count: items.length,
    items,
  });
});

app.get("/api/v2/bots/:instanceId", (req, res) => {
  const instanceId = String(req.params.instanceId || "").trim();
  const b = botInstances.get(instanceId);
  if (!b) return res.status(404).json({ ok: false, error: "instance not found" });
  const snap = computeLatestPnlSnapshot();
  const latest = latestSnapshotForBot(snap, b);
  const rt = botRuntimes.get(instanceId) || null;
  const srt = botStrategyRuntimes.get(instanceId) || null;
  return res.json({
    ok: true,
    ...b,
    latestPnlUsd: b.latestPnlUsd ?? latest?.pnlUsd ?? null,
    latestBalanceUsd: b.latestBalanceUsd ?? latest?.balanceUsd ?? null,
    strategyRuntime: srt
      ? {
          loadedAtMs: srt.loadedAtMs,
          lastError: srt.lastError,
          strategyPath: srt.strategyPath,
        }
      : null,
    runtime: rt,
  });
});

app.get("/api/v2/bots/:instanceId/run-index", (req, res) => {
  try {
    const instanceId = String(req.params.instanceId || "").trim();
    const inst = botInstances.get(instanceId);
    if (!inst) return res.status(404).json({ ok: false, error: "instance not found" });
    const runNum = Math.floor(Number(inst.runNum));
    const idxPath = botRunIndexPath(inst.strategyId, runNum);
    if (!fs.existsSync(idxPath)) {
      const rt = botRuntimes.get(instanceId) || null;
      writeBotRunIndex(inst, rt);
    }
    if (!fs.existsSync(idxPath)) {
      return res.status(404).json({ ok: false, error: "run index not found", runNum, strategyId: inst.strategyId });
    }
    const payload = JSON.parse(fs.readFileSync(idxPath, "utf8"));
    return res.json({ ok: true, path: idxPath, ...payload });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.get("/api/v2/bots/isolation", (_req, res) => {
  const now = Date.now();
  const activeItems = Array.from(botInstances.values()).filter((b) => b.status !== "stopped");
  const bySlug = new Map<string, string[]>();
  const instances = activeItems.map((b) => {
    const rt = botRuntimes.get(b.instanceId) || null;
    const expectedSlug = String(b.marketSlug || "").trim().toLowerCase();
    const runtimeSlug = String(rt?.marketSlug || b.marketSlug || "").trim().toLowerCase();
    const lastTickMs = Number(rt?.lastTickMs);
    const runtimeAgeMs = Number.isFinite(lastTickMs) ? Math.max(0, now - lastTickMs) : null;
    if (expectedSlug) {
      const rows = bySlug.get(expectedSlug) || [];
      rows.push(b.instanceId);
      bySlug.set(expectedSlug, rows);
    }
    return {
      instanceId: b.instanceId,
      strategyId: b.strategyId,
      status: b.status,
      expectedMarketSlug: b.marketSlug,
      runtimeMarketSlug: rt?.marketSlug ?? null,
      marketSlugMatch: !runtimeSlug || !expectedSlug ? true : runtimeSlug === expectedSlug,
      runtimeLastTickMs: Number.isFinite(lastTickMs) ? lastTickMs : null,
      runtimeAgeMs,
      runtimeTicks: Number.isFinite(Number(rt?.ticks)) ? Number(rt?.ticks) : 0,
      runtimeErrorCount: Number.isFinite(Number(rt?.errorCount)) ? Number(rt?.errorCount) : 0,
      runtimeLastError: rt?.lastError ?? null,
      runtimeAction: rt?.lastAction ?? null,
      runtimePnlUsd: Number.isFinite(Number(rt?.realizedPnlUsd)) ? Number(rt?.realizedPnlUsd) : null,
      runtimeBalanceUsd: Number.isFinite(Number(rt?.balanceUsd)) ? Number(rt?.balanceUsd) : null,
    };
  });
  const duplicateSlugs = Array.from(bySlug.entries())
    .filter(([, inst]) => inst.length > 1)
    .map(([slug, instanceIds]) => ({ slug, instanceIds }));
  const mismatchedCount = instances.filter((x) => !x.marketSlugMatch).length;
  const staleCount = instances.filter((x) => Number.isFinite(Number(x.runtimeAgeMs)) && Number(x.runtimeAgeMs) > 20_000).length;
  return res.json({
    ok: true,
    asOfMs: now,
    summary: {
      active: instances.length,
      mismatchedCount,
      staleCount,
      duplicateSlugCount: duplicateSlugs.length,
      isolationOk: mismatchedCount === 0,
    },
    duplicateSlugs,
    instances,
  });
});

app.post("/api/v2/bots/:instanceId/stop", (req, res) => {
  const instanceId = String(req.params.instanceId || "").trim();
  const item = botInstances.get(instanceId);
  if (!item) return res.status(404).json({ ok: false, error: "instance not found" });
  const stoppedAtMs = Date.now();
  item.status = "stopped";
  item.stoppedAtMs = stoppedAtMs;
  const rt = botRuntimes.get(instanceId);
  if (rt) {
    if (rt.entered && rt.side && Number.isFinite(Number(rt.entryPx)) && Number.isFinite(Number(rt.shares))) {
      const sideAtExit = rt.side;
      const sideBid = rt.side === "UP" ? Number(rt.upBid) : Number(rt.downBid);
      const exitPx = Number.isFinite(sideBid) ? sideBid : Number(rt.entryPx);
      const pnlUsd = (exitPx - Number(rt.entryPx)) * Number(rt.shares);
      rt.realizedPnlUsd += pnlUsd;
      rt.balanceUsd += pnlUsd;
      rt.closedTrades += 1;
      rt.sessionClosedTrades = Number(rt.sessionClosedTrades || 0) + 1;
      if (pnlUsd > 0) rt.wins += 1;
      else if (pnlUsd < 0) {
        rt.losses += 1;
        rt.sessionLosses = Number(rt.sessionLosses || 0) + 1;
      }
      rt.lastExitType = "settle";
      rt.lastAction = "exit_manual_stop";
      rt.entered = false;
      rt.side = null;
      rt.entryPx = null;
      rt.entryTsMs = null;
      rt.shares = null;
      rt.notionalUsd = null;
      item.latestPnlUsd = Number(rt.realizedPnlUsd.toFixed(6));
      item.latestBalanceUsd = Number(rt.balanceUsd.toFixed(6));
      appendBotRunEvent(item, rt, {
        event: "exit",
        side: sideAtExit,
        pnlUsd,
        exitPx,
        exitType: "manual_stop_settle",
      });
    }
    rt.lastError = rt.lastError || "stopped";
    rt.lastAction = "stopped";
    writeBotRunSummary(item, rt);
  }
  if (rt) {
    appendBotRunEvent(item, rt, { event: "stop", reason: "manual_stop", stoppedAtMs });
  }
  // Hard terminate: remove bot and all runtimes from memory/state so only
  // active tests remain visible and scheduled.
  botRuntimes.delete(instanceId);
  botStrategyRuntimes.delete(instanceId);
  botInstances.delete(instanceId);
  persistMultiMarketState();
  broadcast({ type: "bot_status", t: nowMs(), instanceId, status: "stopped", stoppedAtMs, removed: true });
  return res.json({ ok: true, instanceId, status: "stopped", stoppedAtMs, removed: true });
});

app.get("/api/v2/portfolio/summary", (_req, res) => {
  const snap = computeLatestPnlSnapshot();
  const items = Array.from(botInstances.values()).map((b) => {
    const latest = latestSnapshotForBot(snap, b);
    return {
      ...b,
      latestPnlUsd: b.latestPnlUsd ?? latest?.pnlUsd ?? null,
      latestBalanceUsd: b.latestBalanceUsd ?? latest?.balanceUsd ?? null,
    };
  });
  const running = items.filter((x) => x.status === "running").length;
  const watching = items.filter((x) => x.status === "watching").length;
  const stopped = items.filter((x) => x.status === "stopped").length;
  const errors = items.filter((x) => x.status === "error").length;
  const totalLatestPnlUsd = Number(
    items.reduce((acc, x) => acc + (Number.isFinite(Number(x.latestPnlUsd)) ? Number(x.latestPnlUsd) : 0), 0).toFixed(2)
  );
  const totalLatestBalanceUsd = Number(
    items.reduce((acc, x) => acc + (Number.isFinite(Number(x.latestBalanceUsd)) ? Number(x.latestBalanceUsd) : 0), 0).toFixed(2)
  );
  return res.json({
    ok: true,
    asOfMs: Date.now(),
    totals: {
      markets: items.length,
      running,
      watching,
      stopped,
      errors,
      totalLatestPnlUsd,
      totalLatestBalanceUsd,
    },
  });
});

app.get("/api/v2/portfolio/markets", (_req, res) => {
  const snap = computeLatestPnlSnapshot();
  const items = Array.from(botInstances.values()).map((b) => {
    const latest = latestSnapshotForBot(snap, b);
    const rt = botRuntimes.get(b.instanceId);
    return {
      marketId: b.marketId,
      marketSlug: b.marketSlug,
      marketTitle: b.marketTitle,
      strategyId: b.strategyId,
      strategyPath: b.strategyPath,
      mode: b.mode,
      status: b.status,
      watchOnly: b.watchOnly,
      launchedAtMs: b.launchedAtMs,
      latestPnlUsd: b.latestPnlUsd ?? latest?.pnlUsd ?? null,
      latestBalanceUsd: b.latestBalanceUsd ?? latest?.balanceUsd ?? null,
      runtime: rt
        ? {
            lastTickMs: rt.lastTickMs,
            upBid: rt.upBid,
            downBid: rt.downBid,
            ticks: rt.ticks,
            errorCount: rt.errorCount,
            lastError: rt.lastError,
            entered: rt.entered,
            side: rt.side,
            entryPx: rt.entryPx,
            realizedPnlUsd: rt.realizedPnlUsd,
            balanceUsd: rt.balanceUsd,
            closedTrades: rt.closedTrades,
            wins: rt.wins,
            losses: rt.losses,
            lastAction: rt.lastAction,
            lastExitType: rt.lastExitType,
          }
        : null,
      instanceId: b.instanceId,
      runId: b.runId,
    };
  });
  return res.json({ ok: true, asOfMs: Date.now(), count: items.length, items });
});

app.get("/api/v2/system/risk", (_req, res) => {
  return res.json({
    ok: true,
    limits: {
      maxInstances: BOT_MAX_INSTANCES,
      maxPerStrategy: BOT_MAX_PER_STRATEGY,
      globalMaxNotionalUsd: BOT_GLOBAL_MAX_NOTIONAL_USD,
      maxBetUsd: BOT_MAX_BET_USD,
    },
    usage: {
      activeInstances: activeBotInstances().length,
      openNotionalUsd: Number(currentGlobalOpenNotionalUsd().toFixed(6)),
      byStrategy: {
        momentum_hc: countByStrategy("momentum_hc"),
        confidence_b_phase1: countByStrategy("confidence_b_phase1"),
        confidence_c_pmodel: countByStrategy("confidence_c_pmodel"),
        profit_locker: countByStrategy("profit_locker"),
        kalman: countByStrategy("kalman"),
        profit_locker_inflection: countByStrategy("profit_locker_inflection"),
      },
    },
  });
});

// ===== Polymarket lookups (Gamma) =====
async function gammaEventBySlug(slug: string): Promise<any | null> {
  const data = await getJson(`${GAMMA_BASE}/events`, { slug });
  if (Array.isArray(data)) return data[0] ?? null;
  if (data && typeof data === "object") {
    if (Array.isArray((data as any).data)) return (data as any).data[0] ?? null;
    if ((data as any).slug === slug) return data;
  }
  return null;
}

async function gammaEventBySlugQuick(slug: string, timeoutMs = 900): Promise<any | null> {
  try {
    const ctl = new AbortController();
    const to = setTimeout(() => ctl.abort(), Math.max(120, Math.floor(Number(timeoutMs) || 900)));
    const u = new URL(`${GAMMA_BASE}/events`);
    u.searchParams.set("slug", String(slug || ""));
    const r = await fetch(u.toString(), { method: "GET", signal: ctl.signal });
    clearTimeout(to);
    if (!r.ok) return null;
    const data = await r.json().catch(() => null);
    if (Array.isArray(data)) return data[0] ?? null;
    if (data && typeof data === "object") {
      if (Array.isArray((data as any).data)) return (data as any).data[0] ?? null;
      if ((data as any).slug === slug) return data;
    }
    return null;
  } catch {
    return null;
  }
}

function parseMarketVolumeUsd(mkt: any): number | null {
  const candidates = [mkt?.volumeNum, mkt?.volume, mkt?.volumeUsd, mkt?.volumeUSD, mkt?.totalVolume];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n) && n >= 0) return n;
  }
  return null;
}

type HotMarket = {
  marketId: string;
  slug: string;
  title: string;
  category: string;
  volume24hUsd: number;
  volume1hUsd: number;
  volumeVelocity: number;
  launchedAtMs: number | null;
  hotScore: number;
  isNew: boolean;
  recommendationReason: string;
};

function toScannerText(m: any): string {
  const tags =
    Array.isArray(m?.tags)
      ? m.tags
          .map((t: any) => (typeof t === "string" ? t : t?.name || t?.label || t?.slug || ""))
          .filter(Boolean)
          .join(" ")
      : "";
  return [
    m?.slug,
    m?.marketSlug,
    m?.question,
    m?.title,
    m?.description,
    m?.category,
    m?.groupItemTitle,
    m?.eventCategory,
    tags,
  ]
    .map((v) => String(v || ""))
    .join(" ")
    .toLowerCase();
}

function parseEpochMsLike(v: any): number | null {
  const n = Number(v);
  if (Number.isFinite(n)) {
    if (n > 1e12) return n;
    if (n > 1e9) return n * 1000;
  }
  if (typeof v === "string" && v.trim()) {
    const p = Date.parse(v);
    if (Number.isFinite(p)) return p;
  }
  return null;
}

function classifyDurationBucketByMinutes(mins: number): "5m" | "15m" | "hourly" | "daily" | "weekly" | "monthly" | null {
  if (!Number.isFinite(mins) || mins <= 0) return null;
  if (mins <= 7) return "5m";
  if (mins <= 20) return "15m";
  if (mins <= 90) return "hourly";
  if (mins <= 36 * 60) return "daily";
  if (mins <= 10 * 24 * 60) return "weekly";
  if (mins <= 45 * 24 * 60) return "monthly";
  return null;
}

function marketDurationBucket(m: any): "5m" | "15m" | "hourly" | "daily" | "weekly" | "monthly" | null {
  const startMs =
    parseEpochMsLike(m?.startDate) ??
    parseEpochMsLike(m?.start_time) ??
    parseEpochMsLike(m?.startTime) ??
    parseEpochMsLike(m?.startTimestamp) ??
    parseEpochMsLike(m?.startTs);
  const endMs =
    parseEpochMsLike(m?.endDate) ??
    parseEpochMsLike(m?.end_time) ??
    parseEpochMsLike(m?.endTime) ??
    parseEpochMsLike(m?.endTimestamp) ??
    parseEpochMsLike(m?.endTs);
  if (startMs != null && endMs != null && endMs > startMs) {
    const mins = (endMs - startMs) / 60000;
    return classifyDurationBucketByMinutes(mins);
  }
  return null;
}

function hasAllowedDurationToken(text: string): boolean {
  return (
    /(?:\b5m\b|\b5[- ]?min(?:ute)?s?\b)/i.test(text) ||
    /(?:\b15m\b|\b15[- ]?min(?:ute)?s?\b)/i.test(text) ||
    /(?:\bhourly\b|\b1h\b|\b1 hour\b)/i.test(text) ||
    /(?:\bdaily\b|\b1d\b)/i.test(text) ||
    /(?:\bweekly\b|\b1w\b)/i.test(text) ||
    /(?:\bmonthly\b|\b1mo\b|\b1 month\b)/i.test(text)
  );
}

function isCryptoScannerMarket(m: any): boolean {
  const text = toScannerText(m);
  const isCrypto =
    /\bcrypto\b/i.test(text) ||
    /\bbitcoin\b|\bbtc\b|\beth(?:ereum)?\b|\bsol(?:ana)?\b|\bdoge\b|\bxrp\b|\bbnb\b|\bltc\b|\bavax\b|\bada\b/i.test(text);
  if (!isCrypto) return false;
  const byDuration = marketDurationBucket(m);
  if (byDuration) return true;
  return hasAllowedDurationToken(text);
}

function isCryptoMarketRelaxed(m: any): boolean {
  const text = toScannerText(m);
  return (
    /\bcrypto\b/i.test(text) ||
    /\bbitcoin\b|\bbtc\b|\beth(?:ereum)?\b|\bsol(?:ana)?\b|\bdoge\b|\bxrp\b|\bbnb\b|\bltc\b|\bavax\b|\bada\b/i.test(text)
  );
}

async function fetchGammaMarkets(limit: number): Promise<any[]> {
  const capped = Math.max(25, Math.min(500, limit));
  const candidates = [
    `${GAMMA_BASE}/markets?active=true&closed=false&limit=${capped}`,
    `${GAMMA_BASE}/markets?limit=${capped}`,
  ];
  for (const url of candidates) {
    try {
      const r = await fetch(url, { method: "GET" });
      if (!r.ok) continue;
      const data = await r.json();
      if (Array.isArray(data)) return data;
      if (Array.isArray((data as any)?.data)) return (data as any).data;
    } catch {}
  }
  return [];
}

function toHotMarket(m: any): HotMarket | null {
  const slug = String(m?.slug || m?.marketSlug || "").trim();
  if (!slug) return null;
  const marketId = String(m?.id || m?.marketId || slug);
  const title = String(m?.question || m?.title || slug);
  const category = String(m?.category || m?.groupItemTitle || "unknown");
  const text = toScannerText(m);
  const volume24hUsd = Number(parseMarketVolumeUsd(m) ?? 0);
  const volume1hUsd = Math.max(0, volume24hUsd * 0.2);
  const volumeVelocity = volume24hUsd > 0 ? Math.max(0.1, Math.min(10, volume1hUsd / Math.max(1, volume24hUsd / 24))) : 0;
  const launchedAtMsRaw = Number(
    m?.createdAtTimestamp ??
      m?.createdAtEpochMs ??
      (m?.createdAt ? Date.parse(String(m.createdAt)) : NaN)
  );
  const launchedAtMs = Number.isFinite(launchedAtMsRaw) ? launchedAtMsRaw : null;
  const ageMs = launchedAtMs != null ? Math.max(0, Date.now() - launchedAtMs) : Number.POSITIVE_INFINITY;
  const isNew = ageMs <= 48 * 60 * 60 * 1000;
  const freshnessBoost = isNew ? 1.2 : 1.0;
  const durationBoost = (marketDurationBucket(m) || hasAllowedDurationToken(text)) ? 8 : 0;
  const btcUpDownBoost = /\bbitcoin\b.*\bup\b.*\bdown\b|\bbtc\b.*\bup\b.*\bdown\b|up-or-down/i.test(text) ? 12 : 0;
  const hotScore = Number((Math.log10(volume24hUsd + 1) * 25 * freshnessBoost + volumeVelocity * 10 + durationBoost + btcUpDownBoost).toFixed(2));
  const reason =
    btcUpDownBoost > 0
      ? "Bitcoin up/down priority market"
      : isNew
      ? "High-volume new listing"
      : "Top volume market";
  return {
    marketId,
    slug,
    title,
    category,
    volume24hUsd: Number(volume24hUsd.toFixed(2)),
    volume1hUsd: Number(volume1hUsd.toFixed(2)),
    volumeVelocity: Number(volumeVelocity.toFixed(3)),
    launchedAtMs,
    hotScore,
    isNew,
    recommendationReason: reason,
  };
}

async function discoverHotMarkets(limit: number, includeNew: boolean): Promise<HotMarket[]> {
  const gammaRows = await fetchGammaMarkets(Math.max(240, limit * 12));
  let hot = gammaRows
    .filter((m) => isCryptoScannerMarket(m))
    .map(toHotMarket)
    .filter((x): x is HotMarket => !!x);
  // Gamma rows sometimes omit duration/category fields; avoid empty scanner by
  // relaxing to crypto-only symbols/categories when strict duration parsing yields nothing.
  if (!hot.length) {
    hot = gammaRows
      .filter((m) => isCryptoMarketRelaxed(m))
      .map(toHotMarket)
      .filter((x): x is HotMarket => !!x);
  }
  if (!hot.length) {
    const fallback = await getMostCurrent5mMarket().catch(() => null);
    if (fallback) {
      hot = [
        {
          marketId: fallback.slug,
          slug: fallback.slug,
          title: fallback.slug,
          category: "crypto",
          volume24hUsd: Number(fallback.volumeUsd || 0),
          volume1hUsd: Number((Number(fallback.volumeUsd || 0) * 0.2).toFixed(2)),
          volumeVelocity: 1,
          launchedAtMs: Number(fallback.startMs),
          hotScore: 50,
          isNew: true,
          recommendationReason: "Fallback current tradable market",
        },
      ];
    }
  }
  const intervalRows = await getCurrentBtcIntervalMarkets().catch(() => []);
  if (Array.isArray(intervalRows) && intervalRows.length) {
    const intervalHot: HotMarket[] = intervalRows.map((x) => ({
      marketId: x.slug,
      slug: x.slug,
      title: x.slug,
      category: "crypto",
      volume24hUsd: Number(x.volumeUsd || 0),
      volume1hUsd: Number((Number(x.volumeUsd || 0) * 0.2).toFixed(2)),
      volumeVelocity: 1.5,
      launchedAtMs: Number(x.startMs),
      hotScore: x.bucket === "5m" ? 99 : x.bucket === "15m" ? 96 : x.bucket === "hourly" ? 94 : 92,
      isNew: true,
      recommendationReason: `Current BTC ${x.bucket} market`,
    }));
    hot = [...intervalHot, ...hot];
  }
  hot = dedupeHotMarketsBySlug(hot);
  hot = hot
    .filter((x) => includeNew || !x.isNew)
    .sort((a, b) => Number(b.hotScore) - Number(a.hotScore) || Number(b.volume24hUsd) - Number(a.volume24hUsd))
    .slice(0, limit);
  return hot;
}

type IntervalBucket = "5m" | "15m" | "hourly" | "daily";

function inferIntervalBucketFromSlug(slugLike: string): IntervalBucket | null {
  const slug = String(slugLike || "").toLowerCase();
  if (!slug) return null;
  if (/(?:^|-)5m-/.test(slug) || slug.includes("updown-5m")) return "5m";
  if (/(?:^|-)15m-/.test(slug) || slug.includes("updown-15m") || slug.includes("updown-15min")) return "15m";
  if (slug.includes("hourly") || slug.includes("updown-1h")) return "hourly";
  if (slug.includes("daily") || slug.includes("updown-1d")) return "daily";
  return null;
}

async function auditBtcIntervalMarkets(limit: number = 80) {
  const mustHave: IntervalBucket[] = ["5m", "15m", "hourly", "daily"];
  const currentRows = await getCurrentBtcIntervalMarkets().catch(() => []);
  const hot = await discoverHotMarkets(Math.max(20, limit), true).catch(() => []);

  const byBucket: Record<IntervalBucket, { fromCurrent: boolean; fromHot: boolean; ok: boolean; slugCurrent: string | null; slugHot: string | null }> = {
    "5m": { fromCurrent: false, fromHot: false, ok: false, slugCurrent: null, slugHot: null },
    "15m": { fromCurrent: false, fromHot: false, ok: false, slugCurrent: null, slugHot: null },
    "hourly": { fromCurrent: false, fromHot: false, ok: false, slugCurrent: null, slugHot: null },
    "daily": { fromCurrent: false, fromHot: false, ok: false, slugCurrent: null, slugHot: null },
  };

  for (const row of currentRows || []) {
    const b = (row as any)?.bucket as IntervalBucket;
    if (!b || !byBucket[b]) continue;
    byBucket[b].fromCurrent = true;
    byBucket[b].slugCurrent = String((row as any)?.slug || "") || null;
  }
  for (const item of hot || []) {
    const slug = String((item as any)?.slug || "");
    const reason = String((item as any)?.recommendationReason || "").toLowerCase();
    let b = inferIntervalBucketFromSlug(slug);
    if (!b) {
      if (reason.includes("btc 5m")) b = "5m";
      else if (reason.includes("btc 15m")) b = "15m";
      else if (reason.includes("btc hourly")) b = "hourly";
      else if (reason.includes("btc daily")) b = "daily";
    }
    if (!b || !byBucket[b]) continue;
    byBucket[b].fromHot = true;
    byBucket[b].slugHot = slug || byBucket[b].slugHot;
  }
  for (const b of mustHave) {
    byBucket[b].ok = !!(byBucket[b].fromCurrent && byBucket[b].fromHot);
  }
  const okCount = mustHave.filter((b) => byBucket[b].ok).length;
  return {
    ok: okCount === mustHave.length,
    asOfMs: Date.now(),
    required: mustHave,
    okCount,
    total: mustHave.length,
    byBucket,
  };
}

function dedupeHotMarketsBySlug(items: HotMarket[]): HotMarket[] {
  const bySlug = new Map<string, HotMarket>();
  for (const row of items || []) {
    const slug = String(row?.slug || "").trim();
    if (!slug) continue;
    const prev = bySlug.get(slug);
    if (!prev) {
      bySlug.set(slug, row);
      continue;
    }
    const prevScore = Number(prev.hotScore || 0);
    const nextScore = Number(row.hotScore || 0);
    bySlug.set(slug, nextScore >= prevScore ? row : prev);
  }
  return Array.from(bySlug.values());
}

let latestPnlSnapshotCache:
  | {
      builtAtMs: number;
      filesSig: string;
      snap: Map<string, { pnlUsd: number | null; balanceUsd: number | null; tsMs: number }>;
    }
  | null = null;

function tradeLogFilesSig(files: string[]): string {
  const parts: string[] = [];
  for (const f of files) {
    try {
      const st = fs.statSync(f);
      parts.push(`${path.basename(f)}:${Math.round(Number(st.mtimeMs || 0))}:${Number(st.size || 0)}`);
    } catch {
      parts.push(`${path.basename(f)}:0:0`);
    }
  }
  return parts.join("|");
}

function computeLatestPnlSnapshot(): Map<string, { pnlUsd: number | null; balanceUsd: number | null; tsMs: number }> {
  const out = new Map<string, { pnlUsd: number | null; balanceUsd: number | null; tsMs: number }>();
  const files = listTradeLogFiles();
  const sig = tradeLogFilesSig(files);
  const now = Date.now();
  if (
    latestPnlSnapshotCache &&
    latestPnlSnapshotCache.filesSig === sig &&
    (now - Number(latestPnlSnapshotCache.builtAtMs || 0)) <= SNAPSHOT_CACHE_TTL_MS
  ) {
    return latestPnlSnapshotCache.snap;
  }
  const rows = readJsonlFiles(files);
  for (const r of rows) {
    const slug = String(r?.market?.slug || r?.st?.marketSlug || "").trim();
    if (!slug) continue;
    const action = String(r?.action || "");
    if (!/^(EXIT|STOP)_(UP|DOWN)_(PAPER|LIVE)$/.test(action)) continue;
    const t = Number(r?.t);
    if (!Number.isFinite(t)) continue;
    const runIdRaw = Number(r?.runId);
    const runId = Number.isFinite(runIdRaw) ? Math.floor(runIdRaw) : null;
    const keys = [slug];
    if (runId != null) keys.push(`${runId}:${slug}`);
    const pnlUsdRaw = Number(r?.st?.pnlUsd);
    const balRaw = Number(r?.st?.balanceUsd);
    const nextVal = {
      pnlUsd: Number.isFinite(pnlUsdRaw) ? Number(pnlUsdRaw) : null,
      balanceUsd: Number.isFinite(balRaw) ? Number(balRaw) : null,
      tsMs: t,
    };
    for (const k of keys) {
      const prev = out.get(k);
      if (prev && Number(prev.tsMs) > t) continue;
      out.set(k, nextVal);
    }
  }
  latestPnlSnapshotCache = {
    builtAtMs: now,
    filesSig: sig,
    snap: out,
  };
  return out;
}

function latestSnapshotForBot(
  snap: Map<string, { pnlUsd: number | null; balanceUsd: number | null; tsMs: number }>,
  bot: Pick<BotInstance, "runNum" | "marketSlug" | "marketId">
) {
  const slug = String(bot.marketSlug || bot.marketId || "").trim();
  if (!slug) return null;
  const runNum = Number(bot.runNum);
  const byRun = Number.isFinite(runNum) ? snap.get(`${Math.floor(runNum)}:${slug}`) : null;
  if (byRun) return byRun;
  return snap.get(slug) || null;
}

function floorToIntervalEpoch(tsSec: number, intervalSec: number) {
  const iv = Math.max(60, Math.floor(Number(intervalSec) || 60));
  return tsSec - (tsSec % iv);
}

async function findCurrentMarketForBaseSlugs(
  baseSlugs: string[],
  intervalSec: number
): Promise<{ slug: string; startMs: number; endMs: number; upToken: string; downToken: string; volumeUsd: number | null } | null> {
  const nowSec = Math.floor(Date.now() / 1000);
  const floorTs = floorToIntervalEpoch(nowSec, intervalSec);
  const starts = [floorTs, floorTs + intervalSec, floorTs - intervalSec];
  for (const base of baseSlugs) {
    const baseSlug = String(base || "").trim();
    if (!baseSlug) continue;
    for (const startTs of starts) {
      const slug = `${baseSlug}-${startTs}`;
      const evt = await gammaEventBySlug(slug).catch(() => null);
      if (!evt) continue;
      const markets = evt?.markets;
      if (!Array.isArray(markets) || !markets.length) continue;
      const mkt = markets[0];
      const { up, down } = extractUpDownTokensFromMarket(mkt);
      const volumeUsd = parseMarketVolumeUsd(mkt);
      const endDate = mkt?.endDate ?? evt?.endDate;
      if (!endDate) continue;
      const endMs = parseIsoZ(String(endDate)).getTime();
      const startMs = startTs * 1000;
      const n = Date.now();
      if (n >= startMs && n < endMs) {
        return { slug, startMs, endMs, upToken: up, downToken: down, volumeUsd };
      }
    }
  }
  return null;
}

async function getCurrentBtcIntervalMarkets(): Promise<Array<{ bucket: "5m" | "15m" | "hourly" | "daily"; slug: string; startMs: number; endMs: number; upToken: string; downToken: string; volumeUsd: number | null }>> {
  const out: Array<{ bucket: "5m" | "15m" | "hourly" | "daily"; slug: string; startMs: number; endMs: number; upToken: string; downToken: string; volumeUsd: number | null }> = [];
  const defs: Array<{ bucket: "5m" | "15m" | "hourly" | "daily"; intervalSec: number; baseSlugs: string[] }> = [
    { bucket: "5m", intervalSec: 300, baseSlugs: Array.from(new Set(BTC_INTERVAL_SLUGS["5m"].filter(Boolean))) },
    { bucket: "15m", intervalSec: 900, baseSlugs: Array.from(new Set(BTC_INTERVAL_SLUGS["15m"].filter(Boolean))) },
    { bucket: "hourly", intervalSec: 3600, baseSlugs: Array.from(new Set(BTC_INTERVAL_SLUGS["hourly"].filter(Boolean))) },
    { bucket: "daily", intervalSec: 86400, baseSlugs: Array.from(new Set(BTC_INTERVAL_SLUGS["daily"].filter(Boolean))) },
  ];

  // First pass: discover currently-live interval markets directly from Gamma metadata,
  // which supports human-time slugs like "bitcoin-up-or-down-march-1-5am-et".
  try {
    const rows = await fetchGammaMarkets(260);
    const now = Date.now();
    const byBucket = new Map<"5m" | "15m" | "hourly" | "daily", any>();
    for (const m of rows) {
      if (!isCryptoMarketRelaxed(m)) continue;
      const bucket = marketDurationBucket(m);
      if (!(bucket === "5m" || bucket === "15m" || bucket === "hourly" || bucket === "daily")) continue;
      const startMs =
        parseEpochMsLike(m?.startDate) ??
        parseEpochMsLike(m?.start_time) ??
        parseEpochMsLike(m?.startTime) ??
        parseEpochMsLike(m?.startTimestamp) ??
        parseEpochMsLike(m?.startTs);
      const endMs =
        parseEpochMsLike(m?.endDate) ??
        parseEpochMsLike(m?.end_time) ??
        parseEpochMsLike(m?.endTime) ??
        parseEpochMsLike(m?.endTimestamp) ??
        parseEpochMsLike(m?.endTs);
      if (!(Number.isFinite(Number(startMs)) && Number.isFinite(Number(endMs)) && Number(endMs) > Number(startMs))) continue;
      if (!(now >= Number(startMs) && now < Number(endMs))) continue;
      const prev = byBucket.get(bucket);
      const prevVol = Number(parseMarketVolumeUsd(prev) || 0);
      const vol = Number(parseMarketVolumeUsd(m) || 0);
      if (!prev || vol >= prevVol) byBucket.set(bucket, m);
    }

    for (const [bucket, m] of byBucket.entries()) {
      try {
        const slug = String(m?.slug || m?.marketSlug || "").trim();
        if (!slug) continue;
        const marketStartMs =
          parseEpochMsLike(m?.startDate) ??
          parseEpochMsLike(m?.start_time) ??
          parseEpochMsLike(m?.startTime) ??
          parseEpochMsLike(m?.startTimestamp) ??
          parseEpochMsLike(m?.startTs);
        const marketEndMs =
          parseEpochMsLike(m?.endDate) ??
          parseEpochMsLike(m?.end_time) ??
          parseEpochMsLike(m?.endTime) ??
          parseEpochMsLike(m?.endTimestamp) ??
          parseEpochMsLike(m?.endTs);
        if (!(Number.isFinite(Number(marketStartMs)) && Number.isFinite(Number(marketEndMs)))) continue;
        const evt = await gammaEventBySlug(slug).catch(() => null);
        const mkt = Array.isArray(evt?.markets) && evt?.markets?.length ? evt.markets[0] : m;
        const { up, down } = extractUpDownTokensFromMarket(mkt);
        out.push({
          bucket,
          slug,
          startMs: Number(marketStartMs),
          endMs: Number(marketEndMs),
          upToken: up,
          downToken: down,
          volumeUsd: parseMarketVolumeUsd(mkt),
        });
      } catch {}
    }
  } catch {}

  const seenBuckets = new Set(out.map((x) => x.bucket));
  // Fallback pass: fixed base slug patterns.
  for (const def of defs) {
    if (seenBuckets.has(def.bucket)) continue;
    const one = await findCurrentMarketForBaseSlugs(def.baseSlugs, def.intervalSec).catch(() => null);
    if (!one) continue;
    out.push({ bucket: def.bucket, ...one });
  }
  return out;
}

async function getMostCurrent5mMarket(): Promise<{ slug: string; startMs: number; endMs: number; upToken: string; downToken: string; volumeUsd: number | null }> {
  const baseSlugs = Array.from(new Set([BASE_SLUG, ...BTC_INTERVAL_SLUGS["5m"].filter(Boolean)].map((s) => String(s || "").trim()).filter(Boolean)));
  for (;;) {
    const nowSec = Math.floor(Date.now() / 1000);
    const floorTs = floorTo5mEpoch(nowSec);
    const starts = [floorTs, floorTs - 300, floorTs + 300];
    const candidateRows: Array<{ slug: string; startTs: number }> = [];
    for (const base of baseSlugs) {
      for (const ts of starts) candidateRows.push({ slug: `${base}-${ts}`, startTs: ts });
    }
    const settled = await Promise.all(
      candidateRows.map(async (c) => ({
        slug: c.slug,
        startTs: c.startTs,
        evt: await gammaEventBySlugQuick(c.slug, 900),
      }))
    );
    let best: { slug: string; startMs: number; endMs: number; upToken: string; downToken: string; volumeUsd: number | null } | null = null;
    const n = Date.now();
    for (const row of settled) {
      const evt = row.evt;
      if (!evt) continue;
      const markets = evt?.markets;
      if (!Array.isArray(markets) || !markets.length) continue;
      const mkt = markets[0];
      const { up, down } = extractUpDownTokensFromMarket(mkt);
      const endDate = mkt?.endDate ?? evt?.endDate;
      if (!endDate) continue;
      const endMs = parseIsoZ(String(endDate)).getTime();
      const startMs = Number(row.startTs) * 1000;
      if (!(n >= startMs && n < endMs)) continue;
      const volumeUsd = parseMarketVolumeUsd(mkt);
      const cand = { slug: row.slug, startMs, endMs, upToken: up, downToken: down, volumeUsd };
      if (!best || Number(cand.startMs) > Number(best.startMs)) best = cand;
    }
    if (best) return best;
    await sleep(250);
  }
}

let lastMarketMetricsRefreshMs = 0;
async function refreshCurrentMarketMetricsMaybe(force = false) {
  const now = nowMs();
  if (!force && now - lastMarketMetricsRefreshMs < 10_000) return;
  if (!current.slug) return;
  lastMarketMetricsRefreshMs = now;
  try {
    const evt = await gammaEventBySlug(current.slug);
    const mkt = Array.isArray(evt?.markets) && evt.markets.length ? evt.markets[0] : null;
    if (!mkt) return;
    const volumeUsd = parseMarketVolumeUsd(mkt);
    if (Number.isFinite(Number(volumeUsd))) current.volumeUsd = Number(volumeUsd);
  } catch {}
}

// ===== LIVE balance polling =====
async function refreshLiveBalanceMaybe() {
  // In paper-only mode (or when live creds are absent), skip live balance polling.
  if (!uiLive.enabled) return;
  if (!process.env.POLY_PRIVATE_KEY) {
    const now = nowMs();
    const last = ((refreshLiveBalanceMaybe as any).__lastNoKeyWarnMs ?? 0) as number;
    if (now - last > 30_000) {
      (refreshLiveBalanceMaybe as any).__lastNoKeyWarnMs = now;
      console.warn("[LIVE BAL] Skipping live balance poll: POLY_PRIVATE_KEY not configured");
    }
    return;
  }

  const now = Date.now();
  if (now - liveAccount.lastFetchMs < 4000) return;
  liveAccount.lastFetchMs = now;

  try {
    const client = await getClobClient();
    await (client as any).updateBalanceAllowance?.({ asset_type: "COLLATERAL" });
    const resp = await (client as any).getBalanceAllowance?.({ asset_type: "COLLATERAL" });

    if (resp) console.log("[LIVE BAL RAW]", JSON.stringify(resp, null, 2));

    const bal = Number(resp?.balance ?? "0");
    if (Number.isFinite(bal) && bal > 0) {
      liveAccount.balanceUsd = bal / 1e6;
      console.log(`[LIVE BAL] Success: ${liveAccount.balanceUsd} USDC`);
    } else {
      console.warn("[LIVE BAL] Zero or invalid:", bal, resp);
    }
  } catch (e: any) {
    console.error("[LIVE BAL ERROR]", e.message, e.stack);
    broadcast({ type: "status", t: nowMs(), status: `live balance error: ${e.message}` });
  }
}

// ===== Trading =====

async function getTickNegRisk(tokenID: string): Promise<{ tickSize: string; negRisk: boolean }> {
  // Cache token metadata to avoid extra RTT before every order.
  const cacheKey = String(tokenID);
  const hit = (getTickNegRisk as any).__cache?.get(cacheKey);
  if (hit) return hit;
  const client = await getClobClient();
  // @ts-ignore
  const tickSize = (await client.getTickSize(tokenID)) || "0.01";
  // @ts-ignore
  const negRisk = Boolean(await client.getNegRisk(tokenID));
  const out = { tickSize: String(tickSize), negRisk };
  if (!(getTickNegRisk as any).__cache) (getTickNegRisk as any).__cache = new Map<string, { tickSize: string; negRisk: boolean }>();
  (getTickNegRisk as any).__cache.set(cacheKey, out);
  return out;
}

function myAddressSetFromClient(client: any): Set<string> {
  const signerAddr = String((client as any)?.signer?.address || "").toLowerCase();
  const funderAddr = String((client as any)?.funder || process.env.POLY_FUNDER_ADDRESS || "").toLowerCase();
  return new Set<string>([signerAddr, funderAddr].filter(Boolean));
}

async function inferRecentBuyFillFromTrades(
  client: any,
  tokenId: string,
  fallbackPx: number,
  startedAtMs: number
): Promise<{ filledPx: number; shares: number } | null> {
  const trades = await (client as any).getTrades?.({ asset_id: tokenId }, true);
  const list = coerceTradesList(trades);
  const mine = myAddressSetFromClient(client);
  let inferredShares = 0;
  let inferredNotional = 0;
  for (const tr of list) {
    const tsMs = tradeTsMs(tr);
    if (Number.isFinite(tsMs as number) && (tsMs as number) < startedAtMs - 10_000) continue;
    const delta = signedTradeDeltaForMe(tr, mine);
    if (delta <= 0) continue;
    const px = Number(tr?.price ?? tr?.avgPrice ?? tr?.avg_fill_price ?? fallbackPx);
    const usePx = Number.isFinite(px) && px > 0 ? px : fallbackPx;
    inferredShares += delta;
    inferredNotional += delta * usePx;
  }
  if (!(inferredShares > 0)) return null;
  const inferredPx = inferredNotional > 0 ? inferredNotional / inferredShares : fallbackPx;
  return { filledPx: inferredPx, shares: inferredShares };
}

async function getLiveTokenPositionShares(client: any, tokenId: string): Promise<number | null> {
  try {
    const positions =
      (await (client as any).getPositions?.()) ??
      (await (client as any).getOpenPositions?.()) ??
      null;
    const posList = Array.isArray(positions) ? positions : [];
    if (posList.length) {
      const p = posList.find(
        (x: any) => String(x?.asset_id ?? x?.assetId ?? x?.token_id ?? x?.tokenId ?? "") === String(tokenId)
      );
      const v = Number(p?.size ?? p?.position ?? p?.shares ?? p?.quantity ?? p?.balance ?? 0);
      return Number.isFinite(v) ? Math.max(0, v) : 0;
    }
  } catch (e: any) {
    console.warn(`[POSITION SHARES] positions lookup failed tokenId=${tokenId}: ${String(e?.message ?? e)}`);
  }

  try {
    const trades = await (client as any).getTrades?.({ asset_id: tokenId }, true);
    const list = coerceTradesList(trades);
    const mine = myAddressSetFromClient(client);
    let net = 0;
    for (const tr of list) net += signedTradeDeltaForMe(tr, mine);
    return Math.max(0, net);
  } catch (e: any) {
    console.warn(`[POSITION SHARES] trade fallback failed tokenId=${tokenId}: ${String(e?.message ?? e)}`);
  }

  return null;
}

async function cancelOrderRobust(client: any, orderId: string): Promise<void> {
  const attempts: Array<() => Promise<any>> = [
    // @polymarket/clob-client v5 expects OrderPayload: { orderID: string }
    () => (client as any).cancelOrder?.({ orderID: orderId }),
    // Compatible fallback for some gateway variants.
    () => (client as any).cancelOrders?.([orderId]),
    // Legacy payload variants as last resort.
    () => (client as any).cancelOrder?.({ orderId: orderId }),
    () => (client as any).cancelOrder?.({ id: orderId }),
    () => (client as any).cancelOrder?.(orderId),
  ];
  let lastErr: any = null;
  for (const a of attempts) {
    try {
      await a();
      return;
    } catch (e: any) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error(`cancel failed orderId=${orderId}`);
}

function orderStatusUpper(order: any): string {
  return String(order?.status || "").toUpperCase();
}

function isTerminalOrderStatus(status: string): boolean {
  return (
    status.includes("FILLED") ||
    status.includes("CANCEL") ||
    status.includes("EXPIRED") ||
    status.includes("REJECT")
  );
}

function openOrderId(order: any): string {
  return String(order?.id ?? order?.orderID ?? order?.orderId ?? "");
}

function openOrderTokenId(order: any): string {
  return String(order?.asset_id ?? order?.assetId ?? order?.token_id ?? order?.tokenId ?? "");
}

async function getOpenBuyOrderIdsForToken(client: any, tokenId: string): Promise<Set<string>> {
  const open = await (client as any).getOpenOrders?.({ asset_id: tokenId });
  const openOrders = Array.isArray(open) ? open : [];
  const ids = new Set<string>();
  for (const o of openOrders) {
    if (String(o?.side || "").toUpperCase() !== "BUY") continue;
    if (openOrderTokenId(o) !== String(tokenId)) continue;
    const oid = openOrderId(o);
    if (oid) ids.add(oid);
  }
  return ids;
}

async function cancelBuyOrderAndVerifyClosed(
  client: any,
  orderId: string,
  tokenId: string,
  reason: string
): Promise<boolean> {
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      await cancelOrderRobust(client, orderId);
    } catch (e: any) {
      console.warn(
        `[BUY LIMIT] cancel attempt failed reason=${reason} orderId=${orderId} attempt=${attempt}: ${String(
          e?.message ?? e
        )}`
      );
    }

    try {
      const ord = await (client as any).getOrder(orderId);
      const status = orderStatusUpper(ord);
      if (isTerminalOrderStatus(status)) {
        console.log(`[BUY LIMIT] cancel verified by status reason=${reason} orderId=${orderId} status=${status}`);
        return true;
      }
    } catch {}

    try {
      const openBuyIds = await getOpenBuyOrderIdsForToken(client, tokenId);
      if (!openBuyIds.has(orderId)) {
        console.log(`[BUY LIMIT] cancel verified by open-orders reason=${reason} orderId=${orderId}`);
        return true;
      }
    } catch {}

    await sleep(250);
  }
  return false;
}

async function placeLiveLimitBuy(
  side: OutcomeSide,
  price: number,
  notionalUsd: number,
  ui: UiConfig,
  ttlMs: number
): Promise<{ filledPx: number; shares: number; tpOrderId: string | null; tokenId: string; filledAtMs: number }> {
  if (side == null) {
    console.error("[CRITICAL] side is undefined in placeLiveLimitBuy");
    console.trace();
    throw new Error("side is undefined - cannot place buy order");
  }
  if (!["UP", "DOWN"].includes(side)) throw new Error(`Invalid side value: ${side}`);

  console.log(`[BUY LIMIT] Attempt: slug=${current.slug} side=${side}, price=${price}, notionalUsd=${notionalUsd}`);

  if (!Number.isFinite(price) || price <= 0 || price >= 1) throw new Error(`invalid buy price: ${price} (must be >0 and <1)`);
  if (!Number.isFinite(notionalUsd) || notionalUsd <= 0) throw new Error(`invalid notionalUsd: ${notionalUsd}`);
  const preBuyBalanceUsd = Number(liveAccount.balanceUsd);

  const rawSize = notionalUsd / price;
  let size = Math.min(rawSize, MAX_SHARES_PER_TRADE);
  if (!Number.isFinite(size) || size <= 0) throw new Error(`invalid calculated size: ${size}`);
  if (rawSize > MAX_SHARES_PER_TRADE) {
    console.log(`[BUY] share cap applied rawSize=${roundTo6(rawSize)} cappedSize=${roundTo6(size)} max=${MAX_SHARES_PER_TRADE}`);
  }
  if (size < MIN_LIVE_ORDER_SHARES) {
    const minSize = Math.min(MIN_LIVE_ORDER_SHARES, MAX_SHARES_PER_TRADE);
    const minNotionalUsd = minSize * price;
    const balUsd = Number.isFinite(preBuyBalanceUsd) ? preBuyBalanceUsd : 0;
    if (balUsd >= minNotionalUsd) {
      console.log(
        `[BUY] auto-bump size ${roundTo6(size)} -> ${roundTo6(minSize)} to satisfy minimum ` +
        `${MIN_LIVE_ORDER_SHARES} shares (minNotionalUsd=${roundTo6(minNotionalUsd)})`
      );
      size = minSize;
    } else {
      throw new Error(
        `[BUY] size ${roundTo6(size)} below exchange minimum ${MIN_LIVE_ORDER_SHARES} shares ` +
        `(notionalUsd=${notionalUsd}, price=${price}, minNotionalUsd=${roundTo6(minNotionalUsd)}, balanceUsd=${roundTo6(balUsd)})`
      );
    }
  }

  const client = await getClobClient();
  const tokenId = tokenIdForSide(side);

  const { tickSize, negRisk } = await getTickNegRisk(tokenId);

  const ttl = Math.max(0, Number(ttlMs) || 0);
  if (!Number.isFinite(ttl) || ttl <= 0) throw new Error(`[BUY] invalid ttlMs: ${ttlMs}`);
  const buyStartedAtMs = Date.now();
  const deadline = Date.now() + ttl;

  console.log(`[BUY] tokenId=${tokenId}, tickSize=${tickSize}, negRisk=${negRisk}, size=${roundTo6(size)} ttlMs=${ttl}`);

  let order: any;
  try {
    order = await client.createAndPostOrder(
      {
        tokenID: tokenId,
        price: roundTo6(price),
        side: ClobSide.BUY,
        size: roundTo6(size),
        feeRateBps: 1000,
      },
      { tickSize: tickSize as any, negRisk },

      OrderType.GTC
    );
  } catch (e: any) {
    console.error("[BUY LIMIT ERROR]", e?.message || e, e?.stack || "");
    broadcast({ type: "status", t: nowMs(), status: `buy limit failed: ${String(e?.message ?? e)}` });
    throw e;
  }

  const orderId = (order as any)?.orderID || (order as any)?.orderId || (order as any)?.id;
  if (!orderId) {
    const errHint =
      (order as any)?.error ||
      (order as any)?.message ||
      (order ? JSON.stringify(order).slice(0, 300) : "empty response");
    throw new Error(`[BUY] order did not return orderID: ${String(errHint)}`);
  }
  console.log(`[BUY LIMIT] Placed orderId=${orderId}`);
  broadcast({ type: "status", t: nowMs(), status: `BUY ORDER PLACED side=${side} orderId=${orderId}` });

  // ✅ CHANGE: accept ANY filled shares (partial or complete)
  let lastTradeCheckMs = 0;
  while (Date.now() < deadline) {
    const loopNow = Date.now();
    const st = await (client as any).getOrder(orderId);
    const status = String(st?.status || "").toUpperCase();

    const filledShares = Number(st?.filledSize ?? st?.sizeFilled ?? 0);
    const avgPx = Number(st?.avgFillPrice ?? st?.avgPrice ?? st?.filledPrice ?? NaN);

    if (Number.isFinite(filledShares) && filledShares > 0) {
      const filledPx = Number.isFinite(avgPx) ? avgPx : price;
      const filledAtMs = nowMs();
      console.log(`[BUY LIMIT] FILLED(partial/complete) orderId=${orderId} status=${status} px=${filledPx} shares=${filledShares}`);
      broadcast({
        type: "status",
        t: filledAtMs,
        status: `BUY ORDER FILLED orderId=${orderId} px=${filledPx} shares=${roundTo6(filledShares)}`,
      });
      let tpOrderId: string | null = null;
      try {
        tpOrderId = await placeImmediateTpSellAfterBuy(side, filledShares, ui, tokenId, {
          buyFilledAtMs: filledAtMs,
          source: "order_poll",
          entryFillPx: filledPx,
        });
      } catch (tpErr: any) {
        console.error(`[IMMEDIATE TP SELL ERROR] ${String(tpErr?.message ?? tpErr)}`);
      }
      if (filledShares + 1e-6 < size) {
        const canceled = await cancelBuyOrderAndVerifyClosed(client as any, String(orderId), tokenId, "partial_fill");
        if (!canceled) {
          console.warn(
            `[BUY LIMIT] partial fill remainder cancel not confirmed orderId=${orderId} filled=${roundTo6(
              filledShares
            )} original=${roundTo6(size)}`
          );
        }
      }
      return { filledPx, shares: filledShares, tpOrderId, tokenId, filledAtMs };
    }

    if (loopNow - lastTradeCheckMs >= BUY_TRADE_CHECK_MS) {
      lastTradeCheckMs = loopNow;
      try {
        const inferred = await inferRecentBuyFillFromTrades(client as any, tokenId, price, buyStartedAtMs);
        if (inferred && inferred.shares > 0) {
          const filledAtMs = nowMs();
          console.log(
            `[BUY LIMIT] FAST INFERRED FILLED from trades orderId=${orderId} px=${inferred.filledPx} shares=${roundTo6(inferred.shares)}`
          );
          broadcast({
            type: "status",
            t: filledAtMs,
            status: `BUY ORDER FILLED inferred_from_trades_fast orderId=${orderId} px=${inferred.filledPx} shares=${roundTo6(
              inferred.shares
            )}`,
          });
          let tpOrderId: string | null = null;
          try {
            tpOrderId = await placeImmediateTpSellAfterBuy(side, inferred.shares, ui, tokenId, {
              buyFilledAtMs: filledAtMs,
              source: "trade_infer_fast",
              entryFillPx: inferred.filledPx,
            });
          } catch (tpErr: any) {
            console.error(`[IMMEDIATE TP SELL ERROR] ${String(tpErr?.message ?? tpErr)}`);
          }
          return { filledPx: inferred.filledPx, shares: inferred.shares, tpOrderId, tokenId, filledAtMs };
        }
      } catch (e: any) {
        console.warn(`[BUY LIMIT] fast trade check failed orderId=${orderId}: ${String(e?.message ?? e)}`);
      }
    }

    await sleep(BUY_FILL_POLL_MS);
  }

  console.log(`[BUY LIMIT] TTL expired -> cancel orderId=${orderId}`);
  const timeoutCanceled = await cancelBuyOrderAndVerifyClosed(client as any, String(orderId), tokenId, "ttl_expired");
  if (!timeoutCanceled) {
    throw new Error(`[BUY LIMIT] ttl expired and cancel not confirmed (orderId=${orderId})`);
  }

  // ✅ TTL edge re-check (also accepts partial fills)
  try {
    const st2 = await (client as any).getOrder(orderId);
    const status2 = String(st2?.status || "").toUpperCase();

    const filledShares2 = Number(st2?.filledSize ?? st2?.sizeFilled ?? 0);
    const avgPx2 = Number(st2?.avgFillPrice ?? st2?.avgPrice ?? st2?.filledPrice ?? NaN);

    if (Number.isFinite(filledShares2) && filledShares2 > 0) {
      const filledPx = Number.isFinite(avgPx2) ? avgPx2 : price;
      const filledAtMs = nowMs();
      console.log(`[BUY LIMIT] FILLED after TTL edge orderId=${orderId} status=${status2} px=${filledPx} shares=${filledShares2}`);
      broadcast({
        type: "status",
        t: filledAtMs,
        status: `BUY ORDER FILLED orderId=${orderId} px=${filledPx} shares=${roundTo6(filledShares2)}`,
      });
      let tpOrderId: string | null = null;
      try {
        tpOrderId = await placeImmediateTpSellAfterBuy(side, filledShares2, ui, tokenId, {
          buyFilledAtMs: filledAtMs,
          source: "ttl_edge",
          entryFillPx: filledPx,
        });
      } catch (tpErr: any) {
        console.error(`[IMMEDIATE TP SELL ERROR] ${String(tpErr?.message ?? tpErr)}`);
      }
      return { filledPx, shares: filledShares2, tpOrderId, tokenId, filledAtMs };
    }
  } catch {}

  // Final fallback: infer fill from recent exchange trades for this token.
  try {
    const inferred = await inferRecentBuyFillFromTrades(client as any, tokenId, price, buyStartedAtMs);
    if (inferred && inferred.shares > 0) {
      const inferredPx = inferred.filledPx;
      const inferredShares = inferred.shares;
      const filledAtMs = nowMs();
      console.log(`[BUY LIMIT] INFERRED FILLED from trades px=${inferredPx} shares=${roundTo6(inferredShares)} tokenId=${tokenId}`);
      broadcast({
        type: "status",
        t: filledAtMs,
        status: `BUY ORDER FILLED inferred_from_trades px=${inferredPx} shares=${roundTo6(inferredShares)}`,
      });
      let tpOrderId: string | null = null;
      try {
        tpOrderId = await placeImmediateTpSellAfterBuy(side, inferredShares, ui, tokenId, {
          buyFilledAtMs: filledAtMs,
          source: "trade_infer",
          entryFillPx: inferredPx,
        });
      } catch (tpErr: any) {
        console.error(`[IMMEDIATE TP SELL ERROR] ${String(tpErr?.message ?? tpErr)}`);
      }
      return { filledPx: inferredPx, shares: inferredShares, tpOrderId, tokenId, filledAtMs };
    }
  } catch (e: any) {
    console.warn(`[BUY LIMIT] trade-infer fallback failed: ${String(e?.message ?? e)}`);
  }

  // Last-resort fallback: infer fill from balance delta when order polling is inconsistent.
  // This is intentionally conservative and only applies right after a live buy attempt.
  try {
    await (client as any).updateBalanceAllowance?.({ asset_type: "COLLATERAL" });
    const balResp = await (client as any).getBalanceAllowance?.({ asset_type: "COLLATERAL" });
    const postBalRaw = Number(balResp?.balance ?? NaN);
    const postBalUsd = Number.isFinite(postBalRaw) ? postBalRaw / 1e6 : NaN;
    if (Number.isFinite(postBalUsd) && postBalUsd > 0) liveAccount.balanceUsd = postBalUsd;

    if (Number.isFinite(preBuyBalanceUsd) && Number.isFinite(postBalUsd)) {
      const spentUsd = Math.max(0, preBuyBalanceUsd - postBalUsd);
      if (spentUsd >= 0.25) {
        const inferredSharesRaw = spentUsd / price;
        const inferredShares = Math.min(Math.max(inferredSharesRaw, 0), MAX_SHARES_PER_TRADE);
        if (Number.isFinite(inferredShares) && inferredShares > 0) {
          const filledAtMs = nowMs();
          console.warn(
            `[BUY LIMIT] INFERRED FILLED from balance delta spent=${roundTo6(spentUsd)} pre=${preBuyBalanceUsd} post=${postBalUsd} ` +
            `px=${price} shares=${roundTo6(inferredShares)} tokenId=${tokenId}`
          );
          broadcast({
            type: "status",
            t: filledAtMs,
            status: `BUY ORDER FILLED inferred_from_balance px=${price} shares=${roundTo6(inferredShares)}`,
          });
          let tpOrderId: string | null = null;
          try {
            tpOrderId = await placeImmediateTpSellAfterBuy(side, inferredShares, ui, tokenId, {
              buyFilledAtMs: filledAtMs,
              source: "balance_infer",
              entryFillPx: price,
            });
          } catch (tpErr: any) {
            console.error(`[IMMEDIATE TP SELL ERROR] ${String(tpErr?.message ?? tpErr)}`);
          }
          return { filledPx: price, shares: inferredShares, tpOrderId, tokenId, filledAtMs };
        }
      }
    }
  } catch (e: any) {
    console.warn(`[BUY LIMIT] balance-infer fallback failed: ${String(e?.message ?? e)}`);
  }

  throw new Error(`[BUY LIMIT] Not filled within ttlMs=${ttl} (orderId=${orderId})`);
}

async function placeImmediateTpSellAfterBuy(
  side: OutcomeSide,
  shares: number,
  ui: UiConfig,
  tokenIdOverride?: string | null,
  meta?: { buyFilledAtMs?: number; source?: string; entryFillPx?: number }
): Promise<string> {
  if (!Number.isFinite(shares) || shares <= 0) throw new Error(`invalid shares for immediate TP sell: ${shares}`);
  const entryFillPx = Number(meta?.entryFillPx);
  const tpPx = computeImmediateTpSellPx(ui, entryFillPx);
  const tpStartMs = nowMs();
  const buyToTpStartMs = Number.isFinite(Number(meta?.buyFilledAtMs)) ? Math.max(0, tpStartMs - Number(meta?.buyFilledAtMs)) : null;
  console.log(
    `[TP LATENCY] source=${meta?.source ?? "unknown"} side=${side} ` +
    `buyFilledToTpStartMs=${buyToTpStartMs ?? "na"} shares=${roundTo6(shares)} ` +
    `tpPx=${tpPx} mode=absolute_ui_exit entryFillPx=${Number.isFinite(entryFillPx) ? entryFillPx : "na"}`
  );

  const client = await getClobClient();
  const tokenId = tokenIdOverride || tokenIdForSide(side);
  const { tickSize, negRisk } = await getTickNegRisk(tokenId);

  // Idempotency guard: if a TP sell is already open for this token, reuse it.
  try {
    const open = await (client as any).getOpenOrders?.({ asset_id: tokenId });
    const openOrders = Array.isArray(open) ? open : [];
    const existingSell = openOrders.find((o: any) => String(o?.side || "").toUpperCase() === "SELL");
    if (existingSell) {
      const existingId = String(existingSell?.id ?? existingSell?.orderID ?? existingSell?.orderId ?? "");
      if (existingId) {
        console.log(`[IMMEDIATE TP SELL] existing open sell reused orderId=${existingId} side=${side} tokenId=${tokenId}`);
        return existingId;
      }
    }
  } catch (e: any) {
    console.warn(`[IMMEDIATE TP SELL] open-order precheck failed: ${String(e?.message ?? e)}`);
  }
  let lastErr: any = null;
  const requestedShares = floorTo6(shares);
  if (!(requestedShares > 0)) throw new Error(`[IMMEDIATE TP SELL] invalid floored shares: ${requestedShares}`);
  for (let attempt = 1; attempt <= 40; attempt++) {
    let sellShares = requestedShares;
    const available = await getLiveTokenPositionShares(client as any, tokenId);
    if (Number.isFinite(available as number) && Number(available) > 0) {
      const capped = floorTo6(Math.min(sellShares, Number(available)));
      if (capped > 0 && capped < sellShares) {
        console.log(`[IMMEDIATE TP SELL] clamped shares ${roundTo6(sellShares)} -> ${roundTo6(capped)} from position`);
      }
      sellShares = capped;
    }
    // If position visibility lags (available=0/null), still attempt posting the original size.
    if (!(sellShares > 0)) sellShares = requestedShares;

    try {
      // Avoid posting duplicates if a previous attempt already placed a sell.
      const open = await (client as any).getOpenOrders?.({ asset_id: tokenId });
      const openOrders = Array.isArray(open) ? open : [];
      const existingSell = openOrders.find((o: any) => String(o?.side || "").toUpperCase() === "SELL");
      if (existingSell) {
        const existingId = String(existingSell?.id ?? existingSell?.orderID ?? existingSell?.orderId ?? "");
        if (existingId) {
          console.log(`[IMMEDIATE TP SELL] existing open sell reused orderId=${existingId} side=${side} tokenId=${tokenId}`);
          return existingId;
        }
      }

      const ord = await client.createAndPostOrder(
        {
          tokenID: tokenId,
          price: roundTo6(tpPx),
          side: ClobSide.SELL,
          size: sellShares,
          feeRateBps: 1000,
        },
        { tickSize: tickSize as any, negRisk },
        OrderType.GTC
      );

      const orderId = (ord as any)?.orderID || (ord as any)?.orderId || (ord as any)?.id;
      if (!orderId) throw new Error("[IMMEDIATE TP SELL] no orderId returned");
      const tpPlacedMs = nowMs();
      const tpRoundTripMs = tpPlacedMs - tpStartMs;
      const buyToTpPlacedMs = Number.isFinite(Number(meta?.buyFilledAtMs)) ? Math.max(0, tpPlacedMs - Number(meta?.buyFilledAtMs)) : null;

      console.log(
        `[IMMEDIATE TP SELL] placed side=${side} px=${tpPx} shares=${roundTo6(sellShares)} orderId=${orderId} slug=${current.slug} ` +
        `tpSubmitRoundTripMs=${tpRoundTripMs} buyFilledToTpPlacedMs=${buyToTpPlacedMs ?? "na"} attempt=${attempt}`
      );
      broadcast({
        type: "status",
        t: nowMs(),
        status:
          `SELL ORDER PLACED orderId=${orderId} px=${tpPx} shares=${roundTo6(sellShares)} (immediate TP)` +
          ` latencyMs=${buyToTpPlacedMs ?? "na"}`,
      });
      return String(orderId);
    } catch (e: any) {
      lastErr = e;
      const msg = String(e?.message ?? e).toLowerCase();
      if (msg.includes("not enough balance") || msg.includes("allowance")) {
        await sleep(Math.min(200 + 100 * attempt, 1200));
        continue;
      }
      throw e;
    }
  }
  throw lastErr ?? new Error("[IMMEDIATE TP SELL] failed after retries");
}

async function cancelOpenSellOrdersForSide(side: OutcomeSide, reason: string, tokenIdOverride?: string | null): Promise<number> {
  if (!side) return 0;
  const client = await getClobClient();
  const tokenId = tokenIdOverride || tokenIdForSide(side);
  const open = await (client as any).getOpenOrders?.({ asset_id: tokenId });
  const openOrders = Array.isArray(open) ? open : [];
  const sellOrders = openOrders.filter((o: any) => String(o?.side || "").toUpperCase() === "SELL");
  let canceled = 0;
  for (const o of sellOrders) {
    const oid = String(o?.id ?? o?.orderID ?? o?.orderId ?? "");
    if (!oid) continue;
    try {
      await cancelOrderRobust(client as any, String(oid));
      canceled += 1;
      console.log(`[CANCEL OPEN SELL] reason=${reason} side=${side} orderId=${oid}`);
    } catch (e: any) {
      console.warn(`[CANCEL OPEN SELL ERROR] reason=${reason} side=${side} orderId=${oid} err=${String(e?.message ?? e)}`);
    }
  }
  if (canceled > 0) {
    broadcast({
      type: "status",
      t: nowMs(),
      status: `Canceled ${canceled} open sell order(s) for ${side} before ${reason}`,
    });
  }
  return canceled;
}

async function maybeEnsureLiveTakeProfitOrder() {
  if (!uiLive.enabled) return;
  if (!stLive.entered || stLive.exited || !stLive.side) return;
  if (!Number.isFinite(stLive.shares) || (stLive.shares as number) <= 0) return;
  if (stLive.tpOrderInFlight) return;

  const now = Date.now();
  if (stLive.lastTpArmAttemptMs && now - stLive.lastTpArmAttemptMs < 3000) return;
  stLive.lastTpArmAttemptMs = now;
  stLive.tpOrderInFlight = true;

  try {
    const client = await getClobClient();
    const tokenId = stLive.positionTokenId || tokenIdForSide(stLive.side);
    const open = await (client as any).getOpenOrders?.({ asset_id: tokenId });
    const openOrders = Array.isArray(open) ? open : [];
    const hasOpenSell = openOrders.some((o: any) => String(o?.side || "").toUpperCase() === "SELL");
    if (hasOpenSell) return;

    const oid = await placeImmediateTpSellAfterBuy(stLive.side, Number(stLive.shares), uiLive, tokenId, {
      source: "tp_rearm",
      entryFillPx: Number(stLive.entryPx),
      buyFilledAtMs: Number(stLive.buyFilledAtMs ?? NaN),
    });
    stLive.tpOrderId = oid;
  } catch (e: any) {
    console.error(`[TP ARM ERROR] ${String(e?.message ?? e)}`);
    broadcast({ type: "status", t: nowMs(), status: `tp arm failed: ${String(e?.message ?? e)}` });
  } finally {
    stLive.tpOrderInFlight = false;
  }
}

async function placeLiveMarketSell(side: OutcomeSide, shares: number, _ui: UiConfig, st?: TradeState): Promise<{ filledPx: number }> {
  if (side == null) {
    console.error("[CRITICAL] side is undefined in placeLiveMarketSell");
    console.trace();
    throw new Error("side is undefined - cannot place market sell order");
  }
  if (!["UP", "DOWN"].includes(side)) throw new Error(`Invalid side value: ${side}`);

  console.log(`[SELL MKT] Attempt: slug=${current.slug} side=${side} shares=${shares}`);

  const client = await getClobClient();
  const tokenId = st?.positionTokenId || tokenIdForSide(side);
  if (!Number.isFinite(shares) || shares <= 0) throw new Error(`invalid shares: ${shares}`);

  const marketFn = (client as any).createAndPostMarketOrder;
  if (typeof marketFn === "function") {
    const { tickSize, negRisk } = await getTickNegRisk(tokenId);
    const order = await marketFn.call(
      client,
      { tokenID: tokenId, side: ClobSide.SELL, size: floorTo6(shares), feeRateBps: 1000 },
      { tickSize, negRisk }
    );

    const filledPx = Number(order?.avgPrice ?? order?.price);
    const px = Number.isFinite(filledPx) ? filledPx : 0.01;
    console.log(`[SELL MKT] Done: px=${px}`);
    return { filledPx: px };
  }

  // fallback: aggressive limit sell
  const { tickSize, negRisk } = await getTickNegRisk(tokenId);
  const order = await (client as any).createAndPostOrder(
    { tokenID: tokenId, price: 0.01, side: (ClobSide as any).SELL ?? "SELL", size: floorTo6(shares), feeRateBps: 1000 },
    { tickSize: tickSize as any, negRisk },

    OrderType.GTC
  );

  const filledPx = Number(order?.avgPrice ?? order?.price ?? 0.01);
  const px = Number.isFinite(filledPx) ? filledPx : 0.01;
  console.log(`[SELL MKT fallback] Done: px=${px}`);
  return { filledPx: px };
}

async function placeLiveLimitSell(
  side: OutcomeSide,
  price: number,
  shares: number,
  _ui: UiConfig,
  st: TradeState,
  opts: { retryEveryMs: number; perOrderTtlMs: number; untilTsMs: number; tag: string }
): Promise<{ filledPx: number; filledShares: number }> {
  if (side == null) throw new Error("side is undefined - cannot place sell order");
  if (!["UP", "DOWN"].includes(side)) throw new Error(`Invalid side value: ${side}`);
  if (!Number.isFinite(price) || price <= 0 || price >= 1) throw new Error(`invalid sell price: ${price} (must be >0 and <1)`);
  if (!Number.isFinite(shares) || shares <= 0) throw new Error(`invalid shares: ${shares}`);

  const client = await getClobClient();
  const tokenId = st.positionTokenId || tokenIdForSide(side);
  const { tickSize, negRisk } = await getTickNegRisk(tokenId);

  const until = Number(opts.untilTsMs);
  const retryEvery = Math.max(100, Number(opts.retryEveryMs) || 1000);
  const perTtl = Math.max(250, Number(opts.perOrderTtlMs) || 1000);

  let remaining = shares;
  let filledSharesTotal = 0;
  let filledNotional = 0; // sum(px * filledShares)

  console.log(
    `[SELL LIMIT] START tag=${opts.tag} slug=${current.slug} side=${side} ` +
    `price=${price} shares=${roundTo6(shares)} remaining=${roundTo6(remaining)} ` +
    `retryEveryMs=${retryEvery} perOrderTtlMs=${perTtl} until=${new Date(until).toISOString()}`
  );

  while (Date.now() < until && remaining > 1e-9) {
    const available = await getLiveTokenPositionShares(client as any, tokenId);
    if (Number.isFinite(available as number) && Number(available) > 0) {
      const capped = floorTo6(Math.min(remaining, Number(available)));
      if (capped <= 0) {
        const avgFilledPx = filledSharesTotal > 0 ? filledNotional / filledSharesTotal : price;
        if (filledSharesTotal > 0) {
          const err: any = new Error("[SELL LIMIT] no remaining sellable shares (position depleted)");
          err.partialFilledShares = filledSharesTotal;
          err.remainingShares = 0;
          err.avgFilledPx = avgFilledPx;
          throw err;
        }
        throw new Error("[SELL LIMIT] no sellable shares available for token");
      }
      if (capped < remaining - 1e-9) {
        console.log(`[SELL LIMIT] clamp remaining ${roundTo6(remaining)} -> ${roundTo6(capped)} from position`);
        remaining = capped;
      }
    } else if (Number.isFinite(available as number) && Number(available) <= 0) {
      console.log(`[SELL LIMIT] position lookup shows 0 shares; proceeding with requested remaining=${roundTo6(remaining)} due possible lag`);
    }

    console.log(
      `[SELL LIMIT] PLACE tag=${opts.tag} slug=${current.slug} side=${side} px=${price} remaining=${roundTo6(remaining)}`
    );

    let order: any;
    try {
      order = await client.createAndPostOrder(
        {
          tokenID: tokenId,
          price: roundTo6(price),
          side: ClobSide.SELL,
          size: floorTo6(remaining),
          feeRateBps: 1000,
        },
        { tickSize: tickSize as any, negRisk },
        OrderType.GTC
      );
    } catch (e: any) {
      console.error(`[SELL LIMIT ERROR] create/post failed tag=${opts.tag}:`, e?.message ?? e);
      const msg = String(e?.message ?? e).toLowerCase();
      if (msg.includes("not enough balance") || msg.includes("allowance")) {
        const avail2 = await getLiveTokenPositionShares(client as any, tokenId);
        if (Number.isFinite(avail2 as number) && Number(avail2) > 0) {
          const capped2 = floorTo6(Math.min(remaining, Number(avail2)));
          if (capped2 > 0 && capped2 < remaining) {
            console.log(`[SELL LIMIT] rebalance remaining ${roundTo6(remaining)} -> ${roundTo6(capped2)} after allowance error`);
            remaining = capped2;
          }
        }
      }
      await sleep(retryEvery);
      continue;
    }

    const orderId = (order as any)?.orderID || (order as any)?.orderId || (order as any)?.id;
    if (!orderId) {
      console.error(`[SELL LIMIT ERROR] no orderId returned tag=${opts.tag}`);
      await sleep(retryEvery);
      continue;
    }
    const orderKind = opts.tag === "SL" ? "STOP" : "SELL";
    broadcast({
      type: "status",
      t: nowMs(),
      status: `${orderKind} ORDER PLACED orderId=${orderId} px=${price} shares=${roundTo6(remaining)}`,
    });

    console.log(`[SELL LIMIT] ORDER tag=${opts.tag} orderId=${orderId} perOrderTtlMs=${perTtl}`);

    const orderDeadline = Date.now() + perTtl;

    // IMPORTANT: track delta fills for THIS order
    let lastSeenFilledThisOrder = 0;

    while (Date.now() < orderDeadline) {
      let ord: any;
      try {
        ord = await (client as any).getOrder(orderId);
      } catch (e: any) {
        console.warn(`[SELL LIMIT] getOrder failed tag=${opts.tag} orderId=${orderId}: ${e?.message ?? e}`);
        await sleep(250);
        continue;
      }

      const status = String(ord?.status || "").toUpperCase();
      const filledThisOrder = Number(ord?.filledSize ?? ord?.sizeFilled ?? 0);
      const avgPx = Number(ord?.avgFillPrice ?? ord?.avgPrice ?? ord?.filledPrice ?? NaN);

      // delta fill since last poll for this order
      if (Number.isFinite(filledThisOrder) && filledThisOrder > lastSeenFilledThisOrder + 1e-9) {
        const deltaRaw = filledThisOrder - lastSeenFilledThisOrder;
        const delta = Math.min(deltaRaw, remaining);
        const px = Number.isFinite(avgPx) ? avgPx : price;

        lastSeenFilledThisOrder = filledThisOrder;

        filledSharesTotal += delta;
        filledNotional += px * delta;
        remaining = Math.max(0, remaining - delta);

        console.log(
          `[SELL LIMIT] FILL tag=${opts.tag} orderId=${orderId} status=${status} ` +
          `delta=${roundTo6(delta)} filledTotal=${roundTo6(filledSharesTotal)} remaining=${roundTo6(remaining)} px=${px}`
        );

        if (remaining <= 1e-9) {
          const avgFilledPx = filledSharesTotal > 0 ? filledNotional / filledSharesTotal : price;
          const orderKindFilled = opts.tag === "SL" ? "STOP" : "SELL";
          broadcast({
            type: "status",
            t: nowMs(),
            status: `${orderKindFilled} ORDER FILLED orderId=${orderId} px=${avgFilledPx} shares=${roundTo6(filledSharesTotal)}`,
          });
          return { filledPx: avgFilledPx, filledShares: filledSharesTotal };
        }
      }

      if (status === "FILLED") {
        // If status says FILLED but we didn't catch delta (timing), treat as fully filled for this order.
        const px = Number.isFinite(avgPx) ? avgPx : price;

        // If nothing recorded, assume this order filled the remaining
        if (filledSharesTotal <= 0) {
          filledSharesTotal = shares;
          filledNotional = px * shares;
          remaining = 0;
        }

        const avgFilledPx = filledSharesTotal > 0 ? filledNotional / filledSharesTotal : px;
        console.log(`[SELL LIMIT] FILLED tag=${opts.tag} orderId=${orderId} avgPx=${avgFilledPx} filledShares=${roundTo6(filledSharesTotal)}`);
        const orderKindFilled = opts.tag === "SL" ? "STOP" : "SELL";
        broadcast({
          type: "status",
          t: nowMs(),
          status: `${orderKindFilled} ORDER FILLED orderId=${orderId} px=${avgFilledPx} shares=${roundTo6(filledSharesTotal)}`,
        });
        return { filledPx: avgFilledPx, filledShares: filledSharesTotal };
      }

      await sleep(250);
    }

    console.log(`[SELL LIMIT] TTL expired -> cancel tag=${opts.tag} orderId=${orderId}`);
    try {
      await cancelOrderRobust(client as any, String(orderId));
    } catch (e: any) {
      console.warn(`[SELL LIMIT] cancel failed tag=${opts.tag} orderId=${orderId}: ${e?.message ?? e}`);
    }

    await sleep(retryEvery);
  }

  const avgFilledPx = filledSharesTotal > 0 ? filledNotional / filledSharesTotal : price;

  if (filledSharesTotal > 0) {
    console.log(
      `[SELL LIMIT] END (partial fill) tag=${opts.tag} filledShares=${roundTo6(filledSharesTotal)} remaining=${roundTo6(remaining)} avgPx=${avgFilledPx}`
    );
    const err: any = new Error(
      `[SELL LIMIT] Partially filled by untilTsMs tag=${opts.tag} filled=${roundTo6(filledSharesTotal)} remaining=${roundTo6(remaining)}`
    );
    err.partialFilledShares = filledSharesTotal;
    err.remainingShares = remaining;
    err.avgFilledPx = avgFilledPx;
    throw err;
  }

  console.log(`[SELL LIMIT] END (no fill) tag=${opts.tag} sold=0 remaining=${roundTo6(remaining)}`);
  throw new Error(`[SELL LIMIT] Not filled by untilTsMs tag=${opts.tag} remaining=${roundTo6(remaining)}`);
}


// ===== Session lifecycle =====
function resetEngineForNewSession(engine: Engine, newSession: { slug: string; startMs: number; endMs: number }) {
  if (engine === "live") {
    if (pendingUiLive) {
      uiLive = pendingUiLive;
      pendingUiLive = null;
      broadcast({ type: "status", t: nowMs(), status: "applied queued config at new session (live engine)", live: { ui: uiLive } });
    }

    stLive = {
      marketSlug: newSession.slug,
      marketStartMs: newSession.startMs,
      marketEndMs: newSession.endMs,

      entered: false,
      exited: false,
      side: null,

      entryTsMs: null,
      exitTsMs: null,

      entryPx: null,
      exitPx: null,

      shares: null,
      notionalUsd: null,
      grossPnlUsd: null,
      entryFeeUsd: null,
      exitFeeUsd: null,
      totalFeesUsd: null,

      pnlUsd: null,
      roiPct: null,
      holdSec: null,

      balanceUsd: liveAccount.balanceUsd,

      stopFallbackTriggered: false,
      exitInFlight: false,

      enterInFlight: false,
      lastEnterAttemptMs: 0,
      buyAttemptedThisSession: false,
      buyAttemptedSessionSlug: newSession.slug,
      buyFilledThisSession: false,
      tpOrderInFlight: false,
      lastTpArmAttemptMs: 0,
      tpOrderId: null,
      buyFilledAtMs: null,
      firstSellAttemptAtMs: null,
      positionTokenId: null,
      pendingEntrySide: null,
      pendingEntrySignalPx: null,
      pendingEntryLimitPx: null,
      pendingEntryBaseLimitPx: null,
      pendingEntryRepriceCount: 0,
      pendingEntryMode: null,
      pendingEntrySubtype: null,
      pendingEntryPlacedAtMs: null,
      pendingEntryExpiresAtMs: null,
      pendingEntryNotionalUsd: null,
      pendingTpLimitPx: null,
      pendingTpPlacedAtMs: null,
      entryMode: null,
      entrySubtype: null,
    };

    reloadEngineStrategy("live", "new session");
    void syncArtifactsToPara("new_session_live");
    return;
  }

  // Settle each paper strategy lane independently at session boundary.
  for (const lane of PAPER_LANES) {
    const laneSt = paperLaneStates[lane];
    if (
      laneSt.entered &&
      !laneSt.exited &&
      (laneSt.side === "UP" || laneSt.side === "DOWN") &&
      Number.isFinite(Number(laneSt.entryPx)) &&
      Number.isFinite(Number(laneSt.shares))
    ) {
      const side = laneSt.side as OutcomeSide;
      const now = nowMs();
      const entryPx = Number(laneSt.entryPx);
      const shares = Number(laneSt.shares);
      const notionalUsd = Number.isFinite(Number(laneSt.notionalUsd)) && Number(laneSt.notionalUsd) > 0
        ? Number(laneSt.notionalUsd)
        : entryPx * shares;
      const lastSideBid = inferSideBid(
        side,
        { bid: lastObservedBids.upBid },
        { bid: lastObservedBids.downBid }
      );
      const exitPx = Number.isFinite(Number(lastSideBid)) && Number(lastSideBid) > 0.5 ? 1 : 0;

      laneSt.exited = true;
      laneSt.exitTsMs = now;
      laneSt.exitPx = exitPx;
      const { pnlUsd } = applyExitAccounting(laneSt, exitPx, "none");
      clearPendingTp(laneSt);
      laneSt.roiPct = notionalUsd > 0 ? (pnlUsd / notionalUsd) * 100 : null;
      laneSt.holdSec = laneSt.entryTsMs ? (now - laneSt.entryTsMs) / 1000 : null;
      paperAccount.balanceUsd += pnlUsd;
      laneSt.balanceUsd = paperAccount.balanceUsd;

      emitTrade("paper", `EXIT_${side}_PAPER`, {
        strategyLane: lane,
        mode: lane,
        via: "session_rollover",
        exitType: "SETTLE_ROUND",
        reason: "session_end_round_last_bid",
        targetSellPx: exitPx,
        fillPx: exitPx,
        actualFillPx: exitPx,
        signalPx: Number.isFinite(Number(lastSideBid)) ? Number(lastSideBid) : null,
      }, laneSt, uiPaper);
    }
  }

  const blankPaper = {
    marketSlug: newSession.slug,
    marketStartMs: newSession.startMs,
    marketEndMs: newSession.endMs,

    entered: false,
    exited: false,
    side: null,

    entryTsMs: null,
    exitTsMs: null,

    entryPx: null,
    exitPx: null,

    shares: null,
    notionalUsd: null,
    grossPnlUsd: null,
    entryFeeUsd: null,
    exitFeeUsd: null,
    totalFeesUsd: null,

    pnlUsd: null,
    roiPct: null,
    holdSec: null,

    balanceUsd: paperAccount.balanceUsd,

    stopFallbackTriggered: false,
    exitInFlight: false,

    enterInFlight: false,
    lastEnterAttemptMs: 0,
    buyAttemptedThisSession: false,
    buyAttemptedSessionSlug: newSession.slug,
    buyFilledThisSession: false,
    tpOrderInFlight: false,
    lastTpArmAttemptMs: 0,
    tpOrderId: null,
    buyFilledAtMs: null,
    firstSellAttemptAtMs: null,
    positionTokenId: null,
    pendingEntrySide: null,
    pendingEntrySignalPx: null,
    pendingEntryLimitPx: null,
    pendingEntryBaseLimitPx: null,
    pendingEntryRepriceCount: 0,
    pendingEntryMode: null,
    pendingEntrySubtype: null,
    pendingEntryPlacedAtMs: null,
    pendingEntryExpiresAtMs: null,
    pendingEntryNotionalUsd: null,
    pendingTpLimitPx: null,
    pendingTpPlacedAtMs: null,
    entryMode: null,
    entrySubtype: null,
    __stoppedThisSession: false,
  };
  stPaper = { ...blankPaper };
  paperLaneStates.BASE = { ...blankPaper };
  paperLaneStates.NO_CROSS_WINNER = { ...blankPaper };
  paperSessionLockLane = null;
  syncPaperAggregateState();

  reloadEngineStrategy("paper", "new session");
  void syncArtifactsToPara("new_session_paper");
}

// ===== Fallback Bot decision engine =====
async function maybeEnterGeneric(
  engine: Engine,
  upBA: { bid: number | null; ask?: number | null },
  dnBA: { bid: number | null; ask?: number | null },
  elapsedSec: number
) {
  const ui = engine === "paper" ? uiPaper : uiLive;
  const st = engine === "paper" ? stPaper : stLive;

  if (engine === "live" && !ui.enabled) return;
  if (st.entered || st.exited) return;
  if (!isNewEntryAllowed(engine, st)) return;
  if (elapsedSec < ui.minEntrySec) return;

  const upBid = upBA.bid;
  const dnBid = dnBA.bid;

  const upHit = Number.isFinite(upBid) && (upBid as number) >= ui.entry;
  const dnHit = Number.isFinite(dnBid) && (dnBid as number) >= ui.entry;
  if (!upHit && !dnHit) return;

  let side: OutcomeSide;
  if (upHit && dnHit) side = (upBid as number) >= (dnBid as number) ? "UP" : "DOWN";
  else side = upHit ? "UP" : "DOWN";

  const entryPx = side === "UP" ? (upBid as number) : (dnBid as number);

  const balanceUsd = engine === "paper" ? paperAccount.balanceUsd : liveAccount.balanceUsd ?? 0;
  const dynamicNotionalUsd = calcNotionalUsd({
    balanceUsd,
    betUsd: ui.betUsd,
    maxBetUsd: ui.maxBetUsd,
    kellyOn: ui.kellyOn,
    kellyMult: ui.kellyMult,
    kellyCap: ui.kellyCap,
  });
  // Paper should always use the configured bet sizing (no Kelly override).
  const notionalUsd =
    engine === "paper"
      ? clamp(ui.betUsd, 0, Math.min(ui.maxBetUsd, balanceUsd))
      : dynamicNotionalUsd;
  if (notionalUsd <= 0) return;

  if (engine === "paper") {
    st.entered = true;
    st.side = side;
    st.entryTsMs = nowMs();
    st.entryPx = entryPx;
    st.notionalUsd = notionalUsd;
    st.entryMode = null;
    st.entrySubtype = null;

    st.shares = entryPx > 0 ? (notionalUsd / entryPx) : 0;
    st.notionalUsd = notionalUsd;
    applyEntryFeeAccounting(st, entryPx, Number(st.shares ?? 0));
    st.balanceUsd = paperAccount.balanceUsd;

    emitTrade("paper", `ENTER_${side}_PAPER`, {
      signalPx: entryPx,
      signalThresholdPx: ui.entry,
      buyOrderPx: entryPx,
      fillPx: entryPx,
      actualFillPx: entryPx,
      slippageFromSignalPx: 0,
      slippageFromThresholdPx: entryPx - ui.entry,
      via: "generic",
    });
    const tpPx = computeImmediateTpSellPx(ui, Number(entryPx));
    const tpPlacedAtMs = nowMs();
    st.pendingTpLimitPx = tpPx;
    st.pendingTpPlacedAtMs = tpPlacedAtMs;
    st.tpOrderId = `paper-tp-${current.slug}-${side}-${tpPlacedAtMs}`;
    emitTrade("paper", `EXIT_ORDER_${side}_PAPER`, {
      phase: "exit_order",
      via: "tp_limit_after_entry_fill",
      exitType: "EXIT",
      signalPx: entryPx,
      targetSellPx: tpPx,
      limitPx: tpPx,
      orderPlacedAtMs: tpPlacedAtMs,
      shares: Number(st.shares ?? 0),
      tpOrderId: st.tpOrderId,
    });
    return;
  }

  // LIVE: one-buy-per-session + in-flight + cooldown
  if ((st.buyAttemptedSessionSlug ?? null) !== (current.slug ?? null)) {
    st.buyAttemptedSessionSlug = current.slug;
    st.buyAttemptedThisSession = false;
    st.buyFilledThisSession = false;
  }
  if (st.buyFilledThisSession) return;
  if (st.buyAttemptedThisSession) return;
  if (st.enterInFlight) return;

  const now = nowMs();
  if (st.lastEnterAttemptMs && now - st.lastEnterAttemptMs < 3000) return;

  st.lastEnterAttemptMs = now;
  st.enterInFlight = true;
  st.buyAttemptedThisSession = true;
  st.buyAttemptedSessionSlug = current.slug;

  st.entered = true;
  st.side = side;
  st.entryTsMs = nowMs();
  st.entryPx = entryPx;
  st.notionalUsd = notionalUsd;

  try {
    const immediate = LIVE_ENTRY_STYLE !== "limit";
    const buyPx = immediate
      ? immediateBuyPxForSide(side, upBA, dnBA, ui.entry)
      : buyPxForSide(side, upBA, dnBA, configuredEntryLimitPx(ui));
    const ttlMs = immediate ? LIVE_ENTRY_IMMEDIATE_TTL_MS : LIVE_BUY_TTL_MS;
    console.log(`[ENTER LIVE][generic] style=${immediate ? "immediate" : "limit"} side=${side} buyPx=${buyPx} ttlMs=${ttlMs}`);
    const out = await placeLiveLimitBuy(side, buyPx, notionalUsd, ui, ttlMs);
    st.entryPx = out.filledPx;
    st.shares = out.shares;
    st.notionalUsd = Number.isFinite(out.shares) && Number.isFinite(out.filledPx) ? out.shares * out.filledPx : st.notionalUsd;
    applyEntryFeeAccounting(st, Number(st.entryPx), Number(st.shares ?? 0));
    st.balanceUsd = liveAccount.balanceUsd;
    st.buyFilledThisSession = true;
    st.tpOrderId = out.tpOrderId ?? null;
    st.buyFilledAtMs = out.filledAtMs ?? nowMs();
    st.firstSellAttemptAtMs = null;
    st.positionTokenId = out.tokenId ?? st.positionTokenId ?? tokenIdForSide(side);
    emitTrade("live", `ENTER_${side}_LIVE`, {
      signalPx: entryPx,
      signalThresholdPx: ui.entry,
      buyOrderPx: buyPx,
      fillPx: out.filledPx,
      actualFillPx: out.filledPx,
      slippageFromSignalPx: Number(out.filledPx) - Number(entryPx),
      slippageFromThresholdPx: Number(out.filledPx) - Number(ui.entry),
      via: "generic",
    });
  } catch (e: any) {
    st.entered = false;
    st.side = null;
    st.entryTsMs = null;
    st.entryPx = null;
    st.entryMode = null;
    st.entrySubtype = null;
    st.shares = null;
    st.notionalUsd = null;
    st.grossPnlUsd = null;
    st.entryFeeUsd = null;
    st.exitFeeUsd = null;
    st.totalFeesUsd = null;
    st.positionTokenId = null;
    st.buyFilledAtMs = null;
    st.firstSellAttemptAtMs = null;
    const errMsg = String(e?.message ?? e);
    if (shouldRearmLiveEntryAfterError(errMsg)) {
      st.buyAttemptedThisSession = false;
      console.warn(`[LIVE ENTER REARM] generic reason=${errMsg}`);
    }

    console.log("[LIVE] enter failed:", e);
    broadcast({ type: "status", t: nowMs(), status: `enter failed (live): ${e?.stack ?? e?.message ?? String(e)}` });
  } finally {
    st.enterInFlight = false;
  }
}

async function maybeExitGeneric(
  engine: Engine,
  upBA: { bid: number | null },
  dnBA: { bid: number | null },
  opts?: { stopOnly?: boolean }
) {
  const ui = engine === "paper" ? uiPaper : uiLive;
  const st = engine === "paper" ? stPaper : stLive;

  if (engine === "live" && !ui.enabled) return;
  if (!st.entered || st.exited || !st.side) {
    if (engine === "live" && opts?.stopOnly && ui.useStop) {
      await reconcileLivePositionMaybe(true, "stop-only-no-live-state");
      if (st.entered && !st.exited && st.side) {
        console.warn(
          `[LIVE POS SYNC] recovered during stop-only path side=${st.side} shares=${roundTo6(Number(st.shares ?? 0))} slug=${current.slug}`
        );
        // Continue in this same tick using recovered state.
      } else {
      const t = nowMs();
      if (!((st as any).__lastNoLivePositionStopLogMs) || t - (st as any).__lastNoLivePositionStopLogMs >= STOP_SKIP_LOG_INTERVAL_MS) {
        console.log(
          `[STOP SKIP] engine=live reason=no_live_position entered=${st.entered} exited=${st.exited} side=${st.side ?? "-"}`
        );
        (st as any).__lastNoLivePositionStopLogMs = t;
      }
      }
    }
    if (!st.entered || st.exited || !st.side) return;
  }
  if (engine === "live" && st.exitInFlight) return;

  const side = st.side;
  const curBid = inferSideBid(side, upBA, dnBA);
  if (!Number.isFinite(Number(curBid))) return;

  const px = Number(curBid);
  const elapsedSec = current.startMs > 0 ? Math.max(0, (nowMs() - current.startMs) / 1000) : 0;
  const shouldProfitExit = !opts?.stopOnly && px >= ui.exit;
  const shouldStop = shouldTriggerStop(ui, side, px, elapsedSec, engine, "generic");
  if (!shouldProfitExit && !shouldStop) return;

  const exitReason = shouldStop ? "STOP" : "EXIT";
  console.log(
    `[SELL SIGNAL] (generic) slug=${current.slug} engine=${engine} reason=${exitReason} side=${side} px=${px} ` +
      `shares=${st.shares ?? "—"} entryPx=${st.entryPx ?? "—"}`
  );
  emitTrade(engine, `SIGNAL_${exitReason}_${side}_${engine.toUpperCase()}`, {
    phase: "signal",
    via: "generic",
    signalPx: px,
    exitType: exitReason,
  });

  if (engine === "paper") {
    st.exited = true;
    st.exitTsMs = nowMs();
    st.exitPx = px;
    st.stopFallbackTriggered = false;
    const exitFeeMode: ExitFeeMode = shouldStop ? "taker" : "maker";
    const { pnlUsd } = applyExitAccounting(st, px, exitFeeMode);
    clearPendingTp(st);
    st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;

    paperAccount.balanceUsd += pnlUsd;
    st.balanceUsd = paperAccount.balanceUsd;

    emitTrade("paper", `${exitReason}_${side}_PAPER`);
    return;
  }

  try {
    if (!st.shares || st.shares <= 0) throw new Error("No shares to sell");

    const sessionEndMs = st.marketEndMs ?? (Date.now() + 60_000);
    if (st.firstSellAttemptAtMs == null) st.firstSellAttemptAtMs = nowMs();
    const buyFillToSellAttemptMs =
      Number.isFinite(Number(st.buyFilledAtMs)) ? Math.max(0, Number(st.firstSellAttemptAtMs) - Number(st.buyFilledAtMs)) : null;
    console.log(
      `[EXIT LATENCY] engine=${engine} reason=${exitReason} side=${side} ` +
      `buyFillToSellAttemptMs=${buyFillToSellAttemptMs ?? "na"}`
    );

    if (exitReason === "EXIT") {
      console.log(`[SELL EXPECTED] (generic TP) retryEvery=5000ms slug=${current.slug} side=${side} targetPx=${ui.exit}`);

      const out = await placeLiveLimitSell(side, ui.exit, st.shares, ui, st, {
        retryEveryMs: 5000,
        perOrderTtlMs: 4500,
        untilTsMs: sessionEndMs,
        tag: "TP",
      });

      st.exited = true;
      st.exitTsMs = nowMs();
      st.exitPx = out.filledPx;
      st.stopFallbackTriggered = false;
      st.positionTokenId = null;
      applyExitAccounting(st, Number(st.exitPx), "maker");

      st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;
      emitTrade("live", `EXIT_${side}_LIVE`);
      return;
    }

    await cancelOpenSellOrdersForSide(side, "generic-stop", st.positionTokenId);
    console.log(`[SELL EXPECTED] (generic SL) retryEvery=1000ms slug=${current.slug} side=${side} targetPx=${ui.stop}`);

    st.exitInFlight = true;
    let filled = false;
    const started = Date.now();
    try {
      const out = await placeLiveLimitSell(side, ui.stop, st.shares, ui, st, {
        retryEveryMs: 1000,
        perOrderTtlMs: 900,
        untilTsMs: sessionEndMs,
        tag: "SL",
      });
      st.exited = true;
      st.exitTsMs = nowMs();
      st.exitPx = out.filledPx;
      st.stopFallbackTriggered = true;
      st.positionTokenId = null;
      applyExitAccounting(st, Number(st.exitPx), "taker");
      filled = true;
    } catch (e: any) {
      const partial = Number(e?.partialFilledShares ?? 0);
      if (Number.isFinite(partial) && partial > 0 && Number.isFinite(Number(st.shares))) {
        st.shares = Math.max(0, Number(st.shares) - partial);
      }
      console.log(`[GENERIC STOP LIMIT FAILED] side=${side} err=${String(e?.message ?? e)} remaining=${st.shares}`);
    }

    if (!filled && st.shares && st.shares > 0) {
      const elapsed = Date.now() - started;
      if (elapsed < 3000) await sleep(3000 - elapsed);
      await cancelOpenSellOrdersForSide(side, "generic-stop-market-fallback", st.positionTokenId);
      broadcast({
        type: "status",
        t: nowMs(),
        status: `STOP ORDER PLACED market_fallback side=${side} shares=${roundTo6(st.shares)}`,
      });
      const out2 = await placeLiveMarketSell(side, st.shares, ui, st);
      st.exited = true;
      st.exitTsMs = nowMs();
      st.exitPx = out2.filledPx;
      st.stopFallbackTriggered = true;
      st.positionTokenId = null;
      applyExitAccounting(st, Number(st.exitPx), "taker");
      broadcast({
        type: "status",
        t: nowMs(),
        status: `STOP ORDER FILLED market_fallback px=${out2.filledPx} shares=${roundTo6(st.shares)}`,
      });
    }

    st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;
    if (st.exited) {
      emitTrade("live", `STOP_${side}_LIVE`);
      return;
    }
    throw new Error("generic stop did not complete");
  } catch (e: any) {
    console.error("[LIVE SELL ERROR]", e.message, e.stack || e);
    throw e;
  } finally {
    if (engine === "live") st.exitInFlight = false;
  }
}

// ===== Strategy-driven engine tick =====
async function runEngineTick(
  engine: Engine,
  upBA: { bid: number | null; ask?: number | null },
  dnBA: { bid: number | null; ask?: number | null },
  elapsedSec: number,
  isFinal: boolean,
  lane?: StrategyLane
) {
  const ui = engine === "paper" ? uiPaper : uiLive;
  const st = engine === "paper" && lane ? paperLaneStates[lane] : engine === "paper" ? stPaper : stLive;
  const strat = engine === "paper" && lane ? strategyPaperByLane[lane] : engine === "paper" ? strategyPaper : strategyLive;
  const laneTag = lane ? ` lane=${lane}` : "";
  const emitTradeScoped = (action: string, extra?: Record<string, any>) => {
    const payload = { ...(extra || {}) };
    if (lane) {
      if (payload.mode == null) payload.mode = lane;
      payload.strategyLane = lane;
    }
    emitTrade(engine, action, payload, st, ui);
  };
  const computePaperTpLimitPx = (entryFillPx: number | null): number => {
    const mode = String(st.entryMode ?? st.pendingEntryMode ?? "").toUpperCase();
    const subtype = String(st.entrySubtype ?? st.pendingEntrySubtype ?? "").toUpperCase();
    if (mode === "NO_CROSS_WINNER") {
      return subtype === "HC_SAME_SIDE" ? 0.99 : 0.96;
    }
    return computeImmediateTpSellPx(ui, Number(entryFillPx));
  };
  const armPaperTpOrder = (side: OutcomeSide, entryFillPx: number | null) => {
    if (engine !== "paper" || st.exited || !st.entered || !side || !Number.isFinite(Number(st.shares)) || Number(st.shares) <= 0) {
      return;
    }
    const tpPx = computePaperTpLimitPx(Number(entryFillPx));
    if (!Number.isFinite(tpPx) || tpPx <= 0 || tpPx >= 1) return;
    const alreadyArmed =
      Number.isFinite(Number(st.pendingTpLimitPx)) &&
      Number(st.pendingTpLimitPx) > 0 &&
      !!st.tpOrderId;
    if (alreadyArmed) return;
    const orderPlacedAtMs = nowMs();
    st.pendingTpLimitPx = tpPx;
    st.pendingTpPlacedAtMs = orderPlacedAtMs;
    st.tpOrderId = `paper-tp-${current.slug}-${side}-${orderPlacedAtMs}`;
    emitTradeScoped(`EXIT_ORDER_${side}_PAPER`, {
      phase: "exit_order",
      via: "tp_limit_after_entry_fill",
      exitType: "EXIT",
      signalPx: Number.isFinite(Number(entryFillPx)) ? Number(entryFillPx) : null,
      targetSellPx: tpPx,
      limitPx: tpPx,
      orderPlacedAtMs,
      shares: Number(st.shares),
      tpOrderId: st.tpOrderId,
      mode: lane ?? null,
    });
  };

  if (engine === "live" && !ui.enabled) return;

  if (!strat) {
    if ((st as any).__lastNoStrategyLogMs == null) (st as any).__lastNoStrategyLogMs = 0;
    const tNow = nowMs();
    if (tNow - (st as any).__lastNoStrategyLogMs > 2000) {
      console.warn(`[NO STRATEGY] engine=${engine}${laneTag} trading disabled until external makeStrategy loads`);
      broadcast({
        type: "status",
        t: tNow,
        status: `NO STRATEGY (${engine}${lane ? ":" + lane : ""}): trading disabled until external makeStrategy loads`,
      });
      (st as any).__lastNoStrategyLogMs = tNow;
    }
    return;
  }

  // Infer missing side from opposite bid in binary markets (UP ~= 1 - DOWN).
  const upBidRaw = Number(upBA.bid ?? NaN);
  const dnBidRaw = Number(dnBA.bid ?? NaN);
  const upBid = Number.isFinite(upBidRaw) ? upBidRaw : (Number.isFinite(dnBidRaw) ? clamp01(1 - dnBidRaw) : NaN);
  const dnBid = Number.isFinite(dnBidRaw) ? dnBidRaw : (Number.isFinite(upBidRaw) ? clamp01(1 - upBidRaw) : NaN);

  const rawAction = strat.onTick({ elapsedSec, upBid, downBid: dnBid, isFinal });
  let action = normalizeStrategyAction(rawAction);
  if (!action && rawAction && typeof rawAction === "object") {
    if ((st as any).__lastUnrecognizedActionLogMs == null) (st as any).__lastUnrecognizedActionLogMs = 0;
    const tNow = nowMs();
    if (tNow - (st as any).__lastUnrecognizedActionLogMs > 2000) {
      console.warn(`[STRAT ACTION IGNORED] engine=${engine}${laneTag} raw=${JSON.stringify(rawAction)}`);
      (st as any).__lastUnrecognizedActionLogMs = tNow;
    }
  }

  // HC safeguard: if BASE exited in this same 5m session, allow HC entry from
  // server state even if strategy shared-state missed the transition.
  // Priority: opposite-side (legacy best hitter), then same-side-after-stop.
  if (
    engine === "paper" &&
    lane === "NO_CROSS_WINNER" &&
    !action &&
    !st.entered &&
    !st.exited &&
    !st.enterInFlight
  ) {
    const sec = Number(elapsedSec);
    const nearClose = Number.isFinite(sec) && sec >= (300 - HC_BLOCK_LAST_SEC);
    if (nearClose) {
      const stAny = st as any;
      stAny.__hcFallbackSameSideStreak = 0;
    } else {
    const stAny = st as any;
    const baseLane = paperLaneStates.BASE;
    const baseExitedSameSession =
      !!baseLane &&
      !!baseLane.exited &&
      !!baseLane.side &&
      String(baseLane.marketSlug || "") === String(current.slug || "");
    if (baseExitedSameSession) {
      const baseSide: OutcomeSide = baseLane.side as OutcomeSide;
      const oppositeSide: OutcomeSide = baseSide === "UP" ? "DOWN" : "UP";
      const oppositeBid = inferSideBid(oppositeSide, upBA, dnBA);
      const oppositeLead = Number(ui.highConfidenceLead);

      let selectedSide: OutcomeSide | null = null;
      let selectedBid: number | null = null;
      let selectedReason = "";

      if (Number.isFinite(Number(oppositeBid)) && Number.isFinite(oppositeLead) && Number(oppositeBid) >= oppositeLead) {
        selectedSide = oppositeSide;
        selectedBid = Number(oppositeBid);
        selectedReason = "opposite_after_base_exit";
      } else if (!!baseLane.stopFallbackTriggered) {
        const sameSide: OutcomeSide = baseSide;
        const sameSideBid = inferSideBid(sameSide, upBA, dnBA);
        if (Number.isFinite(Number(sameSideBid)) && Number(sameSideBid) >= HC_SAME_SIDE_MIN_BID) {
          stAny.__hcFallbackSameSideStreak = Number(stAny.__hcFallbackSameSideStreak || 0) + 1;
        } else {
          stAny.__hcFallbackSameSideStreak = 0;
        }
        const stopTsMs = Number(baseLane.exitTsMs ?? 0);
        const sinceStopSec = Number.isFinite(stopTsMs) && stopTsMs > 0 ? (nowMs() - stopTsMs) / 1000 : NaN;
        if (
          Number(stAny.__hcFallbackSameSideStreak || 0) >= HC_SAME_SIDE_PERSISTENCE_TICKS &&
          Number.isFinite(sinceStopSec) &&
          sinceStopSec >= HC_SAME_SIDE_MIN_SEC_AFTER_STOP
        ) {
          selectedSide = sameSide;
          selectedBid = Number(sameSideBid);
          selectedReason = "same_side_after_stop";
        }
      }

      if (selectedSide && Number.isFinite(Number(selectedBid))) {
        const fallbackLimitPx = clamp01(Math.min(0.9, Number(selectedBid) + 0.03));
        const hcSubtype = selectedReason === "same_side_after_stop" ? "HC_SAME_SIDE" : "HC_OPPOSITE";
        action = {
          enter: {
            side: selectedSide,
            entryPx: Number(selectedBid),
            limitPx: fallbackLimitPx,
            ttlMs: LIVE_BUY_TTL_MS,
            mode: "NO_CROSS_WINNER",
            hcSubtype,
          },
        };
        console.log(
          `[HC FALLBACK SIGNAL] lane=${lane} slug=${current.slug} side=${selectedSide} reason=${selectedReason} ` +
          `bid=${Number(selectedBid).toFixed(2)} oppLead=${Number(ui.highConfidenceLead).toFixed(2)} ` +
          `sameMinBid=${HC_SAME_SIDE_MIN_BID.toFixed(2)} samePersist=${Number(stAny.__hcFallbackSameSideStreak || 0)}/${HC_SAME_SIDE_PERSISTENCE_TICKS} ` +
          `limit=${fallbackLimitPx.toFixed(2)}`
        );
      }
    } else {
      stAny.__hcFallbackSameSideStreak = 0;
    }
    }
  }
  // Server-side HC stop guard: only for HC opposite-side positions.
  // Same-side HC stop behavior remains strategy-driven.
  if (engine === "paper" && lane === "NO_CROSS_WINNER") {
    const stAny = st as any;
    const hcSubtype = String(st.entrySubtype ?? st.pendingEntrySubtype ?? "HC_OPPOSITE").toUpperCase();
    const isOpposite = hcSubtype === "HC_OPPOSITE";
    if (st.entered && !st.exited && (st.side === "UP" || st.side === "DOWN") && isOpposite) {
      const hcSideBid = inferSideBid(st.side as OutcomeSide, upBA, dnBA);
      const bid = Number(hcSideBid);
      if (Number.isFinite(bid)) {
        let kx = Number(stAny.__hcOppStopKalmanX);
        let kp = Number(stAny.__hcOppStopKalmanP);
        const kPrev = Number(stAny.__hcOppStopKalmanPrev);
        if (!Number.isFinite(kx)) {
          kx = bid;
          kp = 1;
        } else {
          kp = (Number.isFinite(kp) ? kp : 1) + HC_OPP_STOP_KALMAN_Q;
          const gain = kp / (kp + HC_OPP_STOP_KALMAN_R);
          kx = kx + gain * (bid - kx);
          kp = (1 - gain) * kp;
        }
        const kSlope = Number.isFinite(kPrev) ? (kx - kPrev) : 0;
        stAny.__hcOppStopKalmanX = kx;
        stAny.__hcOppStopKalmanP = kp;
        stAny.__hcOppStopKalmanPrev = kx;

        if (bid <= HC_OPP_STOP_HARD_FAILSAFE_PX && !(action && action.exit)) {
          action = {
            ...(action || {}),
            exit: {
              type: "STOP",
              side: st.side as OutcomeSide,
              exitPx: HC_OPP_STOP_HARD_FAILSAFE_PX,
              stopSource: "HC_OPP_SERVER_FILTER",
              stopReason: "HC_OPP_HARD_FAILSAFE",
              stopMeta: {
                lane,
                hcSubtype,
                armed: Boolean(stAny.__hcOppStopArmed),
                bid,
                thr: HC_OPP_STOP_THR,
                hardFailPx: HC_OPP_STOP_HARD_FAILSAFE_PX,
                rawStreak: Number(stAny.__hcOppStopRawStreak || 0),
                rawNeed: HC_OPP_STOP_RAW_CONFIRM_TICKS,
                kalStreak: Number(stAny.__hcOppStopKalmanStreak || 0),
                kalNeed: HC_OPP_STOP_KALMAN_CONFIRM_TICKS,
                kalmanPx: kx,
                kalmanSlope: kSlope,
                elapsedSec,
              },
            },
          };
          console.log(
            `[HC SERVER STOP FAILSAFE] lane=${lane} slug=${current.slug} side=${st.side} ` +
              `bid=${bid.toFixed(3)} hard=${HC_OPP_STOP_HARD_FAILSAFE_PX.toFixed(3)}`
          );
        } else {
          if (!stAny.__hcOppStopArmed && bid <= HC_OPP_STOP_HYST_ARM_PX) stAny.__hcOppStopArmed = true;
          if (stAny.__hcOppStopArmed && bid >= HC_OPP_STOP_HYST_DISARM_PX) {
            stAny.__hcOppStopArmed = false;
            stAny.__hcOppStopRawStreak = 0;
            stAny.__hcOppStopKalmanStreak = 0;
          }

          if (stAny.__hcOppStopArmed && bid <= HC_OPP_STOP_THR) {
            stAny.__hcOppStopRawStreak = Number(stAny.__hcOppStopRawStreak || 0) + 1;
          } else {
            stAny.__hcOppStopRawStreak = 0;
          }
          if (stAny.__hcOppStopArmed && kx <= HC_OPP_STOP_THR && kSlope <= 0) {
            stAny.__hcOppStopKalmanStreak = Number(stAny.__hcOppStopKalmanStreak || 0) + 1;
          } else {
            stAny.__hcOppStopKalmanStreak = 0;
          }

          const earlyExtra = Number.isFinite(elapsedSec) && Number(elapsedSec) < HC_OPP_STOP_EARLY_SEC
            ? HC_OPP_STOP_EARLY_EXTRA_TICKS
            : 0;
          const rawNeed = HC_OPP_STOP_RAW_CONFIRM_TICKS + earlyExtra;
          const kalNeed = HC_OPP_STOP_KALMAN_CONFIRM_TICKS + earlyExtra;
          const rawStreak = Number(stAny.__hcOppStopRawStreak || 0);
          const kalStreak = Number(stAny.__hcOppStopKalmanStreak || 0);
          if (rawStreak >= rawNeed && kalStreak >= kalNeed && !(action && action.exit)) {
            action = {
              ...(action || {}),
              exit: {
                type: "STOP",
                side: st.side as OutcomeSide,
                exitPx: HC_OPP_STOP_THR,
                stopSource: "HC_OPP_SERVER_FILTER",
                stopReason: "HC_OPP_FILTER_CONFIRM",
                stopMeta: {
                  lane,
                  hcSubtype,
                  armed: Boolean(stAny.__hcOppStopArmed),
                  bid,
                  thr: HC_OPP_STOP_THR,
                  hardFailPx: HC_OPP_STOP_HARD_FAILSAFE_PX,
                  rawStreak,
                  rawNeed,
                  kalStreak,
                  kalNeed,
                  kalmanPx: kx,
                  kalmanSlope: kSlope,
                  elapsedSec,
                },
              },
            };
            console.log(
              `[HC SERVER STOP] lane=${lane} slug=${current.slug} side=${st.side} subtype=${hcSubtype} ` +
                `bid=${bid.toFixed(3)} kf=${kx.toFixed(3)} slope=${kSlope.toFixed(5)} ` +
                `thr=${HC_OPP_STOP_THR.toFixed(3)} raw=${rawStreak}/${rawNeed} kal=${kalStreak}/${kalNeed}`
            );
          }
        }
      } else {
        stAny.__hcOppStopRawStreak = 0;
        stAny.__hcOppStopKalmanStreak = 0;
      }
    } else {
      stAny.__hcServerStopStreak = 0;
      stAny.__hcOppStopRawStreak = 0;
      stAny.__hcOppStopKalmanStreak = 0;
      stAny.__hcOppStopArmed = false;
      stAny.__hcOppStopKalmanX = NaN;
      stAny.__hcOppStopKalmanP = NaN;
      stAny.__hcOppStopKalmanPrev = NaN;
    }
  }
  // DEBUG: log strategy decisions (throttled)
if ((st as any).__lastStratLogMs == null) (st as any).__lastStratLogMs = 0;
const _n = nowMs();
if (_n - (st as any).__lastStratLogMs > 1000) {
  console.log(
    `[STRAT OUT] engine=${engine}${laneTag} entered=${st.entered} exited=${st.exited} side=${st.side} ` +
    `elapsed=${elapsedSec.toFixed(2)} up=${upBid} down=${dnBid} action=${JSON.stringify(action)}`
  );
  (st as any).__lastStratLogMs = _n;
}

  if (engine === "paper" && st.enterInFlight && !st.entered) {
    const pendingSide = st.pendingEntrySide as OutcomeSide | null;
    const pendingLimitPx = Number(st.pendingEntryLimitPx);
    const now = nowMs();
    const expiresAt = Number(st.pendingEntryExpiresAtMs ?? 0);
    const sideBid = pendingSide ? inferSideBid(pendingSide, upBA, dnBA) : null;

    const pendingTtlMsRaw = Number(st.pendingEntryExpiresAtMs ?? 0) - Number(st.pendingEntryPlacedAtMs ?? 0);
    const pendingTtlMs = Number.isFinite(pendingTtlMsRaw) && pendingTtlMsRaw > 0 ? pendingTtlMsRaw : LIVE_BUY_TTL_MS;
    const pendingPlacedAt = Number(st.pendingEntryPlacedAtMs ?? 0);
    const pendingMode = String(st.pendingEntryMode || "").toUpperCase();

    // Adaptive HC entry repricing for second-path opposite entries.
    // Reprice schedule uses equally-spaced checkpoints across TTL.
    if (
      pendingMode === "NO_CROSS_WINNER" &&
      pendingSide &&
      Number.isFinite(pendingPlacedAt) &&
      Number.isFinite(expiresAt) &&
      pendingPlacedAt > 0 &&
      expiresAt > pendingPlacedAt &&
      HC_ENTRY_REPRICE_OFFSETS.length > 0
    ) {
      const baseLimitRaw = Number(st.pendingEntryBaseLimitPx);
      const baseLimit = Number.isFinite(baseLimitRaw) && baseLimitRaw > 0 ? baseLimitRaw : Number(st.pendingEntryLimitPx);
      const ttlMs = Math.max(1, expiresAt - pendingPlacedAt);
      const elapsedMs = Math.max(0, now - pendingPlacedAt);
      let desiredRepriceCount = 0;
      for (let i = 0; i < HC_ENTRY_REPRICE_OFFSETS.length; i++) {
        const triggerMs = ((i + 1) / (HC_ENTRY_REPRICE_OFFSETS.length + 1)) * ttlMs;
        if (elapsedMs >= triggerMs) desiredRepriceCount = i + 1;
      }
      const currentRepriceCount = Math.max(0, Math.floor(Number(st.pendingEntryRepriceCount ?? 0)));
      if (desiredRepriceCount > currentRepriceCount && Number.isFinite(baseLimit) && baseLimit > 0) {
        const nextRepriceCount = Math.min(desiredRepriceCount, HC_ENTRY_REPRICE_OFFSETS.length);
        const oldLimitPx = Number(st.pendingEntryLimitPx);
        const offset = HC_ENTRY_REPRICE_OFFSETS[nextRepriceCount - 1];
        const nextLimitPx = clamp01(Math.min(HC_ENTRY_REPRICE_CAP_PX, baseLimit + offset));
        if (Number.isFinite(nextLimitPx) && nextLimitPx > 0 && (!Number.isFinite(oldLimitPx) || nextLimitPx > oldLimitPx + 1e-9)) {
          st.pendingEntryLimitPx = nextLimitPx;
          st.pendingEntryRepriceCount = nextRepriceCount;
          emitTradeScoped(`ENTER_REPRICE_${pendingSide}_PAPER`, {
            phase: "entry_reprice",
            via: "adaptive_hc_reprice",
            mode: st.pendingEntryMode ?? null,
            signalPx: st.pendingEntrySignalPx,
            signalThresholdPx: ui.entry,
            oldLimitPx: Number.isFinite(oldLimitPx) ? oldLimitPx : null,
            buyOrderPx: nextLimitPx,
            limitPx: nextLimitPx,
            ttlMs: pendingTtlMs,
            repriceCount: nextRepriceCount,
            repriceOffsetPx: offset,
            repriceCapPx: HC_ENTRY_REPRICE_CAP_PX,
          });
        }
      }
    }

    if (Number.isFinite(expiresAt) && now > expiresAt) {
      emitTradeScoped(`ENTER_CANCEL_${pendingSide ?? "UP"}_PAPER`, {
        via: "strategy",
        reason: "ttl_expired",
        signalPx: st.pendingEntrySignalPx,
        signalThresholdPx: ui.entry,
        buyOrderPx: st.pendingEntryLimitPx,
        limitPx: st.pendingEntryLimitPx,
        ttlMs: pendingTtlMs,
      });
      if (engine === "paper" && lane) unlockPaperSessionLane(lane);
      clearPendingEntry(st);
    } else if (
      pendingSide &&
      Number.isFinite(pendingLimitPx) &&
      Number.isFinite(Number(sideBid)) &&
      Number(sideBid) <= pendingLimitPx
    ) {
      // Buy limit semantics: only fill at or below the posted buy limit.
      const fillPx = Number(sideBid);
      const notionalUsd = Number(st.pendingEntryNotionalUsd ?? 0);
      st.entered = true;
      st.exited = false;
      st.side = pendingSide;
      st.entryTsMs = now;
      st.entryPx = fillPx;
      st.notionalUsd = notionalUsd;
      st.shares = fillPx > 0 ? notionalUsd / fillPx : 0;
      st.entryMode = st.pendingEntryMode ?? null;
      st.entrySubtype = st.pendingEntrySubtype ?? null;
      applyEntryFeeAccounting(st, fillPx, Number(st.shares ?? 0));
      st.balanceUsd = paperAccount.balanceUsd;

      emitTradeScoped(`ENTER_${pendingSide}_PAPER`, {
        via: "strategy",
        mode: st.pendingEntryMode ?? null,
        hcSubtype: st.pendingEntrySubtype ?? null,
        signalPx: st.pendingEntrySignalPx,
        signalThresholdPx: ui.entry,
        buyOrderPx: st.pendingEntryLimitPx,
        limitPx: st.pendingEntryLimitPx,
        fillPx,
        actualFillPx: fillPx,
        slippageFromSignalPx:
          Number.isFinite(Number(st.pendingEntrySignalPx)) ? fillPx - Number(st.pendingEntrySignalPx) : null,
        slippageFromThresholdPx: fillPx - ui.entry,
      });
      armPaperTpOrder(pendingSide, fillPx);
      if (engine === "paper" && lane) lockPaperSessionLane(lane, "open");
      clearPendingEntry(st);
    }
  }

  if (
    engine === "paper" &&
    st.entered &&
    !st.exited &&
    (st.side === "UP" || st.side === "DOWN") &&
    Number.isFinite(Number(st.pendingTpLimitPx)) &&
    Number(st.pendingTpLimitPx) > 0 &&
    Number.isFinite(Number(st.shares)) &&
    Number(st.shares) > 0
  ) {
    const side = st.side as OutcomeSide;
    const sideBid = inferSideBid(side, upBA, dnBA);
    const tpLimitPx = Number(st.pendingTpLimitPx);
    if (Number.isFinite(Number(sideBid)) && Number(sideBid) >= tpLimitPx) {
      const requiredShares = Number(st.shares);
      const fillEstimate = await estimateSellLimitFillability(tokenIdForSide(side), tpLimitPx, requiredShares);
      const fillableShares = Number(fillEstimate?.fillableShares ?? 0);
      const wouldFill =
        fillEstimate?.wouldFill === true ||
        (Number.isFinite(fillableShares) && fillableShares + 1e-6 >= requiredShares);
      if (wouldFill) {
        const estPx = Number(fillEstimate?.estVwapPx);
        const fillPx = clamp01(
          Number.isFinite(estPx) && estPx >= tpLimitPx ? estPx : Math.max(tpLimitPx, Number(sideBid))
        );
        st.exited = true;
        st.exitTsMs = nowMs();
        st.exitPx = fillPx;
        st.stopFallbackTriggered = false;
        const { pnlUsd } = applyExitAccounting(st, fillPx, "maker");
        clearPendingTp(st);
        st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;
        paperAccount.balanceUsd += pnlUsd;
        st.balanceUsd = paperAccount.balanceUsd;
        emitTradeScoped(`EXIT_${side}_PAPER`, {
          via: "tp_limit_order",
          exitType: "EXIT",
          signalPx: Number(sideBid),
          targetSellPx: tpLimitPx,
          limitPx: tpLimitPx,
          fillPx,
          actualFillPx: fillPx,
          fillEstimate,
        });
        if (lane) unlockPaperSessionLane(lane);
        if (lane === "BASE") {
          rearmPaperLaneStrategy("NO_CROSS_WINNER", "base_exited_rearm_hc_second_path");
        }
        return;
      }
    }
  }

  if (!action) return;

  // ===== ENTER =====
  if (action.enter && !st.entered && !st.exited && !st.enterInFlight) {
    if (String(action?.enter?.mode || "").toUpperCase() === "NO_CROSS_WINNER") {
      const sec = Number(elapsedSec);
      if (Number.isFinite(sec) && sec >= (300 - HC_BLOCK_LAST_SEC)) {
        if (engine === "paper" && lane) rearmPaperLaneStrategy(lane, "blocked_hc_last2s");
        return;
      }
    }
    // Prevent simultaneous dual-lane entries in a single tick/session.
    // Allow second-path HC only after BASE lane is fully closed.
    if (engine === "paper" && lane) {
      // Hard invariant: if any other lane is pending/open, this lane cannot enter.
      if (hasOtherPaperLanePendingOrOpen(lane)) {
        rearmPaperLaneStrategy(lane, "blocked_other_lane_pending_or_open");
        return;
      }
      if (isPaperSessionLockedForLane(lane)) {
        rearmPaperLaneStrategy(lane, "blocked_session_lock");
        return;
      }
      const baseLane = paperLaneStates.BASE;
      const hcLane = paperLaneStates.NO_CROSS_WINNER;
      if (lane === "NO_CROSS_WINNER") {
        const baseActive =
          !!baseLane &&
          ((baseLane.enterInFlight && !baseLane.entered) || (baseLane.entered && !baseLane.exited));
        if (baseActive) {
          // HC second path is allowed only after momentum is fully off.
          rearmPaperLaneStrategy(lane, "blocked_base_active_wait_for_exit");
          return;
        }
      } else if (lane === "BASE") {
        const baseStoppedThisSession = !!(baseLane as any)?.__stoppedThisSession;
        if (baseStoppedThisSession) {
          rearmPaperLaneStrategy(lane, "blocked_base_reentry_wait_hc_after_stop");
          return;
        }
        const hcEverEntered = !!hcLane && !!hcLane.entered;
        if (hcEverEntered) return; // If HC got in first, momentum stays off this session.
        const hcActive =
          !!hcLane &&
          ((hcLane.enterInFlight && !hcLane.entered) || (hcLane.entered && !hcLane.exited));
        if (hcActive) return;
      }
    }
    if (!isNewEntryAllowed(engine, st)) return;
    const side = action.enter.side;

    const entryPx = Number.isFinite(Number(action.enter.entryPx))
      ? Number(action.enter.entryPx)
      : side === "UP"
      ? upBid
      : dnBid;

    const balanceUsd = engine === "paper" ? paperAccount.balanceUsd : liveAccount.balanceUsd ?? 0;

    const dynamicNotionalUsd = calcNotionalUsd({
      balanceUsd,
      betUsd: ui.betUsd,
      maxBetUsd: ui.maxBetUsd,
      kellyOn: ui.kellyOn,
      kellyMult: ui.kellyMult,
      kellyCap: ui.kellyCap,
    });
    // Paper should always use the configured bet sizing (no Kelly override).
    const notionalUsd =
      engine === "paper"
        ? clamp(ui.betUsd, 0, Math.min(ui.maxBetUsd, balanceUsd))
        : dynamicNotionalUsd;
    if (notionalUsd <= 0) return;

    const actionLimitPxRaw = Number(action.enter.limitPx);
    const actionLimitPx = Number.isFinite(actionLimitPxRaw) ? clamp01(actionLimitPxRaw) : null;
    const actionTtlMsRaw = Number(action.enter.ttlMs);
    const actionTtlMs = Number.isFinite(actionTtlMsRaw) && actionTtlMsRaw > 0 ? Math.floor(actionTtlMsRaw) : LIVE_BUY_TTL_MS;

    if (engine === "paper") {
      const placedAt = nowMs();
      const limitPx = actionLimitPx ?? configuredEntryLimitPx(ui);
      const requiredSharesEst = limitPx > 0 ? notionalUsd / limitPx : null;
      const fillEstimate =
        side && Number.isFinite(Number(requiredSharesEst))
          ? await estimateBuyLimitFillability(tokenIdForSide(side), limitPx, Number(requiredSharesEst))
          : null;
      if (action?.enter?.mode === "NO_CROSS_WINNER") {
        const hcSubtype = String(action?.enter?.hcSubtype || "HC_OPPOSITE");
        console.log(
          `[HC SIGNAL] engine=paper side=${side} signalPx=${entryPx} limitPx=${limitPx} ttlMs=${actionTtlMs} ` +
          `subtype=${hcSubtype} oppLead=${ui.highConfidenceLead} ` +
          `sameMinBid=${HC_SAME_SIDE_MIN_BID} samePersist=${HC_SAME_SIDE_PERSISTENCE_TICKS} sameWaitSec=${HC_SAME_SIDE_MIN_SEC_AFTER_STOP}`
        );
        broadcast({
          type: "status",
          t: nowMs(),
          status:
            `HC signal (paper): side=${side} signal=${entryPx.toFixed(2)} limit=${limitPx.toFixed(2)} ` +
            `subtype=${hcSubtype}`,
        });
      }
      st.enterInFlight = true;
      st.pendingEntrySide = side;
      st.pendingEntrySignalPx = entryPx;
      st.pendingEntryLimitPx = limitPx;
      st.pendingEntryBaseLimitPx = limitPx;
      st.pendingEntryRepriceCount = 0;
      st.pendingEntryPlacedAtMs = placedAt;
      st.pendingEntryExpiresAtMs = placedAt + actionTtlMs;
      st.pendingEntryNotionalUsd = notionalUsd;
      st.pendingEntryMode = action?.enter?.mode ?? null;
      st.pendingEntrySubtype = action?.enter?.hcSubtype ?? null;
      if (lane) lockPaperSessionLane(lane, "pending");

      emitTradeScoped(`ENTER_ORDER_${side}_PAPER`, {
        via: "strategy",
        mode: action?.enter?.mode ?? null,
        hcSubtype: action?.enter?.hcSubtype ?? null,
        signalPx: entryPx,
        signalThresholdPx: ui.entry,
        buyOrderPx: limitPx,
        limitPx,
        ttlMs: actionTtlMs,
        orderPlacedAtMs: placedAt,
        fillEstimate,
      });
      return;
    }

    // LIVE: 1 buy attempt per session + no overlap
    if ((st.buyAttemptedSessionSlug ?? null) !== current.slug) {
      st.buyAttemptedSessionSlug = current.slug;
      st.buyAttemptedThisSession = false;
      st.buyFilledThisSession = false;
    }
    if (st.buyFilledThisSession) return;
    if (st.buyAttemptedThisSession) return;
    if (st.enterInFlight) return;
    const now = nowMs();
    if (st.lastEnterAttemptMs && now - st.lastEnterAttemptMs < 3000) return;

    st.lastEnterAttemptMs = now;
    st.buyAttemptedThisSession = true;
    st.enterInFlight = true;

    st.entered = true;
    st.side = side;
    st.entryTsMs = nowMs();
    st.entryPx = entryPx;
    st.notionalUsd = notionalUsd;
    st.entryMode = action?.enter?.mode ?? null;
    st.entrySubtype = action?.enter?.hcSubtype ?? null;

    try {
    const strictLimitPx = actionLimitPx ?? configuredEntryLimitPx(ui);
    const requiredSharesEst = strictLimitPx > 0 ? notionalUsd / strictLimitPx : null;
    const fillEstimate =
      side && Number.isFinite(Number(requiredSharesEst))
        ? await estimateBuyLimitFillability(tokenIdForSide(side), strictLimitPx, Number(requiredSharesEst))
        : null;
    if (action?.enter?.mode === "NO_CROSS_WINNER") {
      const hcSubtype = String(action?.enter?.hcSubtype || "HC_OPPOSITE");
      console.log(
        `[HC SIGNAL] engine=live side=${side} signalPx=${entryPx} limitPx=${strictLimitPx} ttlMs=${actionTtlMs} ` +
        `subtype=${hcSubtype} oppLead=${ui.highConfidenceLead} ` +
        `sameMinBid=${HC_SAME_SIDE_MIN_BID} samePersist=${HC_SAME_SIDE_PERSISTENCE_TICKS} sameWaitSec=${HC_SAME_SIDE_MIN_SEC_AFTER_STOP}`
      );
      broadcast({
        type: "status",
        t: nowMs(),
        status:
          `HC signal (live): side=${side} signal=${entryPx.toFixed(2)} limit=${strictLimitPx.toFixed(2)} ` +
          `subtype=${hcSubtype}`,
      });
    }
      const buyPx = buyPxForSide(side, upBA, dnBA, strictLimitPx);
      const ttlMs = actionTtlMs;
      console.log(
        `[ENTER LIVE][strategy] style=limit_strict ` +
          `attempt side=${side} buyPx=${buyPx} limitPx=${strictLimitPx} notionalUsd=${notionalUsd} ttlMs=${ttlMs} slug=${current.slug}`
      );
      const out = await placeLiveLimitBuy(side, buyPx, notionalUsd, ui, ttlMs);
      st.entryPx = out.filledPx;
      st.shares = out.shares;
      st.notionalUsd = Number.isFinite(out.shares) && Number.isFinite(out.filledPx) ? out.shares * out.filledPx : st.notionalUsd;
      applyEntryFeeAccounting(st, Number(st.entryPx), Number(st.shares ?? 0));
      st.balanceUsd = liveAccount.balanceUsd;
      st.buyFilledThisSession = true;
      st.tpOrderId = out.tpOrderId ?? null;
      st.buyFilledAtMs = out.filledAtMs ?? nowMs();
      st.firstSellAttemptAtMs = null;
      st.positionTokenId = out.tokenId ?? st.positionTokenId ?? tokenIdForSide(side);

      emitTradeScoped(`ENTER_${side}_LIVE`, {
        via: "strategy",
        mode: action?.enter?.mode ?? null,
        hcSubtype: action?.enter?.hcSubtype ?? null,
        signalPx: entryPx,
        signalThresholdPx: ui.entry,
        buyOrderPx: buyPx,
        fillPx: out.filledPx,
        actualFillPx: out.filledPx,
        slippageFromSignalPx: Number(out.filledPx) - Number(entryPx),
        slippageFromThresholdPx: Number(out.filledPx) - Number(ui.entry),
        fillEstimate,
      });
    } catch (e: any) {
      console.error(`[LIVE ENTER ERROR] ${String(e?.message ?? e)}`);
      st.entered = false;
      st.side = null;
      st.entryTsMs = null;
      st.entryPx = null;
      st.entryMode = null;
      st.entrySubtype = null;
      st.shares = null;
      st.notionalUsd = null;
      st.grossPnlUsd = null;
      st.entryFeeUsd = null;
      st.exitFeeUsd = null;
      st.totalFeesUsd = null;
      st.positionTokenId = null;
      st.buyFilledAtMs = null;
      st.firstSellAttemptAtMs = null;
      st.buyAttemptedSessionSlug = current.slug;
      st.buyFilledThisSession = false;
      const errMsg = String(e?.message ?? e);
      if (shouldRearmLiveEntryAfterError(errMsg)) {
        st.buyAttemptedThisSession = false;
        console.warn(`[LIVE ENTER REARM] strategy reason=${errMsg}`);
      }

      broadcast({ type: "status", t: nowMs(), status: `enter failed (live,strategy): ${String(e?.message ?? e)}` });
    } finally {
      st.enterInFlight = false;
    }
  }

    // ===== EXIT =====
  if (action.exit) {
    const exitTypePre = String(action.exit.type || "EXIT").toUpperCase();
    const isStopPre = exitTypePre.includes("STOP");
    if (engine === "live" && isStopPre && (!st.entered || st.exited || !st.side)) {
      await reconcileLivePositionMaybe(true, "strategy-stop-no-live-state");
      if (st.entered && !st.exited && st.side) {
        console.warn(
          `[LIVE POS SYNC] recovered during strategy stop path side=${st.side} shares=${roundTo6(Number(st.shares ?? 0))} slug=${current.slug}`
        );
      }
    }
    if (engine === "live" && isStopPre && (!st.entered || st.exited || !st.side)) {
      const t = nowMs();
      if (!((st as any).__lastNoLivePositionStopLogMs) || t - (st as any).__lastNoLivePositionStopLogMs >= STOP_SKIP_LOG_INTERVAL_MS) {
        console.log(
          `[STOP SKIP] engine=live reason=no_live_position entered=${st.entered} exited=${st.exited} side=${st.side ?? "-"}`
        );
        (st as any).__lastNoLivePositionStopLogMs = t;
      }
    }
  }
  if (action.exit && st.entered && !st.exited) {
  const side = (st.side ?? action.exit.side) as ("UP" | "DOWN");
  if (!side) return;


    // guard against overlapping exits
    if (engine === "live" && st.exitInFlight) {
      return;
    }

    const exitType = String(action.exit.type || "EXIT").toUpperCase();
    const exitPx = Number.isFinite(Number(action.exit.exitPx))
      ? Number(action.exit.exitPx)
      : side === "UP"
        ? upBid
        : dnBid;
    const stopReason = action?.exit?.stopReason != null ? String(action.exit.stopReason) : null;
    const stopSource = action?.exit?.stopSource != null ? String(action.exit.stopSource) : null;
    const stopMeta = action?.exit?.stopMeta && typeof action.exit.stopMeta === "object"
      ? (action.exit.stopMeta as Record<string, any>)
      : null;

    const isStop = exitType.includes("STOP");
    const defaultStopSource =
      isStop
        ? String(st.entryMode ?? "").toUpperCase() === "NO_CROSS_WINNER"
          ? "STRATEGY_HC"
          : "STRATEGY_MOMENTUM"
        : null;
    const resolvedStopSource = stopSource ?? defaultStopSource;
    const resolvedStopReason = stopReason ?? (isStop ? "STRATEGY_STOP_THRESHOLD" : null);
    const label = isStop ? "STOP" : "EXIT";
    const sidePxInferred = inferSideBid(side, upBA, dnBA);
    const stopGatePx = Number.isFinite(Number(sidePxInferred))
      ? Number(sidePxInferred)
      : exitPx;
    if (isStop && !shouldTriggerStop(ui, side, Number(stopGatePx), elapsedSec, engine, "strategy")) return;
    const targetSellPx = clamp01(
      Number.isFinite(exitPx)
        ? exitPx
        : isStop
          ? ui.stop
          : ui.exit
    );
    // For paper STOPs, book at current side price to reflect slippage/gaps.
    const paperExecPx =
      isStop && Number.isFinite(Number(sidePxInferred))
        ? Number(sidePxInferred)
        : exitPx;

    // 🔥 LOG ALWAYS when we *intend* to exit
    console.log(
      `[EXIT SIGNAL][strategy] engine=${engine} label=${label} exitType=${exitType} side=${side} ` +
      `exitPx=${exitPx} shares=${st.shares} entered=${st.entered} exited=${st.exited} market=${current.slug}`
    );
    broadcast({
      type: "status",
      t: nowMs(),
      status:
        `EXIT SIGNAL (strategy) engine=${engine} label=${label} side=${side} ` +
        `exitPx=${exitPx} shares=${st.shares ?? "null"} market=${current.slug}`,
    });
    emitTradeScoped(`SIGNAL_${label}_${side}_${engine.toUpperCase()}`, {
      phase: "signal",
      via: "strategy",
      exitType,
      signalPx: exitPx,
      targetSellPx,
      stopReason: resolvedStopReason,
      stopSource: resolvedStopSource,
      stopMeta,
    });

    // PAPER: STOP exits immediately; profit exits are handled by resting TP limit order.
    if (engine === "paper") {
      if (!isStop) {
        if (!(Number.isFinite(Number(st.pendingTpLimitPx)) && Number(st.pendingTpLimitPx) > 0 && st.tpOrderId)) {
          const tpPx = computePaperTpLimitPx(Number(st.entryPx));
          const orderPlacedAtMs = nowMs();
          st.pendingTpLimitPx = tpPx;
          st.pendingTpPlacedAtMs = orderPlacedAtMs;
          st.tpOrderId = `paper-tp-${current.slug}-${side}-${orderPlacedAtMs}`;
          emitTradeScoped(`EXIT_ORDER_${side}_PAPER`, {
            phase: "exit_order",
            via: "tp_limit_on_exit_signal",
            exitType: "EXIT",
            signalPx: Number.isFinite(Number(sidePxInferred)) ? Number(sidePxInferred) : Number(exitPx),
            targetSellPx: tpPx,
            limitPx: tpPx,
            orderPlacedAtMs,
            shares: Number(st.shares ?? 0),
            tpOrderId: st.tpOrderId,
          });
        }
        return;
      }
      st.exited = true;
      st.exitTsMs = nowMs();
      st.exitPx = Number.isFinite(paperExecPx) ? paperExecPx : null;
      st.stopFallbackTriggered = isStop;
      const exitFeeMode: ExitFeeMode = isStop ? "taker" : "maker";
      const { pnlUsd } = applyExitAccounting(st, Number(st.exitPx), exitFeeMode);
      clearPendingTp(st);
      st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;

      paperAccount.balanceUsd += pnlUsd;
      st.balanceUsd = paperAccount.balanceUsd;

      console.log(`[PAPER EXIT] label=${label} side=${side} exitPx=${paperExecPx} pnlUsd=${pnlUsd}`);
      emitTradeScoped(`${label}_${side}_PAPER`, {
        via: "strategy",
        exitType,
        stopReason: resolvedStopReason,
        stopSource: resolvedStopSource,
        stopMeta,
      });
      if (lane === "BASE" && isStop) {
        (st as any).__stoppedThisSession = true;
      }
      if (lane) unlockPaperSessionLane(lane);
      // If momentum just exited, force-rearm HC lane so second-path evaluation
      // starts from a fresh strategy state on the next tick.
      if (lane === "BASE") {
        rearmPaperLaneStrategy("NO_CROSS_WINNER", "base_exited_rearm_hc_second_path");
      }
      return;
    }

    // LIVE: must have shares to sell
    if (!st.shares || st.shares <= 0) {
      console.log(`[EXIT BLOCKED] live side=${side} reason=no_shares shares=${st.shares}`);
      broadcast({ type: "status", t: nowMs(), status: `EXIT BLOCKED (live): no shares to sell` });
      return;
    }

    st.exitInFlight = true;

    try {
      const sessionEndMs = st.marketEndMs ?? (Date.now() + 60_000);
      if (st.firstSellAttemptAtMs == null) st.firstSellAttemptAtMs = nowMs();
      const buyFillToSellAttemptMs =
        Number.isFinite(Number(st.buyFilledAtMs)) ? Math.max(0, Number(st.firstSellAttemptAtMs) - Number(st.buyFilledAtMs)) : null;
      console.log(
        `[EXIT LATENCY] engine=${engine} label=${label} side=${side} ` +
        `buyFillToSellAttemptMs=${buyFillToSellAttemptMs ?? "na"}`
      );

      // 🔥 LOG: sell attempt about to happen
      console.log(
        `[SELL ATTEMPT] label=${label} side=${side} targetPx=${targetSellPx} ` +
        `shares=${st.shares} sessionEnd=${new Date(sessionEndMs).toISOString()}`
      );
      broadcast({
        type: "status",
        t: nowMs(),
        status:
          `SELL ATTEMPT live label=${label} side=${side} targetPx=${targetSellPx} shares=${st.shares}`,
      });

      if (!isStop) {
        const out = await placeLiveLimitSell(side, targetSellPx, st.shares, ui, st, {
          retryEveryMs: 5000,
          perOrderTtlMs: 4500,
          untilTsMs: sessionEndMs,
          tag: "TP",
        });

        // ✅ mark exited only after success
        st.exited = true;
        st.exitTsMs = nowMs();
        st.exitPx = out.filledPx;
        st.stopFallbackTriggered = false;
        st.positionTokenId = null;
        applyExitAccounting(st, Number(st.exitPx), "maker");
        st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;

        console.log(`[SELL FILLED] TP side=${side} filledPx=${out.filledPx}`);
      emitTradeScoped(`EXIT_${side}_LIVE`, {
        via: "strategy",
        exitType,
        stopReason: resolvedStopReason,
        stopSource: resolvedStopSource,
        stopMeta,
      });
      return;
    }

      // STOP: try limit first, then fallback to market
      let filled = false;
      const started = Date.now();
      await cancelOpenSellOrdersForSide(side, "strategy-stop", st.positionTokenId);

      try {
        const out = await placeLiveLimitSell(side, targetSellPx, st.shares, ui, st, {
          retryEveryMs: 1000,
          perOrderTtlMs: 900,
          untilTsMs: sessionEndMs,
          tag: "SL",
        });

        st.exited = true;
        st.exitTsMs = nowMs();
        st.exitPx = out.filledPx;
        st.stopFallbackTriggered = true;
        st.positionTokenId = null;
        applyExitAccounting(st, Number(st.exitPx), "taker");
        filled = true;

        console.log(`[SELL FILLED] SL side=${side} filledPx=${out.filledPx}`);
      } catch (e: any) {
        const partial = Number(e?.partialFilledShares ?? 0);
        if (Number.isFinite(partial) && partial > 0 && Number.isFinite(Number(st.shares))) {
          st.shares = Math.max(0, Number(st.shares) - partial);
          console.log(`[SELL LIMIT STOP PARTIAL] side=${side} partial=${roundTo6(partial)} remaining=${roundTo6(Number(st.shares))}`);
        }
        console.log(`[SELL LIMIT STOP FAILED] side=${side} err=${String(e?.message ?? e)}`);
      }

      if (!filled && st.shares && st.shares > 0) {
        const elapsed = Date.now() - started;
        if (elapsed < 3000) await sleep(3000 - elapsed);

        await cancelOpenSellOrdersForSide(side, "strategy-stop-market-fallback", st.positionTokenId);
        console.log(`[SELL FALLBACK] market sell side=${side} shares=${st.shares}`);
        broadcast({
          type: "status",
          t: nowMs(),
          status: `STOP ORDER PLACED market_fallback side=${side} shares=${roundTo6(st.shares)}`,
        });
        const out2 = await placeLiveMarketSell(side, st.shares, ui, st);

        // ✅ mark exited only after success
        st.exited = true;
        st.exitTsMs = nowMs();
        st.exitPx = out2.filledPx;
        st.stopFallbackTriggered = true;
        st.positionTokenId = null;
        applyExitAccounting(st, Number(st.exitPx), "taker");
        broadcast({
          type: "status",
          t: nowMs(),
          status: `STOP ORDER FILLED market_fallback px=${out2.filledPx} shares=${roundTo6(st.shares)}`,
        });

        console.log(`[SELL FILLED] MKT side=${side} filledPx=${out2.filledPx}`);
      }

      st.holdSec = st.entryTsMs && st.exitTsMs ? (st.exitTsMs - st.entryTsMs) / 1000 : null;

      emitTradeScoped(`STOP_${side}_LIVE`, {
        via: "strategy",
        exitType,
        stopReason: resolvedStopReason,
        stopSource: resolvedStopSource,
        stopMeta,
      });
    } catch (e: any) {
      // ✅ IMPORTANT: do NOT leave exited=true on failure; allow retries next tick
      console.error(`[LIVE EXIT ERROR] ${String(e?.message ?? e)}`);
      broadcast({ type: "status", t: nowMs(), status: `exit failed (live,strategy): ${String(e?.message ?? e)}` });

      // keep exited=false so it tries again
      st.exited = false;
      st.exitTsMs = null;
      st.exitPx = null;
    } finally {
      st.exitInFlight = false;
    }
  }

}

// ===== Main loop =====
async function mainLoop() {
  console.log(`[${isoNow()}] server starting main loop @16Hz`);
  const periodMs = 62.5;
  let lastHeartbeat = 0;

  for (;;) {
    try {
      const now = Date.now();

      if (!current.slug || now >= current.endMs) {
        const prevSlug = String(current.slug || "");
        const prevEndMs = Number(current.endMs || 0);
        const next = await getMostCurrent5mMarket();
        current = {
          slug: next.slug,
          startMs: next.startMs,
          endMs: next.endMs,
          upToken: next.upToken,
          downToken: next.downToken,
          volumeUsd: next.volumeUsd,
        };
        entryGuardResyncSessionSlug = null;

        if (entryBlockedSessionSlug == null) {
          entryBlockedSessionSlug = current.slug;
          entryBlockActive = true;
          console.log(`[ENTRY GUARD] armed on startup session=${current.slug}; new entries blocked until next session`);
        } else if (entryBlockActive && current.slug !== entryBlockedSessionSlug) {
          entryBlockActive = false;
          console.log(`[ENTRY GUARD] released at new session=${current.slug}; new entries now allowed`);
        }

        resetEngineForNewSession("paper", { slug: current.slug, startMs: current.startMs, endMs: current.endMs });
        resetEngineForNewSession("live", { slug: current.slug, startMs: current.startMs, endMs: current.endMs });

        const rolloverLagMs = Number.isFinite(prevEndMs) && prevEndMs > 0 ? Math.max(0, Date.now() - prevEndMs) : 0;
        console.log(`[${isoNow()}] NEW SESSION ${current.slug} up=${current.upToken} down=${current.downToken}`);
        console.log(`[ROLLOVER] prev=${prevSlug || "none"} next=${current.slug} lagMs=${rolloverLagMs}`);
        broadcast({
          type: "market",
          t: nowMs(),
          current: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd },
        });
        broadcastState();
        await reconcileLivePositionMaybe(true, "startup-session");
      }

      await refreshLiveBalanceMaybe();
      await reconcileLivePositionMaybe(false, "periodic");
      await refreshCurrentMarketMetricsMaybe(false);

      const [upBAraw, dnBAraw] = await Promise.all([
        getBestBidAskSafe(current.upToken, "UP"),
        getBestBidAskSafe(current.downToken, "DOWN"),
      ]);
      const reconciled = reconcileBinaryBidPair(upBAraw, dnBAraw);
      const upBA = reconciled.up;
      const dnBA = reconciled.down;
      if (reconciled.synthetic) {
        const key = `synthetic:${current.slug}`;
        const last = Number(((mainLoop as any).__lastSyntheticWarnMs?.get(key) ?? 0));
        const nowWarn = nowMs();
        if (!Number.isFinite(last) || (nowWarn - last) > 2000) {
          if (!(mainLoop as any).__lastSyntheticWarnMs) (mainLoop as any).__lastSyntheticWarnMs = new Map<string, number>();
          (mainLoop as any).__lastSyntheticWarnMs.set(key, nowWarn);
          console.warn(
            `[BID RECONCILE] session=${current.slug} synthesized missing side ` +
            `upRaw=${String(upBAraw?.bid)} downRaw=${String(dnBAraw?.bid)} -> ` +
            `up=${String(upBA?.bid)} down=${String(dnBA?.bid)}`
          );
        }
      }
      lastObservedBids = {
        upBid: Number.isFinite(Number(upBA?.bid)) ? Number(upBA.bid) : null,
        downBid: Number.isFinite(Number(dnBA?.bid)) ? Number(dnBA.bid) : null,
        tsMs: nowMs(),
      };
      recordSessionTracePoint(current.slug, nowMs(), lastObservedBids.upBid, lastObservedBids.downBid);

      const elapsedSec = current.startMs ? (nowMs() - current.startMs) / 1000 : 0;
      const isFinal = Date.now() >= current.endMs;

      stPaper.balanceUsd = paperAccount.balanceUsd;
      for (const lane of PAPER_LANES) {
        paperLaneStates[lane].balanceUsd = paperAccount.balanceUsd;
      }
      stLive.balanceUsd = liveAccount.balanceUsd ?? stLive.balanceUsd ?? null;

      const laneOrder: StrategyLane[] =
        Math.floor(elapsedSec * 10) % 2 === 0
          ? ["BASE", "NO_CROSS_WINNER"]
          : ["NO_CROSS_WINNER", "BASE"];
      for (const lane of laneOrder) {
        await runEngineTick("paper", upBA, dnBA, elapsedSec, isFinal, lane);
      }
      syncPaperAggregateState();
      if (uiLive.enabled) await runEngineTick("live", upBA, dnBA, elapsedSec, isFinal);
      if (uiLive.enabled) await maybeExitGeneric("live", upBA, dnBA, { stopOnly: true });
      await maybeEnsureLiveTakeProfitOrder();

      broadcast({
        type: "prices",
        t: nowMs(),
        elapsed: elapsedSec,
        up: { bid: upBA.bid },
        down: { bid: dnBA.bid },
        market: { slug: current.slug, startMs: current.startMs, endMs: current.endMs, volumeUsd: current.volumeUsd },

        ui: withDerivedUi(activeUi(), activeBalanceUsd()),
        st: activeSt(),

        paper: { ui: withDerivedUi(uiPaper, paperAccount.balanceUsd), st: stPaper, balanceUsd: paperAccount.balanceUsd },
        live: {
          ui: withDerivedUi(uiLive, liveAccount.balanceUsd ?? stLive.balanceUsd ?? null),
          st: stLive,
          balanceUsd: liveAccount.balanceUsd,
        },
        pendingUiLive,
        health: buildHealthSnapshot(),
      });

      if (now - lastHeartbeat >= 1000) {
        const strategyFile = path.basename(STRATEGY_PATH);
        const strategyState = `strategyFile=${strategyFile} strategyPath=${STRATEGY_PATH} strategyLoaded(paper=${!!strategyPaper},live=${!!strategyLive})`;
        const paperCfg =
          `en=${uiPaper.enabled} paper buy limit=${uiPaper.entry} paper take profit=${uiPaper.exit} ` +
          `paper stop loss=${uiPaper.stop} useStop=${uiPaper.useStop} stopGate=${uiPaper.useStopTimeGate} ` +
          `stopStartSec=${uiPaper.stopStartSec} minEntrySec=${uiPaper.minEntrySec} ` +
          `hcSpread=${uiPaper.highConfidenceSpread} hcLead=${uiPaper.highConfidenceLead}`;
        const liveCfg =
          `en=${uiLive.enabled} live buy limit=${uiLive.entry} live take profit=${uiLive.exit} ` +
          `live stop loss=${uiLive.stop} useStop=${uiLive.useStop} stopGate=${uiLive.useStopTimeGate} ` +
          `stopStartSec=${uiLive.stopStartSec} minEntrySec=${uiLive.minEntrySec} ` +
          `hcSpread=${uiLive.highConfidenceSpread} hcLead=${uiLive.highConfidenceLead} ` +
          `entryStyle=${LIVE_ENTRY_STYLE} immTtlMs=${LIVE_ENTRY_IMMEDIATE_TTL_MS}`;
        const pendingLiveCfg = pendingUiLive
          ? ` pendingLive(entry=${pendingUiLive.entry} tp=${pendingUiLive.exit} sl=${pendingUiLive.stop} ` +
            `useStop=${pendingUiLive.useStop} stopGate=${pendingUiLive.useStopTimeGate} ` +
            `stopStartSec=${pendingUiLive.stopStartSec} en=${pendingUiLive.enabled})`
          : "";

        console.log(
          `[HEARTBEAT ${isoNow()}] v=${LIVEFIX_RUNTIME_VERSION} ${current.slug} t=${elapsedSec.toFixed(2)}s | ` +
            `UP bid=${upBA.bid} DOWN bid=${dnBA.bid} | ` +
            `${strategyState} | ` +
            `PAPER_CFG(${paperCfg}) bal=${paperAccount.balanceUsd.toFixed(2)} st(side=${stPaper.side ?? "-"} entered=${stPaper.entered} exited=${stPaper.exited} shares=${stPaper.shares ?? "-"}) | ` +
            `LIVE_CFG(${liveCfg}) bal=${liveAccount.balanceUsd ?? "—"} st(side=${stLive.side ?? "-"} entered=${stLive.entered} exited=${stLive.exited} shares=${stLive.shares ?? "-"} token=${stLive.positionTokenId ?? "-"})${pendingLiveCfg}`
        );
        lastHeartbeat = now;
      }

      await sleep(periodMs);
    } catch (e: any) {
      console.log(`[${isoNow()}] loop error: ${String(e?.message ?? e)}`);
      broadcast({ type: "status", t: nowMs(), status: `loop error: ${String(e?.message ?? e)}` });
      await sleep(500);
    }
  }
}

// ===== Start =====
server.listen(PORT, () => {
  registerServerRun();
  console.log(`[${isoNow()}] listening on http://localhost:${PORT} runtime=${LIVEFIX_RUNTIME_VERSION}`);
  console.log(`[RUN META] tradeLogDir=${TRADE_LOG_DIR} runMeta=${RUNS_META_PATH} runId=${SERVER_RUN_ID}`);
  broadcast({ type: "status", t: nowMs(), status: `listening on :${PORT} runId=${SERVER_RUN_ID}` });

  loadRuntimeStateSnapshot();
  loadMultiMarketState();
  if (!entryBlockedSessionSlug && current.slug) {
    entryBlockedSessionSlug = current.slug;
    entryBlockActive = true;
    console.log(`[ENTRY GUARD] armed from snapshot session=${current.slug}; new entries blocked until next session`);
  }

  if (BACKGROUND_REPORTS_ENABLED) {
    writeTradeSummarySnapshot();
    setInterval(writeTradeSummarySnapshot, 60_000);
    writeRunPathSummarySnapshot();
    setInterval(writeRunPathSummarySnapshot, 30_000);
    writeSessionVolatilitySnapshot();
    setInterval(writeSessionVolatilitySnapshot, 30_000);
    writeMissedSameSideHcSnapshot();
    setInterval(writeMissedSameSideHcSnapshot, 30_000);
    const initialDaily = trailing24hDailyReport();
    if (initialDaily) {
      broadcast({
        type: "status",
        t: nowMs(),
        status: `daily report updated: pnl24h=${Number(initialDaily?.pnlUsd?.total ?? 0).toFixed(2)} winRate=${initialDaily?.performance?.winRatePct ?? "n/a"}%`,
      });
    }
    setInterval(() => {
      const rep = trailing24hDailyReport();
      if (rep) {
        broadcast({
          type: "status",
          t: nowMs(),
          status: `daily report updated: pnl24h=${Number(rep?.pnlUsd?.total ?? 0).toFixed(2)} winRate=${rep?.performance?.winRatePct ?? "n/a"}%`,
        });
      }
    }, 15 * 60_000);
  } else {
    console.log("[PERF] background report writers disabled (set BACKGROUND_REPORTS_ENABLED=1 to enable)");
  }
  writeRuntimeStateSnapshot();
  setInterval(writeRuntimeStateSnapshot, RUNTIME_STATE_SNAPSHOT_INTERVAL_MS);
  setInterval(persistMultiMarketState, MULTI_STATE_PERSIST_INTERVAL_MS);
  void tickAllBotRuntimes();
  setInterval(() => {
    void tickAllBotRuntimes();
  }, BOT_RUNTIME_POLL_MS);
  console.log(`[BOT RUNTIME] poll interval=${BOT_RUNTIME_POLL_MS}ms`);
  if (PARA_SYNC_ENABLED) {
    void bootstrapParaFolders()
      .then(() => syncArtifactsToPara("startup"))
      .catch((e: any) => {
        console.warn(`[PARA BOOTSTRAP ERROR] ${String(e?.message ?? e)}`);
      });
    setInterval(() => {
      void syncArtifactsToPara("interval");
    }, PARA_SYNC_INTERVAL_MS);
  }

  reloadStrategyFactorySafe("startup");
  reloadEngineStrategy("paper", "startup");
  reloadEngineStrategy("live", "startup");
  setupStrategyHotReloadWatcher();

  void mainLoop();
});
