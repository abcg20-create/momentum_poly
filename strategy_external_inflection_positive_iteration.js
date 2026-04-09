(function () {
  'use strict';

  const { computeEntryFromBudget } = require('./execution_model_shared.js');

  const PRESET = Object.freeze({
    name: 'INFLECTION_POSITIVE_ITERATION_V1',
    strategyId: 'inflection_positive_iteration',
    sizingProfile: 'default',
    minEntrySec: 100,
    entryBreakoutThr: 0.55,
    crossReentryThr: 0.55,
    maxEntriesPerSession: 3,
    bet: 25,
    stopLossPx: 0.45,
    stopConfirmTicks: 3,
    profitProtectLadder: [
      { triggerPctBet: 12, stopPx: 0.50 },
      { triggerPctBet: 17, stopPx: 0.55 },
      { triggerPctBet: 22, stopPx: 0.60 },
    ],
    partialStagePctBet: 12,
    partialArmPctBet: 22,
    partialTargetPctBet: 27,
    partialQtyPct: 0.20,
    profitLockMinPeakPctBet: 0.42,
    profitLockDrawdownPct: 0.20,
    kalmanQ: 0.00005,
    kalmanR: 0.0008,
    inflectEps: 0.0035,
    inflectPeakMin: 0.57,
    inflectNegConfirmTicks: 3,
    inflectNegSlopeMax: -0.0003,
    inflectMinDrop: 0.012,
    inflectPromLookback: 22,
    inflectPromMin: 0.018,
    inflectMinSpacingTicks: 10,
    exitNegSlopeConfirmTicks: 5,
    rawCrossCooldownSec: 7,
    resampleMs: 50,
    checkpointDelayMs: 0,
    slopeLookbackMs: 2500,
    minSlopeSamples: 4,
    emaFastMs: 250,
    emaSlowMs: 1200,
    rollingLowLookbackMs: 2500,
    recentHighLookbackMs: 500,
    recoveryMinPx: 0.06,
    breakoutEpsilonPx: 0.01,
    emaSlopeMinPx: 0.002,
    entryConfirmTimeoutMs: 1500,
    acceptSyntheticQuoteMaxAgeMs: 2000,
    immediateDualTpOnFill: true,
  });

  const GOLD_BETS_V1_WINDOWS = Object.freeze({
    1: Object.freeze([
      Object.freeze({ startSec: 180, endSec: 210, betUsd: 100 }),
      Object.freeze({ startSec: 210, endSec: 240, betUsd: 100 }),
      Object.freeze({ startSec: 240, endSec: 270, betUsd: 100 }),
      Object.freeze({ startSec: 270, endSec: 297, betUsd: 100 }),
    ]),
    2: Object.freeze([
      Object.freeze({ startSec: 210, endSec: 240, betUsd: 100 }),
      Object.freeze({ startSec: 240, endSec: 270, betUsd: 100 }),
      Object.freeze({ startSec: 270, endSec: 297, betUsd: 100 }),
    ]),
    3: Object.freeze([
      Object.freeze({ startSec: 210, endSec: 240, betUsd: 100 }),
      Object.freeze({ startSec: 240, endSec: 270, betUsd: 100 }),
      Object.freeze({ startSec: 270, endSec: 297, betUsd: 100 }),
    ]),
  });

  const GOLD_BETS_V2_WINDOWS = Object.freeze({
    1: Object.freeze([
      Object.freeze({ startSec: 90, endSec: 120, betUsd: 100 }),
      Object.freeze({ startSec: 120, endSec: 150, betUsd: 100 }),
      Object.freeze({ startSec: 150, endSec: 180, betUsd: 100 }),
    ]),
    2: Object.freeze([
      Object.freeze({ startSec: 210, endSec: 240, betUsd: 100 }),
      Object.freeze({ startSec: 240, endSec: 270, betUsd: 100 }),
      Object.freeze({ startSec: 270, endSec: 297, betUsd: 100 }),
    ]),
    3: Object.freeze([
      Object.freeze({ startSec: 210, endSec: 240, betUsd: 100 }),
      Object.freeze({ startSec: 240, endSec: 270, betUsd: 100 }),
      Object.freeze({ startSec: 270, endSec: 297, betUsd: 100 }),
    ]),
  });

  function toNum(v, d) {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
  }

  function clamp01(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return NaN;
    return Math.max(0, Math.min(1, n));
  }

  function normalizeSide(v) {
    return String(v || '').toUpperCase() === 'DOWN' ? 'DOWN' : 'UP';
  }

  function sidePx(side, upBid, downBid) {
    return normalizeSide(side) === 'DOWN' ? Number(downBid) : Number(upBid);
  }

  function crossed(prevDiff, currDiff) {
    if (!Number.isFinite(prevDiff) || !Number.isFinite(currDiff)) return false;
    if (prevDiff === 0 || currDiff === 0) return prevDiff !== currDiff;
    return (prevDiff > 0 && currDiff < 0) || (prevDiff < 0 && currDiff > 0);
  }

  function kalmanUpdate(state, measurement, q, r) {
    const z = Number(measurement);
    if (!Number.isFinite(z)) return state;
    if (!state || !Number.isFinite(Number(state.x)) || !Number.isFinite(Number(state.p))) {
      return { x: z, p: 1 };
    }
    const pPred = Number(state.p) + q;
    const k = pPred / (pPred + r);
    const x = Number(state.x) + k * (z - Number(state.x));
    return { x: x, p: (1 - k) * pPred };
  }

  function trailingMin(series, start, endInclusive) {
    let m = Infinity;
    for (let i = start; i <= endInclusive; i += 1) {
      const v = Number(series[i]);
      if (!Number.isFinite(v)) continue;
      if (v < m) m = v;
    }
    return Number.isFinite(m) ? m : NaN;
  }

  function detectLiveSafeInflectionAt(series, i, cfg, sideState) {
    if (!Array.isArray(series) || i < 1 || !sideState) return null;
    const prev = Number(series[i - 1]);
    const cur = Number(series[i]);
    if (!Number.isFinite(prev) || !Number.isFinite(cur)) return null;

    if (!Number.isFinite(sideState.peakVal)) {
      sideState.peakVal = cur;
      sideState.peakIdx = i;
      sideState.negStreak = 0;
      return null;
    }

    const slope = cur - prev;
    if (cur >= sideState.peakVal) {
      sideState.peakVal = cur;
      sideState.peakIdx = i;
      sideState.negStreak = 0;
      return null;
    }

    if (slope <= cfg.inflectNegSlopeMax) sideState.negStreak += 1;
    else sideState.negStreak = 0;

    if (sideState.negStreak < cfg.inflectNegConfirmTicks) return null;
    if (!Number.isFinite(sideState.peakVal) || sideState.peakVal < cfg.inflectPeakMin) return null;

    const drop = sideState.peakVal - cur;
    if (!Number.isFinite(drop) || drop < cfg.inflectMinDrop) return null;

    const lookback = Math.max(3, Math.floor(Number(cfg.inflectPromLookback) || 22));
    const baselineStart = Math.max(0, sideState.peakIdx - lookback + 1);
    const baseline = trailingMin(series, baselineStart, sideState.peakIdx);
    if (!Number.isFinite(baseline)) return null;
    const prominence = sideState.peakVal - baseline;
    if (!Number.isFinite(prominence) || prominence < cfg.inflectPromMin) return null;

    if (
      Number.isFinite(sideState.lastEmitPeakIdx) &&
      sideState.peakIdx - sideState.lastEmitPeakIdx < cfg.inflectMinSpacingTicks
    ) {
      return null;
    }

    sideState.lastEmitPeakIdx = sideState.peakIdx;
    return {
      peakIdx: sideState.peakIdx,
      peakVal: sideState.peakVal,
      detectIdx: i,
      detectVal: cur,
      drop: drop,
      prominence: prominence,
    };
  }

  function cloneSimple(v) {
    if (v == null) return null;
    try {
      return JSON.parse(JSON.stringify(v));
    } catch {
      return null;
    }
  }

  function resolveCheckpoint(observed, startIdx, targetMs, cutoffMs) {
    if (!Array.isArray(observed) || !observed.length) return null;
    let best = null;
    let fallback = null;
    const maxLocalDist = Math.max(0, Number(cutoffMs) - Number(targetMs));
    let nextStartIdx = Math.max(0, Math.floor(Number(startIdx) || 0));
    for (let i = nextStartIdx; i < observed.length; i += 1) {
      const row = observed[i];
      const tMs = Number(row && row.tMs);
      if (!Number.isFinite(tMs)) continue;
      if (tMs > cutoffMs) break;
      nextStartIdx = Math.max(0, i - 1);
      fallback = {
        tMs,
        dist: Math.abs(tMs - targetMs),
        upBid: Number(row.upBid),
        downBid: Number(row.downBid),
      };
      const dist = Math.abs(tMs - targetMs);
      if (dist > maxLocalDist + 1e-9) continue;
      if (
        !best ||
        dist < best.dist - 1e-9 ||
        (Math.abs(dist - best.dist) <= 1e-9 && tMs < best.tMs)
      ) {
        best = {
          tMs,
          dist,
          upBid: Number(row.upBid),
          downBid: Number(row.downBid),
        };
      }
    }
    return {
      point: best || fallback,
      nextStartIdx,
    };
  }

  function buildResampleWindow(resampled, lookbackMs, minSamples, side) {
    if (!Array.isArray(resampled) || !resampled.length) return null;
    const normalizedSide = normalizeSide(side);
    const latest = resampled[resampled.length - 1];
    const startMs = Number(latest.checkpointMs) - Number(lookbackMs);
    const points = [];
    for (let i = 0; i < resampled.length; i += 1) {
      const row = resampled[i];
      if (Number(row.checkpointMs) < startMs - 1e-9) continue;
      points.push({
        checkpointMs: Number(row.checkpointMs),
        sourceTsMs: Number(row.sourceTsMs),
        px: normalizedSide === 'DOWN' ? Number(row.downBid) : Number(row.upBid),
      });
    }
    if (points.length < minSamples) return null;
    let prev = Number(points[0].px);
    if (!Number.isFinite(prev)) return null;
    for (let i = 1; i < points.length; i += 1) {
      const cur = Number(points[i].px);
      if (!Number.isFinite(cur)) return null;
      if (!(cur > prev)) return null;
      prev = cur;
    }
    return {
      points,
      gain: Number(points[points.length - 1].px) - Number(points[0].px),
      latestCheckpointMs: Number(latest.checkpointMs),
    };
  }

  function latestPointsWithin(resampled, lookbackMs) {
    if (!Array.isArray(resampled) || !resampled.length) return [];
    const latest = resampled[resampled.length - 1];
    const startMs = Number(latest.checkpointMs) - Number(lookbackMs);
    return resampled.filter((row) => Number(row.checkpointMs) >= startMs - 1e-9);
  }

  function sidePoints(rows, side) {
    const normalizedSide = normalizeSide(side);
    return rows.map((row) => ({
      checkpointMs: Number(row.checkpointMs),
      sourceTsMs: Number(row.sourceTsMs),
      px: normalizedSide === 'DOWN' ? Number(row.downBid) : Number(row.upBid),
    })).filter((row) => Number.isFinite(row.px));
  }

  function computeEmaSeries(points, periodSamples) {
    if (!Array.isArray(points) || !points.length) return [];
    const n = Math.max(1, Math.floor(Number(periodSamples) || 1));
    const alpha = 2 / (n + 1);
    const out = [];
    let ema = Number(points[0].px);
    out.push(ema);
    for (let i = 1; i < points.length; i += 1) {
      const px = Number(points[i].px);
      ema = (alpha * px) + ((1 - alpha) * ema);
      out.push(ema);
    }
    return out;
  }

  function buildIterationSignal(resampled, cfg, side) {
    const normalizedSide = normalizeSide(side);
    const lookbackMs = Math.max(
      Number(cfg.rollingLowLookbackMs),
      Number(cfg.recentHighLookbackMs),
      Number(cfg.emaSlowMs) * 3
    );
    const rows = latestPointsWithin(resampled, lookbackMs);
    const points = sidePoints(rows, normalizedSide);
    if (points.length < Math.max(4, Math.ceil(Number(cfg.emaSlowMs) / Number(cfg.resampleMs)))) return null;

    const latest = points[points.length - 1];
    const latestPx = Number(latest.px);
    if (!(Number.isFinite(latestPx) && latestPx >= Number(cfg.entryBreakoutThr))) return null;

    const fastSamples = Math.max(2, Math.round(Number(cfg.emaFastMs) / Number(cfg.resampleMs)));
    const slowSamples = Math.max(fastSamples + 1, Math.round(Number(cfg.emaSlowMs) / Number(cfg.resampleMs)));
    const fastSeries = computeEmaSeries(points, fastSamples);
    const slowSeries = computeEmaSeries(points, slowSamples);
    if (fastSeries.length < 2 || slowSeries.length < 2) return null;

    const emaFast = Number(fastSeries[fastSeries.length - 1]);
    const emaSlow = Number(slowSeries[slowSeries.length - 1]);
    const emaFastPrev = Number(fastSeries[fastSeries.length - 2]);
    const emaSlope = emaFast - emaFastPrev;
    if (!(Number.isFinite(emaFast) && Number.isFinite(emaSlow) && Number.isFinite(emaSlope))) return null;
    if (!(emaFast > emaSlow)) return null;
    if (!(emaSlope >= Number(cfg.emaSlopeMinPx))) return null;

    const rollingLowStartMs = Number(latest.checkpointMs) - Number(cfg.rollingLowLookbackMs);
    const recentHighStartMs = Number(latest.checkpointMs) - Number(cfg.recentHighLookbackMs);
    const rollingLowPoints = points.filter((point) => Number(point.checkpointMs) >= rollingLowStartMs - 1e-9);
    const recentHighPoints = points.filter((point) => Number(point.checkpointMs) >= recentHighStartMs - 1e-9);
    if (!rollingLowPoints.length || !recentHighPoints.length) return null;

    const rollingLow = Math.min(...rollingLowPoints.map((point) => Number(point.px)));
    const recentHigh = Math.max(...recentHighPoints.map((point) => Number(point.px)));
    if (!(latestPx >= (rollingLow + Number(cfg.recoveryMinPx)))) return null;
    if (!(latestPx >= (recentHigh - Number(cfg.breakoutEpsilonPx)))) return null;

    return {
      side: normalizedSide,
      px: latestPx,
      latestCheckpointMs: Number(latest.checkpointMs),
      emaFast,
      emaSlow,
      emaSlope,
      rollingLow,
      recentHigh,
      points,
    };
  }

  const TradeStrategy = {};
  TradeStrategy.VERSION = PRESET.name;
  TradeStrategy.STRATEGY_ID = PRESET.strategyId;
  TradeStrategy.EXECUTION_MODEL = 'strategy_runtime';

  TradeStrategy.makeStrategy = function makeStrategy(params) {
    const presetLadder = Array.isArray(PRESET.profitProtectLadder)
      ? PRESET.profitProtectLadder.map((x) => ({
          triggerPctBet: Number(x.triggerPctBet),
          stopPx: Number(x.stopPx),
        }))
      : [];

    const cfg = {
      ...PRESET,
      sizingProfile: String(params && params.sizingProfile || PRESET.sizingProfile).trim() || PRESET.sizingProfile,
      minEntrySec: PRESET.minEntrySec,
      entryBreakoutThr: PRESET.entryBreakoutThr,
      crossReentryThr: PRESET.crossReentryThr,
      maxEntriesPerSession: Math.max(1, Math.floor(toNum(params && params.maxEntriesPerSession, PRESET.maxEntriesPerSession))),
      bet: Math.max(1, toNum(params && params.bet, PRESET.bet)),
      stopLossPx: clamp01(toNum(params && params.stopLossPx, toNum(params && params.stop, PRESET.stopLossPx))),
      stopConfirmTicks: Math.max(1, Math.floor(toNum(params && params.stopConfirmTicks, PRESET.stopConfirmTicks))),
      profitProtectLadder: presetLadder,
      partialStagePctBet: Math.max(0, toNum(params && params.partialStagePctBet, PRESET.partialStagePctBet)),
      partialArmPctBet: Math.max(0, toNum(params && params.partialArmPctBet, PRESET.partialArmPctBet)),
      partialTargetPctBet: Math.max(0, toNum(params && params.partialTargetPctBet, PRESET.partialTargetPctBet)),
      partialQtyPct: Math.max(0, Math.min(1, toNum(params && params.partialQtyPct, PRESET.partialQtyPct))),
      profitLockMinPeakPctBet: Math.max(0, toNum(params && params.profitLockMinPeakPctBet, PRESET.profitLockMinPeakPctBet)),
      profitLockDrawdownPct: Math.max(0, Math.min(0.95, toNum(params && params.profitLockDrawdownPct, PRESET.profitLockDrawdownPct))),
      kalmanQ: Math.max(0, toNum(params && params.kalmanQ, PRESET.kalmanQ)),
      kalmanR: Math.max(1e-12, toNum(params && params.kalmanR, PRESET.kalmanR)),
      inflectEps: Math.max(0, toNum(params && params.inflectEps, PRESET.inflectEps)),
      inflectPeakMin: clamp01(toNum(params && params.inflectPeakMin, PRESET.inflectPeakMin)),
      inflectNegConfirmTicks: Math.max(1, Math.floor(toNum(params && params.inflectNegConfirmTicks, PRESET.inflectNegConfirmTicks))),
      inflectNegSlopeMax: toNum(params && params.inflectNegSlopeMax, PRESET.inflectNegSlopeMax),
      inflectMinDrop: Math.max(0, toNum(params && params.inflectMinDrop, PRESET.inflectMinDrop)),
      inflectPromLookback: Math.max(3, Math.floor(toNum(params && params.inflectPromLookback, PRESET.inflectPromLookback))),
      inflectPromMin: Math.max(0, toNum(params && params.inflectPromMin, PRESET.inflectPromMin)),
      inflectMinSpacingTicks: Math.max(0, Math.floor(toNum(params && params.inflectMinSpacingTicks, PRESET.inflectMinSpacingTicks))),
      exitNegSlopeConfirmTicks: Math.max(1, Math.floor(toNum(params && params.exitNegSlopeConfirmTicks, PRESET.exitNegSlopeConfirmTicks))),
      rawCrossCooldownSec: Math.max(0, toNum(params && params.rawCrossCooldownSec, PRESET.rawCrossCooldownSec)),
      resampleMs: Math.max(25, Math.floor(toNum(params && params.resampleMs, PRESET.resampleMs))),
      checkpointDelayMs: Math.max(0, Math.floor(toNum(params && params.checkpointDelayMs, PRESET.checkpointDelayMs))),
      slopeLookbackMs: Math.max(250, Math.floor(toNum(params && params.slopeLookbackMs, PRESET.slopeLookbackMs))),
      minSlopeSamples: Math.max(2, Math.floor(toNum(params && params.minSlopeSamples, PRESET.minSlopeSamples))),
      emaFastMs: Math.max(50, Math.floor(toNum(params && params.emaFastMs, PRESET.emaFastMs))),
      emaSlowMs: Math.max(100, Math.floor(toNum(params && params.emaSlowMs, PRESET.emaSlowMs))),
      rollingLowLookbackMs: Math.max(250, Math.floor(toNum(params && params.rollingLowLookbackMs, PRESET.rollingLowLookbackMs))),
      recentHighLookbackMs: Math.max(100, Math.floor(toNum(params && params.recentHighLookbackMs, PRESET.recentHighLookbackMs))),
      recoveryMinPx: Math.max(0, toNum(params && params.recoveryMinPx, PRESET.recoveryMinPx)),
      breakoutEpsilonPx: Math.max(0, toNum(params && params.breakoutEpsilonPx, PRESET.breakoutEpsilonPx)),
      emaSlopeMinPx: Math.max(0, toNum(params && params.emaSlopeMinPx, PRESET.emaSlopeMinPx)),
      entryConfirmTimeoutMs: Math.max(250, Math.floor(toNum(params && params.entryConfirmTimeoutMs, PRESET.entryConfirmTimeoutMs))),
      acceptSyntheticQuoteMaxAgeMs: Math.max(250, Math.floor(toNum(params && params.acceptSyntheticQuoteMaxAgeMs, PRESET.acceptSyntheticQuoteMaxAgeMs))),
    };

    const seed = params && params.initialState != null && typeof params.initialState === 'object'
      ? cloneSimple(params.initialState)
      : null;
    function seedNum(key, fallback) {
      return seed && Number.isFinite(Number(seed[key])) ? Number(seed[key]) : fallback;
    }
    function seedInt(key, fallback) {
      return Math.floor(seedNum(key, fallback));
    }

    let tickIdx = seedInt('tickIdx', -1);
    let lastElapsedSec = seedNum('lastElapsedSec', 0);
    let lastUpBid = seedNum('lastUpBid', NaN);
    let lastDownBid = seedNum('lastDownBid', NaN);
    let lastRawDiff = seedNum('lastRawDiff', NaN);
    let crossCooldownUntilMs = seedNum('crossCooldownUntilMs', -Infinity);
    let nextCheckpointMs = seedNum('nextCheckpointMs', cfg.resampleMs);
    let lastResampleSourceTsMs = seedNum('lastResampleSourceTsMs', NaN);
    let observedSearchStartIdx = seedInt('observedSearchStartIdx', 0);
    const observed = Array.isArray(seed && seed.observed) ? cloneSimple(seed.observed) : [];
    const resampled = Array.isArray(seed && seed.resampled) ? cloneSimple(seed.resampled) : [];

    let upKState = seed && seed.upKState && typeof seed.upKState === 'object' ? cloneSimple(seed.upKState) : null;
    let dnKState = seed && seed.dnKState && typeof seed.dnKState === 'object' ? cloneSimple(seed.dnKState) : null;
    const upKal = Array.isArray(seed && seed.upKal) ? cloneSimple(seed.upKal) : [];
    const dnKal = Array.isArray(seed && seed.dnKal) ? cloneSimple(seed.dnKal) : [];
    const inflectStateBySide = seed && seed.inflectStateBySide && typeof seed.inflectStateBySide === 'object'
      ? cloneSimple(seed.inflectStateBySide)
      : {
          UP: { peakVal: NaN, peakIdx: -1, negStreak: 0, lastEmitPeakIdx: -1 },
          DOWN: { peakVal: NaN, peakIdx: -1, negStreak: 0, lastEmitPeakIdx: -1 },
        };

    let entriesThisSession = Math.max(0, seedInt('entriesThisSession', 0));
    let pureObservedTicks = Math.max(0, seedInt('pureObservedTicks', 0));
    let nonPureSkippedTicks = Math.max(0, seedInt('nonPureSkippedTicks', 0));
    let acceptedSyntheticTicks = Math.max(0, seedInt('acceptedSyntheticTicks', 0));
    let finalSyntheticTicks = Math.max(0, seedInt('finalSyntheticTicks', 0));
    let duplicateQuoteSeqSkippedTicks = Math.max(0, seedInt('duplicateQuoteSeqSkippedTicks', 0));
    let lastAcceptedQuoteSeq = Number.isFinite(seedNum('lastAcceptedQuoteSeq', NaN)) ? seedNum('lastAcceptedQuoteSeq', NaN) : null;
    let lastQuoteMeta = seed && seed.lastQuoteMeta && typeof seed.lastQuoteMeta === 'object'
      ? cloneSimple(seed.lastQuoteMeta)
      : null;
    const rearmBlocked = seed && seed.rearmBlocked && typeof seed.rearmBlocked === 'object'
      ? {
          UP: !!seed.rearmBlocked.UP,
          DOWN: !!seed.rearmBlocked.DOWN,
        }
      : { UP: false, DOWN: false };
    let activeTrade = seed && seed.activeTrade && typeof seed.activeTrade === 'object'
      ? cloneSimple(seed.activeTrade)
      : null;
    if (activeTrade && activeTrade.partial && typeof activeTrade.partial === 'object') {
      activeTrade.partial.stageTriggered = !!activeTrade.partial.stageTriggered;
      activeTrade.partial.armTriggered = !!activeTrade.partial.armTriggered;
      activeTrade.partial.orderPlaced = !!activeTrade.partial.orderPlaced;
      activeTrade.partial.filled = !!activeTrade.partial.filled;
      activeTrade.partial.completed = !!activeTrade.partial.completed;
    }
    if (activeTrade && Number.isFinite(Number(activeTrade.tradeNum))) {
      entriesThisSession = Math.max(entriesThisSession, Math.floor(Number(activeTrade.tradeNum)));
    }

    function betUsdForTradeNum(_tradeNum) {
      const tradeNum = Math.max(1, Math.floor(Number(_tradeNum) || 1));
      const profile = String(cfg.sizingProfile || '').trim().toLowerCase();
      let windows = null;
      if (profile === 'gold_bets_v1' || profile === 'gold bets v1') {
        windows = GOLD_BETS_V1_WINDOWS[tradeNum] || [];
      } else if (profile === 'gold_bets_v2' || profile === 'gold bets v2') {
        windows = GOLD_BETS_V2_WINDOWS[tradeNum] || [];
      }
      if (Array.isArray(windows) && windows.length) {
        for (let i = 0; i < windows.length; i += 1) {
          const w = windows[i];
          if (lastElapsedSec >= Number(w.startSec) && lastElapsedSec < Number(w.endSec)) {
            return Math.max(cfg.bet, Number(w.betUsd));
          }
        }
      }
      return cfg.bet;
    }

    const observedRetentionMs = Math.max(
      cfg.resampleMs * 4,
      cfg.checkpointDelayMs + cfg.resampleMs * 2
    );
    const snapshotResampleLookbackMs = Math.max(
      Number(cfg.slopeLookbackMs),
      Number(cfg.rollingLowLookbackMs),
      Number(cfg.recentHighLookbackMs),
      Number(cfg.emaSlowMs) * 3
    ) + (cfg.resampleMs * 4);
    const snapshotObservedLookbackMs = Math.max(
      1000,
      snapshotResampleLookbackMs,
      Number(cfg.entryConfirmTimeoutMs)
    );
    const snapshotKalTail = Math.max(64, Math.ceil(snapshotResampleLookbackMs / Math.max(1, cfg.resampleMs)));

    function trimObserved(nowMs) {
      if (!Array.isArray(observed) || observed.length <= 2) return;
      const cutoffMs = Math.max(0, Number(nowMs) - Number(snapshotObservedLookbackMs) - Number(observedRetentionMs));
      let trimCount = 0;
      while (trimCount < (observed.length - 2)) {
        const tMs = Number(observed[trimCount] && observed[trimCount].tMs);
        if (!Number.isFinite(tMs) || tMs >= cutoffMs) break;
        trimCount += 1;
      }
      if (trimCount > 0) {
        observed.splice(0, trimCount);
        observedSearchStartIdx = Math.max(0, observedSearchStartIdx - trimCount);
      }
    }

    function maybeFinalizeCheckpoints(nowMs) {
      while ((nextCheckpointMs + cfg.checkpointDelayMs) <= (nowMs + 1e-9)) {
        const resolved = resolveCheckpoint(
          observed,
          observedSearchStartIdx,
          nextCheckpointMs,
          nextCheckpointMs + cfg.checkpointDelayMs
        );
        if (resolved && Number.isFinite(Number(resolved.nextStartIdx))) {
          observedSearchStartIdx = Math.max(0, Math.floor(Number(resolved.nextStartIdx)));
        }
        const point = resolved && resolved.point ? resolved.point : null;
        if (point && Number(point.tMs) !== Number(lastResampleSourceTsMs)) {
          resampled.push({
            checkpointMs: nextCheckpointMs,
            sourceTsMs: Number(point.tMs),
            upBid: Number(point.upBid),
            downBid: Number(point.downBid),
          });
          lastResampleSourceTsMs = Number(point.tMs);
        }
        nextCheckpointMs += cfg.resampleMs;
      }
      trimObserved(nowMs);
    }

    function computeCurrentStopPx(trade) {
      let stopPx = cfg.stopLossPx;
      const peakGross = Number(trade && trade.peakGrossPnlUsd);
      const betUsd = Number(trade && trade.betUsd);
      if (!(Number.isFinite(peakGross) && Number.isFinite(betUsd) && betUsd > 0)) return stopPx;
      for (let i = 0; i < cfg.profitProtectLadder.length; i += 1) {
        const rung = cfg.profitProtectLadder[i];
        const triggerUsd = betUsd * Number(rung.triggerPctBet) / 100;
        if (peakGross >= triggerUsd) {
          stopPx = Math.max(stopPx, Number(rung.stopPx));
        }
      }
      return stopPx;
    }

    function currentGrossPnlUsd(trade, markPx) {
      if (!trade) return NaN;
      const realizedGross = Number(trade.realizedGrossPnlUsd || 0);
      const curShares = Number(trade.currentShares);
      const entryPx = Number(trade.entryPx);
      if (!(Number.isFinite(markPx) && Number.isFinite(curShares) && Number.isFinite(entryPx))) {
        return realizedGross;
      }
      return realizedGross + ((markPx - entryPx) * curShares);
    }

    function buildPendingExitSnapshot() {
      if (!activeTrade) return null;
      if (activeTrade.exitPending) {
        return {
          side: activeTrade.side,
          type: activeTrade.exitPending.type,
          exitPx: activeTrade.exitPending.exitPx,
          shares: activeTrade.exitPending.shares,
          qtyPct: activeTrade.exitPending.qtyPct,
          orderType: activeTrade.exitPending.orderType,
          feeMode: activeTrade.exitPending.feeMode || null,
          exitReason: activeTrade.exitPending.stopReason || null,
          atSec: lastElapsedSec,
        };
      }
      const partial = activeTrade.partial;
      if (
        partial &&
        partial.orderPlaced &&
        !partial.filled &&
        Number.isFinite(Number(partial.limitPx)) &&
        Number.isFinite(Number(partial.shares)) &&
        Number(partial.shares) > 0
      ) {
        return {
          side: activeTrade.side,
          type: 'PARTIAL_TP_27',
          exitPx: Number(partial.limitPx),
          shares: Number(partial.shares),
          orderType: 'LIMIT',
          feeMode: 'none',
          exitReason: 'PARTIAL_STAGE12_ARM22_TARGET27',
          atSec: lastElapsedSec,
        };
      }
      return null;
    }

    function armTradeEntry(side, entryPx, sideKal, nowMs, extra) {
      if (!(entriesThisSession < cfg.maxEntriesPerSession)) return null;
      const betUsd = betUsdForTradeNum(entriesThisSession + 1);
      const px = clamp01(entryPx);
      if (!(Number.isFinite(px) && px > 0)) return null;
      const entryFill = computeEntryFromBudget(betUsd, px, { feeMode: 'taker', allowZeroOrOne: false });
      const shares = Number(entryFill.shares || 0);
      if (!(Number.isFinite(shares) && shares > 0)) return null;
      entriesThisSession += 1;
      activeTrade = {
        tradeNum: entriesThisSession,
        status: 'enter_pending',
        intentTsMs: Number(nowMs),
        intentEntryPx: px,
        side: normalizeSide(side),
        betUsd: betUsd,
        entryPx: px,
        entryTsMs: Number(nowMs),
        entrySec: lastElapsedSec,
        originalShares: shares,
        currentShares: shares,
        realizedGrossPnlUsd: 0,
        peakGrossPnlUsd: 0,
        lastSideKal: Number.isFinite(Number(sideKal)) ? Number(sideKal) : NaN,
        entryTag: String(extra && extra.tag || '').trim() || undefined,
        negSlopeStreak: 0,
        stopBreachStreak: 0,
        partial: {
          orderPlaced: false,
          filled: false,
          completed: false,
          shares: shares * cfg.partialQtyPct,
          limitPx: clamp01(px * (1 + cfg.partialTargetPctBet / 100)),
          stageTriggered: false,
          armTriggered: false,
          fillDetectedTsMs: null,
        },
        exitPending: null,
      };
      return {
        enter: {
          side: normalizeSide(side),
          entryPx: px,
          notionalUsd: betUsd,
          betUsd: betUsd,
          orderType: 'MARKET',
          entryFeeMode: 'taker',
          ...(String(extra && extra.tag || '').trim() ? { tag: String(extra.tag).trim() } : {}),
        },
      };
    }

    function queueFullExit(type, exitPx, extra) {
      if (!activeTrade) return null;
      const spec = {
        type: String(type || 'EXIT').toUpperCase(),
        side: activeTrade.side,
        exitPx: Number.isFinite(Number(exitPx)) ? clamp01(exitPx) : undefined,
        shares: Number.isFinite(Number(activeTrade.currentShares)) ? Number(activeTrade.currentShares) : undefined,
        qtyPct: undefined,
        orderType: String(extra && extra.orderType || 'MARKET').toUpperCase(),
        tag: String(extra && extra.tag || '').trim() || undefined,
        stopReason: String(extra && extra.stopReason || '').trim() || undefined,
        feeMode: String(extra && extra.feeMode || '').trim().toLowerCase() || undefined,
      };
      activeTrade.exitPending = spec;
      return {
        exit: {
          type: spec.type,
          side: spec.side,
          ...(Number.isFinite(Number(spec.exitPx)) ? { exitPx: Number(spec.exitPx) } : {}),
          ...(Number.isFinite(Number(spec.shares)) ? { shares: Number(spec.shares) } : {}),
          orderType: spec.orderType,
          ...(spec.tag ? { tag: spec.tag } : {}),
          ...(spec.stopReason ? { stopReason: spec.stopReason } : {}),
          ...(spec.feeMode ? { feeMode: spec.feeMode } : {}),
        },
      };
    }

    function maybeSyncTradeFromRuntime(t, nowMs) {
      const pos = t && t.position && typeof t.position === 'object' ? t.position : null;
      const runtimeEntered = !!pos?.entered && (pos?.side === 'UP' || pos?.side === 'DOWN');
      const runtimeSide = runtimeEntered ? normalizeSide(pos.side) : null;
      const runtimeShares = Number(pos && pos.shares);
      const runtimeEntryPx = Number(pos && pos.entryPx);

      if (!activeTrade && runtimeEntered) {
        const inferredBetUsd = Number.isFinite(Number(pos?.notionalUsd)) && Number(pos.notionalUsd) > 0
          ? Number(pos.notionalUsd)
          : cfg.bet;
        const shares = Number.isFinite(runtimeShares) && runtimeShares > 0
          ? runtimeShares
          : (
              Number.isFinite(runtimeEntryPx) && runtimeEntryPx > 0
                ? (inferredBetUsd / runtimeEntryPx)
                : 0
            );
        activeTrade = {
          tradeNum: Math.min(entriesThisSession + 1, cfg.maxEntriesPerSession),
          status: 'open',
          intentTsMs: Number(nowMs),
          intentEntryPx: runtimeEntryPx,
          side: runtimeSide,
          betUsd: inferredBetUsd,
          entryPx: runtimeEntryPx,
          entryTsMs: Number(nowMs),
          entrySec: lastElapsedSec,
          originalShares: shares,
          currentShares: shares,
          realizedGrossPnlUsd: 0,
          peakGrossPnlUsd: 0,
          lastSideKal: NaN,
          negSlopeStreak: 0,
          stopBreachStreak: 0,
          partial: {
            orderPlaced: false,
            filled: false,
            completed: false,
            shares: shares * cfg.partialQtyPct,
            limitPx: clamp01(runtimeEntryPx * (1 + cfg.partialTargetPctBet / 100)),
            stageTriggered: false,
            armTriggered: false,
            fillDetectedTsMs: null,
          },
          exitPending: null,
        };
        entriesThisSession = Math.max(entriesThisSession, Number(activeTrade.tradeNum) || 0);
        return;
      }

      if (!activeTrade) return;

      if (runtimeEntered && runtimeSide === activeTrade.side) {
        if (activeTrade.status === 'enter_pending') {
          activeTrade.status = 'open';
          if (Number.isFinite(runtimeEntryPx) && runtimeEntryPx > 0) {
            activeTrade.entryPx = runtimeEntryPx;
            activeTrade.intentEntryPx = runtimeEntryPx;
            activeTrade.partial.limitPx = clamp01(runtimeEntryPx * (1 + cfg.partialTargetPctBet / 100));
          }
          if (Number.isFinite(runtimeShares) && runtimeShares > 0) {
            activeTrade.originalShares = runtimeShares;
            activeTrade.currentShares = runtimeShares;
            activeTrade.partial.shares = runtimeShares * cfg.partialQtyPct;
          }
          if (cfg.immediateDualTpOnFill && activeTrade.partial) {
            activeTrade.partial.stageTriggered = true;
            activeTrade.partial.armTriggered = true;
            activeTrade.partial.orderPlaced = true;
            activeTrade.partial.completed = false;
          }
        } else if (activeTrade.status !== 'open') {
          activeTrade.status = 'open';
        }

        if (Number.isFinite(runtimeShares) && runtimeShares > 0) {
          const prevShares = Number(activeTrade.currentShares);
          if (
            activeTrade.partial &&
            activeTrade.partial.orderPlaced &&
            !activeTrade.partial.filled &&
            Number.isFinite(prevShares) &&
            runtimeShares < (prevShares - 1e-6)
          ) {
            const soldShares = Math.max(0, prevShares - runtimeShares);
            if (soldShares > 0) {
              activeTrade.partial.filled = true;
              activeTrade.partial.completed = true;
              activeTrade.partial.orderPlaced = false;
              activeTrade.partial.fillDetectedTsMs = Number(nowMs);
              activeTrade.realizedGrossPnlUsd += soldShares * (Number(activeTrade.partial.limitPx) - Number(activeTrade.entryPx));
            }
          }
          activeTrade.currentShares = runtimeShares;
        }
        return;
      }

      if (!runtimeEntered) {
        if (activeTrade.status === 'enter_pending') {
          if ((Number(nowMs) - Number(activeTrade.intentTsMs || nowMs)) >= cfg.entryConfirmTimeoutMs) {
            entriesThisSession = Math.max(0, entriesThisSession - 1);
            activeTrade = null;
          }
          return;
        }

        if (activeTrade.exitPending) {
          const exitedSide = activeTrade.side;
          const exitPx = Number(activeTrade.exitPending.exitPx);
          const exitType = String(activeTrade.exitPending.type || '').toUpperCase();
          if (!(exitType.includes('STOP') && Number.isFinite(exitPx) && exitPx < cfg.crossReentryThr)) {
            rearmBlocked[exitedSide] = true;
          } else {
            rearmBlocked[exitedSide] = false;
          }
          activeTrade = null;
          return;
        }

        rearmBlocked[activeTrade.side] = true;
        activeTrade = null;
      }
    }

    function maybeSelectEntry(upBid, downBid) {
      if (activeTrade) return null;
      if (!(entriesThisSession < cfg.maxEntriesPerSession)) return null;
      const candidates = [];
      const upSignal = buildIterationSignal(resampled, cfg, 'UP');
      const dnSignal = buildIterationSignal(resampled, cfg, 'DOWN');
      if (
        !rearmBlocked.UP &&
        Number.isFinite(upBid) &&
        upBid >= cfg.entryBreakoutThr &&
        upSignal
      ) {
        candidates.push({
          side: 'UP',
          px: upBid,
          gain: Number(upSignal.px) - Number(upSignal.rollingLow),
          emaSpread: Number(upSignal.emaFast) - Number(upSignal.emaSlow),
          emaSlope: Number(upSignal.emaSlope),
          latestCheckpointMs: upSignal.latestCheckpointMs,
        });
      }
      if (
        !rearmBlocked.DOWN &&
        Number.isFinite(downBid) &&
        downBid >= cfg.entryBreakoutThr &&
        dnSignal
      ) {
        candidates.push({
          side: 'DOWN',
          px: downBid,
          gain: Number(dnSignal.px) - Number(dnSignal.rollingLow),
          emaSpread: Number(dnSignal.emaFast) - Number(dnSignal.emaSlow),
          emaSlope: Number(dnSignal.emaSlope),
          latestCheckpointMs: dnSignal.latestCheckpointMs,
        });
      }
      if (!candidates.length) return null;
      candidates.sort((a, b) => {
        if (Number(b.gain) !== Number(a.gain)) return Number(b.gain) - Number(a.gain);
        if (Number(b.emaSpread) !== Number(a.emaSpread)) return Number(b.emaSpread) - Number(a.emaSpread);
        if (Number(b.emaSlope) !== Number(a.emaSlope)) return Number(b.emaSlope) - Number(a.emaSlope);
        if (Number(b.px) !== Number(a.px)) return Number(b.px) - Number(a.px);
        return String(a.side).localeCompare(String(b.side));
      });
      return candidates[0];
    }

    return {
      onTick: function onTick(t) {
        const quoteMeta = t && t.quoteMeta && typeof t.quoteMeta === 'object'
          ? cloneSimple(t.quoteMeta)
          : null;
        const upRaw = Number(t && t.upBid);
        const downRaw = Number(t && t.downBid);
        const upBid = Number.isFinite(upRaw) ? upRaw : lastUpBid;
        const downBid = Number.isFinite(downRaw) ? downRaw : lastDownBid;
        if (!Number.isFinite(upBid) || !Number.isFinite(downBid)) return null;
        lastUpBid = upBid;
        lastDownBid = downBid;

        const elapsedSec = Number(t && t.elapsedSec);
        if (!Number.isFinite(elapsedSec)) return null;
        if (elapsedSec + 1 < lastElapsedSec) {
          tickIdx = -1;
          lastElapsedSec = 0;
          lastUpBid = NaN;
          lastDownBid = NaN;
          lastRawDiff = NaN;
          crossCooldownUntilMs = -Infinity;
          nextCheckpointMs = cfg.resampleMs;
          lastResampleSourceTsMs = NaN;
          observedSearchStartIdx = 0;
          observed.length = 0;
          resampled.length = 0;
          upKal.length = 0;
          dnKal.length = 0;
          upKState = null;
          dnKState = null;
          inflectStateBySide.UP = { peakVal: NaN, peakIdx: -1, negStreak: 0, lastEmitPeakIdx: -1 };
          inflectStateBySide.DOWN = { peakVal: NaN, peakIdx: -1, negStreak: 0, lastEmitPeakIdx: -1 };
          entriesThisSession = 0;
          rearmBlocked.UP = false;
          rearmBlocked.DOWN = false;
          activeTrade = null;
          pureObservedTicks = 0;
          nonPureSkippedTicks = 0;
          acceptedSyntheticTicks = 0;
          finalSyntheticTicks = 0;
          duplicateQuoteSeqSkippedTicks = 0;
          lastAcceptedQuoteSeq = null;
          lastQuoteMeta = null;
        }
        lastElapsedSec = elapsedSec;
        const nowMs = elapsedSec * 1000;
        const isPureQuote = !quoteMeta || quoteMeta.pure !== false;
        const allowSyntheticFinal = !!(t && t.isFinal && quoteMeta && quoteMeta.allowSyntheticFinal);
        const quoteSource = String(quoteMeta && quoteMeta.source || '').trim().toLowerCase();
        const quotePairAgeMs = Number(quoteMeta && quoteMeta.pairAgeMs);
        const syntheticExecutionMaxAgeMs = Math.max(25, Math.min(Number(cfg.acceptSyntheticQuoteMaxAgeMs), 100));
        const allowFreshSyntheticQuote =
          !isPureQuote &&
          (quoteSource === 'rest_pump' || quoteSource === 'rest_fallback') &&
          Number.isFinite(quotePairAgeMs) &&
          quotePairAgeMs >= 0 &&
          quotePairAgeMs <= syntheticExecutionMaxAgeMs;
        lastQuoteMeta = quoteMeta;

        if (!isPureQuote && !allowSyntheticFinal && !allowFreshSyntheticQuote) {
          nonPureSkippedTicks += 1;
          maybeSyncTradeFromRuntime(t, nowMs);
          return null;
        }
        if (allowFreshSyntheticQuote) {
          acceptedSyntheticTicks += 1;
        }
        if (!isPureQuote && allowSyntheticFinal) {
          finalSyntheticTicks += 1;
          maybeSyncTradeFromRuntime(t, nowMs);
          if (activeTrade && activeTrade.exitPending) {
            const spec = activeTrade.exitPending;
            return {
              exit: {
                type: String(spec.type || 'EXIT'),
                side: activeTrade.side,
                ...(Number.isFinite(Number(spec.exitPx)) ? { exitPx: Number(spec.exitPx) } : {}),
                ...(Number.isFinite(Number(spec.shares)) ? { shares: Number(spec.shares) } : {}),
                orderType: String(spec.orderType || 'MARKET'),
                ...(spec.tag ? { tag: spec.tag } : {}),
                ...(spec.stopReason ? { stopReason: spec.stopReason } : {}),
                ...(spec.feeMode ? { feeMode: String(spec.feeMode) } : {}),
              },
            };
          }
          if (activeTrade && activeTrade.status === 'open') {
            const markPx = sidePx(activeTrade.side, upBid, downBid);
            return queueFullExit('SETTLE', markPx, {
              orderType: 'MARKET',
              stopReason: 'SESSION_FINAL',
              feeMode: 'none',
            });
          }
          return null;
        }

        const quoteSeq = Number.isFinite(Number(quoteMeta && quoteMeta.quoteSeq))
          ? Number(quoteMeta.quoteSeq)
          : null;
        if (
          !allowSyntheticFinal &&
          quoteSeq != null &&
          lastAcceptedQuoteSeq != null &&
          Number(quoteSeq) === Number(lastAcceptedQuoteSeq)
        ) {
          duplicateQuoteSeqSkippedTicks += 1;
          maybeSyncTradeFromRuntime(t, nowMs);
          return null;
        }

        tickIdx += 1;
        pureObservedTicks += 1;
        if (quoteSeq != null) lastAcceptedQuoteSeq = Number(quoteSeq);
        observed.push({ tMs: nowMs, upBid: upBid, downBid: downBid });
        maybeFinalizeCheckpoints(nowMs);

        upKState = kalmanUpdate(upKState, upBid, cfg.kalmanQ, cfg.kalmanR);
        dnKState = kalmanUpdate(dnKState, downBid, cfg.kalmanQ, cfg.kalmanR);
        const upK = Number(upKState && upKState.x);
        const dnK = Number(dnKState && dnKState.x);
        upKal.push(Number.isFinite(upK) ? upK : NaN);
        dnKal.push(Number.isFinite(dnK) ? dnK : NaN);

        const curDiff = Number.isFinite(upBid) && Number.isFinite(downBid) ? (upBid - downBid) : NaN;
        if (crossed(lastRawDiff, curDiff)) {
          crossCooldownUntilMs = Math.max(Number(crossCooldownUntilMs || -Infinity), nowMs + (cfg.rawCrossCooldownSec * 1000));
        }
        lastRawDiff = curDiff;

        if (upBid < cfg.crossReentryThr) rearmBlocked.UP = false;
        if (downBid < cfg.crossReentryThr) rearmBlocked.DOWN = false;

        maybeSyncTradeFromRuntime(t, nowMs);

        if (activeTrade && activeTrade.exitPending) {
          const spec = activeTrade.exitPending;
          return {
            exit: {
              type: String(spec.type || 'EXIT'),
              side: activeTrade.side,
              ...(Number.isFinite(Number(spec.exitPx)) ? { exitPx: Number(spec.exitPx) } : {}),
              ...(Number.isFinite(Number(spec.shares)) ? { shares: Number(spec.shares) } : {}),
              orderType: String(spec.orderType || 'MARKET'),
              ...(spec.tag ? { tag: spec.tag } : {}),
              ...(spec.stopReason ? { stopReason: spec.stopReason } : {}),
              ...(spec.feeMode ? { feeMode: String(spec.feeMode) } : {}),
            },
          };
        }

        if (activeTrade && activeTrade.status === 'open') {
          const markPx = sidePx(activeTrade.side, upBid, downBid);
          const sideKal = activeTrade.side === 'DOWN' ? dnK : upK;
          const sideSeries = activeTrade.side === 'DOWN' ? dnKal : upKal;
          const grossPnlUsd = currentGrossPnlUsd(activeTrade, markPx);
          if (Number.isFinite(grossPnlUsd)) {
            activeTrade.peakGrossPnlUsd = Math.max(Number(activeTrade.peakGrossPnlUsd || 0), grossPnlUsd);
          }

          if (Number.isFinite(sideKal) && Number.isFinite(activeTrade.lastSideKal)) {
            const slope = sideKal - activeTrade.lastSideKal;
            if (Number.isFinite(slope) && slope <= cfg.inflectNegSlopeMax) activeTrade.negSlopeStreak += 1;
            else activeTrade.negSlopeStreak = 0;
          } else {
            activeTrade.negSlopeStreak = 0;
          }
          if (Number.isFinite(sideKal)) activeTrade.lastSideKal = sideKal;

          const currentStopPx = computeCurrentStopPx(activeTrade);
          if (Number.isFinite(markPx) && Number.isFinite(currentStopPx) && markPx <= currentStopPx) {
            activeTrade.stopBreachStreak = Number(activeTrade.stopBreachStreak || 0) + 1;
          } else {
            activeTrade.stopBreachStreak = 0;
          }
          const stopTriggered =
            Number.isFinite(markPx) &&
            Number.isFinite(currentStopPx) &&
            Number(activeTrade.stopBreachStreak || 0) >= cfg.stopConfirmTicks;
          const stopLabel = stopTriggered
            ? (currentStopPx >= 0.60 ? 'STOP_PROTECTED_060'
              : currentStopPx >= 0.55 ? 'STOP_PROTECTED_055'
                : currentStopPx >= 0.50 ? 'STOP_PROTECTED_050'
                  : 'STOP_LOSS_RAW')
            : null;

          const profitLockArmUsd = Number(activeTrade.betUsd) * cfg.profitLockMinPeakPctBet;
          const armReached =
            Number.isFinite(Number(activeTrade.peakGrossPnlUsd)) &&
            Number(activeTrade.peakGrossPnlUsd) >= profitLockArmUsd;

          if (
            armReached &&
            Number.isFinite(grossPnlUsd) &&
            grossPnlUsd > 0 &&
            activeTrade.negSlopeStreak >= cfg.exitNegSlopeConfirmTicks
          ) {
            const floorUsd = Number(activeTrade.peakGrossPnlUsd) * (1 - cfg.profitLockDrawdownPct);
            if (grossPnlUsd <= floorUsd) {
              return queueFullExit('PROFIT_LOCK', markPx, {
                orderType: 'MARKET',
                stopReason: 'PROFIT_LOCK_DRAWDOWN',
                feeMode: 'taker',
              });
            }
          }

          if (armReached) {
            const inflectEvt = detectLiveSafeInflectionAt(
              sideSeries,
              tickIdx,
              cfg,
              inflectStateBySide[activeTrade.side]
            );
            if (inflectEvt) {
              return queueFullExit('INFLECT_DOWN', markPx, {
                orderType: 'MARKET',
                stopReason: 'INFLECTION_ROLLOVER',
                feeMode: 'taker',
              });
            }
          }

          if (stopTriggered) {
            return queueFullExit(stopLabel, markPx, {
              orderType: 'MARKET',
              stopReason: 'RAW_STOP_LADDER',
              feeMode: 'taker',
            });
          }

          if (t && t.isFinal) {
            return queueFullExit('SETTLE', markPx, {
              orderType: 'MARKET',
              stopReason: 'SESSION_FINAL',
              feeMode: 'none',
            });
          }

          const partialStageUsd = Number(activeTrade.betUsd) * cfg.partialStagePctBet / 100;
          const partialArmUsd = Number(activeTrade.betUsd) * cfg.partialArmPctBet / 100;
          if (!activeTrade.partial.stageTriggered && Number(activeTrade.peakGrossPnlUsd) >= partialStageUsd) {
            activeTrade.partial.stageTriggered = true;
          }
          if (
            activeTrade.partial.stageTriggered &&
            !activeTrade.partial.armTriggered &&
            Number(activeTrade.peakGrossPnlUsd) >= partialArmUsd
          ) {
            activeTrade.partial.armTriggered = true;
          }
          if (
            activeTrade.partial.stageTriggered &&
            activeTrade.partial.armTriggered &&
            !activeTrade.partial.orderPlaced &&
            !activeTrade.partial.filled &&
            !activeTrade.partial.completed &&
            Number(activeTrade.currentShares) > 1e-9
          ) {
            const partialShares = Math.max(
              0,
              Math.min(Number(activeTrade.currentShares), Number(activeTrade.partial.shares))
            );
            const partialLimitPx = clamp01(Number(activeTrade.partial.limitPx));
            if (partialShares > 1e-9 && Number.isFinite(partialLimitPx) && partialLimitPx > 0) {
              activeTrade.partial.orderPlaced = true;
              activeTrade.partial.completed = false;
              return {
                exit: {
                  type: 'PARTIAL_TP_27',
                  side: activeTrade.side,
                  exitPx: partialLimitPx,
                  shares: partialShares,
                  orderType: 'LIMIT',
                  feeMode: 'none',
                  tag: 'PARTIAL_STAGE12_ARM22_TARGET27',
                },
              };
            }
          }

          return null;
        }

        if (!(lastElapsedSec >= cfg.minEntrySec)) return null;
        if (Number.isFinite(crossCooldownUntilMs) && nowMs < crossCooldownUntilMs) return null;

        const candidate = maybeSelectEntry(upBid, downBid);
        if (!candidate) return null;
        const sideKal = candidate.side === 'DOWN' ? dnK : upK;
        return armTradeEntry(candidate.side, candidate.px, sideKal, nowMs, { tag: candidate.tag });
      },

      snapshot: function snapshot() {
        const latestResampled = resampled.length ? resampled[resampled.length - 1] : null;
        const resampleSnapshotStartMs = latestResampled
          ? (Number(latestResampled.checkpointMs) - Number(snapshotResampleLookbackMs))
          : -Infinity;
        const observedSnapshotStartMs = (lastElapsedSec * 1000) - Number(snapshotObservedLookbackMs);
        const observedTail = observed.filter((row) => Number(row && row.tMs) >= observedSnapshotStartMs - 1e-9);
        const resampledTail = resampled.filter((row) => Number(row && row.checkpointMs) >= resampleSnapshotStartMs - 1e-9);
        const upKalTail = upKal.slice(-snapshotKalTail);
        const dnKalTail = dnKal.slice(-snapshotKalTail);
        const currentStopPx = activeTrade ? computeCurrentStopPx(activeTrade) : null;
        return {
          preset: cfg.name,
          strategyId: PRESET.strategyId,
          cfg: {
            minEntrySec: cfg.minEntrySec,
            entryBreakoutThr: cfg.entryBreakoutThr,
            sizingProfile: cfg.sizingProfile,
            crossReentryThr: cfg.crossReentryThr,
            maxEntriesPerSession: cfg.maxEntriesPerSession,
            bet: cfg.bet,
            stopLossPx: cfg.stopLossPx,
            stopConfirmTicks: cfg.stopConfirmTicks,
            profitProtectLadder: cloneSimple(cfg.profitProtectLadder),
            partialStagePctBet: cfg.partialStagePctBet,
            partialArmPctBet: cfg.partialArmPctBet,
            partialTargetPctBet: cfg.partialTargetPctBet,
            partialQtyPct: cfg.partialQtyPct,
            profitLockMinPeakPctBet: cfg.profitLockMinPeakPctBet,
            profitLockDrawdownPct: cfg.profitLockDrawdownPct,
            rawCrossCooldownSec: cfg.rawCrossCooldownSec,
            resampleMs: cfg.resampleMs,
            checkpointDelayMs: cfg.checkpointDelayMs,
            slopeLookbackMs: cfg.slopeLookbackMs,
            minSlopeSamples: cfg.minSlopeSamples,
            emaFastMs: cfg.emaFastMs,
            emaSlowMs: cfg.emaSlowMs,
            rollingLowLookbackMs: cfg.rollingLowLookbackMs,
            recentHighLookbackMs: cfg.recentHighLookbackMs,
            recoveryMinPx: cfg.recoveryMinPx,
            breakoutEpsilonPx: cfg.breakoutEpsilonPx,
            emaSlopeMinPx: cfg.emaSlopeMinPx,
            entryConfirmTimeoutMs: cfg.entryConfirmTimeoutMs,
          },
          state: {
            tickIdx,
            elapsedSec: lastElapsedSec,
            lastElapsedSec,
            lastUpBid,
            lastDownBid,
            lastRawDiff,
            crossCooldownUntilMs,
            nextCheckpointMs,
            lastResampleSourceTsMs,
            observedSearchStartIdx,
            entriesThisSession: entriesThisSession,
            pureObservedTicks,
            nonPureSkippedTicks,
            acceptedSyntheticTicks,
            finalSyntheticTicks,
            duplicateQuoteSeqSkippedTicks,
            lastAcceptedQuoteSeq,
            lastQuoteMeta: cloneSimple(lastQuoteMeta),
            rearmBlocked: { UP: !!rearmBlocked.UP, DOWN: !!rearmBlocked.DOWN },
            crossCooldownUntilSec: Number.isFinite(crossCooldownUntilMs) ? (crossCooldownUntilMs / 1000) : null,
            observedCount: observed.length,
            resampleCount: resampled.length,
            observed: cloneSimple(observedTail),
            resampled: cloneSimple(resampledTail),
            latestResampled,
            upKal: cloneSimple(upKalTail),
            downKal: cloneSimple(dnKalTail),
            upKState: cloneSimple(upKState),
            dnKState: cloneSimple(dnKState),
            inflectStateBySide: cloneSimple(inflectStateBySide),
            activeTrade: activeTrade
              ? {
                  tradeNum: activeTrade.tradeNum,
                  status: activeTrade.status,
                  side: activeTrade.side,
                  betUsd: activeTrade.betUsd,
                  entryPx: activeTrade.entryPx,
                  entrySec: activeTrade.entrySec,
                  currentShares: activeTrade.currentShares,
                  originalShares: activeTrade.originalShares,
                  realizedGrossPnlUsd: activeTrade.realizedGrossPnlUsd,
                  peakGrossPnlUsd: activeTrade.peakGrossPnlUsd,
                  negSlopeStreak: activeTrade.negSlopeStreak,
                  stopBreachStreak: activeTrade.stopBreachStreak,
                  currentStopPx,
                  partial: cloneSimple(activeTrade.partial),
                  exitPending: cloneSimple(activeTrade.exitPending),
                }
              : null,
            pendingExit: buildPendingExitSnapshot(),
          },
        };
      },
    };
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = TradeStrategy;
  }
  if (typeof window !== 'undefined') {
    window.TradeStrategy = TradeStrategy;
  }
})();
