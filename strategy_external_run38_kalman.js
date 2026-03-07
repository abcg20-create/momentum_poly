(function () {
  'use strict';

  const PRESET = Object.freeze({
    name: 'RUN38_KALMAN_INFLECT_PAPER',
    entryBreakoutThr: 0.55,
    entryMaxPx: 0.62,
    crossReentryThr: 0.55,
    crossReentryMaxPx: 0.62,
    postCrossConfirmTicks: 4,
    crossCooldownTicks: 2,
    crossConfirmTicks: 6,
    postEntryCrossGraceTicks: 6,
    postEntryCrossLossBypassUsd: NaN,
    postEntryCrossLossBypassPctBet: 0.10,
    crossMidPx: 0.50,
    crossEmergencyBelowCrossPct: 0.10,
    crossLeadMin: 0.03,
    kalmanQ: 0.00005,
    kalmanR: 0.0008,
    inflectEps: 0.0035,
    inflectMinTurn: 0.0055,
    inflectMinSlopeMag: 0.004,
    inflectMinGapMs: 9000,
    inflectConfirmTicks: 3,
    inflectFastConfirmTicks: 1,
    inflectFastDropMin: 0.01,
    inflectFastSlopeMin: 0.008,
    hardTpKalman: 0.97,
    profitLockMinPeakPctBet: 0.45,
    profitLockDrawdownPct: 0.20,
    exitNegSlopeConfirmTicks: 5,
    postCrossProtectTicks: 12,
    postCrossExitNegSlopeTicks: 5,
    earlyFadeNegSlopeTicks: 10,
    earlyFadeMinSlope: -0.004,
    flipRateMax: 0.10,
    flipRateStartTicks: 15,
    flipRateWindowTicks: 25,
    maxEntriesPerSession: 4,
    bet: 25,
  });

  function toNum(v, d) {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
  }

  function clamp01(v) {
    if (!Number.isFinite(Number(v))) return NaN;
    return Math.max(0, Math.min(1, Number(v)));
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

  function crossed(prevDiff, currDiff) {
    if (!Number.isFinite(prevDiff) || !Number.isFinite(currDiff)) return false;
    if (prevDiff === 0 || currDiff === 0) return prevDiff !== currDiff;
    return (prevDiff > 0 && currDiff < 0) || (prevDiff < 0 && currDiff > 0);
  }

  function detectTurnDownAt(series, i, cfg, lastTurnDownMs, nowMs) {
    if (!Array.isArray(series) || i < 2) return false;
    const k0 = Number(series[i - 2]);
    const k1 = Number(series[i - 1]);
    const k2 = Number(series[i]);
    if (!Number.isFinite(k0) || !Number.isFinite(k1) || !Number.isFinite(k2)) return false;
    const sPrev = k1 - k0;
    const sNow = k2 - k1;
    const turn = sNow - sPrev;
    const hit =
      sPrev > cfg.inflectEps &&
      sNow <= -cfg.inflectEps &&
      Math.abs(turn) >= cfg.inflectMinTurn &&
      Math.max(Math.abs(sPrev), Math.abs(sNow)) >= cfg.inflectMinSlopeMag;
    if (!hit) return false;
    return Number(nowMs) - Number(lastTurnDownMs || -Infinity) >= cfg.inflectMinGapMs;
  }

  function slopeFlipRateAt(series, i, cfg) {
    if (!Array.isArray(series) || i < 2) return NaN;
    const win = Math.max(3, Math.floor(Number(cfg.flipRateWindowTicks) || 25));
    const start = Math.max(1, i - win + 1);
    let prev = 0;
    let transitions = 0;
    let flips = 0;
    for (let k = start; k <= i; k++) {
      const a = Number(series[k - 1]);
      const b = Number(series[k]);
      if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
      const s = b - a;
      const sign = s > cfg.inflectEps ? 1 : s < -cfg.inflectEps ? -1 : 0;
      if (sign === 0) continue;
      if (prev !== 0) {
        transitions += 1;
        if (sign !== prev) flips += 1;
      }
      prev = sign;
    }
    if (transitions <= 0) return 0;
    return flips / transitions;
  }

  const TradeStrategy = {};
  function oppositeSide(s) {
    return s === 'UP' ? 'DOWN' : 'UP';
  }

  TradeStrategy.VERSION = PRESET.name;

  TradeStrategy.makeStrategy = function makeStrategy(params) {
    const cfg = {
      ...PRESET,
      bet: toNum(params && params.bet, PRESET.bet),
      entryBreakoutThr: toNum(params && params.entryBreakoutThr, PRESET.entryBreakoutThr),
      entryMaxPx: clamp01(toNum(params && params.entryMaxPx, PRESET.entryMaxPx)),
      crossReentryThr: toNum(params && params.crossReentryThr, PRESET.crossReentryThr),
      crossReentryMaxPx: clamp01(toNum(params && params.crossReentryMaxPx, PRESET.crossReentryMaxPx)),
      postCrossConfirmTicks: Math.max(1, Math.floor(toNum(params && params.postCrossConfirmTicks, PRESET.postCrossConfirmTicks))),
      crossCooldownTicks: Math.max(0, Math.floor(toNum(params && params.crossCooldownTicks, PRESET.crossCooldownTicks))),
      crossConfirmTicks: Math.max(1, Math.floor(toNum(params && params.crossConfirmTicks, PRESET.crossConfirmTicks))),
      postEntryCrossGraceTicks: Math.max(0, Math.floor(toNum(params && params.postEntryCrossGraceTicks, PRESET.postEntryCrossGraceTicks))),
      postEntryCrossLossBypassUsd: toNum(params && params.postEntryCrossLossBypassUsd, PRESET.postEntryCrossLossBypassUsd),
      postEntryCrossLossBypassPctBet: Math.max(0, toNum(params && params.postEntryCrossLossBypassPctBet, PRESET.postEntryCrossLossBypassPctBet)),
      crossMidPx: clamp01(toNum(params && params.crossMidPx, PRESET.crossMidPx)),
      crossEmergencyBelowCrossPct: Math.max(0, toNum(params && params.crossEmergencyBelowCrossPct, PRESET.crossEmergencyBelowCrossPct)),
      crossLeadMin: Math.max(0, toNum(params && params.crossLeadMin, PRESET.crossLeadMin)),
      flipRateMax: Math.max(0, toNum(params && params.flipRateMax, PRESET.flipRateMax)),
      flipRateStartTicks: Math.max(0, Math.floor(toNum(params && params.flipRateStartTicks, PRESET.flipRateStartTicks))),
      flipRateWindowTicks: Math.max(3, Math.floor(toNum(params && params.flipRateWindowTicks, PRESET.flipRateWindowTicks))),
      maxEntriesPerSession: Math.max(1, Math.floor(toNum(params && params.maxEntriesPerSession, PRESET.maxEntriesPerSession))),
      // Optional absolute override; otherwise use % of bet.
      profitLockMinPeakUsd: toNum(params && params.profitLockMinPeakUsd, NaN),
      profitLockMinPeakPctBet: Math.max(0, toNum(params && params.profitLockMinPeakPctBet, PRESET.profitLockMinPeakPctBet)),
    };

    let tickIdx = -1;
    let lastMs = 0;
    let lastUpBid = NaN;
    let lastDownBid = NaN;

    let upKState = null;
    let dnKState = null;
    const upKal = [];
    const dnKal = [];

    let seededEntryDone = false;
    let entriesThisSession = 0;
    let consecutiveLosingExits = 0;
    let awaitProfitReset = false;
    let requireRawResetAfterProfitExit = false;
    let waitingSide = null;
    let waitingSideStreak = 0;
    let cooldownTicksLeft = 0;
    let ticksSinceLastCross = 1e9;

    let inPos = false;
    let side = null;
    let entryPx = NaN;
    let shares = NaN;
    let lastSideKal = NaN;
    let ticksInPos = 0;
    let crossExitStreak = 0;
    let peakUnrealizedUsd = 0;
    let negSlopeStreak = 0;

    let pendingInflect = null;
    const lastTurnDownMsBySide = { UP: -Infinity, DOWN: -Infinity };
    let postLossReentryArmed = false;
    let postLossReentrySide = null;
    let postLossReentrySeenBelow = false;

    function currentEntryCap() {
      // Safety cap: if two exits in a row are losses, limit this session to 3 entries.
      if (consecutiveLosingExits >= 2) return Math.min(cfg.maxEntriesPerSession, 3);
      return cfg.maxEntriesPerSession;
    }

    function enter(nextSide, px, sideKal) {
      if (entriesThisSession >= currentEntryCap()) return null;
      const entry = clamp01(px);
      if (!Number.isFinite(entry) || entry <= 0) return null;
      const sh = cfg.bet / entry;
      if (!Number.isFinite(sh) || sh <= 0) return null;
      entriesThisSession += 1;
      inPos = true;
      side = nextSide;
      entryPx = entry;
      shares = sh;
      lastSideKal = Number.isFinite(Number(sideKal)) ? Number(sideKal) : NaN;
      ticksInPos = 0;
      crossExitStreak = 0;
      peakUnrealizedUsd = 0;
      negSlopeStreak = 0;
      pendingInflect = null;
      waitingSide = null;
      waitingSideStreak = 0;
      return { enter: { side: nextSide, entryPx: entry } };
    }

    function exit(type, px) {
      const exitKind = String(type || '').toUpperCase();
      const isCrossExit = exitKind === 'CROSS';
      const exitingSide = side;
      const exitPx = clamp01(px);
      const realizedPnl =
        Number.isFinite(exitPx) &&
        Number.isFinite(entryPx) &&
        Number.isFinite(shares)
          ? shares * (exitPx - entryPx)
          : NaN;
      const wasProfitExit = Number.isFinite(realizedPnl) && realizedPnl > 0;
      if (Number.isFinite(realizedPnl) && realizedPnl < 0) consecutiveLosingExits += 1;
      else if (Number.isFinite(realizedPnl)) consecutiveLosingExits = 0;
      inPos = false;
      side = null;
      entryPx = NaN;
      shares = NaN;
      lastSideKal = NaN;
      ticksInPos = 0;
      crossExitStreak = 0;
      peakUnrealizedUsd = 0;
      negSlopeStreak = 0;
      pendingInflect = null;
      cooldownTicksLeft = Math.max(cooldownTicksLeft, cfg.crossCooldownTicks);
      // Keep queued follow-on side only for CROSS exits.
      if (!isCrossExit) waitingSide = null;
      waitingSideStreak = 0;
      if (!isCrossExit) {
        // Reset gating is required only after profitable non-cross exits.
        // For loss exits, allow immediate re-qualification so a valid
        // post-cross continuation can trigger trade-2 without waiting for a
        // full dip-reset cycle.
        awaitProfitReset = !!wasProfitExit;
        // After a profitable trade, reset must be based on live market price
        // (not just Kalman state) dipping below entry trigger.
        requireRawResetAfterProfitExit = !!wasProfitExit;
      }
      // After any losing exit (including CROSS), explicitly arm trade-2 on
      // the opposite side. This avoids misses when cross-queue logic expects
      // a fresh kalman re-break event that may not occur.
      if (!wasProfitExit && (exitingSide === 'UP' || exitingSide === 'DOWN')) {
        postLossReentryArmed = true;
        postLossReentrySide = oppositeSide(exitingSide);
        postLossReentrySeenBelow = false;
      } else {
        postLossReentryArmed = false;
        postLossReentrySide = null;
        postLossReentrySeenBelow = false;
      }
      return { exit: { type: type, side: exitingSide, exitPx: Number.isFinite(exitPx) ? exitPx : undefined } };
    }

    function queueSideAfterCross(upK, dnK) {
      if (Number.isFinite(upK) && Number.isFinite(dnK)) {
        waitingSide = upK >= dnK ? 'UP' : 'DOWN';
        waitingSideStreak = 0;
      } else {
        waitingSide = null;
        waitingSideStreak = 0;
      }
    }

    return {
      onTick: function onTick(t) {
        const upRaw = Number(t && t.upBid);
        const downRaw = Number(t && t.downBid);
        const upBid = Number.isFinite(upRaw) ? upRaw : lastUpBid;
        const downBid = Number.isFinite(downRaw) ? downRaw : lastDownBid;
        if (!Number.isFinite(upBid) || !Number.isFinite(downBid)) return null;
        lastUpBid = upBid;
        lastDownBid = downBid;

        tickIdx += 1;
        lastMs += 1000;
        if (cooldownTicksLeft > 0) cooldownTicksLeft -= 1;
        ticksSinceLastCross += 1;

        upKState = kalmanUpdate(upKState, upBid, cfg.kalmanQ, cfg.kalmanR);
        dnKState = kalmanUpdate(dnKState, downBid, cfg.kalmanQ, cfg.kalmanR);

        const upK = Number(upKState && upKState.x);
        const dnK = Number(dnKState && dnKState.x);
        upKal.push(Number.isFinite(upK) ? upK : NaN);
        dnKal.push(Number.isFinite(dnK) ? dnK : NaN);

        const upKPrev = tickIdx > 0 ? Number(upKal[tickIdx - 1]) : NaN;
        const dnKPrev = tickIdx > 0 ? Number(dnKal[tickIdx - 1]) : NaN;

        const prevDiff = Number.isFinite(upKPrev) && Number.isFinite(dnKPrev) ? upKPrev - dnKPrev : NaN;
        const curDiff = Number.isFinite(upK) && Number.isFinite(dnK) ? upK - dnK : NaN;
        const hasCross = crossed(prevDiff, curDiff);
        if (hasCross) ticksSinceLastCross = 0;
        const minExitNegTicks = ticksSinceLastCross <= cfg.postCrossProtectTicks
          ? cfg.postCrossExitNegSlopeTicks
          : cfg.exitNegSlopeConfirmTicks;

        if (inPos && side) {
          ticksInPos += 1;
          const sideKal = side === 'UP' ? upK : dnK;
          const sideRaw = side === 'UP' ? upBid : downBid;
          const markPx = Number.isFinite(sideRaw) ? sideRaw : sideKal;
          const unrealized = Number.isFinite(markPx) && Number.isFinite(entryPx) && Number.isFinite(shares)
            ? shares * (markPx - entryPx)
            : NaN;

          if (Number.isFinite(unrealized)) {
            peakUnrealizedUsd = Math.max(peakUnrealizedUsd, unrealized);
          }

          const sideSlope = Number.isFinite(sideKal) && Number.isFinite(lastSideKal) ? (sideKal - lastSideKal) : NaN;
          if (Number.isFinite(sideSlope) && sideSlope <= cfg.earlyFadeMinSlope) negSlopeStreak += 1;
          else negSlopeStreak = 0;
          if (Number.isFinite(sideKal)) lastSideKal = sideKal;

          if (Number.isFinite(sideKal) && sideKal >= cfg.hardTpKalman) {
            return exit('TP_KALMAN', sideRaw);
          }

          const minPeakForProfitLockUsd = Number.isFinite(cfg.profitLockMinPeakUsd) && cfg.profitLockMinPeakUsd > 0
            ? cfg.profitLockMinPeakUsd
            : (cfg.bet * cfg.profitLockMinPeakPctBet);

          if (
            peakUnrealizedUsd >= minPeakForProfitLockUsd &&
            Number.isFinite(unrealized) &&
            negSlopeStreak >= minExitNegTicks
          ) {
            const floor = peakUnrealizedUsd * (1 - cfg.profitLockDrawdownPct);
            if (unrealized <= floor && unrealized > 0) {
              return exit('PROFIT_LOCK', sideRaw);
            }
          }

          if (Number.isFinite(unrealized) && unrealized > 0 && negSlopeStreak >= cfg.earlyFadeNegSlopeTicks) {
            return exit('EARLY_FADE', sideRaw);
          }

          const sideSeries = side === 'UP' ? upKal : dnKal;
          const turnDown = detectTurnDownAt(sideSeries, tickIdx, cfg, lastTurnDownMsBySide[side], lastMs);

          if (turnDown && !pendingInflect) {
            pendingInflect = { side: side, inflectIdx: tickIdx };
            lastTurnDownMsBySide[side] = lastMs;
          }

          if (pendingInflect && pendingInflect.side === side) {
            const s = pendingInflect.inflectIdx;
            if (tickIdx >= s + cfg.inflectFastConfirmTicks) {
              const kS = Number(sideSeries[s]);
              const kN = Number(sideSeries[s + cfg.inflectFastConfirmTicks]);
              const drop = Number.isFinite(kS) && Number.isFinite(kN) ? (kS - kN) : NaN;
              const slope = drop / cfg.inflectFastConfirmTicks;
              const fastConfirmed =
                Number.isFinite(drop) &&
                drop >= cfg.inflectFastDropMin &&
                Number.isFinite(slope) &&
                slope >= cfg.inflectFastSlopeMin &&
                kN < kS;
              if (fastConfirmed && negSlopeStreak >= minExitNegTicks) {
                return exit('INFLECT_DOWN_FAST', sideRaw);
              }
            }

            if (tickIdx >= s + cfg.inflectConfirmTicks) {
              const k0 = Number(sideSeries[s]);
              const k1 = Number(sideSeries[s + 1]);
              const k2 = Number(sideSeries[s + 2]);
              const k3 = Number(sideSeries[s + 3]);
              const confirmed = Number.isFinite(k0) && Number.isFinite(k1) && Number.isFinite(k2) && Number.isFinite(k3)
                && k1 < k0 && k2 < k1 && k3 < k2;
              if (confirmed && negSlopeStreak >= minExitNegTicks) {
                return exit('INFLECT_DOWN', sideRaw);
              }
              pendingInflect = null;
            }
          }

          if (Number.isFinite(curDiff)) {
            const crossAgainstSide = side === 'UP' ? (curDiff < 0) : (curDiff > 0);
            crossExitStreak = crossAgainstSide ? (crossExitStreak + 1) : 0;
          } else {
            crossExitStreak = 0;
          }

          const crossConfirmed = crossExitStreak >= cfg.crossConfirmTicks;
          const crossWindowActive = ticksSinceLastCross <= Math.max(1, cfg.crossConfirmTicks + 1);
          const emergencyCrossPx =
            Number.isFinite(cfg.crossMidPx) ? (cfg.crossMidPx * (1 - cfg.crossEmergencyBelowCrossPct)) : NaN;
          const emergencyCrossBypass =
            crossExitStreak >= 1 &&
            Number.isFinite(sideRaw) &&
            Number.isFinite(emergencyCrossPx) &&
            sideRaw <= emergencyCrossPx;

          // Default: require sustained cross confirmation.
          // Emergency bypass: if held-side price falls >=10% below cross midpoint, exit immediately.
          if ((crossWindowActive && crossConfirmed) || emergencyCrossBypass) {
            queueSideAfterCross(upK, dnK);
            return exit('CROSS', sideRaw);
          }
        }

        if (hasCross && !awaitProfitReset) {
          queueSideAfterCross(upK, dnK);
        }

        if (!inPos) {
          if (awaitProfitReset) {
            const leadRaw = Number.isFinite(upBid) && Number.isFinite(downBid) ? Math.max(upBid, downBid) : NaN;
            const leadKal = Number.isFinite(upK) && Number.isFinite(dnK) ? Math.max(upK, dnK) : NaN;
            const resetBasis = requireRawResetAfterProfitExit ? leadRaw : leadKal;
            if (Number.isFinite(resetBasis) && resetBasis < cfg.entryBreakoutThr) {
              awaitProfitReset = false;
              requireRawResetAfterProfitExit = false;
              seededEntryDone = false;
              waitingSide = null;
              waitingSideStreak = 0;
            }
            return null;
          }
          if (postLossReentryArmed && (postLossReentrySide === 'UP' || postLossReentrySide === 'DOWN') && cooldownTicksLeft <= 0) {
            const nextSide = postLossReentrySide;
            const sideKal = nextSide === 'UP' ? upK : dnK;
            const sideKalPrev = nextSide === 'UP' ? upKPrev : dnKPrev;
            const sideRaw = nextSide === 'UP' ? upBid : downBid;
            if (
              !postLossReentrySeenBelow &&
              (
                (Number.isFinite(sideRaw) && sideRaw < cfg.crossReentryThr) ||
                (Number.isFinite(sideKal) && sideKal < cfg.crossReentryThr)
              )
            ) {
              postLossReentrySeenBelow = true;
            }
            const sideAboveThr = Number.isFinite(sideKal) && sideKal >= cfg.crossReentryThr;
            const sideCrossedFromBelow =
              sideAboveThr &&
              (
                (Number.isFinite(sideKalPrev) && sideKalPrev < cfg.crossReentryThr) ||
                postLossReentrySeenBelow
              );
            const sidePriceOk = !Number.isFinite(cfg.crossReentryMaxPx) || (Number.isFinite(sideRaw) && sideRaw <= cfg.crossReentryMaxPx);
            if (sideCrossedFromBelow && sidePriceOk) {
              const px = nextSide === 'UP' ? upBid : downBid;
              const action = enter(nextSide, px, sideKal);
              if (action) {
                postLossReentryArmed = false;
                postLossReentrySide = null;
                postLossReentrySeenBelow = false;
                return action;
              }
            }
          }
          if (!seededEntryDone) {
            const flipGateActive = tickIdx >= cfg.flipRateStartTicks;
            const upFlipRate = slopeFlipRateAt(upKal, tickIdx, cfg);
            const dnFlipRate = slopeFlipRateAt(dnKal, tickIdx, cfg);
            const upFlipOk = !flipGateActive || !Number.isFinite(upFlipRate) || upFlipRate <= cfg.flipRateMax;
            const dnFlipOk = !flipGateActive || !Number.isFinite(dnFlipRate) || dnFlipRate <= cfg.flipRateMax;
            const upBreak = Number.isFinite(upK) && upK >= cfg.entryBreakoutThr &&
              ((Number.isFinite(upKPrev) && upKPrev < cfg.entryBreakoutThr) || !Number.isFinite(upKPrev)) &&
              upFlipOk &&
              (!Number.isFinite(cfg.entryMaxPx) || upBid <= cfg.entryMaxPx);
            const dnBreak = Number.isFinite(dnK) && dnK >= cfg.entryBreakoutThr &&
              ((Number.isFinite(dnKPrev) && dnKPrev < cfg.entryBreakoutThr) || !Number.isFinite(dnKPrev)) &&
              dnFlipOk &&
              (!Number.isFinite(cfg.entryMaxPx) || downBid <= cfg.entryMaxPx);

            if (upBreak || dnBreak) {
              const nextSide = upBreak && dnBreak ? (upK >= dnK ? 'UP' : 'DOWN') : (upBreak ? 'UP' : 'DOWN');
              const px = nextSide === 'UP' ? upBid : downBid;
              const sideKal = nextSide === 'UP' ? upK : dnK;
              const action = enter(nextSide, px, sideKal);
              if (action) {
                seededEntryDone = true;
                postLossReentryArmed = false;
                postLossReentrySide = null;
                postLossReentrySeenBelow = false;
                return action;
              }
            }
          } else if (waitingSide && cooldownTicksLeft <= 0) {
            const sideKal = waitingSide === 'UP' ? upK : dnK;
            const sideKalPrev = waitingSide === 'UP' ? upKPrev : dnKPrev;
            const sideRaw = waitingSide === 'UP' ? upBid : downBid;
            const leadMag = Number.isFinite(upK) && Number.isFinite(dnK) ? Math.abs(upK - dnK) : NaN;
            const sideSeries = waitingSide === 'UP' ? upKal : dnKal;
            const flipGateActive = tickIdx >= cfg.flipRateStartTicks;
            const sideFlipRate = slopeFlipRateAt(sideSeries, tickIdx, cfg);
            const sideFlipOk = !flipGateActive || !Number.isFinite(sideFlipRate) || sideFlipRate <= cfg.flipRateMax;
            const sideCrossedFromBelow =
              Number.isFinite(sideKal) &&
              sideKal >= cfg.crossReentryThr &&
              (Number.isFinite(sideKalPrev) ? sideKalPrev < cfg.crossReentryThr : true);
            const sidePriceOk = !Number.isFinite(cfg.crossReentryMaxPx) || (Number.isFinite(sideRaw) && sideRaw <= cfg.crossReentryMaxPx);
            if (sideCrossedFromBelow && sidePriceOk && Number.isFinite(leadMag) && leadMag >= cfg.crossLeadMin && sideFlipOk) {
              waitingSideStreak += 1;
              if (waitingSideStreak >= cfg.postCrossConfirmTicks) {
                const px = waitingSide === 'UP' ? upBid : downBid;
                const action = enter(waitingSide, px, sideKal);
                if (action) {
                  postLossReentryArmed = false;
                  postLossReentrySide = null;
                  postLossReentrySeenBelow = false;
                  return action;
                }
              }
            } else {
              waitingSideStreak = 0;
            }
          } else if (cooldownTicksLeft <= 0) {
            // After non-cross exits (e.g., stop/fade/inflect), there may be no
            // queued cross side. Allow first valid side to re-break from below.
            const upSeries = upKal;
            const dnSeries = dnKal;
            const upFlipRate = slopeFlipRateAt(upSeries, tickIdx, cfg);
            const dnFlipRate = slopeFlipRateAt(dnSeries, tickIdx, cfg);
            const flipGateActive = tickIdx >= cfg.flipRateStartTicks;
            const upFlipOk = !flipGateActive || !Number.isFinite(upFlipRate) || upFlipRate <= cfg.flipRateMax;
            const dnFlipOk = !flipGateActive || !Number.isFinite(dnFlipRate) || dnFlipRate <= cfg.flipRateMax;
            const upCrossedFromBelow =
              Number.isFinite(upK) &&
              upK >= cfg.crossReentryThr &&
              (Number.isFinite(upKPrev) ? upKPrev < cfg.crossReentryThr : true);
            const dnCrossedFromBelow =
              Number.isFinite(dnK) &&
              dnK >= cfg.crossReentryThr &&
              (Number.isFinite(dnKPrev) ? dnKPrev < cfg.crossReentryThr : true);
            const upPriceOk =
              !Number.isFinite(cfg.crossReentryMaxPx) || (Number.isFinite(upBid) && upBid <= cfg.crossReentryMaxPx);
            const dnPriceOk =
              !Number.isFinite(cfg.crossReentryMaxPx) || (Number.isFinite(downBid) && downBid <= cfg.crossReentryMaxPx);
            const upBreak = upCrossedFromBelow && upPriceOk && upFlipOk;
            const dnBreak = dnCrossedFromBelow && dnPriceOk && dnFlipOk;
            if (upBreak || dnBreak) {
              const nextSide = upBreak && dnBreak ? (upK >= dnK ? 'UP' : 'DOWN') : (upBreak ? 'UP' : 'DOWN');
              const px = nextSide === 'UP' ? upBid : downBid;
              const sideKal = nextSide === 'UP' ? upK : dnK;
              const action = enter(nextSide, px, sideKal);
              if (action) {
                postLossReentryArmed = false;
                postLossReentrySide = null;
                postLossReentrySeenBelow = false;
                return action;
              }
            }
          }
        }

        if (t && t.isFinal && inPos && side) {
          const px = side === 'UP' ? upBid : downBid;
          return exit('SETTLE', px);
        }

        return null;
      },

      snapshot: function snapshot() {
        return {
          preset: cfg.name,
          inPos: inPos,
          side: side,
          seededEntryDone: seededEntryDone,
          entriesThisSession: entriesThisSession,
          consecutiveLosingExits: consecutiveLosingExits,
          maxEntriesPerSession: cfg.maxEntriesPerSession,
          effectiveEntryCap: currentEntryCap(),
          awaitProfitReset: awaitProfitReset,
          requireRawResetAfterProfitExit: requireRawResetAfterProfitExit,
          ticksInPos: ticksInPos,
          crossExitStreak: crossExitStreak,
          waitingSide: waitingSide,
          waitingSideStreak: waitingSideStreak,
          cooldownTicksLeft: cooldownTicksLeft,
          pendingInflect: pendingInflect,
          peakUnrealizedUsd: peakUnrealizedUsd,
          negSlopeStreak: negSlopeStreak,
          upKal: upKal.length ? upKal[upKal.length - 1] : null,
          downKal: dnKal.length ? dnKal[dnKal.length - 1] : null,
          upFlipRate: upKal.length > 2 ? slopeFlipRateAt(upKal, upKal.length - 1, cfg) : null,
          downFlipRate: dnKal.length > 2 ? slopeFlipRateAt(dnKal, dnKal.length - 1, cfg) : null,
          flipRateMax: cfg.flipRateMax,
          profitLockMinPeakUsd: (Number.isFinite(cfg.profitLockMinPeakUsd) && cfg.profitLockMinPeakUsd > 0)
            ? cfg.profitLockMinPeakUsd
            : (cfg.bet * cfg.profitLockMinPeakPctBet),
          profitLockMinPeakPctBet: cfg.profitLockMinPeakPctBet,
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
