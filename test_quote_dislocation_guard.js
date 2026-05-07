#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");

const files = ["main_bot_server_strategy_isolated.ts"];

for (const file of files) {
  const source = fs.readFileSync(path.join(__dirname, file), "utf8");

  assert(
    source.includes("const QUOTE_DISLOCATION_EDGE_MIN_PX = Math.max(") &&
      source.includes("const QUOTE_DISLOCATION_MIN_MOVE_PX = Math.max(") &&
      source.includes("const QUOTE_DISLOCATION_CONFIRM_MIN_COUNT = Math.max(") &&
      source.includes("const QUOTE_DISLOCATION_CONFIRM_MIN_MS = Math.max("),
    `${file}: quote dislocation guard constants should exist`
  );

  assert(
    source.includes("type SessionQuoteDislocationCandidate = {") &&
      source.includes("let currentSessionQuoteDislocationCandidate: SessionQuoteDislocationCandidate | null = null;"),
    `${file}: quote dislocation candidate state should exist`
  );

  assert(
    /function shouldQuarantineQuoteDislocation\([\s\S]*?isNearEdgeDominantQuotePair\(prevUp, prevDown\)[\s\S]*?prevSide !== nextSide[\s\S]*?winnerDrop < QUOTE_DISLOCATION_MIN_MOVE_PX[\s\S]*?QUOTE_DISLOCATION_CONFIRM_MIN_COUNT[\s\S]*?QUOTE_DISLOCATION_CONFIRM_MIN_MS[\s\S]*?return true;/.test(source),
    `${file}: dislocation guard should quarantine abrupt winner/loser moves until corroborated`
  );

  assert(
    /let quotePairTrusted = isQuotePairTrustedForCurrentSession\(pair, upBid, dnBid, sessionAgeMs\);[\s\S]*?const quotePairDislocationQuarantined =[\s\S]*?shouldQuarantineQuoteDislocation\(pair, upBid, dnBid, nowTickMs\);[\s\S]*?if \(quotePairDislocationQuarantined\) quotePairTrusted = false;[\s\S]*?const pairUsable = \(pairFresh \|\| pairUsableFromHealthyStream\) && quotePairTrusted;/.test(source),
    `${file}: main loop should route dislocated quote pairs through the hold-last-good path`
  );

  assert(
    source.includes('quote_dislocation_quarantine') &&
      source.includes("currentSessionQuoteDislocationCandidate = null;"),
    `${file}: dislocation quarantine should be observable and reset on session changes`
  );
}

console.log("quote dislocation guards verified");
