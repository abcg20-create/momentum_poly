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
	    source.includes("const QUOTE_DISLOCATION_CONFIRM_MIN_MS = Math.max(") &&
	    source.includes("const QUOTE_DISLOCATION_WS_CONFIRM_MIN_COUNT = Math.max(") &&
	    source.includes("const QUOTE_DISLOCATION_WS_CONFIRM_MIN_MS = Math.max("),
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
	  /function commitQuotePairCache\([\s\S]*?shouldQuarantineCanonicalQuotePair\(row, reason\)[\s\S]*?quotePairCache = row;/.test(source) &&
	    source.includes('commitQuotePairCache(nextPair, "token_rows")') &&
	    source.includes('commitQuotePairCache(nextPair, "rest_pump")') &&
	    source.includes('commitQuotePairCache(nextPair, "rest_fallback")'),
	  `${file}: canonical quote pair cache should reject dislocated raw samples before hot-path triggers`
	);

	const canonicalGuard = source.match(/function shouldQuarantineCanonicalQuotePair\([\s\S]*?\n}\n\nfunction commitQuotePairCache/)?.[0] || "";
	assert(
	  canonicalGuard.includes("shouldQuarantineQuoteDislocation(rowLike, rowLike.upBid, rowLike.downBid, nowMs())") &&
	    !canonicalGuard.includes("isQuotePairTrustedForCurrentSession"),
	  `${file}: canonical quote cache should only quarantine dislocations, not ordinary trust warmup samples`
	);

	assert(
	  /function buildQuoteBroadcastPayload\(\)[\s\S]*?rawPairTrusted[\s\S]*?shouldQuarantineQuoteDislocation\(pairAny, pairAny\.upBid, pairAny\.downBid, now\)[\s\S]*?source: "last_good_hold"/.test(source),
	  `${file}: /lite quote broadcasts should use held-good quotes during dislocation quarantine`
	);

	assert(
	  /const runtimeQuoteDislocationQuarantined =[\s\S]*?shouldQuarantineQuoteDislocation\(runtimePairMeta, upBA\?\.bid, downBA\?\.bid, tickTsMs\);[\s\S]*?if \(!runtimeQuoteTrusted \|\| runtimeQuoteDislocationQuarantined\)[\s\S]*?lastGoodObservedBids\.upBid/.test(source),
	  `${file}: bot runtime hot path should share the same dislocation quarantine`
	);

  assert(
    source.includes('quote_dislocation_quarantine') &&
      source.includes("currentSessionQuoteDislocationCandidate = null;"),
    `${file}: dislocation quarantine should be observable and reset on session changes`
  );
}

console.log("quote dislocation guards verified");
