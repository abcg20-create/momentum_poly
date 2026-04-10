# Gold Bets Performance

## Gold Bets v1

Strategy:
- `Trade 1`: `180-210s`, `210-240s`, `240-270s`, `270-297s`
- `Trade 2`: `210-240s`, `240-270s`, `270-297s`
- `Trade 3`: `210-240s`, `240-270s`, `270-297s`

Source basis:
- Run `5`
- Strategy: `inflection_positive_iteration`
- Review window: run start through `2026-04-06 9:00 PM` Los Angeles time

Notes:
- This was the first gold-bet profile written into the strategy as `gold_bets_v1`.
- Explicit strategy file: `strategy_external_inflection_positive_iteration_goldbetsv1.js`
- It came from the strongest late-session trade/interval cells in the Run Audit.
- It is more selective and more interval-specific than `v2`.

## Gold Bets v2

Strategy:
- `Trade 1`: `090-120s`, `120-150s`, `150-180s`
- `Trade 2`: `210-240s`, `240-270s`, `270-297s`
- `Trade 3`: `210-240s`, `240-270s`, `270-297s`

Reasoning:
- Use larger size on early `Trade 1` entries where the first-confidence regime looked strongest.
- Use larger size on `Trade 2` and `Trade 3` only once the session is more mature, from `210s` onward.
- This is intended to be a broader, more repeatable rule set rather than a sparse collection of top cells.

Notes:
- `Gold Bets v2` is now written into the strategy as `gold_bets_v2`.
- Explicit strategy file: `strategy_external_inflection_positive_iteration_goldbetsv2.js`
- In the Run Audit, profile-aware default gold selections now recognize `gold_bets_v2` as a first-class preset.
- This version is meant to be easier to reason about operationally: early confidence for `Trade 1`, late confidence for follow-on trades.

`Gold Bets v3`
- `Trade 1`: `90-180s`
- `Trade 2`: `295-300s`
- `Trade 3`: `295-300s`
- Explicit strategy file: `strategy_external_inflection_positive_iteration_goldbetsv3.js`
- Strategy sizing profile key: `gold_bets_v3`
