#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import os
import re
import shlex
import statistics
import subprocess
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_HOST = "ec2-user@18.224.94.56"
DEFAULT_KEY = "/Users/aliathar/.ssh/polymarket-aws.pem"
DEFAULT_PUBLIC_BASE = "http://127.0.0.1:18891"
DEFAULT_HOST_PORT = "8791"
DEFAULT_REMOTE_ROOT = "/home/ec2-user/polymarket-bot/src"

ET = ZoneInfo("America/New_York")
PT = ZoneInfo("America/Los_Angeles")

LOCAL_TRACE_DIRS = [
    "/Users/aliathar/polymarket-bot/src/btc_5m_data",
    "/Users/aliathar/btc_5m_data",
]

REMOTE_TRACE_DIRS = [
    "/home/ec2-user/5m_btc_market_data",
    "/home/ec2-user/recordings/btc_5m/5m_btc_market_data",
    "/home/ec2-user/polymarket-bot/src/btc_5m_data",
    "/home/ec2-user/btc_5m_data",
]

INTERVAL_BUCKET_SEC = 30
SESSION_WINDOW_SEC = 300
ENTRY_CUTOFF_SEC = 297
TRACE_MISSING_TOLERANCE_PCT = 0.30
RUN_AUDIT_INCREMENTAL_TAIL_SESSIONS = 5
RUN_AUDIT_INITIAL_VISIBLE_SESSIONS = 8
RUN_AUDIT_SESSION_LOAD_STEP = 8
RUN_AUDIT_INLINE_DETAIL_SESSION_LIMIT = max(24, RUN_AUDIT_INITIAL_VISIBLE_SESSIONS + (RUN_AUDIT_SESSION_LOAD_STEP * 2))
AUDIT_CODE_VERSION = "run_audit_mv_v27"
RUN_AUDIT_STATE_FILE = "run_audit_state.json"
RUN_AUDIT_SESSION_CACHE_DIR = "run_audit_sessions"
SESSION_RESOLUTION_TRUTH_FILE = "session_resolution_truth.jsonl"
SESSION_VENUE_TRUTH_FILE = "session_venue_truth.jsonl"
RECONCILE_SOFT_PENDING_MS = 20_000
RECONCILE_HARD_WARNING_MS = 30_000
RECONCILE_STALE_MS = 60_000

DEFAULT_GOLD_SELECTION_KEYS_BY_STRATEGY: dict[str, tuple[str, ...]] = {
    "inflection_positive_iteration": (
        "1|180-210s",
        "1|210-240s",
        "1|240-270s",
        "1|270-297s",
        "2|210-240s",
        "2|240-270s",
        "2|270-297s",
        "3|210-240s",
        "3|240-270s",
        "3|270-297s",
    ),
}

DEFAULT_GOLD_SELECTION_KEYS_BY_PROFILE: dict[str, tuple[str, ...]] = {
    "gold_bets_v1": (
        "1|180-210s",
        "1|210-240s",
        "1|240-270s",
        "1|270-297s",
        "2|210-240s",
        "2|240-270s",
        "2|270-297s",
        "3|210-240s",
        "3|240-270s",
        "3|270-297s",
    ),
    "gold_bets_v2": (
        "1|090-120s",
        "1|120-150s",
        "1|150-180s",
        "2|210-240s",
        "2|240-270s",
        "2|270-297s",
        "3|210-240s",
        "3|240-270s",
        "3|270-297s",
    ),
}

TAKER_EFFECTIVE_RATE_TABLE = [
    (0.01, 0.0),
    (0.05, 0.0006),
    (0.10, 0.0020),
    (0.15, 0.0041),
    (0.20, 0.0064),
    (0.25, 0.0088),
    (0.30, 0.0110),
    (0.35, 0.0129),
    (0.40, 0.0144),
    (0.45, 0.0153),
    (0.50, 0.0156),
    (0.55, 0.0153),
    (0.60, 0.0144),
    (0.65, 0.0129),
    (0.70, 0.0110),
    (0.75, 0.0088),
    (0.80, 0.0064),
    (0.85, 0.0041),
    (0.90, 0.0020),
    (0.95, 0.0006),
    (0.99, 0.0),
]


def n(v: Any, default: float = float("nan")) -> float:
    try:
        x = float(v)
    except Exception:
        return default
    return x if math.isfinite(x) else default


def i(v: Any, default: int = 0) -> int:
    x = n(v, float(default))
    return int(x) if math.isfinite(x) else default


def is_finite(v: Any) -> bool:
    return math.isfinite(n(v))


def round4(v: Any) -> float:
    return round(float(v or 0.0) + 1e-12, 4)


def round6(v: Any) -> float:
    return round(float(v or 0.0) + 1e-12, 6)


def latency_ms(start_ts: Any, end_ts: Any) -> int | None:
    start = n(start_ts)
    end = n(end_ts)
    if not (math.isfinite(start) and math.isfinite(end)):
        return None
    return int(round(end - start))


def price_delta_metrics(signal_px: Any, fill_px: Any) -> dict[str, float | None]:
    signal = n(signal_px)
    fill = n(fill_px)
    if not (math.isfinite(signal) and math.isfinite(fill)):
        return {"deltaPx": None, "deltaPct": None}
    delta_px = fill - signal
    delta_pct = (delta_px / signal * 100.0) if signal > 0 else None
    return {
        "deltaPx": round6(delta_px),
        "deltaPct": round4(delta_pct) if delta_pct is not None and math.isfinite(delta_pct) else None,
    }


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, list):
        return [sanitize_json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): sanitize_json_value(v) for k, v in value.items()}
    return value


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(sanitize_json_value(payload), indent=2))


def slug_start_ms(slug: str) -> int | None:
    m = str(slug or "").rsplit("-", 1)
    if len(m) != 2 or not m[-1].isdigit():
        return None
    return int(m[-1]) * 1000


def fmt_usd(v: Any) -> str:
    x = float(v or 0.0)
    sign = "+" if x >= 0 else "-"
    return f"{sign}${abs(x):.2f}"


def fmt_usd_abs(v: Any) -> str:
    return f"${abs(float(v or 0.0)):.2f}"


def fmt_px(v: Any) -> str:
    x = n(v)
    return f"{x:.3f}" if math.isfinite(x) else "—"


def fmt_pct(v: Any) -> str:
    x = n(v)
    return f"{x:.2f}%" if math.isfinite(x) else "—"


def fmt_ts(ms: Any) -> str:
    x = n(ms)
    if not math.isfinite(x):
        return "—"
    return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc).astimezone(PT).strftime("%m/%d %I:%M:%S.%f %p PT")


def fmt_sec_from_start(ts_ms: Any, start_ms: Any) -> str:
    x = n(ts_ms)
    s = n(start_ms)
    if not (math.isfinite(x) and math.isfinite(s)):
        return "—"
    return f"{(x - s) / 1000.0:.2f}s"


def esc(s: Any) -> str:
    return html.escape(str(s if s is not None else ""))


def short_order_id(v: Any) -> str:
    s = str(v or "").strip()
    return s[-4:] if s else ""


def build_et_session_label(ts_sec: int) -> str:
    d = datetime.fromtimestamp(ts_sec, tz=timezone.utc).astimezone(ET)
    return d.strftime("%-I_%M%p") if sys.platform != "win32" else d.strftime("%#I_%M%p")


def fmt_et_window_title(start_et: datetime) -> str:
    end_et = start_et + timedelta(minutes=5)
    month_day = start_et.strftime("%B %-d") if sys.platform != "win32" else start_et.strftime("%B %#d")

    def hm_ampm(dt: datetime) -> str:
        return dt.strftime("%-I:%M%p") if sys.platform != "win32" else dt.strftime("%#I:%M%p")

    def hm(dt: datetime) -> str:
        return dt.strftime("%-I:%M") if sys.platform != "win32" else dt.strftime("%#I:%M")

    return "Bitcoin Up or Down - 5 min\n" + f"{month_day}, {hm(start_et)}-{hm_ampm(end_et)} ET"


def safe_filename(s: str) -> str:
    s = s.replace("\n", " - ")
    s = " ".join(s.split()).strip()
    keep = []
    for ch in s:
        if ch.isalnum() or ch in "._ -":
            keep.append(ch)
    return "".join(keep).replace(" ", "_")[:180]


def old_trace_filename_for_slug(slug: str) -> str | None:
    start_ms = slug_start_ms(slug)
    if not start_ms:
        return None
    start_sec = start_ms // 1000
    end_sec = start_sec + SESSION_WINDOW_SEC
    d = datetime.fromtimestamp(start_sec, tz=timezone.utc).astimezone(ET)
    month = d.strftime("%B")
    day = d.strftime("%-d") if sys.platform != "win32" else d.strftime("%#d")
    return f"Bitcoin Up or Down - {month} {day}, {build_et_session_label(start_sec)}-{build_et_session_label(end_sec)} ET.csv"


def new_trace_filename_for_slug(slug: str) -> str | None:
    start_ms = slug_start_ms(slug)
    if not start_ms:
        return None
    start_et = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).astimezone(ET)
    return safe_filename(fmt_et_window_title(start_et)) + ".csv"


def interval_label(offset_sec: Any, bucket_sec: int = INTERVAL_BUCKET_SEC, cutoff_sec: int = ENTRY_CUTOFF_SEC) -> str:
    x = n(offset_sec)
    if not math.isfinite(x):
        return "unknown"
    lo = int(max(0, math.floor(x // bucket_sec) * bucket_sec))
    hi = min(cutoff_sec, lo + bucket_sec)
    return f"{lo:03d}-{hi:03d}s"


def taker_effective_rate_at_price(px: Any) -> float:
    x = n(px, 0.0)
    if x <= 0:
        return 0.0
    if x <= TAKER_EFFECTIVE_RATE_TABLE[0][0]:
        return TAKER_EFFECTIVE_RATE_TABLE[0][1]
    for i in range(1, len(TAKER_EFFECTIVE_RATE_TABLE)):
        px_hi, rate_hi = TAKER_EFFECTIVE_RATE_TABLE[i]
        px_lo, rate_lo = TAKER_EFFECTIVE_RATE_TABLE[i - 1]
        if x <= px_hi:
            span = px_hi - px_lo
            if span <= 0:
                return rate_hi
            t = (x - px_lo) / span
            return rate_lo + (rate_hi - rate_lo) * t
    return TAKER_EFFECTIVE_RATE_TABLE[-1][1]


def calc_taker_fee_usd(px: Any, shares: Any) -> float:
    p = n(px, 0.0)
    s = n(shares, 0.0)
    if p <= 0 or s <= 0:
        return 0.0
    return round4(p * s * taker_effective_rate_at_price(p))


def compute_entry_from_budget(budget_usd: Any, px: Any, fee_mode: str = "taker") -> dict[str, float]:
    budget = n(budget_usd, 0.0)
    entry_px = n(px, 0.0)
    if budget <= 0 or entry_px <= 0:
        return {"budgetUsd": 0.0, "principalUsd": 0.0, "entryFeeUsd": 0.0, "shares": 0.0}
    fee_rate = taker_effective_rate_at_price(entry_px) if fee_mode == "taker" else 0.0
    principal = budget / (1.0 + fee_rate)
    shares = principal / entry_px
    return {
        "budgetUsd": round4(budget),
        "principalUsd": round4(principal),
        "entryFeeUsd": round4(budget - principal),
        "shares": round4(shares),
    }


@dataclass
class SourceConfig:
    mode: str
    host: str
    key: str
    host_port: str
    public_base: str
    remote_root: str
    run_dir: str | None = None


def run_ssh(host: str, key: str, command: str) -> str:
    return subprocess.check_output(
        ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", host, command],
        text=True,
    )


def read_text(cfg: SourceConfig, path_str: str) -> str:
    if cfg.mode == "local":
        return Path(path_str).read_text(encoding="utf-8")
    return run_ssh(cfg.host, cfg.key, f"cat {shlex.quote(path_str)}")


def read_json(cfg: SourceConfig, path_str: str) -> Any:
    return json.loads(read_text(cfg, path_str))


def read_jsonl(cfg: SourceConfig, path_str: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    text = read_text(cfg, path_str)
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def file_exists(cfg: SourceConfig, path_str: str) -> bool:
    if cfg.mode == "local":
        return Path(path_str).exists()
    out = run_ssh(cfg.host, cfg.key, f"python3 - <<'PY'\nimport os\nprint('1' if os.path.exists({path_str!r}) else '0')\nPY")
    return out.strip() == "1"


def canonical_host_root(cfg: SourceConfig) -> str:
    if cfg.mode == "local":
        if not cfg.run_dir:
            return ""
        return str(Path(cfg.run_dir).resolve().parent.parent)
    return f"{cfg.remote_root}/trade_logs/hosts/host_{cfg.host_port}"


def canonical_session_artifact_path(cfg: SourceConfig, slug: str, include_trace: bool) -> str:
    base = canonical_host_root(cfg)
    fidelity = "high_fidelity.json" if include_trace else "low_fidelity.json"
    return f"{base}/sessions/{slug}/{fidelity}"


def read_canonical_session_trace(cfg: SourceConfig, slug: str) -> dict[str, Any] | None:
    for include_trace in (False, True):
        path_str = canonical_session_artifact_path(cfg, slug, include_trace)
        if not path_str or not file_exists(cfg, path_str):
            continue
        try:
            payload = read_json(cfg, path_str)
        except Exception:
            continue
        session = payload.get("session") if isinstance(payload, dict) else None
        if not isinstance(session, dict):
            continue
        trace = session.get("trace")
        if not isinstance(trace, dict):
            continue
        xs = [int(x) for x in (trace.get("xMs") or []) if is_finite(x)]
        up = list(trace.get("up") or [])
        down = list(trace.get("down") or [])
        if len(xs) < 2 or len(up) != len(xs) or len(down) != len(xs):
            continue
        out = dict(trace)
        out["source"] = str(
            out.get("source")
            or session.get("traceSource")
            or ("canonical_session_high_fidelity" if include_trace else "canonical_session_low_fidelity")
        )
        out["sourceFile"] = path_str
        return out
    return None


def read_cached_session_compact(cfg: SourceConfig, slug: str) -> dict[str, Any] | None:
    run_dir = str(cfg.run_dir or "").strip()
    if not run_dir:
        return None
    candidate_paths = [
        str(Path(run_dir).resolve() / "session_audits" / f"{slug}.compact.json"),
        str(run_audit_session_dir_for_run(run_dir) / f"{slug}.compact.json"),
    ]
    for path_str in candidate_paths:
        if not path_str or not file_exists(cfg, path_str):
            continue
        try:
            payload = read_json(cfg, path_str)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def session_audit_timeline_rows(compact: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(compact, dict):
        return []
    side_audit = compact.get("sideAudit")
    if not isinstance(side_audit, dict):
        return []
    rows: list[dict[str, Any]] = []
    for side in ("UP", "DOWN"):
        lane = side_audit.get(side)
        if not isinstance(lane, dict):
            continue
        for row in lane.get("timeline") or []:
            if isinstance(row, dict):
                merged = dict(row)
                if not merged.get("side"):
                    merged["side"] = side
                rows.append(merged)
    rows.sort(key=lambda row: (i(row.get("tsMs") or row.get("eventTsMs") or 0), str(row.get("event") or "")))
    return rows


def session_audit_financials(compact: dict[str, Any] | None) -> dict[str, float] | None:
    if not isinstance(compact, dict):
        return None
    for source in (compact.get("financials"), compact.get("sessionSummary")):
        if not isinstance(source, dict):
            continue
        net_val = n(source.get("netPnlUsd", source.get("actualNetPnlUsd", source.get("pnlUsd"))), float("nan"))
        gross_val = n(source.get("grossPnlUsd"), float("nan"))
        fees_val = n(source.get("feesUsd"), float("nan"))
        out = {}
        if math.isfinite(gross_val):
            out["grossPnlUsd"] = round6(gross_val)
        if math.isfinite(fees_val):
            out["feesUsd"] = round6(fees_val)
        if math.isfinite(net_val):
            out["netPnlUsd"] = round6(net_val)
            out["actualNetPnlUsd"] = round6(net_val)
        if out:
            return out
    return None


def read_cached_session_trace(cfg: SourceConfig, slug: str) -> dict[str, Any] | None:
    payload = read_cached_session_compact(cfg, slug)
    session = payload.get("session") if isinstance(payload, dict) else None
    if not isinstance(session, dict):
        return None
    trace = session.get("trace")
    if not isinstance(trace, dict):
        return None
    xs = [int(x) for x in (trace.get("xMs") or []) if is_finite(x)]
    up = list(trace.get("up") or [])
    down = list(trace.get("down") or [])
    if len(xs) < 2 or len(up) != len(xs) or len(down) != len(xs):
        return None
    out = dict(trace)
    out["source"] = str(out.get("source") or session.get("traceSource") or "cached_session_compact")
    return out


def norm_side(v: Any) -> str:
    s = str(v or "").strip().upper()
    return s if s in {"UP", "DOWN"} else ""


def other_side(side: Any) -> str:
    s = norm_side(side)
    if s == "UP":
        return "DOWN"
    if s == "DOWN":
        return "UP"
    return ""


def read_resolution_truth_by_slug(cfg: SourceConfig, run_dir: str) -> dict[str, dict[str, Any]]:
    path_str = f"{run_dir}/{SESSION_RESOLUTION_TRUTH_FILE}"
    if not file_exists(cfg, path_str):
        return {}
    rows = read_jsonl(cfg, path_str)
    by_slug: dict[str, dict[str, Any]] = {}
    for raw in rows:
        slug = str(raw.get("slug") or "").strip()
        if not slug:
            continue
        truth_status = str(raw.get("truthStatus") or raw.get("status") or "pending").strip().lower() or "pending"
        normalized = {
            "schemaVersion": str(raw.get("schemaVersion") or "1.0"),
            "slug": slug,
            "runNum": i(raw.get("runNum")) if is_finite(raw.get("runNum")) else None,
            "runId": str(raw.get("runId") or "").strip() or None,
            "instanceId": str(raw.get("instanceId") or "").strip() or None,
            "truthStatus": truth_status,
            "finalOutcomeSide": norm_side(raw.get("finalOutcomeSide") or raw.get("resolvedOutcomeSide") or raw.get("winningSide")) or None,
            "finalPriceUp": round6(n(raw.get("finalPriceUp"))) if is_finite(raw.get("finalPriceUp")) else None,
            "finalPriceDown": round6(n(raw.get("finalPriceDown"))) if is_finite(raw.get("finalPriceDown")) else None,
            "resolvedAtMs": i(raw.get("resolvedAtMs")) if is_finite(raw.get("resolvedAtMs")) else None,
            "checkedAtMs": i(raw.get("checkedAtMs")) if is_finite(raw.get("checkedAtMs")) else None,
            "updatedAtMs": i(raw.get("updatedAtMs")) if is_finite(raw.get("updatedAtMs")) else None,
            "truthSource": str(raw.get("truthSource") or "").strip() or None,
            "sourceRef": str(raw.get("sourceRef") or "").strip() or None,
            "notes": str(raw.get("notes") or "").strip() or None,
        }
        prev = by_slug.get(slug)
        prev_rank = 2 if str((prev or {}).get("truthStatus") or "") == "resolved" else (1 if str((prev or {}).get("truthStatus") or "") == "ambiguous" else 0)
        next_rank = 2 if truth_status == "resolved" else (1 if truth_status == "ambiguous" else 0)
        prev_ts = int((prev or {}).get("resolvedAtMs") or (prev or {}).get("updatedAtMs") or (prev or {}).get("checkedAtMs") or 0)
        next_ts = int(normalized.get("resolvedAtMs") or normalized.get("updatedAtMs") or normalized.get("checkedAtMs") or 0)
        if not prev or next_rank > prev_rank or (next_rank == prev_rank and next_ts >= prev_ts):
            by_slug[slug] = normalized
    return by_slug


def read_session_venue_truth_by_slug(cfg: SourceConfig, run_dir: str) -> dict[str, dict[str, Any]]:
    path_str = f"{run_dir}/{SESSION_VENUE_TRUTH_FILE}"
    if not file_exists(cfg, path_str):
        return {}
    rows = read_jsonl(cfg, path_str)
    by_slug: dict[str, dict[str, Any]] = {}
    for raw in rows:
        slug = str(raw.get("slug") or "").strip()
        if not slug:
            continue
        prev = by_slug.get(slug)
        prev_ts = i((prev or {}).get("recordedAtMs")) if isinstance(prev, dict) else 0
        next_ts = i(raw.get("recordedAtMs"))
        if not prev or next_ts >= prev_ts:
            by_slug[slug] = raw
    return by_slug


def truth_exit_px_for_side(truth_row: dict[str, Any] | None, side: Any) -> float | None:
    s = norm_side(side)
    if not s or not isinstance(truth_row, dict):
        return None
    if s == "UP" and is_finite(truth_row.get("finalPriceUp")):
        return round6(n(truth_row.get("finalPriceUp")))
    if s == "DOWN" and is_finite(truth_row.get("finalPriceDown")):
        return round6(n(truth_row.get("finalPriceDown")))
    final_side = norm_side(truth_row.get("finalOutcomeSide"))
    if not final_side:
        return None
    return 1.0 if final_side == s else 0.0


def build_session_truth_verification(session: dict[str, Any], truth_row: dict[str, Any] | None) -> dict[str, Any]:
    lanes = [lane for lane in (session.get("lanes") or []) if str(lane.get("exitType") or "").strip().lower() == "settle"]
    verification: dict[str, Any] = {
        "truthStatus": str((truth_row or {}).get("truthStatus") or "pending"),
        "truthRow": truth_row or None,
        "finalOutcomeSide": (truth_row or {}).get("finalOutcomeSide"),
        "finalPriceUp": (truth_row or {}).get("finalPriceUp"),
        "finalPriceDown": (truth_row or {}).get("finalPriceDown"),
        "settledLaneCount": len(lanes),
        "roundedInferenceSide": None,
        "roundedInferenceMatchesTruth": None,
        "truthNetPnlDeltaUsd": None,
        "correctionApplied": False,
        "mismatchReason": "",
        "laneChecks": [],
    }
    if not lanes:
        verification["mismatchReason"] = "no_settle_lanes" if not truth_row else "resolved_truth_without_settle_lane"
        return verification
    if not truth_row or str(truth_row.get("truthStatus") or "").lower() != "resolved":
        verification["mismatchReason"] = "truth_missing" if not truth_row else "truth_not_resolved"
        return verification

    lane_checks: list[dict[str, Any]] = []
    compared_count = 0
    matched_count = 0
    delta_usd = 0.0
    inferred_sides: list[str] = []
    for lane in lanes:
        side = norm_side(lane.get("side"))
        recorded_exit_px = n(lane.get("exitPx"))
        inferred_outcome_side = ""
        if side and math.isfinite(recorded_exit_px):
            inferred_outcome_side = side if recorded_exit_px >= 0.5 else other_side(side)
        if inferred_outcome_side:
            inferred_sides.append(inferred_outcome_side)
        truth_outcome_side = norm_side(truth_row.get("finalOutcomeSide"))
        matched = None
        if inferred_outcome_side and truth_outcome_side:
            matched = inferred_outcome_side == truth_outcome_side
            compared_count += 1
            if matched:
                matched_count += 1
        truth_exit_px = truth_exit_px_for_side(truth_row, side)
        lane_delta = None
        truth_adjusted_net = None
        shares = n(lane.get("shares"))
        lane_net = n(lane.get("pnlUsd"))
        if math.isfinite(truth_exit_px or float("nan")) and math.isfinite(recorded_exit_px) and math.isfinite(shares) and math.isfinite(lane_net):
            lane_delta = round6((float(truth_exit_px) - recorded_exit_px) * shares)
            truth_adjusted_net = round6(lane_net + lane_delta)
            delta_usd += lane_delta
        lane_checks.append({
            "side": side or None,
            "recordedExitPx": round6(recorded_exit_px) if math.isfinite(recorded_exit_px) else None,
            "truthExitPx": round6(truth_exit_px) if truth_exit_px is not None and math.isfinite(truth_exit_px) else None,
            "inferredOutcomeSide": inferred_outcome_side or None,
            "truthOutcomeSide": truth_outcome_side or None,
            "matched": matched,
            "recordedNetPnlUsd": round6(lane_net) if math.isfinite(lane_net) else None,
            "truthAdjustedNetPnlUsd": truth_adjusted_net,
            "pnlDeltaUsd": lane_delta,
        })

    session_net = n(session.get("pnlUsd"))
    verification["roundedInferenceSide"] = inferred_sides[0] if len(set(inferred_sides)) == 1 and inferred_sides else None
    verification["roundedInferenceMatchesTruth"] = (matched_count == compared_count) if compared_count else None
    verification["truthNetPnlDeltaUsd"] = round6(delta_usd) if compared_count else None
    verification["correctionApplied"] = compared_count > 0 and abs(delta_usd) > 1e-9
    verification["mismatchReason"] = "rounded_settle_mismatch" if compared_count and matched_count != compared_count else ""
    verification["laneChecks"] = lane_checks
    return verification


def build_trace_indexes(cfg: SourceConfig) -> tuple[dict[str, str], dict[str, str]]:
    local_idx: dict[str, str] = {}
    for d in local_machine_trace_dirs():
        p = Path(d)
        if not p.exists():
            continue
        for child in p.glob("*.csv"):
            local_idx.setdefault(child.name, str(child))
    remote_idx: dict[str, str] = {}
    if str(cfg.host or "").strip() and str(cfg.key or "").strip():
        payload = run_ssh(
            cfg.host,
            cfg.key,
            "python3 - <<'PY'\n"
            "import json, os\n"
            f"dirs = {json.dumps(REMOTE_TRACE_DIRS)}\n"
            "out = {}\n"
            "for d in dirs:\n"
            "    if not os.path.isdir(d):\n"
            "        continue\n"
            "    try:\n"
            "        for name in os.listdir(d):\n"
            "            if not name.endswith('.csv'):\n"
            "                continue\n"
            "            out.setdefault(name, os.path.join(d, name))\n"
            "    except Exception:\n"
            "        pass\n"
            "print(json.dumps(out))\n"
            "PY",
        )
        try:
            remote_idx = json.loads(payload)
        except Exception:
            remote_idx = {}
    return local_idx, remote_idx


def build_run_index_trace_index(run_dir: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    try:
        p = Path(run_dir)
        if not p.exists():
            return out
        for child in p.glob("index_*_run_*.json"):
            try:
                payload = json.loads(child.read_text(encoding="utf-8"))
            except Exception:
                continue
            traces = payload.get("tracesBySlug") or {}
            if not isinstance(traces, dict):
                continue
            for slug, trace in traces.items():
                if not isinstance(trace, dict):
                    continue
                x_ms = trace.get("xMs") if isinstance(trace.get("xMs"), list) else []
                up = trace.get("up") if isinstance(trace.get("up"), list) else []
                down = trace.get("down") if isinstance(trace.get("down"), list) else []
                if min(len(x_ms), len(up), len(down)) < 2:
                    continue
                out[str(slug)] = {
                    "xMs": x_ms,
                    "up": up,
                    "down": down,
                    "source": "run_index_compact_trace",
                    "sourceFile": str(child),
                }
        return out
    except Exception:
        return {}


def local_machine_trace_dirs() -> list[str]:
    out: list[str] = []
    for d in [*LOCAL_TRACE_DIRS, *REMOTE_TRACE_DIRS]:
        if d in out:
            continue
        if os.path.isdir(d):
            out.append(d)
    return out


def parse_trace_csv_text(text: str, slug: str) -> dict[str, Any] | None:
    rows = [line for line in text.splitlines() if line.strip()]
    if len(rows) < 2:
        return None
    reader = csv.DictReader(rows)
    fieldnames = [str(x or "").strip().lower() for x in (reader.fieldnames or [])]
    if {"timestamp", "outcome", "price"}.issubset(set(fieldnames)):
        by_ts: dict[int, dict[str, float | None]] = {}
        for row in reader:
            try:
                ts = int(float(row.get("timestamp") or 0))
            except Exception:
                continue
            outcome = str(row.get("outcome") or "").strip().upper()
            px = row.get("price")
            val = n(px)
            cur = by_ts.setdefault(ts, {"UP": None, "DOWN": None})
            if outcome == "YES":
                cur["UP"] = val if math.isfinite(val) else None
            elif outcome == "NO":
                cur["DOWN"] = val if math.isfinite(val) else None
        if not by_ts:
            return None
        x_ms = sorted(by_ts)
        return {
            "xMs": x_ms,
            "up": [by_ts[x].get("UP") for x in x_ms],
            "down": [by_ts[x].get("DOWN") for x in x_ms],
            "source": "raw_recorder",
        }
    basic = csv.reader(rows)
    all_rows = list(basic)
    if len(all_rows) < 2:
        return None
    x_ms: list[int] = []
    up: list[float | None] = []
    down: list[float | None] = []
    start_ms = slug_start_ms(slug)
    if not start_ms:
        return None
    for row in all_rows[1:]:
        if len(row) < 3:
            continue
        sec = n(row[0])
        up_px = n(row[1])
        down_px = n(row[2])
        if not math.isfinite(sec):
            continue
        x_ms.append(int(round(start_ms + sec * 1000.0)))
        up.append(up_px if math.isfinite(up_px) else None)
        down.append(down_px if math.isfinite(down_px) else None)
    if not x_ms:
        return None
    return {"xMs": x_ms, "up": up, "down": down, "source": "legacy_csv"}


def build_telemetry_trace_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_slug: dict[str, dict[str, list[Any]]] = {}
    for row in rows:
        slug = str(row.get("sessionSlug") or row.get("marketSlug") or "").strip()
        if not slug:
            continue
        ts_ms = None
        for key in ("t", "eventTsMs", "actualFillTsMs"):
            if is_finite(row.get(key)):
                ts_ms = i(row.get(key))
                break
        if not ts_ms:
            continue
        up_bid = n(row.get("upBid"))
        down_bid = n(row.get("downBid"))
        if not (math.isfinite(up_bid) or math.isfinite(down_bid)):
            continue
        cur = by_slug.setdefault(slug, {"xMs": [], "up": [], "down": []})
        cur["xMs"].append(ts_ms)
        cur["up"].append(up_bid if math.isfinite(up_bid) else None)
        cur["down"].append(down_bid if math.isfinite(down_bid) else None)
    out: dict[str, dict[str, Any]] = {}
    for slug, trace in by_slug.items():
        order = sorted(range(len(trace["xMs"])), key=lambda idx: trace["xMs"][idx])
        x_ms = [trace["xMs"][idx] for idx in order]
        up = [trace["up"][idx] for idx in order]
        down = [trace["down"][idx] for idx in order]
        if x_ms:
            out[slug] = {
                "xMs": x_ms,
                "up": up,
                "down": down,
                "source": "run_telemetry",
            }
    return out


def build_telemetry_trace_index_from_file(path_str: str, target_slugs: set[str]) -> dict[str, dict[str, Any]]:
    by_slug: dict[str, dict[str, list[float]]] = {}
    if not path_str or not os.path.exists(path_str):
        return {}
    with open(path_str, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            slug = str(row.get("marketSlug") or "").strip()
            if not slug or (target_slugs and slug not in target_slugs):
                continue
            ts_ms = i(row.get("t") or row.get("ts") or row.get("timestampMs") or 0)
            if not ts_ms:
                continue
            up_bid = n(((row.get("up") or {}) if isinstance(row.get("up"), dict) else {}).get("bid"))
            down_bid = n(((row.get("down") or {}) if isinstance(row.get("down"), dict) else {}).get("bid"))
            if not (math.isfinite(up_bid) and math.isfinite(down_bid)):
                continue
            cur = by_slug.setdefault(slug, {"xMs": [], "up": [], "down": []})
            cur["xMs"].append(ts_ms)
            cur["up"].append(up_bid)
            cur["down"].append(down_bid)
    out: dict[str, dict[str, Any]] = {}
    for slug, trace in by_slug.items():
        order = sorted(range(len(trace["xMs"])), key=lambda idx: trace["xMs"][idx])
        out[slug] = {
            "xMs": [trace["xMs"][idx] for idx in order],
            "up": [trace["up"][idx] for idx in order],
            "down": [trace["down"][idx] for idx in order],
            "source": "run_telemetry",
        }
    return out


def build_session_trace_index_from_file(path_str: str, target_slugs: set[str]) -> dict[str, dict[str, Any]]:
    by_slug: dict[str, dict[str, list[float]]] = {}
    if not path_str or not os.path.exists(path_str):
        return {}
    with open(path_str, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            slug = str(row.get("slug") or row.get("sessionSlug") or row.get("marketSlug") or "").strip()
            if not slug or (target_slugs and slug not in target_slugs):
                continue
            ts_ms = i(row.get("t") or row.get("ts") or row.get("timestampMs") or 0)
            if not ts_ms:
                continue
            up_bid = n(row.get("upBid"))
            down_bid = n(row.get("downBid"))
            if not (math.isfinite(up_bid) or math.isfinite(down_bid)):
                continue
            cur = by_slug.setdefault(slug, {"xMs": [], "up": [], "down": []})
            cur["xMs"].append(ts_ms)
            cur["up"].append(up_bid if math.isfinite(up_bid) else None)
            cur["down"].append(down_bid if math.isfinite(down_bid) else None)
    out: dict[str, dict[str, Any]] = {}
    for slug, trace in by_slug.items():
        order = sorted(range(len(trace["xMs"])), key=lambda idx: trace["xMs"][idx])
        out[slug] = {
            "xMs": [trace["xMs"][idx] for idx in order],
            "up": [trace["up"][idx] for idx in order],
            "down": [trace["down"][idx] for idx in order],
            "source": "session_trace_runlog",
            "sourceFile": path_str,
        }
    return out


def venue_truth_summary_rows(venue_truth: dict[str, Any] | None) -> list[str]:
    if not isinstance(venue_truth, dict):
        return []
    fills = venue_truth.get("fills") or []
    out: list[str] = []
    for fill in fills:
        if not isinstance(fill, dict):
            continue
        side = str(fill.get("side") or "—")
        direction = str(fill.get("dir") or "—")
        shares = n(fill.get("shares"))
        price = n(fill.get("price"))
        ts_ms = i(fill.get("tsMs"))
        when = sec(((ts_ms - slug_start_ms(str(venue_truth.get("slug") or ""))) / 1000.0)) if ts_ms and venue_truth.get("slug") else "—"
        out.append(
            f"{direction} {side} · {shares:.4f} @ {price:.4f} · t={when}"
            if math.isfinite(shares) and math.isfinite(price)
            else f"{direction} {side}"
        )
    return out


def fetch_trace_for_slug(
    cfg: SourceConfig,
    slug: str,
    local_idx: dict[str, str],
    remote_idx: dict[str, str],
    run_index_trace_idx: dict[str, dict[str, Any]] | None = None,
    telemetry_idx: dict[str, dict[str, Any]] | None = None,
    session_start_ms: int | None = None,
) -> dict[str, Any] | None:
    run_index_trace = run_index_trace_idx.get(slug) if run_index_trace_idx else None
    if run_index_trace:
        return run_index_trace
    telemetry_trace = telemetry_idx.get(slug) if telemetry_idx else None
    if telemetry_trace and session_start_ms is not None:
        telemetry_quality = assess_trace_quality(telemetry_trace, session_start_ms)
        if telemetry_quality.get("usable"):
            return telemetry_trace
    elif telemetry_trace:
        return telemetry_trace
    canonical_trace = read_canonical_session_trace(cfg, slug)
    if canonical_trace:
        return canonical_trace
    cached_compact_trace = read_cached_session_trace(cfg, slug)
    if cached_compact_trace:
        return cached_compact_trace
    names = [old_trace_filename_for_slug(slug), new_trace_filename_for_slug(slug)]
    if cfg.mode == "local":
        for d in local_machine_trace_dirs():
            for name in [x for x in names if x]:
                p = str(Path(d) / name)
                if not os.path.exists(p):
                    continue
                trace = parse_trace_csv_text(Path(p).read_text(encoding="utf-8"), slug)
                if trace:
                    trace["sourceFile"] = p
                    return trace
    for name in [x for x in names if x]:
        p = local_idx.get(name)
        if p:
            trace = parse_trace_csv_text(Path(p).read_text(encoding="utf-8"), slug)
            if trace:
                trace["sourceFile"] = p
                return trace
    for name in [x for x in names if x]:
        p = remote_idx.get(name)
        if p:
            trace = parse_trace_csv_text(read_text(SourceConfig(
                mode="remote",
                host=cfg.host,
                key=cfg.key,
                host_port=cfg.host_port,
                public_base=cfg.public_base,
                remote_root=cfg.remote_root,
                run_dir=None,
            ), p), slug)
            if trace:
                trace["sourceFile"] = p
                return trace
    return telemetry_trace


def assess_trace_quality(trace: dict[str, Any] | None, session_start_ms: int) -> dict[str, Any]:
    end_ms = session_start_ms + SESSION_WINDOW_SEC * 1000
    if not trace or not isinstance(trace, dict):
        return {
            "usable": False,
            "autoIgnore": True,
            "reasons": ["trace_missing"],
            "sampleCount": 0,
            "coveragePct": 0.0,
            "missingPct": 100.0,
            "maxGapSec": None,
            "medianGapSec": None,
        }
    xs = [int(x) for x in (trace.get("xMs") or []) if is_finite(x)]
    if len(xs) < 2:
        return {
            "usable": False,
            "autoIgnore": True,
            "reasons": ["trace_too_short"],
            "sampleCount": len(xs),
            "coveragePct": 0.0,
            "missingPct": 100.0,
            "maxGapSec": None,
            "medianGapSec": None,
        }
    xs = sorted(xs)
    gaps = [max(0, xs[i] - xs[i - 1]) for i in range(1, len(xs))]
    missing_ms = max(0, xs[0] - session_start_ms) + max(0, end_ms - xs[-1])
    # Allow small sampling gaps without calling them "missing".
    for gap in gaps:
        if gap > 1500:
            missing_ms += gap - 1500
    coverage_ms = max(0, min(end_ms, xs[-1]) - max(session_start_ms, xs[0]))
    coverage_pct = (coverage_ms / (SESSION_WINDOW_SEC * 1000.0)) * 100.0
    missing_pct = min(100.0, (missing_ms / (SESSION_WINDOW_SEC * 1000.0)) * 100.0)
    reasons: list[str] = []
    if len(xs) < 60:
        reasons.append("trace_low_resolution")
    if missing_pct > TRACE_MISSING_TOLERANCE_PCT * 100.0:
        reasons.append("trace_missing_gt_30pct")
    return {
        "usable": len(reasons) == 0,
        "autoIgnore": len(reasons) > 0,
        "reasons": reasons,
        "sampleCount": len(xs),
        "coveragePct": round4(coverage_pct),
        "missingPct": round4(missing_pct),
        "maxGapSec": round4(max(gaps) / 1000.0) if gaps else 0.0,
        "medianGapSec": round4(statistics.median(gaps) / 1000.0) if gaps else 0.0,
    }


def fetch_session_audit_svg_fallback(cfg: SourceConfig, run_num: int, slug: str) -> str | None:
    base = str(cfg.public_base or "").strip().rstrip("/")
    if not base:
        return None
    params = {
        "runNum": str(int(run_num)),
        "slug": str(slug),
    }
    qs = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items() if str(v))
    url = f"{base}/api/session-audits/review?{qs}"
    try:
        with urllib.request.urlopen(url, timeout=4) as resp:
            html_text = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None
    match = re.search(r"(<svg[^>]*id=[\"']auditChart[\"'][\s\S]*?</svg>)", html_text, re.IGNORECASE)
    if not match:
        return None
    svg_markup = match.group(1)
    if "class=" not in svg_markup.split(">", 1)[0]:
        svg_markup = svg_markup.replace("<svg ", '<svg class="trace-svg" ', 1)
    return svg_markup


def percentile_from_sorted(nums: list[int], p: float) -> float | None:
    if not nums:
        return None
    if len(nums) == 1:
        return float(nums[0])
    idx = max(0, min(len(nums) - 1, round((len(nums) - 1) * p)))
    return float(nums[idx])


def build_stream_end_snapshot(trace: dict[str, Any] | None, recent_gap_count: int = 500, thresholds: list[int] | None = None) -> dict[str, Any] | None:
    if not trace or not isinstance(trace, dict):
        return None
    xs = [int(x) for x in (trace.get("xMs") or []) if is_finite(x)]
    if len(xs) < 2:
        return None
    xs = sorted(xs)
    gaps = [max(0, xs[i] - xs[i - 1]) for i in range(1, len(xs))]
    if not gaps:
        return None
    tail_count = max(1, int(recent_gap_count))
    tail = gaps[-tail_count:]
    tail_sorted = sorted(tail)
    avg_gap = sum(tail_sorted) / len(tail_sorted)
    use_thresholds = [int(x) for x in (thresholds or [25, 50, 75, 100, 125, 150]) if is_finite(x)]
    stats = []
    for ms in use_thresholds:
        count = sum(1 for g in tail_sorted if g > ms)
        pct = (count / len(tail_sorted) * 100.0) if tail_sorted else 0.0
        stats.append({
            "gapMs": ms,
            "count": count,
            "pct": round4(pct),
        })
    return {
        "mode": "session_end",
        "label": "300s snapshot",
        "basis": "last_500_gaps",
        "recentGapCountTarget": tail_count,
        "sampleCount": len(xs),
        "deltaCount": len(tail_sorted),
        "firstTsMs": xs[0],
        "lastTsMs": xs[-1],
        "durationMs": max(0, xs[-1] - xs[0]),
        "liveGapMs": round4(tail[-1]) if tail else None,
        "avgGapMs": round4(avg_gap),
        "p50GapMs": round4(percentile_from_sorted(tail_sorted, 0.5)) if tail_sorted else None,
        "p95GapMs": round4(percentile_from_sorted(tail_sorted, 0.95)) if tail_sorted else None,
        "maxGapMs": round4(tail_sorted[-1]) if tail_sorted else None,
        "effectiveHz": round4((1000.0 / avg_gap) if avg_gap > 0 else 0.0) if avg_gap > 0 else None,
        "thresholds": stats,
    }


def normalize_event(raw: dict[str, Any], session_start_ms: int, row_id: str) -> dict[str, Any]:
    event = str(raw.get("event") or "").strip().lower()
    ts_keys = []
    if event == "enter_signal":
        ts_keys = ["localDetectedAtMs", "signalTsMs", "eventTsMs", "t", "actualFillTsMs"]
    elif event in {"enter_order", "enter_submit_start"}:
        ts_keys = ["localSubmitAtMs", "orderPlacedAtMs", "eventTsMs", "t", "actualFillTsMs"]
    elif event == "enter_submit_return":
        ts_keys = ["localAcceptedAtMs", "eventTsMs", "t", "actualFillTsMs"]
    elif event in {"enter_first_fill_seen", "enter_fill_confirmed", "enter", "enter_reconciled"}:
        ts_keys = ["localFillObservedAtMs", "eventTsMs", "t", "actualFillTsMs"]
    elif event in {"exit_partial", "exit"}:
        ts_keys = ["actualFillTsMs", "eventTsMs", "t"]
    else:
        ts_keys = ["eventTsMs", "t", "actualFillTsMs"]
    ts_ms = None
    for key in ts_keys:
        val = raw.get(key)
        if is_finite(val):
            ts_ms = i(val)
            break
    px = None
    if event.startswith("enter"):
        px = n(raw.get("actualFillPx", raw.get("entryPx")))
        if not math.isfinite(px):
            px = n(raw.get("signalPx", raw.get("intendedPx")))
    else:
        px = n(raw.get("exitPx"))
    if not math.isfinite(px):
        px = None
    side = str(raw.get("side") or "").strip().upper()
    stage = "fill"
    if event.endswith("_signal"):
        stage = "signal"
    elif event.endswith("_order"):
        stage = "order"
    note_bits = []
    for key in ("exitReasonRaw", "reason", "fillSource", "fillTimestampSource", "orderType"):
        val = raw.get(key)
        if val not in (None, "", False):
            note_bits.append(f"{key}={val}")
    signal_ts_ms = i(raw.get("signalTsMs")) if is_finite(raw.get("signalTsMs")) else None
    order_ts_ms = i(raw.get("orderPlacedAtMs")) if is_finite(raw.get("orderPlacedAtMs")) else None
    fill_ts_ms = i(raw.get("actualFillTsMs")) if is_finite(raw.get("actualFillTsMs")) else None
    event_ts_ms = i(raw.get("eventTsMs")) if is_finite(raw.get("eventTsMs")) else None
    local_detected_ts_ms = i(raw.get("localDetectedAtMs")) if is_finite(raw.get("localDetectedAtMs")) else None
    local_submit_ts_ms = i(raw.get("localSubmitAtMs")) if is_finite(raw.get("localSubmitAtMs")) else None
    local_accepted_ts_ms = i(raw.get("localAcceptedAtMs")) if is_finite(raw.get("localAcceptedAtMs")) else None
    local_fill_observed_ts_ms = i(raw.get("localFillObservedAtMs")) if is_finite(raw.get("localFillObservedAtMs")) else None
    local_signal_ts_ms = local_detected_ts_ms or signal_ts_ms
    local_order_ts_ms = local_submit_ts_ms or order_ts_ms or event_ts_ms
    local_fill_ts_ms = local_fill_observed_ts_ms or event_ts_ms or fill_ts_ms or ts_ms
    signal_px = n(raw.get("signalPx", raw.get("entryPx")))
    intended_px = n(raw.get("intendedPx", raw.get("limitPx", raw.get("targetSellPx"))))
    actual_fill_px = n(raw.get("actualFillPx", raw.get("fillPx")))
    order_id = str(raw.get("orderId") or raw.get("tpOrderId") or "").strip()
    execution_mode = str(raw.get("executionMode") or "").strip().lower()
    fill_source = str(raw.get("fillSource") or "").strip().lower()
    return {
        "rowId": row_id,
        "rawEvent": event,
        "stage": stage,
        "label": event.replace("_", " ").upper(),
        "tsMs": ts_ms,
        "offsetSec": round4((ts_ms - session_start_ms) / 1000.0) if ts_ms is not None else None,
        "side": side,
        "px": px,
        "exitType": str(raw.get("exitType") or "").strip().upper() or None,
        "sharesActual": n(raw.get("sharesClosed", raw.get("sharesRequested", raw.get("shares")))),
        "sharesClosedActual": n(raw.get("sharesClosed")),
        "sharesRemainingActual": n(raw.get("sharesRemaining")),
        "signalPx": signal_px if math.isfinite(signal_px) else None,
        "intendedPx": intended_px if math.isfinite(intended_px) else None,
        "actualFillPx": actual_fill_px if math.isfinite(actual_fill_px) else None,
        "eventTsMs": event_ts_ms,
        "actualFillTsMs": fill_ts_ms,
        "signalTsMs": signal_ts_ms,
        "orderTsMs": order_ts_ms,
        "localSignalTsMs": local_signal_ts_ms,
        "localOrderTsMs": local_order_ts_ms,
        "localAcceptedTsMs": local_accepted_ts_ms,
        "localFillObservedTsMs": local_fill_observed_ts_ms,
        "signalToOrderMs": latency_ms(local_signal_ts_ms, local_order_ts_ms),
        "signalToFillMs": latency_ms(local_signal_ts_ms, local_fill_ts_ms),
        "orderId": order_id,
        "orderIdTail": short_order_id(order_id) or None,
        "mode": str(raw.get("mode") or raw.get("entryMode") or "").strip().lower() or None,
        "executionMode": execution_mode or None,
        "fillSource": fill_source or None,
        "decisionSnapshot": raw.get("decisionSnapshot") if isinstance(raw.get("decisionSnapshot"), dict) else None,
        "actualNetPnlUsd": n(raw.get("pnlUsd")),
        "actualGrossPnlUsd": n(raw.get("grossPnlUsd", raw.get("legGrossPnlUsd"))),
        "actualFeesUsd": (
            n(raw.get("totalFeesUsd", raw.get("legFeesUsd")))
            if is_finite(raw.get("totalFeesUsd")) or is_finite(raw.get("legFeesUsd"))
            else (
                n(raw.get("entryFeeUsd"), 0.0) + n(raw.get("exitFeeUsd"), 0.0)
                if is_finite(raw.get("entryFeeUsd")) or is_finite(raw.get("exitFeeUsd"))
                else None
            )
        ),
        "actualBalanceUsd": n(raw.get("balanceUsd")),
        "nominalUsd": n(raw.get("notionalUsd", raw.get("budgetUsd"))),
        "tradeNum": None,
        "role": None,
        "legIndex": None,
        "note": " | ".join(note_bits),
    }


def sanitize_decision_window(window: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(window, dict):
        return None
    points = []
    for point in window.get("points") or []:
        if not isinstance(point, dict):
            continue
        checkpoint_ms = n(point.get("checkpointMs"))
        px = n(point.get("px"))
        if not math.isfinite(checkpoint_ms) or not math.isfinite(px):
            continue
        source_ts_ms = n(point.get("sourceTsMs"))
        points.append({
            "checkpointMs": int(round(checkpoint_ms)),
            "sourceTsMs": int(round(source_ts_ms)) if math.isfinite(source_ts_ms) else None,
            "px": round6(px),
        })
    return {
        "side": str(window.get("side") or "").strip().upper() or None,
        "latestCheckpointMs": int(round(n(window.get("latestCheckpointMs")))) if is_finite(window.get("latestCheckpointMs")) else None,
        "gain": round6(n(window.get("gain"))) if is_finite(window.get("gain")) else None,
        "passed": bool(window.get("passed")) if isinstance(window.get("passed"), bool) else None,
        "failureReason": str(window.get("failureReason") or "").strip() or None,
        "points": points,
    }


def sanitize_decision_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(snapshot, dict):
        return None
    windows = snapshot.get("windows") if isinstance(snapshot.get("windows"), dict) else {}
    return {
        "strategyId": str(snapshot.get("strategyId") or "").strip() or None,
        "marketSlug": str(snapshot.get("marketSlug") or "").strip() or None,
        "elapsedSec": round4(n(snapshot.get("elapsedSec"))) if is_finite(snapshot.get("elapsedSec")) else None,
        "entryBreakoutThr": round6(n(snapshot.get("entryBreakoutThr"))) if is_finite(snapshot.get("entryBreakoutThr")) else None,
        "minSlopeSamples": int(round(n(snapshot.get("minSlopeSamples")))) if is_finite(snapshot.get("minSlopeSamples")) else None,
        "slopeLookbackMs": int(round(n(snapshot.get("slopeLookbackMs")))) if is_finite(snapshot.get("slopeLookbackMs")) else None,
        "resampleMs": int(round(n(snapshot.get("resampleMs")))) if is_finite(snapshot.get("resampleMs")) else None,
        "upBid": round6(n(snapshot.get("upBid"))) if is_finite(snapshot.get("upBid")) else None,
        "downBid": round6(n(snapshot.get("downBid"))) if is_finite(snapshot.get("downBid")) else None,
        "chosenSide": str(snapshot.get("chosenSide") or "").strip().upper() or None,
        "chosenEntryPx": round6(n(snapshot.get("chosenEntryPx"))) if is_finite(snapshot.get("chosenEntryPx")) else None,
        "rearmBlocked": {
            "UP": bool(((snapshot.get("rearmBlocked") or {}) if isinstance(snapshot.get("rearmBlocked"), dict) else {}).get("UP")),
            "DOWN": bool(((snapshot.get("rearmBlocked") or {}) if isinstance(snapshot.get("rearmBlocked"), dict) else {}).get("DOWN")),
        },
        "windows": {
            "UP": sanitize_decision_window(windows.get("UP") if isinstance(windows, dict) else None),
            "DOWN": sanitize_decision_window(windows.get("DOWN") if isinstance(windows, dict) else None),
        },
    }


def build_session_decision_snapshots(
    raw_rows: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    session_start_ms: int,
) -> list[dict[str, Any]]:
    signal_candidates: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []

    for raw in raw_rows or []:
      event = str(raw.get("event") or "").strip().lower()
      if event not in {"enter_signal", "entry_blocked"}:
          continue
      snapshot = sanitize_decision_snapshot(raw.get("decisionSnapshot") if isinstance(raw.get("decisionSnapshot"), dict) else None)
      if not snapshot:
          continue
      ts_ms = None
      for key in ("localDetectedAtMs", "signalTsMs", "eventTsMs", "t", "actualFillTsMs"):
          if is_finite(raw.get(key)):
              ts_ms = i(raw.get(key))
              break
      side = str(raw.get("side") or snapshot.get("chosenSide") or "").strip().upper() or None
      payload = {
          "event": event,
          "label": "Entry Signal" if event == "enter_signal" else "Entry Blocked",
          "tsMs": ts_ms,
          "offsetSec": round4((ts_ms - session_start_ms) / 1000.0) if ts_ms is not None else None,
          "side": side,
          "signalPx": round6(n(raw.get("signalPx", raw.get("entryPx")))) if is_finite(raw.get("signalPx", raw.get("entryPx"))) else None,
          "entryPx": round6(n(raw.get("entryPx"))) if is_finite(raw.get("entryPx")) else None,
          "reason": str(raw.get("reason") or raw.get("exitReasonRaw") or raw.get("lastAction") or "").strip() or None,
          "snapshot": snapshot,
          "tradeNum": None,
          "tradeKey": None,
      }
      if event == "enter_signal":
          signal_candidates.append(payload)
      else:
          blocked_rows.append(payload)

    used = [False] * len(signal_candidates)
    decision_rows: list[dict[str, Any]] = []
    for trade in trades or []:
        trade_side = str(trade.get("side") or "").strip().upper()
        entry_ts_ms = n(trade.get("entryTsMs"))
        if not trade_side or not math.isfinite(entry_ts_ms):
            continue
        best_idx = None
        best_delta = None
        for idx, cand in enumerate(signal_candidates):
            if used[idx]:
                continue
            if str(cand.get("side") or "").strip().upper() != trade_side:
                continue
            cand_ts_ms = n(cand.get("tsMs"))
            if not math.isfinite(cand_ts_ms) or cand_ts_ms > entry_ts_ms:
                continue
            delta = entry_ts_ms - cand_ts_ms
            if delta < 0 or delta > 30000:
                continue
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_idx = idx
        if best_idx is None:
            continue
        used[best_idx] = True
        matched = dict(signal_candidates[best_idx])
        matched["tradeNum"] = int(trade.get("tradeNum") or 0) or None
        matched["tradeKey"] = str(trade.get("tradeKey") or "").strip() or None
        matched["entryLatencyMs"] = latency_ms(matched.get("tsMs"), trade.get("entryTsMs"))
        decision_rows.append(matched)

    for idx, cand in enumerate(signal_candidates):
        if used[idx]:
            continue
        decision_rows.append(dict(cand))
    decision_rows.extend(blocked_rows)
    decision_rows.sort(key=lambda row: (n(row.get("tsMs"), 0.0), str(row.get("event") or ""), str(row.get("tradeKey") or "")))
    return decision_rows


def infer_entry_fee_mode(enter_row: dict[str, Any], order_row: dict[str, Any] | None) -> str:
    fill_source = str(enter_row.get("fillSource") or "").lower()
    order_type = str((order_row or {}).get("orderType") or enter_row.get("orderType") or "").upper()
    if order_type == "LIMIT" or "limit" in fill_source:
        return "none"
    return "taker"


def infer_exit_fee_mode(fill_row: dict[str, Any], exit_type: str) -> str:
    fs = str(fill_row.get("fillSource") or "").lower()
    if "limit" in fs:
        return "none"
    x = str(exit_type or "").upper()
    if x in {"SETTLE", "SESSION_EXPIRED", "PARTIAL_TP_27", "DERISK"}:
        return "none"
    raw_event = str(fill_row.get("event") or "").lower()
    if raw_event == "exit_partial":
        return "none"
    return "taker"


def partial_stage_key(exit_type: Any) -> str | None:
    x = str(exit_type or "").strip().upper()
    if not x:
        return None
    if "PARTIAL" in x or x == "DERISK":
        return x
    return None


def build_fallback_trades_from_lanes(session: dict[str, Any], session_start_ms: int, timeline_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trades = []
    for idx, lane in enumerate(sorted(session.get("lanes") or [], key=lambda ln: n(ln.get("entryTsMs"))), start=1):
        trade_id = f"{session.get('slug')}-t{idx}"
        row_enter_id = f"{session.get('slug')}-row-fallback-enter-{idx}"
        row_exit_id = f"{session.get('slug')}-row-fallback-exit-{idx}"
        entry_px = n(lane.get("entryPx"))
        exit_px = n(lane.get("exitPx"))
        shares = n(lane.get("shares"))
        entry_ts = i(lane.get("entryTsMs"))
        exit_ts = i(lane.get("exitTsMs"))
        pnl = n(lane.get("pnlUsd"))
        gross = (exit_px - entry_px) * shares if math.isfinite(entry_px) and math.isfinite(exit_px) and math.isfinite(shares) else pnl
        fees = max(0.0, gross - pnl) if math.isfinite(gross) and math.isfinite(pnl) else 0.0
        enter_row = {
            "rowId": row_enter_id,
            "rawEvent": "enter",
            "stage": "fill",
            "label": "ENTER",
            "tsMs": entry_ts,
            "offsetSec": round4((entry_ts - session_start_ms) / 1000.0),
            "side": str(lane.get("side") or "").upper(),
            "px": entry_px,
            "exitType": None,
            "sharesActual": shares,
            "sharesClosedActual": None,
            "sharesRemainingActual": shares,
            "actualNetPnlUsd": None,
            "actualBalanceUsd": None,
            "nominalUsd": n(lane.get("notionalUsd")),
            "tradeNum": idx,
            "role": "entry_fill",
            "legIndex": None,
            "note": "fallback_lane_entry",
        }
        exit_row = {
            "rowId": row_exit_id,
            "rawEvent": "exit",
            "stage": "fill",
            "label": "EXIT",
            "tsMs": exit_ts,
            "offsetSec": round4((exit_ts - session_start_ms) / 1000.0),
            "side": str(lane.get("side") or "").upper(),
            "px": exit_px,
            "exitType": str(lane.get("exitType") or "").upper() or None,
            "sharesActual": shares,
            "sharesClosedActual": shares,
            "sharesRemainingActual": 0.0,
            "actualNetPnlUsd": pnl,
            "actualBalanceUsd": n(lane.get("balanceUsd")),
            "nominalUsd": n(lane.get("notionalUsd")),
            "tradeNum": idx,
            "role": "exit_fill",
            "legIndex": 0,
            "note": str(lane.get("exitReasonRaw") or ""),
        }
        timeline_rows.extend([enter_row, exit_row])
        trades.append({
            "tradeKey": trade_id,
            "tradeNum": idx,
            "side": str(lane.get("side") or "").upper(),
            "entrySignalTsMs": None,
            "entryOrderTsMs": None,
            "entryTsMs": entry_ts,
            "entryPx": entry_px,
            "entryFeeMode": "taker",
            "actualBudgetUsd": n(lane.get("notionalUsd"), 25.0),
            "actualShares": shares,
            "actualEntryFeesUsd": fees,
            "entryRowId": row_enter_id,
            "closeLegs": [{
                "legIndex": 0,
                "signalRowId": None,
                "orderRowId": None,
                "fillRowId": row_exit_id,
                "signalTsMs": None,
                "orderTsMs": None,
                "fillTsMs": exit_ts,
                "exitPx": exit_px,
                "exitType": str(lane.get("exitType") or "").upper() or "",
                "reason": str(lane.get("exitReasonRaw") or ""),
                "actualSharesClosed": shares,
                "shareRatio": 1.0,
                "exitFeeMode": infer_exit_fee_mode(lane, str(lane.get("exitType") or "")),
                "actualNetPnlUsd": pnl,
                "actualGrossPnlUsd": gross,
            }],
            "actualGrossPnlUsd": gross,
            "actualNetPnlUsd": pnl,
            "actualFeesUsd": fees,
            "entryOffsetSec": round4((entry_ts - session_start_ms) / 1000.0),
            "intervalLabel": interval_label((entry_ts - session_start_ms) / 1000.0),
        })
    return trades
def latest_finite_trace_point(trace: dict[str, Any] | None, side: str) -> dict[str, float] | None:
    if not isinstance(trace, dict):
        return None
    x_ms = [int(x) for x in (trace.get("xMs") or []) if is_finite(x)]
    arr = trace.get("up") if norm_side(side) == "UP" else trace.get("down")
    if not isinstance(arr, list):
        return None
    for idx in range(min(len(x_ms), len(arr)) - 1, -1, -1):
        px_val = n(arr[idx], float("nan"))
        ts_ms = x_ms[idx]
        if math.isfinite(px_val):
            return {"tsMs": float(ts_ms), "px": float(px_val)}
    return None


def infer_remaining_shares_for_trade(trade: dict[str, Any]) -> float:
    actual_shares = max(0.0, n(trade.get("actualShares"), 0.0))
    close_legs = trade.get("closeLegs") or []
    if close_legs:
        explicit_remaining = n(((close_legs[-1] or {}).get("timelineRowRef") or {}).get("sharesRemainingActual"), float("nan"))
        if math.isfinite(explicit_remaining):
            return max(0.0, explicit_remaining)
    closed_shares = sum(max(0.0, n(leg.get("actualSharesClosed"), 0.0)) for leg in close_legs)
    return max(0.0, actual_shares - closed_shares)


def synthesize_settle_leg_for_trade(
    trade: dict[str, Any],
    trace: dict[str, Any] | None,
    session_start_ms: int,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    if not isinstance(trade, dict):
        return None
    if any(str(leg.get("exitType") or "").strip().upper() == "SETTLE" for leg in (trade.get("closeLegs") or [])):
        return None
    entry_px = n(trade.get("entryPx"), float("nan"))
    if not math.isfinite(entry_px):
        return None
    remaining_shares = infer_remaining_shares_for_trade(trade)
    if remaining_shares <= 1e-9:
        return None
    final_point = latest_finite_trace_point(trace, str(trade.get("side") or ""))
    if not final_point:
        return None
    last_close_ts_ms = max([n(leg.get("fillTsMs"), 0.0) for leg in (trade.get("closeLegs") or [])] or [n(trade.get("entryTsMs"), 0.0)])
    if final_point["tsMs"] <= last_close_ts_ms:
        return None
    last_side_px = n(final_point.get("px"), float("nan"))
    if not math.isfinite(last_side_px):
        return None
    exit_px = 1.0 if last_side_px > 0.5 else 0.0
    gross_pnl = round6((exit_px - entry_px) * remaining_shares)
    row_id = f"{trade.get('tradeKey')}-settle"
    timeline_row = {
        "rowId": row_id,
        "rawEvent": "exit",
        "stage": "fill",
        "label": "SETTLE",
        "tsMs": int(final_point["tsMs"]),
        "offsetSec": round4((final_point["tsMs"] - session_start_ms) / 1000.0),
        "side": str(trade.get("side") or ""),
        "px": round6(exit_px),
        "exitType": "SETTLE",
        "sharesActual": round6(remaining_shares),
        "sharesClosedActual": round6(remaining_shares),
        "sharesRemainingActual": 0.0,
        "signalPx": None,
        "intendedPx": None,
        "actualFillPx": round6(exit_px),
        "eventTsMs": int(final_point["tsMs"]),
        "actualFillTsMs": int(final_point["tsMs"]),
        "signalTsMs": None,
        "orderTsMs": None,
        "localSignalTsMs": None,
        "localOrderTsMs": None,
        "localAcceptedTsMs": None,
        "localFillObservedTsMs": int(final_point["tsMs"]),
        "signalToOrderMs": None,
        "signalToFillMs": None,
        "orderId": "",
        "orderIdTail": None,
        "mode": str(trade.get("mode") or "paper"),
        "executionMode": "paper",
        "fillSource": "audit_trace_synthesized_settle",
        "decisionSnapshot": None,
        "actualNetPnlUsd": gross_pnl,
        "actualGrossPnlUsd": gross_pnl,
        "actualFeesUsd": 0.0,
        "actualBalanceUsd": None,
        "nominalUsd": n(trade.get("actualBudgetUsd")),
        "tradeNum": trade.get("tradeNum"),
        "role": "exit_fill",
        "legIndex": len(trade.get("closeLegs") or []),
        "note": "reason=session_expired | fillSource=audit_trace_synthesized_settle",
    }
    leg = {
        "legIndex": len(trade.get("closeLegs") or []),
        "signalRowId": None,
        "orderRowId": None,
        "fillRowId": row_id,
        "signalTsMs": None,
        "orderTsMs": None,
        "fillTsMs": int(final_point["tsMs"]),
        "signalPx": None,
        "exitPx": round6(exit_px),
        "exitType": "SETTLE",
        "reason": "session_expired",
        "orderId": None,
        "executionMode": "paper",
        "fillSource": "audit_trace_synthesized_settle",
        "actualSharesClosed": round6(remaining_shares),
        "exitFeeMode": "maker",
        "actualNetPnlUsd": gross_pnl,
        "actualGrossPnlUsd": gross_pnl,
        "fillCount": 1,
        "timelineRowRef": timeline_row,
    }
    return timeline_row, leg


def build_session_timeline_and_trades(session: dict[str, Any], raw_rows: list[dict[str, Any]], trace: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    session_start_ms = i(session.get("startMs") or slug_start_ms(session.get("slug")) or 0)
    rows = sorted(raw_rows, key=lambda r: n(r.get("actualFillTsMs", r.get("eventTsMs", r.get("t"))), 0))
    saw_live_rows = any(
        str(r.get("mode") or "").strip().lower() == "live" or
        str(r.get("executionMode") or "").strip().lower() == "real" or
        str(r.get("fillSource") or "").strip().lower() == "live_exchange"
        for r in rows
    )
    timeline_rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    pending_entry: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"signal": [], "order": []})
    current_trade: dict[str, Any] | None = None
    trade_num = 0
    row_seq = 0

    for raw in rows:
        row_seq += 1
        norm = normalize_event(raw, session_start_ms, f"{session.get('slug')}-row-{row_seq}")
        event = norm["rawEvent"]
        side = norm["side"]

        if event == "enter_signal":
            pending_entry[side]["signal"].append({"raw": raw, "row": norm})
            norm["role"] = "entry_signal"
            timeline_rows.append(norm)
            continue

        if event == "enter_order":
            pending_entry[side]["order"].append({"raw": raw, "row": norm})
            norm["role"] = "entry_order"
            timeline_rows.append(norm)
            continue

        if event == "enter":
            trade_num += 1
            sig = pending_entry[side]["signal"].pop(0) if pending_entry[side]["signal"] else None
            order = pending_entry[side]["order"].pop(0) if pending_entry[side]["order"] else None
            trade_key = f"{session.get('slug')}-t{trade_num}"
            if isinstance((sig or {}).get("row"), dict):
                sig["row"]["tradeNum"] = trade_num
                sig["row"]["tradeKey"] = trade_key
            if isinstance((order or {}).get("row"), dict):
                order["row"]["tradeNum"] = trade_num
                order["row"]["tradeKey"] = trade_key
            norm["tradeNum"] = trade_num
            norm["tradeKey"] = trade_key
            norm["role"] = "entry_fill"
            timeline_rows.append(norm)
            current_trade = {
                "tradeKey": trade_key,
                "tradeNum": trade_num,
                "side": side,
                "mode": str(raw.get("mode") or "").strip().lower() or None,
                "executionMode": str(raw.get("executionMode") or "").strip().lower() or None,
                "fillSource": str(raw.get("fillSource") or "").strip().lower() or None,
                "entrySignalTsMs": (sig or {}).get("row", {}).get("tsMs"),
                "entryOrderTsMs": (order or {}).get("row", {}).get("tsMs"),
                "entryTsMs": norm["tsMs"],
                "entrySignalPx": n((sig or {}).get("raw", {}).get("signalPx", (sig or {}).get("raw", {}).get("entryPx"))),
                "entryPx": n(raw.get("actualFillPx", raw.get("entryPx"))),
                "entryOrderId": str(raw.get("orderId") or "").strip() or None,
                "entryFeeMode": infer_entry_fee_mode(raw, (order or {}).get("raw")),
                "actualBudgetUsd": n(raw.get("notionalUsd"), 25.0),
                "actualShares": n(raw.get("shares")),
                "actualEntryFeesUsd": n(norm.get("actualFeesUsd")),
                "entryRowId": norm["rowId"],
                "closeLegs": [],
                "pendingExitSignals": [],
                "pendingExitOrders": [],
                "partialSignalRows": {},
                "partialOrderRows": {},
                "partialLegByKey": {},
            }
            trades.append(current_trade)
            norm["nominalUsd"] = current_trade["actualBudgetUsd"]
            continue

        if current_trade and side == current_trade["side"]:
            norm["tradeNum"] = current_trade["tradeNum"]
            norm["tradeKey"] = current_trade["tradeKey"]
            if event == "exit_signal":
                p_key = partial_stage_key(raw.get("exitType"))
                if p_key and current_trade["partialSignalRows"].get(p_key):
                    continue
                norm["role"] = "exit_signal"
                current_trade["pendingExitSignals"].append({"raw": raw, "row": norm})
                if p_key:
                    current_trade["partialSignalRows"][p_key] = norm
                timeline_rows.append(norm)
                continue
            if event == "exit_order":
                p_key = partial_stage_key(raw.get("exitType"))
                if p_key and current_trade["partialOrderRows"].get(p_key):
                    continue
                norm["role"] = "exit_order"
                current_trade["pendingExitOrders"].append({"raw": raw, "row": norm})
                if p_key:
                    current_trade["partialOrderRows"][p_key] = norm
                timeline_rows.append(norm)
                continue
            if event in {"exit_partial", "exit"}:
                actual_shares_closed = n(raw.get("sharesClosed", raw.get("sharesRequested", raw.get("shares"))))
                exit_px = n(raw.get("exitPx"))
                entry_px = n(current_trade["entryPx"])
                actual_gross = (exit_px - entry_px) * actual_shares_closed if math.isfinite(exit_px) and math.isfinite(entry_px) and math.isfinite(actual_shares_closed) else n(raw.get("pnlUsd"))
                p_key = partial_stage_key(raw.get("exitType")) if event == "exit_partial" else None
                if p_key and current_trade["partialLegByKey"].get(p_key):
                    leg = current_trade["partialLegByKey"][p_key]
                    leg["fillTsMs"] = norm["tsMs"]
                    leg["reason"] = str(raw.get("exitReasonRaw") or raw.get("reason") or leg.get("reason") or "")
                    leg["actualSharesClosed"] = round6(n(leg.get("actualSharesClosed"), 0.0) + actual_shares_closed)
                    leg["actualNetPnlUsd"] = round6(n(leg.get("actualNetPnlUsd"), 0.0) + n(raw.get("pnlUsd")))
                    leg["actualGrossPnlUsd"] = round6(n(leg.get("actualGrossPnlUsd"), 0.0) + actual_gross)
                    leg["fillCount"] = int(leg.get("fillCount") or 1) + 1
                    leg["lastFillRowId"] = norm["rowId"]
                    row_ref = leg.get("timelineRowRef")
                    if isinstance(row_ref, dict):
                        row_ref["tsMs"] = norm["tsMs"]
                        row_ref["offsetSec"] = round4((n(norm["tsMs"]) - session_start_ms) / 1000.0) if is_finite(norm["tsMs"]) else row_ref.get("offsetSec")
                        row_ref["sharesActual"] = leg["actualSharesClosed"]
                        row_ref["sharesClosedActual"] = leg["actualSharesClosed"]
                        row_ref["sharesRemainingActual"] = n(raw.get("sharesRemaining"))
                        row_ref["actualNetPnlUsd"] = leg["actualNetPnlUsd"]
                        row_ref["actualBalanceUsd"] = n(raw.get("balanceUsd"))
                        note_bits = [b for b in [
                            f"exitReasonRaw={leg['reason']}" if leg.get("reason") else None,
                            f"fillTimestampSource={raw.get('fillTimestampSource')}" if raw.get("fillTimestampSource") else None,
                            f"aggregatedPartialFills={leg['fillCount']}",
                        ] if b]
                        row_ref["note"] = " | ".join(note_bits)
                    continue
                norm["role"] = "exit_fill"
                leg_index = len(current_trade["closeLegs"])
                norm["legIndex"] = leg_index
                signal = current_trade["pendingExitSignals"].pop(0) if current_trade["pendingExitSignals"] else None
                order = current_trade["pendingExitOrders"].pop(0) if current_trade["pendingExitOrders"] else None
                if isinstance((signal or {}).get("row"), dict):
                    signal["row"]["tradeKey"] = current_trade["tradeKey"]
                if isinstance((order or {}).get("row"), dict):
                    order["row"]["tradeKey"] = current_trade["tradeKey"]
                timeline_rows.append(norm)
                leg = {
                    "legIndex": leg_index,
                    "signalRowId": (signal or {}).get("row", {}).get("rowId"),
                    "orderRowId": (order or {}).get("row", {}).get("rowId"),
                    "fillRowId": norm["rowId"],
                    "signalTsMs": (signal or {}).get("row", {}).get("tsMs"),
                    "orderTsMs": (order or {}).get("row", {}).get("tsMs"),
                    "fillTsMs": norm["tsMs"],
                    "signalPx": n((signal or {}).get("row", {}).get("px")),
                    "exitPx": exit_px,
                    "exitType": str(raw.get("exitType") or "").upper(),
                    "reason": str(raw.get("exitReasonRaw") or raw.get("reason") or ""),
                    "orderId": str(raw.get("orderId") or "").strip() or None,
                    "executionMode": str(raw.get("executionMode") or "").strip().lower() or None,
                    "fillSource": str(raw.get("fillSource") or "").strip().lower() or None,
                    "actualSharesClosed": actual_shares_closed,
                    "exitFeeMode": infer_exit_fee_mode(raw, str(raw.get("exitType") or "")),
                    "actualNetPnlUsd": n(raw.get("pnlUsd")),
                    "actualGrossPnlUsd": actual_gross,
                    "fillCount": 1,
                    "timelineRowRef": norm,
                }
                current_trade["closeLegs"].append(leg)
                norm["nominalUsd"] = n(current_trade.get("actualBudgetUsd"))
                if p_key:
                    current_trade["partialLegByKey"][p_key] = leg
                if event == "exit":
                    current_trade = None
                continue

        timeline_rows.append(norm)

    if not trades and not saw_live_rows:
        trades = build_fallback_trades_from_lanes(session, session_start_ms, timeline_rows)

    for trade in trades:
        synthetic_settle = synthesize_settle_leg_for_trade(trade, trace, session_start_ms)
        if synthetic_settle:
            timeline_row, settle_leg = synthetic_settle
            timeline_rows.append(timeline_row)
            trade["closeLegs"].append(settle_leg)
        trade["entrySignalToFillMs"] = latency_ms(trade.get("entrySignalTsMs"), trade.get("entryTsMs"))
        trade["entrySignalToOrderMs"] = latency_ms(trade.get("entrySignalTsMs"), trade.get("entryOrderTsMs"))
        trade.update(price_delta_metrics(trade.get("entrySignalPx"), trade.get("entryPx")))
        for leg in trade["closeLegs"]:
            leg["signalToFillMs"] = latency_ms(leg.get("signalTsMs"), leg.get("fillTsMs"))
            leg.update(price_delta_metrics(leg.get("signalPx"), leg.get("exitPx")))
        actual_row_metrics = actual_trade_row_metrics(trade)
        actual_gross = sum(n(row.get("gross"), 0.0) for row in actual_row_metrics.values())
        actual_net = sum(n(row.get("net"), 0.0) for row in actual_row_metrics.values())
        actual_fees = sum(n(row.get("fees"), 0.0) for row in actual_row_metrics.values())
        total_closed = sum(max(0.0, n(leg.get("actualSharesClosed"), 0.0)) for leg in trade["closeLegs"])
        running = 0.0
        for idx, leg in enumerate(trade["closeLegs"]):
            if idx == len(trade["closeLegs"]) - 1:
                ratio = max(0.0, 1.0 - running)
            else:
                ratio = 0.0 if total_closed <= 0 else max(0.0, n(leg.get("actualSharesClosed"), 0.0) / total_closed)
                running += ratio
            leg["shareRatio"] = round6(ratio)
        trade["actualGrossPnlUsd"] = round6(actual_gross)
        trade["actualNetPnlUsd"] = round6(actual_net)
        trade["actualFeesUsd"] = round6(actual_fees)
        trade["actualRowMetrics"] = actual_row_metrics
        trade["entryOffsetSec"] = round4((n(trade.get("entryTsMs")) - session_start_ms) / 1000.0) if is_finite(trade.get("entryTsMs")) else None
        trade["intervalLabel"] = interval_label(trade["entryOffsetSec"])
        trade["entryOrderIdTail"] = short_order_id(trade.get("entryOrderId")) or None
        for leg in trade["closeLegs"]:
            leg["orderIdTail"] = short_order_id(leg.get("orderId")) or None

    actual_rows_by_id = {}
    for trade in trades:
        for row_id, metrics in (trade.get("actualRowMetrics") or {}).items():
            actual_rows_by_id[row_id] = metrics
    for row in timeline_rows:
        metrics = actual_rows_by_id.get(str(row.get("rowId") or ""))
        if metrics:
            if not is_finite(row.get("sharesActual")) and is_finite(metrics.get("shares")):
                row["sharesActual"] = round6(n(metrics.get("shares")))
            row["actualGrossPnlUsd"] = round6(n(metrics.get("gross"))) if is_finite(metrics.get("gross")) else row.get("actualGrossPnlUsd")
            row["actualFeesUsd"] = round6(n(metrics.get("fees"))) if is_finite(metrics.get("fees")) else row.get("actualFeesUsd")
            row["actualNetPnlUsd"] = round6(n(metrics.get("net"))) if is_finite(metrics.get("net")) else row.get("actualNetPnlUsd")

    timeline_rows.sort(key=lambda r: (n(r.get("tsMs"), 0), str(r.get("rowId") or "")))
    return timeline_rows, trades


def compute_default_bets(sessions: list[dict[str, Any]], base_bet_usd: float = 25.0) -> dict[str, float]:
    trade_nums: set[int] = set()
    for session in sessions:
        for trade in session.get("trades") or []:
            trade_num = int(trade.get("tradeNum") or 0)
            if trade_num > 0:
                trade_nums.add(trade_num)
    out: dict[str, float] = {}
    normalized_base_bet = round4(base_bet_usd if math.isfinite(base_bet_usd) and base_bet_usd > 0 else 25.0)
    max_trade_num = max(4, max(trade_nums, default=0))
    for trade_num in range(1, max_trade_num + 1):
        out[str(trade_num)] = normalized_base_bet
    return out


def default_gold_selection_keys_from_actual_run(
    sessions: list[dict[str, Any]],
    default_bets: dict[str, float],
) -> list[str]:
    selected: set[str] = set()
    for session in sessions:
        for trade in session.get("trades") or []:
            trade_num = str(int(trade.get("tradeNum") or 0))
            if trade_num == "0":
                continue
            interval = str(trade.get("intervalLabel") or "").strip()
            if not interval:
                continue
            actual_budget = n(trade.get("actualBudgetUsd"), float("nan"))
            default_bet = n(default_bets.get(trade_num), float("nan"))
            if math.isfinite(actual_budget) and math.isfinite(default_bet) and actual_budget > (default_bet + 1e-6):
                selected.add(f"{trade_num}|{interval}")
    return sorted(selected, key=lambda key: tuple(
        int(part) if idx == 0 and str(part).isdigit() else str(part)
        for idx, part in enumerate(key.split("|", 1))
    ))


def model_trade(trade: dict[str, Any], bet_usd: float) -> dict[str, Any]:
    actual_budget = n(trade.get("actualBudgetUsd"), 0.0)
    scale = (bet_usd / actual_budget) if (actual_budget > 0 and math.isfinite(actual_budget)) else 0.0
    actual_rows = actual_trade_row_metrics(trade)
    total_shares = round6(n(trade.get("actualShares"), 0.0) * scale)
    entry_row_id = trade.get("entryRowId")
    entry_fees = round6(n((actual_rows.get(entry_row_id) or {}).get("fees"), 0.0) * scale) if entry_row_id else 0.0
    remaining = total_shares
    gross = 0.0
    fees = entry_fees
    net = -entry_fees
    row_metrics = {}
    if entry_row_id:
        row_metrics[entry_row_id] = {
            "shares": round6(total_shares),
            "gross": 0.0,
            "fees": round6(entry_fees),
            "net": round6(-entry_fees),
        }
    close_legs = []
    legs = trade.get("closeLegs") or []
    for idx, leg in enumerate(legs):
        actual_leg_shares = n(leg.get("actualSharesClosed"), 0.0)
        if idx == len(legs) - 1:
            shares_closed = round6(max(0.0, remaining))
        else:
            shares_closed = round6(max(0.0, min(remaining, actual_leg_shares * scale)))
        remaining = max(0.0, remaining - shares_closed)
        fill_row_id = leg.get("fillRowId")
        actual_leg_row = actual_rows.get(fill_row_id) if fill_row_id else None
        actual_gross = n((actual_leg_row or {}).get("gross"), n(leg.get("actualGrossPnlUsd"), 0.0))
        actual_net = n((actual_leg_row or {}).get("net"), n(leg.get("actualNetPnlUsd"), actual_gross))
        actual_fees = n((actual_leg_row or {}).get("fees"), max(0.0, actual_gross - actual_net))
        leg_gross = round6(actual_gross * scale)
        leg_fees = round6(actual_fees * scale)
        leg_net = round6(actual_net * scale)
        gross += leg_gross
        fees += leg_fees
        net += leg_net
        if fill_row_id:
            row_metrics[fill_row_id] = {
                "shares": round6(shares_closed),
                "gross": round6(leg_gross),
                "fees": round6(leg_fees),
                "net": round6(leg_net),
            }
        close_legs.append({
            "legIndex": leg.get("legIndex"),
            "fillRowId": fill_row_id,
            "sharesClosed": round6(shares_closed),
            "grossPnlUsd": round6(leg_gross),
            "feesUsd": round6(leg_fees),
            "netPnlUsd": round6(leg_net),
            "exitPx": round6(n(leg.get("exitPx"), 0.0)),
            "exitType": leg.get("exitType"),
        })
    return {
        "betUsd": round6(bet_usd),
        "entryFeeUsd": round6(entry_fees),
        "shares": round6(total_shares),
        "grossPnlUsd": round6(gross),
        "feesUsd": round6(fees),
        "netPnlUsd": round6(net),
        "rowMetrics": row_metrics,
        "closeLegs": close_legs,
    }


def actual_trade_row_metrics(trade: dict[str, Any]) -> dict[str, dict[str, float]]:
    entry_row_id = trade.get("entryRowId")
    entry_shares = n(trade.get("actualShares"), 0.0)
    total_trade_fees = max(0.0, n(trade.get("actualFeesUsd"), 0.0))
    explicit_entry_fee = n(trade.get("actualEntryFeesUsd"), float("nan"))
    out: dict[str, dict[str, float]] = {}
    leg_fee_total = 0.0
    for leg in trade.get("closeLegs") or []:
        fill_row_id = leg.get("fillRowId")
        gross = n(leg.get("actualGrossPnlUsd"), 0.0)
        net = n(leg.get("actualNetPnlUsd"), gross)
        fees = max(0.0, gross - net)
        if not (fees > 0.0) and str(leg.get("exitFeeMode") or "").strip().lower() == "taker":
            fees = calc_taker_fee_usd(leg.get("exitPx"), leg.get("actualSharesClosed"))
            if math.isfinite(gross) and (not math.isfinite(n(leg.get("actualNetPnlUsd"))) or abs(net - gross) <= 1e-9):
                net = gross - fees
        leg_fee_total += fees
        if fill_row_id:
            out[fill_row_id] = {
                "shares": round6(n(leg.get("actualSharesClosed"), 0.0)),
                "gross": round6(gross),
                "fees": round6(fees),
                "net": round6(net),
            }
    if math.isfinite(explicit_entry_fee) and explicit_entry_fee >= 0:
        entry_fee = explicit_entry_fee
    else:
        entry_fee = max(0.0, total_trade_fees - leg_fee_total)
        if not (entry_fee > 0.0) and str(trade.get("entryFeeMode") or "").strip().lower() == "taker":
            entry_fee = compute_entry_from_budget(trade.get("actualBudgetUsd"), trade.get("entryPx"), "taker").get("entryFeeUsd", 0.0)
    if entry_row_id:
        out[entry_row_id] = {"shares": round6(entry_shares), "gross": 0.0, "fees": round6(entry_fee), "net": round6(-entry_fee)}
    return out


def summarize_by_trade(trades: list[dict[str, Any]], models: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_trade: dict[str, dict[str, Any]] = {}
    for trade in trades:
        key = str(int(trade.get("tradeNum") or 0))
        model = models[trade["tradeKey"]]
        row = by_trade.setdefault(key, {"count": 0, "wins": 0, "losses": 0, "grossPnlUsd": 0.0, "feesUsd": 0.0, "netPnlUsd": 0.0})
        baseline_pnl = n(model["netPnlUsd"], 0.0)
        row["count"] += 1
        row["grossPnlUsd"] += n(model["grossPnlUsd"], 0.0)
        row["feesUsd"] += n(model["feesUsd"], 0.0)
        row["netPnlUsd"] += n(model["netPnlUsd"], 0.0)
        if baseline_pnl > 0:
            row["wins"] += 1
        elif baseline_pnl < 0:
            row["losses"] += 1
    for row in by_trade.values():
        total = row["wins"] + row["losses"]
        row["winRatePct"] = round4((row["wins"] / total) * 100.0) if total else None
        row["pnlPerTradeUsd"] = round4(row["netPnlUsd"] / row["count"]) if row["count"] else 0.0
    return by_trade


def summarize_by_trade_interval(trades: list[dict[str, Any]], models: dict[str, dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for trade in trades:
        trade_key = str(int(trade.get("tradeNum") or 0))
        bucket = str(trade.get("intervalLabel") or "unknown")
        model = models[trade["tradeKey"]]
        row = out[trade_key].setdefault(bucket, {"count": 0, "wins": 0, "losses": 0, "grossPnlUsd": 0.0, "feesUsd": 0.0, "netPnlUsd": 0.0})
        baseline_pnl = n(model["netPnlUsd"], 0.0)
        row["count"] += 1
        row["grossPnlUsd"] += n(model["grossPnlUsd"], 0.0)
        row["feesUsd"] += n(model["feesUsd"], 0.0)
        row["netPnlUsd"] += n(model["netPnlUsd"], 0.0)
        if baseline_pnl > 0:
            row["wins"] += 1
        elif baseline_pnl < 0:
            row["losses"] += 1
    for trade_map in out.values():
        for row in trade_map.values():
            total = row["wins"] + row["losses"]
            row["winRatePct"] = round4((row["wins"] / total) * 100.0) if total else None
            row["pnlPerTradeUsd"] = round4(row["netPnlUsd"] / row["count"]) if row["count"] else 0.0
    return out


def compute_max_drawdown_from_equity_points(points: list[float]) -> dict[str, float]:
    if not points:
        return {"maxDrawdownUsd": 0.0, "maxDrawdownPct": 0.0}
    peak = n(points[0], 0.0)
    max_dd = 0.0
    peak_for_max = peak if peak > 0 else 0.0
    for raw in points:
        bal = n(raw, 0.0)
        if bal > peak:
            peak = bal
        dd = max(0.0, peak - bal)
        if dd > max_dd:
            max_dd = dd
            peak_for_max = peak
    max_dd_pct = ((max_dd / peak_for_max) * 100.0) if peak_for_max > 0 else 0.0
    return {
        "maxDrawdownUsd": round6(max_dd),
        "maxDrawdownPct": round6(max_dd_pct),
    }


def load_prior_doc_for_reuse(out_json: str, cfg: SourceConfig, run_num: int) -> dict[str, Any] | None:
    try:
        p = Path(out_json)
        if not p.exists():
            return None
        doc = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            return None
        run = doc.get("run") or {}
        source = doc.get("source") or {}
        if int(run.get("runNum") or 0) != int(run_num):
            return None
        if str(source.get("hostPort") or "") != str(cfg.host_port):
            return None
        return doc
    except Exception:
        return None


def load_prior_session_artifacts_for_reuse(run_dir: str, run_num: int) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    try:
        for session_dir in [run_audit_session_dir_for_run(run_dir), Path(run_dir) / "session_audits"]:
            if not session_dir.exists():
                continue
            for path in session_dir.glob("*.compact.json"):
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    continue
                if int(payload.get("runNum") or 0) != int(run_num):
                    continue
                slug = str(payload.get("slug") or "").strip()
                session = payload.get("session")
                if slug and isinstance(session, dict):
                    out[slug] = session
    except Exception:
        return {}
    return out


def session_cache_sig(session: dict[str, Any], slug: str, events_for_slug: list[dict[str, Any]], current_slug: str, truth_row: dict[str, Any] | None = None) -> str:
    lanes = []
    for lane in session.get("lanes") or []:
        if not isinstance(lane, dict):
            continue
        lanes.append({
            "tradeNum": lane.get("tradeNum"),
            "side": lane.get("side"),
            "entryTsMs": lane.get("entryTsMs"),
            "exitTsMs": lane.get("exitTsMs"),
            "entryPx": lane.get("entryPx"),
            "exitPx": lane.get("exitPx"),
            "pnlUsd": lane.get("pnlUsd"),
            "grossPnlUsd": lane.get("grossPnlUsd"),
            "feesUsd": lane.get("feesUsd"),
            "exitType": lane.get("exitType"),
            "shares": lane.get("shares"),
            "notionalUsd": lane.get("notionalUsd"),
        })
    event_ts = [i(r.get("t")) for r in events_for_slug if is_finite(r.get("t"))]
    payload = {
        "slug": slug,
        "isCurrent": slug == current_slug,
        "pnlUsd": session.get("pnlUsd"),
        "grossPnlUsd": session.get("grossPnlUsd"),
        "feesUsd": session.get("feesUsd"),
        "balanceUsd": session.get("balanceUsd"),
        "excludeFromPnl": bool(session.get("excludeFromPnl")),
        "auditIssues": list(session.get("auditIssues") or []),
        "laneCount": len(lanes),
        "lanes": lanes,
        "eventCount": len(events_for_slug),
        "lastEventTsMs": max(event_ts) if event_ts else None,
        "truthRow": truth_row or None,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def trace_cache_sig(trace: dict[str, Any] | None, quality: dict[str, Any] | None) -> str:
    payload = {
        "source": (trace or {}).get("source") if isinstance(trace, dict) else None,
        "sourceFile": (trace or {}).get("sourceFile") if isinstance(trace, dict) else None,
        "sampleCount": (quality or {}).get("sampleCount") if isinstance(quality, dict) else None,
        "coveragePct": (quality or {}).get("coveragePct") if isinstance(quality, dict) else None,
        "missingPct": (quality or {}).get("missingPct") if isinstance(quality, dict) else None,
        "reasons": list((quality or {}).get("reasons") or []) if isinstance(quality, dict) else [],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def truth_cache_sig(truth_row: dict[str, Any] | None, venue_truth: dict[str, Any] | None) -> str:
    payload = {
        "truthRow": truth_row or None,
        "venueTruth": venue_truth or None,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def run_audit_session_dir(out_json: str) -> Path:
    return Path(out_json).resolve().parent / RUN_AUDIT_SESSION_CACHE_DIR


def run_audit_state_path(out_json: str) -> Path:
    return Path(out_json).resolve().parent / RUN_AUDIT_STATE_FILE


def run_audit_session_dir_for_run(run_dir: str) -> Path:
    return Path(run_dir).resolve() / RUN_AUDIT_SESSION_CACHE_DIR


def run_audit_state_path_for_run(run_dir: str) -> Path:
    return Path(run_dir).resolve() / RUN_AUDIT_STATE_FILE


def cached_session_was_current(cached_session: dict[str, Any] | None) -> bool:
    if not isinstance(cached_session, dict):
        return False
    meta = cached_session.get("cacheMeta")
    return bool((meta or {}).get("wasCurrentSession")) if isinstance(meta, dict) else False


def run_summary_closed(summary: dict[str, Any] | None) -> bool:
    if not isinstance(summary, dict):
        return False
    status = str(summary.get("status") or "").strip().lower()
    if status in {"running", "starting", "watching"}:
        return False
    ended_at_ms = n(summary.get("endedAtMs"))
    if math.isfinite(ended_at_ms) and ended_at_ms > 0:
        return True
    return status in {"stopped", "finished", "completed", "error"}


def classify_session_mode(raw_rows: list[dict[str, Any]], trades: list[dict[str, Any]]) -> str:
    saw_live = any(
        str(r.get("mode") or "").strip().lower() == "live" or
        str(r.get("executionMode") or "").strip().lower() == "real" or
        str(r.get("fillSource") or "").strip().lower() == "live_exchange"
        for r in (raw_rows or [])
    )
    saw_paper = any(
        str(r.get("mode") or "").strip().lower() == "paper" or
        str(r.get("executionMode") or "").strip().lower() == "paper"
        for r in (raw_rows or [])
    )
    if not saw_live:
        saw_live = any(
            str(t.get("mode") or "").strip().lower() == "live" or
            str(t.get("executionMode") or "").strip().lower() == "real" or
            str(t.get("fillSource") or "").strip().lower() == "live_exchange"
            for t in (trades or [])
        )
    if not saw_paper:
        saw_paper = any(
            str(t.get("mode") or "").strip().lower() == "paper" or
            str(t.get("executionMode") or "").strip().lower() == "paper"
            for t in (trades or [])
        )
    if saw_live and saw_paper:
        return "mixed"
    if saw_live:
        return "live"
    if saw_paper:
        return "paper"
    return "unknown"


def session_has_unresolved_positions(
    session: dict[str, Any] | None,
    trades: list[dict[str, Any]] | None = None,
    timeline_rows: list[dict[str, Any]] | None = None,
) -> bool:
    sess = session if isinstance(session, dict) else {}
    normalized_trades = list(trades or [])
    normalized_rows = list(timeline_rows or [])

    for trade in normalized_trades:
        actual_shares = max(0.0, n(trade.get("actualShares"), 0.0))
        close_legs = trade.get("closeLegs") or []
        closed_shares = sum(max(0.0, n(leg.get("actualSharesClosed"), 0.0)) for leg in close_legs)
        if actual_shares > 0 and not close_legs:
            return True
        if actual_shares > 0 and closed_shares + 1e-6 < actual_shares:
            return True

    if normalized_trades or normalized_rows:
        return False

    for lane in (sess.get("lanes") or []):
        entered = math.isfinite(n(lane.get("entryTsMs"))) or math.isfinite(n(lane.get("entryPx")))
        if not entered:
            continue
        if lane.get("closed") is False:
            return True
        remaining = n(lane.get("sharesRemaining", lane.get("remainingShares")), float("nan"))
        if math.isfinite(remaining) and remaining > 1e-6:
            return True
        if not math.isfinite(n(lane.get("exitTsMs"))):
            return True

    for path in (sess.get("sidePaths") or []):
        entered = bool(path.get("entered")) or math.isfinite(n(path.get("entryTsMs"))) or math.isfinite(n(path.get("entryPx")))
        if not entered:
            continue
        if not bool(path.get("closed")):
            return True
        remaining = n(path.get("sharesRemaining", path.get("remainingShares")), float("nan"))
        if math.isfinite(remaining) and remaining > 1e-6:
            return True
        if not math.isfinite(n(path.get("exitTsMs"))):
            return True

    return False


def session_reconcile_meta(end_ms: Any, unresolved_position: bool, now_ms: int) -> dict[str, Any]:
    if not unresolved_position:
        return {
            "reconcileStatus": "resolved",
            "reconcileAgeMs": None,
            "includedInActualTotals": True,
        }
    ended_ms = i(end_ms or 0)
    age_ms = max(0, now_ms - ended_ms) if ended_ms > 0 else None
    if age_ms is None or age_ms < RECONCILE_SOFT_PENDING_MS:
        status = "soft-pending"
    elif age_ms < RECONCILE_HARD_WARNING_MS:
        status = "warning"
    elif age_ms < RECONCILE_STALE_MS:
        status = "hard-warning"
    else:
        status = "stale"
    return {
        "reconcileStatus": status,
        "reconcileAgeMs": age_ms,
        "includedInActualTotals": False,
    }


def safe_avg(values: list[float]) -> float | None:
    vals = [float(v) for v in values if math.isfinite(v)]
    if not vals:
        return None
    return sum(vals) / len(vals)


def session_has_run_window_evidence(
    slug: str,
    session: dict[str, Any] | None,
    session_events: list[dict[str, Any]],
    venue_truth: dict[str, Any] | None,
    started_at_ms: int | None,
    ended_at_ms: int | None,
) -> bool:
    if not (isinstance(started_at_ms, int) and started_at_ms > 0):
        return True
    start_guard_ms = started_at_ms - 10_000
    end_guard_ms = ended_at_ms + 10_000 if isinstance(ended_at_ms, int) and ended_at_ms > 0 else None

    def in_window(ts_ms: Any) -> bool:
        ts = i(ts_ms or 0)
        if ts <= 0:
            return False
        if ts < start_guard_ms:
            return False
        if end_guard_ms is not None and ts > end_guard_ms:
            return False
        return True

    sess = session if isinstance(session, dict) else {}
    session_start_ms = i(sess.get("startMs") or slug_start_ms(slug) or 0)
    session_end_ms = i(sess.get("endMs") or (session_start_ms + SESSION_WINDOW_SEC * 1000 if session_start_ms else 0))
    if session_start_ms > 0 and session_start_ms >= start_guard_ms:
        if end_guard_ms is None or session_start_ms <= end_guard_ms:
            return True
    if session_end_ms > 0 and session_end_ms >= start_guard_ms:
        if end_guard_ms is None or session_end_ms <= end_guard_ms + SESSION_WINDOW_SEC * 1000:
            return True

    for row in session_events or []:
        if in_window(row.get("t")):
            return True

    vt = venue_truth if isinstance(venue_truth, dict) else {}
    if in_window(vt.get("capturedAtMs")) or in_window(vt.get("sessionStartMs")) or in_window(vt.get("sessionEndMs")):
        return True
    for fill in vt.get("fills") or []:
        if isinstance(fill, dict) and (
            in_window(fill.get("t"))
            or in_window(fill.get("tsMs"))
            or in_window(fill.get("filledAtMs"))
            or in_window(fill.get("matchTimeMs"))
        ):
            return True
    return False


def build_doc(cfg: SourceConfig, run_num: int, out_html: str, out_json: str, t_overrides: dict[str, float | None]) -> dict[str, Any]:
    build_started = time.perf_counter()
    phase_started = build_started
    if cfg.mode == "local":
        if not cfg.run_dir:
            raise RuntimeError("local mode requires --run-dir")
        run_dir = cfg.run_dir
        summary_path = str(Path(run_dir) / "summary.json")
    else:
        run_dir = f"{cfg.remote_root}/trade_logs/hosts/host_{cfg.host_port}/multi_runs/run_{run_num}"
        summary_path = f"{run_dir}/summary.json"
    summary = read_json(cfg, summary_path)
    started_at_ms = i(summary.get("startedAtMs") or summary.get("launchedAtMs") or 0)
    now_ms = int(time.time() * 1000)
    ended_at_ms = (
        i(summary.get("endedAtMs"))
        if is_finite(summary.get("endedAtMs"))
        else (now_ms if str(summary.get("status") or "").lower() == "running" else None)
    )

    strategy_id = str(summary.get("strategyId") or "").strip().lower()
    run_id = str(summary.get("runId") or "").strip()
    run_identity_qs = {
        "runNum": str(run_num),
        "runId": run_id,
        "startedAtMs": str(started_at_ms) if started_at_ms else "",
    }

    def audit_url(base_path: str, extra: dict[str, Any] | None = None) -> str:
        params = dict(run_identity_qs)
        for k in list(params.keys()):
            if not params.get(k):
                params.pop(k, None)
        for k, v in (extra or {}).items():
            if v is None:
                continue
            sv = str(v).strip()
            if sv:
                params[k] = sv
        from urllib.parse import urlencode
        return f"{base_path}?{urlencode(params)}"

    index_path = f"{run_dir}/index_{strategy_id}_run_{run_num}.json"
    if not file_exists(cfg, index_path):
        raise RuntimeError(f"Index file not found: {index_path}")
    events_path = f"{run_dir}/events.jsonl"
    telemetry_path = f"{run_dir}/telemetry.jsonl"
    session_trace_path = str(Path(run_dir).parent.parent / "05_session_trace_run_1.jsonl")
    truth_path = f"{run_dir}/{SESSION_RESOLUTION_TRUTH_FILE}"
    index = read_json(cfg, index_path)
    events = read_jsonl(cfg, events_path)
    truth_by_slug = read_resolution_truth_by_slug(cfg, run_dir)
    venue_truth_by_slug = read_session_venue_truth_by_slug(cfg, run_dir)
    session_slug_set = {
        str(s.get("slug") or "").strip()
        for s in (index.get("sessions") or [])
        if str(s.get("slug") or "").strip()
    }
    telemetry_trace_idx: dict[str, dict[str, Any]] = {}
    prior_doc = load_prior_doc_for_reuse(out_json, cfg, run_num)
    prior_sessions_by_slug = {
        str(s.get("slug") or ""): s
        for s in ((prior_doc or {}).get("sessions") or [])
        if isinstance(s, dict) and str(s.get("slug") or "").strip()
    }
    if cfg.mode == "local":
        prior_sessions_by_slug.update(load_prior_session_artifacts_for_reuse(run_dir, run_num))
    current_slug = str(summary.get("marketSlug") or index.get("run", {}).get("marketSlug") or "").strip()
    run_is_closed = run_summary_closed(summary)
    trace_index_elapsed_ms = int(round((time.perf_counter() - phase_started) * 1000.0))

    events_by_slug: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        slug = str(row.get("marketSlug") or "").strip()
        if slug:
            events_by_slug[slug].append(row)
    # For local run audits, prefer the run's own telemetry traces and avoid indexing the
    # much larger recorder directories unless we truly need that fallback. This keeps
    # memory usage low enough for live route generation on the host.
    if cfg.mode == "local":
        local_trace_idx, remote_trace_idx = {}, {}
    else:
        local_trace_idx, remote_trace_idx = build_trace_indexes(cfg)
    run_index_trace_idx = build_run_index_trace_index(run_dir)

    indexed_sessions = list(index.get("sessions") or [])
    sessions_by_slug: dict[str, dict[str, Any]] = {}
    for session in indexed_sessions:
        slug = str(session.get("slug") or "").strip()
        if slug:
            if not session_has_run_window_evidence(
                slug,
                session,
                events_by_slug.get(slug) or [],
                venue_truth_by_slug.get(slug),
                started_at_ms,
                ended_at_ms,
            ):
                continue
            sessions_by_slug[slug] = session
    for slug, session_events in events_by_slug.items():
        if not session_has_run_window_evidence(
            slug,
            sessions_by_slug.get(slug),
            session_events,
            venue_truth_by_slug.get(slug),
            started_at_ms,
            ended_at_ms,
        ):
            continue
        if slug in sessions_by_slug:
            continue
        synthetic_start_ms = slug_start_ms(slug) or i(min([n(r.get("t"), float("inf")) for r in session_events] or [0]))
        sessions_by_slug[slug] = {
            "slug": slug,
            "startMs": synthetic_start_ms,
            "endMs": synthetic_start_ms + SESSION_WINDOW_SEC * 1000 if synthetic_start_ms else None,
            "pnlUsd": 0.0,
            "grossPnlUsd": 0.0,
            "feesUsd": 0.0,
            "balanceUsd": None,
            "excludeFromPnl": False,
            "auditIssues": ["synthetic_from_live_events"],
            "lanes": [],
        }
    for slug, venue_truth in venue_truth_by_slug.items():
        if not session_has_run_window_evidence(
            slug,
            sessions_by_slug.get(slug),
            events_by_slug.get(slug) or [],
            venue_truth,
            started_at_ms,
            ended_at_ms,
        ):
            continue
        if slug in sessions_by_slug:
            continue
        synthetic_start_ms = slug_start_ms(slug) or i(venue_truth.get("startMs") or 0)
        sessions_by_slug[slug] = {
            "slug": slug,
            "startMs": synthetic_start_ms,
            "endMs": synthetic_start_ms + SESSION_WINDOW_SEC * 1000 if synthetic_start_ms else None,
            "pnlUsd": 0.0,
            "grossPnlUsd": 0.0,
            "feesUsd": 0.0,
            "balanceUsd": None,
            "excludeFromPnl": False,
            "auditIssues": ["synthetic_from_venue_truth"],
            "lanes": [],
        }
    sessions = sorted(sessions_by_slug.values(), key=lambda s: n(s.get("startMs"), slug_start_ms(s.get("slug")) or 0))
    session_order_slugs = [str(s.get("slug") or "").strip() for s in sessions if str(s.get("slug") or "").strip()]
    tail_mutable_slugs = set(session_order_slugs[-RUN_AUDIT_INCREMENTAL_TAIL_SESSIONS:])
    uncached_session_slugs = {
        slug for slug in session_order_slugs
        if slug and slug not in prior_sessions_by_slug
    }
    unresolved_session_slugs = {
        str(s.get("slug") or "").strip()
        for s in sessions
        if str(s.get("slug") or "").strip() and session_has_unresolved_positions(s)
    }
    mutable_slugs = set(tail_mutable_slugs) | set(uncached_session_slugs) | set(unresolved_session_slugs)
    trace_svg_fallback_slugs = set(session_order_slugs[-RUN_AUDIT_INLINE_DETAIL_SESSION_LIMIT:])
    if current_slug and not run_is_closed:
        mutable_slugs.add(current_slug)
    if current_slug and current_slug not in sessions_by_slug:
        synthetic_start_ms = slug_start_ms(current_slug) or (i(summary.get("startedAtMs") or 0) // (SESSION_WINDOW_SEC * 1000) * SESSION_WINDOW_SEC * 1000)
        sessions_by_slug[current_slug] = {
            "slug": current_slug,
            "startMs": synthetic_start_ms,
            "endMs": synthetic_start_ms + SESSION_WINDOW_SEC * 1000 if synthetic_start_ms else None,
            "pnlUsd": 0.0,
            "grossPnlUsd": 0.0,
            "feesUsd": 0.0,
            "balanceUsd": None,
            "excludeFromPnl": False,
            "auditIssues": ["synthetic_current_slug"],
            "lanes": [],
        }
        sessions = sorted(sessions_by_slug.values(), key=lambda s: n(s.get("startMs"), slug_start_ms(s.get("slug")) or 0))
        session_order_slugs = [str(s.get("slug") or "").strip() for s in sessions if str(s.get("slug") or "").strip()]
        tail_mutable_slugs = set(session_order_slugs[-RUN_AUDIT_INCREMENTAL_TAIL_SESSIONS:])
        uncached_session_slugs = {slug for slug in session_order_slugs if slug and slug not in prior_sessions_by_slug}
        unresolved_session_slugs = {
            str(s.get("slug") or "").strip()
            for s in sessions
            if str(s.get("slug") or "").strip() and session_has_unresolved_positions(s)
        }
        mutable_slugs = set(tail_mutable_slugs) | set(uncached_session_slugs) | set(unresolved_session_slugs) | {current_slug}
        trace_svg_fallback_slugs = set(session_order_slugs[-RUN_AUDIT_INLINE_DETAIL_SESSION_LIMIT:])
    mutable_start_ms_candidates = [
        i((sessions_by_slug.get(slug) or {}).get("startMs") or slug_start_ms(slug) or 0)
        for slug in mutable_slugs
        if slug
    ]
    mutable_start_ms = min(mutable_start_ms_candidates) if mutable_start_ms_candidates else None
    if file_exists(cfg, telemetry_path):
        telemetry_target_slugs = set(session_slug_set) if cfg.mode == "local" else (set(mutable_slugs) if mutable_slugs else session_slug_set)
        if cfg.mode == "local":
            telemetry_trace_idx = build_telemetry_trace_index_from_file(telemetry_path, telemetry_target_slugs)
        else:
            telemetry_rows = read_jsonl(cfg, telemetry_path)
            telemetry_trace_idx = {
                slug: trace
                for slug, trace in build_telemetry_trace_index(telemetry_rows).items()
                if not telemetry_target_slugs or slug in telemetry_target_slugs
            }
    if cfg.mode == "local" and file_exists(cfg, session_trace_path):
        telemetry_target_slugs = set(session_slug_set)
        raw_trace_idx = build_session_trace_index_from_file(session_trace_path, telemetry_target_slugs)
        for slug, trace in raw_trace_idx.items():
            telemetry_trace_idx.setdefault(slug, trace)
    built_sessions_by_slug: dict[str, dict[str, Any]] = {}
    actual_all_trades: list[dict[str, Any]] = []
    reused_count = 0
    rebuilt_count = 0
    phase_started = time.perf_counter()

    for slug, cached_session in prior_sessions_by_slug.items():
        if not slug or slug in mutable_slugs:
            continue
        if cached_session_was_current(cached_session) and not run_is_closed:
            continue
        cached_meta = (cached_session or {}).get("cacheMeta") if isinstance(cached_session, dict) else None
        if not isinstance(cached_meta, dict):
            continue
        if str(cached_meta.get("auditCodeVersion") or "").strip() != AUDIT_CODE_VERSION:
            continue
        if not str(cached_meta.get("truthSig") or "").strip():
            continue
        built_sessions_by_slug[slug] = cached_session
        actual_all_trades.extend(cached_session.get("trades") or [])
        reused_count += 1

    for session in sessions:
        slug = str(session.get("slug") or "").strip()
        if not slug:
            continue
        if slug in built_sessions_by_slug:
            continue
        session_events = events_by_slug.get(slug) or []
        cache_sig = session_cache_sig(session, slug, session_events, current_slug, truth_by_slug.get(slug))
        start_ms = i(session.get("startMs") or slug_start_ms(slug) or 0)
        end_ms = start_ms + SESSION_WINDOW_SEC * 1000
        session_audit_compact = read_cached_session_compact(cfg, slug)
        session_audit_rows = session_audit_timeline_rows(session_audit_compact)
        session_audit_fin = session_audit_financials(session_audit_compact)
        trace = fetch_trace_for_slug(cfg, slug, local_trace_idx, remote_trace_idx, run_index_trace_idx, telemetry_trace_idx, start_ms)
        quality = assess_trace_quality(trace, start_ms)
        current_trace_sig = trace_cache_sig(trace, quality)
        cached_session = prior_sessions_by_slug.get(slug)
        cached_meta = (cached_session or {}).get("cacheMeta") if isinstance(cached_session, dict) else None
        cached_sig = str((cached_meta or {}).get("sessionSig") or "").strip() if isinstance(cached_meta, dict) else ""
        cached_trace_sig = str((cached_meta or {}).get("traceSig") or "").strip() if isinstance(cached_meta, dict) else ""
        cached_truth_sig = str((cached_meta or {}).get("truthSig") or "").strip() if isinstance(cached_meta, dict) else ""
        cached_version = str((cached_meta or {}).get("auditCodeVersion") or "").strip() if isinstance(cached_meta, dict) else ""
        current_truth_sig = truth_cache_sig(truth_by_slug.get(slug), venue_truth_by_slug.get(slug))
        cached_was_current = bool((cached_meta or {}).get("wasCurrentSession")) if isinstance(cached_meta, dict) else False
        if (
            cached_session
            and cached_sig == cache_sig
            and cached_trace_sig == current_trace_sig
            and cached_truth_sig == current_truth_sig
            and cached_version == AUDIT_CODE_VERSION
            and (not cached_was_current or run_is_closed)
        ):
            built_sessions_by_slug[slug] = cached_session
            actual_all_trades.extend(cached_session.get("trades") or [])
            reused_count += 1
            continue
        source_rows = session_audit_rows if session_audit_rows else session_events
        timeline_rows, trades = build_session_timeline_and_trades(session, source_rows, trace)
        decision_snapshots = build_session_decision_snapshots(session_events, trades, start_ms)
        session_mode = classify_session_mode(session_events, trades)
        unresolved_position = session_has_unresolved_positions(session, trades, timeline_rows)
        reconcile_meta = session_reconcile_meta(end_ms, unresolved_position, now_ms)
        for trade in trades:
            if not isinstance(trade.get("actualRowMetrics"), dict):
                trade["actualRowMetrics"] = actual_trade_row_metrics(trade)
        actual_session_gross = sum(n(t.get("actualGrossPnlUsd"), 0.0) for t in trades)
        actual_session_net = sum(n(t.get("actualNetPnlUsd"), 0.0) for t in trades)
        actual_session_fees = sum(n(t.get("actualFeesUsd"), 0.0) for t in trades)
        if isinstance(session_audit_fin, dict):
            actual_session_gross = round6(n(session_audit_fin.get("grossPnlUsd"), actual_session_gross))
            actual_session_net = round6(n(session_audit_fin.get("netPnlUsd"), actual_session_net))
            actual_session_fees = round6(n(session_audit_fin.get("feesUsd"), actual_session_fees))
        cached_trace = cached_session.get("trace") if isinstance(cached_session, dict) else None
        cached_has_trace = isinstance(cached_trace, dict) and len(cached_trace.get("xMs") or []) >= 2
        cached_has_trades = isinstance(cached_session, dict) and bool(cached_session.get("trades"))
        cached_resolved = isinstance(cached_session, dict) and not bool(cached_session.get("unresolvedPosition"))
        current_is_synthetic = "synthetic_from_live_events" in list(session.get("auditIssues") or [])
        if isinstance(cached_session, dict) and cached_has_trace and not isinstance(trace, dict):
            trace = cached_trace
            quality = assess_trace_quality(trace, start_ms)
        if (
            isinstance(cached_session, dict)
            and cached_has_trades
            and cached_resolved
            and (unresolved_position or current_is_synthetic)
        ):
            timeline_rows = list(cached_session.get("timelineRows") or timeline_rows)
            trades = list(cached_session.get("trades") or trades)
            for trade in trades:
                if not isinstance(trade.get("actualRowMetrics"), dict):
                    trade["actualRowMetrics"] = actual_trade_row_metrics(trade)
            actual_session_gross = round6(n(cached_session.get("actualSessionGrossPnlUsd"), actual_session_gross))
            actual_session_net = round6(n(cached_session.get("actualSessionNetPnlUsd"), actual_session_net))
            actual_session_fees = round6(n(cached_session.get("actualSessionFeesUsd"), actual_session_fees))
            session_mode = str(cached_session.get("sessionMode") or session_mode)
            unresolved_position = False
            reconcile_meta = session_reconcile_meta(end_ms, unresolved_position, now_ms)
            if cached_has_trace:
                trace = cached_trace
                quality = assess_trace_quality(trace, start_ms)
            if not decision_snapshots and isinstance(cached_session.get("decisionSnapshots"), list):
                decision_snapshots = list(cached_session.get("decisionSnapshots") or [])
        audit_identity = {
            "runNum": str(run_num),
            "runId": str(summary.get("runId") or ""),
            "startedAtMs": str(i(summary.get("startedAtMs") or 0) or ""),
            "slug": slug,
        }
        audit_link = audit_url("/api/session-audits/review", audit_identity)
        trace_svg_fallback = None
        if slug in trace_svg_fallback_slugs:
            trace_svg_fallback = fetch_session_audit_svg_fallback(cfg, int(run_num), slug)
        auto_ignore_reasons = list(quality["reasons"])
        if session.get("excludeFromPnl"):
            auto_ignore_reasons.append("session_excluded_from_pnl")
        if session.get("auditIssues"):
            auto_ignore_reasons.extend([f"audit:{x}" for x in session.get("auditIssues") or []])
        if unresolved_position:
            auto_ignore_reasons.append("reconcile_pending_open_shares")
        session_stub = {
            "slug": slug,
            "startMs": start_ms,
            "endMs": end_ms,
            "timelineRows": timeline_rows,
            "trades": trades,
            "actualSessionNetPnlUsd": round6(actual_session_net),
            "excludeFromPnl": bool(session.get("excludeFromPnl")),
        }
        built_sessions_by_slug[slug] = {
            "slug": slug,
            "startMs": start_ms,
            "endMs": end_ms,
            "actualSessionGrossPnlUsd": round6(actual_session_gross),
            "actualSessionFeesUsd": round6(actual_session_fees),
            "actualSessionNetPnlUsd": round6(actual_session_net),
            "actualLaneNetPnlUsd": round6(n(session.get("pnlUsd"), 0.0)),
            "actualBalanceUsd": n(session.get("balanceUsd")),
            "auditIssues": list(session.get("auditIssues") or []),
            "excludeFromPnl": bool(session.get("excludeFromPnl")),
            "sessionMode": session_mode,
            "unresolvedPosition": unresolved_position,
            "reconcileStatus": reconcile_meta["reconcileStatus"],
            "reconcileAgeMs": reconcile_meta["reconcileAgeMs"],
            "includedInActualTotals": bool(reconcile_meta["includedInActualTotals"]),
            "trace": trace,
            "traceSvgFallback": trace_svg_fallback,
            "traceQuality": quality,
            "streamEndSnapshot": build_stream_end_snapshot(trace),
            "autoIgnore": quality["autoIgnore"] or bool(session.get("excludeFromPnl")),
            "autoIgnoreReasons": auto_ignore_reasons,
            "auditUrl": audit_link,
            "timelineRows": timeline_rows,
            "decisionSnapshots": decision_snapshots,
            "trades": trades,
            "resolutionTruth": truth_by_slug.get(slug),
            "venueTruth": venue_truth_by_slug.get(slug),
            "truthVerification": build_session_truth_verification(session, truth_by_slug.get(slug)),
            "cacheMeta": {
                "sessionSig": cache_sig,
                "traceSig": current_trace_sig,
                "truthSig": current_truth_sig,
                "wasCurrentSession": slug == current_slug,
                "auditCodeVersion": AUDIT_CODE_VERSION,
            },
        }
        actual_all_trades.extend(trades)
        rebuilt_count += 1
    session_build_elapsed_ms = int(round((time.perf_counter() - phase_started) * 1000.0))

    existing_start_ms = sorted(
        [i(s.get("startMs")) for s in built_sessions_by_slug.values() if is_finite(s.get("startMs"))]
    )
    if existing_start_ms:
        min_start_ms = existing_start_ms[0]
        max_start_ms = existing_start_ms[-1]
        for start_ms in range(min_start_ms, max_start_ms + 1, SESSION_WINDOW_SEC * 1000):
            slug = f"btc-updown-5m-{start_ms // 1000}"
            if slug in built_sessions_by_slug:
                continue
            cached_session = prior_sessions_by_slug.get(slug)
            if cached_session and (not cached_session_was_current(cached_session) or run_is_closed):
                cached_meta = (cached_session or {}).get("cacheMeta") if isinstance(cached_session, dict) else None
                if mutable_start_ms is None or start_ms < mutable_start_ms:
                    if (
                        isinstance(cached_meta, dict)
                        and str(cached_meta.get("auditCodeVersion") or "").strip() == AUDIT_CODE_VERSION
                        and str(cached_meta.get("truthSig") or "").strip()
                    ):
                        built_sessions_by_slug[slug] = cached_session
                        actual_all_trades.extend(cached_session.get("trades") or [])
                        reused_count += 1
                        continue
            trace = fetch_trace_for_slug(cfg, slug, local_trace_idx, remote_trace_idx, run_index_trace_idx, telemetry_trace_idx, start_ms)
            quality = assess_trace_quality(trace, start_ms)
            built_sessions_by_slug[slug] = {
                "slug": slug,
                "startMs": start_ms,
                "endMs": start_ms + SESSION_WINDOW_SEC * 1000,
                "actualSessionGrossPnlUsd": 0.0,
                "actualSessionFeesUsd": 0.0,
                "actualSessionNetPnlUsd": 0.0,
                "actualLaneNetPnlUsd": 0.0,
                "actualBalanceUsd": None,
                "auditIssues": [],
                "excludeFromPnl": True,
                "unresolvedPosition": False,
                "reconcileStatus": "resolved",
                "reconcileAgeMs": None,
                "includedInActualTotals": False,
                "trace": trace,
                "traceQuality": quality,
                "autoIgnore": True,
                "forcedIgnore": True,
                "autoIgnoreReasons": ["missing_session_artifact"],
                "auditUrl": "",
                "timelineRows": [],
                "trades": [],
                "resolutionTruth": truth_by_slug.get(slug),
                "venueTruth": venue_truth_by_slug.get(slug),
                "truthVerification": build_session_truth_verification({"slug": slug, "lanes": [], "pnlUsd": 0.0}, truth_by_slug.get(slug)),
                "syntheticMissing": True,
                "cacheMeta": {
                    "sessionSig": f"missing:{slug}",
                    "traceSig": trace_cache_sig(trace, quality),
                    "truthSig": truth_cache_sig(truth_by_slug.get(slug), venue_truth_by_slug.get(slug)),
                    "wasCurrentSession": False,
                    "auditCodeVersion": AUDIT_CODE_VERSION,
                },
            }

    built_sessions = sorted(
        built_sessions_by_slug.values(),
        key=lambda s: n(s.get("startMs"), slug_start_ms(s.get("slug")) or 0),
    )
    unresolved_session_slugs = {
        str(session.get("slug") or "").strip()
        for session in built_sessions
        if str(session.get("slug") or "").strip()
        and session_has_unresolved_positions(session, session.get("trades") or [], session.get("timelineRows") or [])
    }
    for session in built_sessions:
        session["unresolvedPosition"] = bool(str(session.get("slug") or "").strip() in unresolved_session_slugs)
        reconcile_meta = session_reconcile_meta(session.get("endMs"), bool(session.get("unresolvedPosition")), now_ms)
        session["reconcileStatus"] = reconcile_meta["reconcileStatus"]
        session["reconcileAgeMs"] = reconcile_meta["reconcileAgeMs"]
        session["includedInActualTotals"] = bool(reconcile_meta["includedInActualTotals"])

    default_bets = compute_default_bets(built_sessions, n(summary.get("betUsd"), 25.0))
    for trade_num, val in t_overrides.items():
        if val is not None:
            default_bets[str(trade_num)] = round4(val)

    actual_models: dict[str, dict[str, Any]] = {}
    for session in built_sessions:
        for trade in session["trades"]:
            bet = default_bets.get(str(int(trade.get("tradeNum") or 0)), 25.0)
            actual_models[trade["tradeKey"]] = model_trade(trade, bet)

    phase_started = time.perf_counter()
    accounted_sessions = [session for session in built_sessions if bool(session.get("includedInActualTotals", True))]
    accounted_trade_keys = {
        str(trade.get("tradeKey") or "")
        for session in accounted_sessions
        for trade in (session.get("trades") or [])
        if str(trade.get("tradeKey") or "")
    }
    accounted_actual_trades = [trade for trade in actual_all_trades if str(trade.get("tradeKey") or "") in accounted_trade_keys]
    accounted_actual_models = {
        trade_key: model
        for trade_key, model in actual_models.items()
        if trade_key in accounted_trade_keys
    }
    telemetry_rows = read_jsonl(cfg, telemetry_path) if file_exists(cfg, telemetry_path) else []
    cpu_samples = [n(row.get("processCpuPct"), float("nan")) for row in telemetry_rows if isinstance(row, dict)]
    cpu_samples = [float(v) for v in cpu_samples if math.isfinite(v)]
    runtime_lag_samples = [n(row.get("runtimeLagMs"), float("nan")) for row in telemetry_rows if isinstance(row, dict)]
    runtime_lag_samples = [float(v) for v in runtime_lag_samples if math.isfinite(v)]
    cpu_sorted = sorted(cpu_samples)
    runtime_sorted = sorted(runtime_lag_samples)
    end_snapshots = [session.get("streamEndSnapshot") for session in built_sessions if isinstance(session.get("streamEndSnapshot"), dict)]
    weighted_gap_count = sum(max(0.0, n(snap.get("deltaCount"), 0.0)) for snap in end_snapshots)
    weighted_avg_gap = (
        sum(n(snap.get("avgGapMs"), 0.0) * max(0.0, n(snap.get("deltaCount"), 0.0)) for snap in end_snapshots) / weighted_gap_count
        if weighted_gap_count > 0 else None
    )
    weighted_p95_gap = (
        sum(n(snap.get("p95GapMs"), 0.0) * max(0.0, n(snap.get("deltaCount"), 0.0)) for snap in end_snapshots) / weighted_gap_count
        if weighted_gap_count > 0 else None
    )
    weighted_live_gap = (
        sum(n(snap.get("liveGapMs"), 0.0) * max(0.0, n(snap.get("deltaCount"), 0.0)) for snap in end_snapshots) / weighted_gap_count
        if weighted_gap_count > 0 else None
    )
    weighted_effective_hz = (
        sum(n(snap.get("effectiveHz"), 0.0) * max(0.0, n(snap.get("deltaCount"), 0.0)) for snap in end_snapshots) / weighted_gap_count
        if weighted_gap_count > 0 else None
    )
    over_25_counts = 0.0
    for snap in end_snapshots:
        for threshold in (snap.get("thresholds") or []):
            if n(threshold.get("gapMs"), float("nan")) == 25:
                over_25_counts += max(0.0, n(threshold.get("count"), 0.0))
                break
    over_25_pct = (over_25_counts / weighted_gap_count) * 100.0 if weighted_gap_count > 0 else None

    actual_by_trade = summarize_by_trade(accounted_actual_trades, accounted_actual_models)
    actual_by_interval = summarize_by_trade_interval(accounted_actual_trades, accounted_actual_models)

    start_balance = n(summary.get("startBalanceUsd"), 100.0)
    end_balance = n(summary.get("endBalanceUsd"), start_balance)
    hours_tested = ((ended_at_ms or now_ms) - started_at_ms) / 3600000.0 if started_at_ms else 0.0

    actual_trade_gross = sum(n(t.get("actualGrossPnlUsd"), 0.0) for t in accounted_actual_trades)
    actual_trade_fees = sum(n(t.get("actualFeesUsd"), 0.0) for t in accounted_actual_trades)
    actual_trade_net = sum(n(t.get("actualNetPnlUsd"), 0.0) for t in accounted_actual_trades)
    truth_resolved_session_count = 0
    truth_mismatch_session_count = 0
    truth_net_delta_usd = 0.0
    for session in accounted_sessions:
        verification = session.get("truthVerification") or {}
        if str(verification.get("truthStatus") or "") == "resolved":
            truth_resolved_session_count += 1
        if verification.get("roundedInferenceMatchesTruth") is False:
            truth_mismatch_session_count += 1
        truth_net_delta_usd += n(verification.get("truthNetPnlDeltaUsd"), 0.0)
    actual_equity_points = [start_balance]
    for session in built_sessions:
        if bool(session.get("includedInActualTotals", True)):
            actual_equity_points.append(
                n(actual_equity_points[-1], start_balance) + n(session.get("actualSessionNetPnlUsd"), 0.0)
            )
        else:
            actual_equity_points.append(n(actual_equity_points[-1], start_balance))
    actual_drawdown = compute_max_drawdown_from_equity_points(actual_equity_points)
    # Use the same live equity curve for the default modeled baseline so the
    # untuned view matches the actual run equity source.
    modeled_equity_points = list(actual_equity_points)
    total_modeled_fees = sum(n(model.get("feesUsd"), 0.0) for model in accounted_actual_models.values())
    accounted_end_balance = n(actual_equity_points[-1], start_balance)
    actual_equity_net = accounted_end_balance - start_balance
    actual_equity_gross = actual_equity_net + actual_trade_fees
    total_modeled_gross = actual_equity_net + total_modeled_fees
    total_modeled_net = actual_equity_net
    modeled_drawdown = compute_max_drawdown_from_equity_points(modeled_equity_points)
    aggregate_elapsed_ms = int(round((time.perf_counter() - phase_started) * 1000.0))
    trade_wins = sum(1 for trade in accounted_actual_trades if n(trade.get("actualNetPnlUsd"), 0.0) > 0)
    trade_losses = sum(1 for trade in accounted_actual_trades if n(trade.get("actualNetPnlUsd"), 0.0) < 0)
    win_rate = round4((trade_wins / (trade_wins + trade_losses)) * 100.0) if (trade_wins + trade_losses) else None

    execution_fill_source = str(summary.get("executionFillSource") or "").strip().lower()
    is_live_truth_run = str(summary.get("mode") or "").strip().lower() == "live" or "live" in execution_fill_source
    default_gold_selection_keys = default_gold_selection_keys_from_actual_run(built_sessions, default_bets)
    if not default_gold_selection_keys:
        default_gold_selection_keys = default_gold_selection_keys_for_strategy(
            strategy_id,
            str(summary.get("sizingProfile") or ""),
        )

    inline_detail_start_idx = max(0, len(built_sessions) - RUN_AUDIT_INLINE_DETAIL_SESSION_LIMIT)
    inline_sessions = [
        compact_session_for_inline_payload(session, idx >= inline_detail_start_idx)
        for idx, session in enumerate(built_sessions)
    ]

    default_gold_bet_usd = round6(max(n(summary.get("maxBetUsd"), 100.0), max(default_bets.values(), default=25.0)))
    default_gold_bets = {trade_num: default_gold_bet_usd for trade_num in default_bets.keys()}

    default_gold_selection_key_set = set(default_gold_selection_keys)
    selected_baseline_25_net = 0.0
    unchecked_baseline_25_net = 0.0
    baseline_25_bets = {str(k): 25.0 for k in default_bets.keys()}
    for trade in accounted_actual_trades:
        trade_num = str(int(trade.get("tradeNum") or 0))
        interval = str(trade.get("intervalLabel") or "").strip()
        combo_key = f"{trade_num}|{interval}" if trade_num and interval else ""
        selected_bet = baseline_25_bets.get(trade_num, 25.0) if combo_key in default_gold_selection_key_set else baseline_25_bets.get(trade_num, 25.0)
        unchecked_bet = baseline_25_bets.get(trade_num, 25.0)
        selected_baseline_25_net += n(model_trade(trade, selected_bet).get("netPnlUsd"), 0.0)
        unchecked_baseline_25_net += n(model_trade(trade, unchecked_bet).get("netPnlUsd"), 0.0)

    doc = {
        "title": f"Reusable Live Run Audit · Host {cfg.host_port} · Run {run_num}",
        "subtitle": "Institutional run audit with actual run metrics, fee-aware hypothetical resizing, interval filtering, trace quality gates, and per-session overrides.",
        "source": {
            "mode": cfg.mode,
            "host": cfg.host,
            "hostPort": cfg.host_port,
            "publicBase": cfg.public_base,
            "remoteRoot": cfg.remote_root,
            "runDir": run_dir,
            "summaryPath": summary_path,
            "indexPath": index_path,
            "eventsPath": events_path,
            "truthPath": truth_path,
        },
        "links": {
            "runAuditUrl": audit_url(
                "/api/run-audits/review",
                {
                    "runNum": str(run_num),
                    "runId": str(summary.get("runId") or ""),
                    "startedAtMs": str(i(summary.get("startedAtMs") or 0) or ""),
                    "hostPort": str(cfg.host_port),
                    "refresh": "1",
                },
            ),
            "currentSessionAuditUrl": (
                audit_url(
                    "/api/session-audits/review",
                    {
                        "runNum": str(run_num),
                        "runId": str(summary.get("runId") or ""),
                        "startedAtMs": str(i(summary.get("startedAtMs") or 0) or ""),
                        "slug": current_slug,
                    },
                )
                if current_slug else ""
            ),
        },
        "run": {
            "runNum": run_num,
            "runId": summary.get("runId"),
            "runAuditIdentity": {
                "runNum": run_num,
                "runId": run_id,
                "startedAtMs": started_at_ms,
            },
            "strategyId": strategy_id,
            "instanceId": summary.get("instanceId"),
            "mode": summary.get("mode"),
            "status": summary.get("status"),
            "marketTitle": summary.get("marketTitle"),
            "marketSlug": summary.get("marketSlug"),
            "startedAtMs": started_at_ms,
            "endedAtMs": ended_at_ms,
            "hoursTested": round6(hours_tested),
            "startBalanceUsd": round6(start_balance),
            "endBalanceUsd": round6(accounted_end_balance),
            "actualNetPnlUsd": round6(actual_equity_net),
            "actualGrossPnlUsd": round6(actual_equity_gross),
            "actualFeesUsd": round6(actual_trade_fees),
            "serverCpuAvgPct": round6(safe_avg(cpu_samples) or 0.0) if cpu_samples else None,
            "serverCpuP95Pct": round6(percentile_from_sorted(cpu_sorted, 0.95)) if cpu_sorted else None,
            "serverCpuMaxPct": round6(cpu_sorted[-1]) if cpu_sorted else None,
            "serverRuntimeLagAvgMs": round6(safe_avg(runtime_lag_samples) or 0.0) if runtime_lag_samples else None,
            "serverRuntimeLagP95Ms": round6(percentile_from_sorted(runtime_sorted, 0.95)) if runtime_sorted else None,
            "serverRuntimeLagMaxMs": round6(runtime_sorted[-1]) if runtime_sorted else None,
            "serverEndSnapshotSessionCount": len(end_snapshots),
            "serverEndSnapshotGapCount": round6(weighted_gap_count) if weighted_gap_count > 0 else None,
            "serverEndSnapshotAvgGapMs": round6(weighted_avg_gap) if weighted_avg_gap is not None else None,
            "serverEndSnapshotP95GapMs": round6(weighted_p95_gap) if weighted_p95_gap is not None else None,
            "serverEndSnapshotLiveGapMs": round6(weighted_live_gap) if weighted_live_gap is not None else None,
            "serverEndSnapshotEffectiveHz": round6(weighted_effective_hz) if weighted_effective_hz is not None else None,
            "serverEndSnapshotOver25Pct": round6(over_25_pct) if over_25_pct is not None else None,
            "actualTradeNetPnlUsd": round6(actual_trade_net),
            "truthResolvedSessionCount": truth_resolved_session_count,
            "truthMismatchSessionCount": truth_mismatch_session_count,
            "truthNetPnlDeltaUsd": round6(truth_net_delta_usd),
            "actualMaxDrawdownUsd": actual_drawdown["maxDrawdownUsd"],
            "actualMaxDrawdownPct": actual_drawdown["maxDrawdownPct"],
            "actualEquityPoints": [round6(v) for v in actual_equity_points],
            "modeledGrossPnlUsdAtDefaultBets": round6(total_modeled_gross),
            "modeledFeesUsdAtDefaultBets": round6(total_modeled_fees),
            "modeledNetPnlUsdAtDefaultBets": round6(total_modeled_net),
            "modeledMaxDrawdownUsdAtDefaultBets": modeled_drawdown["maxDrawdownUsd"],
            "modeledMaxDrawdownPctAtDefaultBets": modeled_drawdown["maxDrawdownPct"],
            "modeledEquityPointsAtDefaultBets": [round6(v) for v in modeled_equity_points],
            "profitPerHourUsd": round6((actual_equity_net / hours_tested) if hours_tested > 0 else 0.0),
            "tradesClosed": len(accounted_actual_trades),
            "wins": trade_wins,
            "losses": trade_losses,
            "winRatePct": win_rate,
            "executionFillSource": summary.get("executionFillSource"),
            "lastExitType": summary.get("lastExitType"),
            "isLiveTruthRun": is_live_truth_run,
        },
        "buildStats": {
            "reusedSessions": reused_count,
            "rebuiltSessions": rebuilt_count,
            "cachedSessionCount": len(prior_sessions_by_slug),
            "currentSlug": current_slug,
            "runClosed": run_is_closed,
            "incrementalTailSessions": RUN_AUDIT_INCREMENTAL_TAIL_SESSIONS,
            "mutableSessionCount": len(mutable_slugs),
            "mutableSlugs": sorted(mutable_slugs),
            "unresolvedSessionCount": len(unresolved_session_slugs),
            "unresolvedSlugs": sorted(unresolved_session_slugs),
            "auditCodeVersion": AUDIT_CODE_VERSION,
            "perf": {
                "traceIndexMs": trace_index_elapsed_ms,
                "sessionBuildMs": session_build_elapsed_ms,
                "aggregateMs": aggregate_elapsed_ms,
                "totalBuildMs": int(round((time.perf_counter() - build_started) * 1000.0)),
            },
        },
        "controls": {
            "defaultBetUsdByTrade": default_bets,
            "defaultGoldBetUsdByTrade": default_gold_bets,
            "defaultGoldSelectionKeys": default_gold_selection_keys,
            "intervalBucketSec": INTERVAL_BUCKET_SEC,
            "entryCutoffSec": ENTRY_CUTOFF_SEC,
            "goldIntervals": [f"{lo:03d}-{min(ENTRY_CUTOFF_SEC, lo + INTERVAL_BUCKET_SEC):03d}s" for lo in range(0, ENTRY_CUTOFF_SEC, INTERVAL_BUCKET_SEC)],
        },
        "summaries": {
            "actual": {
                "startBalanceUsd": round6(start_balance),
                "endBalanceUsd": round6(accounted_end_balance),
                "actualNetPnlUsd": round6(actual_equity_net),
                "actualGrossPnlUsd": round6(actual_trade_gross),
                "actualFeesUsd": round6(actual_trade_fees),
                "truthResolvedSessionCount": truth_resolved_session_count,
                "truthMismatchSessionCount": truth_mismatch_session_count,
                "truthNetPnlDeltaUsd": round6(truth_net_delta_usd),
                "tradesClosed": len(accounted_actual_trades),
                "wins": trade_wins,
                "losses": trade_losses,
                "winRatePct": win_rate,
                "profitPerHourUsd": round6((actual_equity_net / hours_tested) if hours_tested > 0 else 0.0),
                "actualMaxDrawdownUsd": actual_drawdown["maxDrawdownUsd"],
                "actualMaxDrawdownPct": actual_drawdown["maxDrawdownPct"],
                "actualEquityPoints": [round6(v) for v in actual_equity_points],
            },
            "modeledDefault": {
                "startBalanceUsd": round6(start_balance),
                "endBalanceUsd": round6(start_balance + total_modeled_net),
                "modeledNetPnlUsd": round6(total_modeled_net),
                "modeledGrossPnlUsd": round6(total_modeled_gross),
                "modeledFeesUsd": round6(total_modeled_fees),
                "modeledMaxDrawdownUsd": modeled_drawdown["maxDrawdownUsd"],
                "modeledMaxDrawdownPct": modeled_drawdown["maxDrawdownPct"],
                "modeledEquityPoints": [round6(v) for v in modeled_equity_points],
            },
            "byTradeAtDefaultBets": actual_by_trade,
            "byTradeIntervalAtDefaultBets": actual_by_interval,
        },
        "selfTest": {
            "baselineParity": {
                "actualNetPnlUsd": round6(actual_equity_net),
                "modeledNetPnlUsd": round6(total_modeled_net),
                "actualGrossPnlUsd": round6(actual_trade_gross),
                "modeledGrossPnlUsd": round6(total_modeled_gross),
                "actualFeesUsd": round6(actual_trade_fees),
                "modeledFeesUsd": round6(total_modeled_fees),
                "actualTradeCount": len(accounted_actual_trades),
                "modeledTradeCount": len(accounted_actual_trades),
                "actualSessionCount": len(accounted_sessions),
                "modeledSessionCount": len(accounted_sessions),
                "netDeltaUsd": round6(total_modeled_net - actual_equity_net),
                "grossDeltaUsd": round6(total_modeled_gross - actual_trade_gross),
                "feesDeltaUsd": round6(total_modeled_fees - actual_trade_fees),
                "passes": abs(total_modeled_net - actual_equity_net) <= 1e-6,
            },
            "gold25Equivalence": {
                "selectedGoldAt25NetPnlUsd": round6(selected_baseline_25_net),
                "uncheckedBase25NetPnlUsd": round6(unchecked_baseline_25_net),
                "netDeltaUsd": round6(selected_baseline_25_net - unchecked_baseline_25_net),
                "passes": abs(selected_baseline_25_net - unchecked_baseline_25_net) <= 1e-6,
            },
            "requirements": {
                "defaultSelectionsMatchActual": {
                    "title": "Actual equals Modeled with default selected gold intervals",
                    "passes": abs(total_modeled_net - actual_equity_net) <= 1e-6,
                    "netDeltaUsd": round6(total_modeled_net - actual_equity_net),
                },
                "gold25MatchesUnchecked25": {
                    "title": "Modeled no-gold $25 baseline equals modeled all-default-gold-selected at $25",
                    "passes": abs(selected_baseline_25_net - unchecked_baseline_25_net) <= 1e-6,
                    "netDeltaUsd": round6(selected_baseline_25_net - unchecked_baseline_25_net),
                },
            },
        },
        "sessions": inline_sessions,
        "out": {"html": out_html, "json": out_json},
    }
    return doc


def default_gold_selection_keys_for_strategy(strategy_id: str | None, sizing_profile: str | None = None) -> list[str]:
    profile_key = str(sizing_profile or "").strip().lower()
    if profile_key and profile_key in DEFAULT_GOLD_SELECTION_KEYS_BY_PROFILE:
        return list(DEFAULT_GOLD_SELECTION_KEYS_BY_PROFILE.get(profile_key, ()))
    key = str(strategy_id or "").strip().lower()
    return list(DEFAULT_GOLD_SELECTION_KEYS_BY_STRATEGY.get(key, ()))


def build_run_audit_state(doc: dict[str, Any]) -> dict[str, Any]:
    sessions = list(doc.get("sessions") or [])
    current_slug = str((doc.get("buildStats") or {}).get("currentSlug") or "")
    mutable_slugs = set((doc.get("buildStats") or {}).get("mutableSlugs") or [])
    start_balance = n((doc.get("run") or {}).get("startBalanceUsd"), 100.0)
    running_balance = start_balance
    cumulative_net = 0.0
    cumulative_gross = 0.0
    cumulative_fees = 0.0
    wins = 0
    losses = 0
    peak_balance = running_balance
    max_drawdown = 0.0
    session_dir = run_audit_session_dir_for_run(str((doc.get("source") or {}).get("runDir") or ""))
    state_sessions: list[dict[str, Any]] = []
    for session in sessions:
        slug = str(session.get("slug") or "").strip()
        included_in_actual_totals = bool(session.get("includedInActualTotals", True))
        actual_net = n(session.get("actualSessionNetPnlUsd"), 0.0) if included_in_actual_totals else 0.0
        actual_gross = n(session.get("actualSessionGrossPnlUsd"), 0.0) if included_in_actual_totals else 0.0
        actual_fees = n(session.get("actualSessionFeesUsd"), 0.0) if included_in_actual_totals else 0.0
        running_balance += actual_net
        cumulative_net += actual_net
        cumulative_gross += actual_gross
        cumulative_fees += actual_fees
        if included_in_actual_totals:
            for trade in session.get("trades") or []:
                trade_net = n(trade.get("actualNetPnlUsd"), 0.0)
                if trade_net > 0:
                    wins += 1
                elif trade_net < 0:
                    losses += 1
        peak_balance = max(peak_balance, running_balance)
        max_drawdown = max(max_drawdown, peak_balance - running_balance)
        truth_status = str(((session.get("truthVerification") or {}).get("truthStatus")) or "").strip().lower() or "pending"
        status = "final"
        if bool(session.get("unresolvedPosition")):
            status = "reconcile-pending"
        elif slug == current_slug or slug in mutable_slugs:
            status = "mutable"
        elif truth_status not in {"", "pending", "resolved"}:
            status = "truth-pending"
        state_sessions.append({
            "slug": slug,
            "status": status,
            "startMs": session.get("startMs"),
            "endMs": session.get("endMs"),
            "actualSessionNetPnlUsd": round6(actual_net),
            "actualSessionGrossPnlUsd": round6(actual_gross),
            "actualSessionFeesUsd": round6(actual_fees),
            "actualBalanceUsd": round6(running_balance),
            "traceUsable": bool(((session.get("traceQuality") or {}).get("usable"))),
            "autoIgnore": bool(session.get("autoIgnore")),
            "unresolvedPosition": bool(session.get("unresolvedPosition")),
            "reconcileStatus": session.get("reconcileStatus"),
            "reconcileAgeMs": session.get("reconcileAgeMs"),
            "includedInActualTotals": included_in_actual_totals,
            "truthStatus": truth_status,
            "artifactPath": str(session_dir / f"{slug}.compact.json"),
            "cacheMeta": session.get("cacheMeta") or {},
            "cumulative": {
                "netPnlUsd": round6(cumulative_net),
                "grossPnlUsd": round6(cumulative_gross),
                "feesUsd": round6(cumulative_fees),
                "wins": wins,
                "losses": losses,
                "balanceUsd": round6(running_balance),
                "maxDrawdownUsd": round6(max_drawdown),
            },
        })
    return {
        "schemaVersion": "1.0",
        "auditCodeVersion": AUDIT_CODE_VERSION,
        "generatedAtMs": int(time.time() * 1000),
        "run": {
            "runNum": (doc.get("run") or {}).get("runNum"),
            "runId": (doc.get("run") or {}).get("runId"),
            "startedAtMs": (doc.get("run") or {}).get("startedAtMs"),
            "endedAtMs": (doc.get("run") or {}).get("endedAtMs"),
            "status": (doc.get("run") or {}).get("status"),
            "strategyId": (doc.get("run") or {}).get("strategyId"),
            "instanceId": (doc.get("run") or {}).get("instanceId"),
        },
        "source": doc.get("source") or {},
        "buildStats": doc.get("buildStats") or {},
        "sessionAuditDir": str(session_dir),
        "sessions": state_sessions,
    }


def compact_trade_for_modeling(trade: dict[str, Any]) -> dict[str, Any]:
    return {
        "tradeKey": str(trade.get("tradeKey") or ""),
        "tradeNum": i(trade.get("tradeNum") or 0),
        "intervalLabel": str(trade.get("intervalLabel") or "unknown"),
        "actualBudgetUsd": round6(n(trade.get("actualBudgetUsd"), 0.0)),
        "actualShares": round6(n(trade.get("actualShares"), 0.0)),
        "actualGrossPnlUsd": round6(n(trade.get("actualGrossPnlUsd"), 0.0)),
        "actualFeesUsd": round6(n(trade.get("actualFeesUsd"), 0.0)),
        "actualNetPnlUsd": round6(n(trade.get("actualNetPnlUsd"), 0.0)),
        "closeLegs": [],
        "actualRowMetrics": {},
    }


def compact_session_for_inline_payload(session: dict[str, Any], keep_details: bool) -> dict[str, Any]:
    out = dict(session or {})
    out["modelTrades"] = [compact_trade_for_modeling(trade) for trade in list(session.get("trades") or [])]
    if keep_details:
        return out
    out["trace"] = None
    out["streamEndSnapshot"] = None
    out["timelineRows"] = []
    out["decisionSnapshots"] = []
    out["trades"] = []
    out["venueTruth"] = None
    out["detailDeferred"] = True
    return out


def html_page(doc: dict[str, Any]) -> str:
    payload = json.dumps(doc, separators=(",", ":"), ensure_ascii=True).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(doc["title"])}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg:#08111d;
      --bg2:#0f1a2b;
      --card:#121d30;
      --card2:#0d1728;
      --line:#243650;
      --txt:#e9f0fb;
      --mut:#9bb0cb;
      --blue:#6cb2ff;
      --gold:#ffcc4d;
      --green:#39d98a;
      --red:#ff7b91;
      --amber:#ffb869;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(76,119,204,.20), transparent 34%),
        radial-gradient(circle at top right, rgba(255,204,77,.10), transparent 28%),
        linear-gradient(180deg, #08111d 0%, #0f1726 44%, #0a1019 100%);
      color: var(--txt);
      font: 13px/1.45 ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial;
    }}
    .wrap {{ max-width: 1880px; margin: 0 auto; padding: 18px 22px 40px; }}
    .hero, .controls, .session-card {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: linear-gradient(180deg, rgba(18,29,48,.97), rgba(13,23,40,.98));
      box-shadow: 0 12px 36px rgba(0,0,0,.28);
    }}
    .hero {{ padding: 18px 18px 16px; margin: 0 0 14px; }}
    .hero h1 {{ margin: 0 0 6px; font-size: 30px; letter-spacing: .02em; }}
    .sub {{ color: var(--mut); }}
    .hero-grid {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; margin-top: 12px; }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(8,16,28,.64);
      padding: 10px 12px;
      min-height: 84px;
    }}
    .metric b {{
      display: block;
      color: var(--mut);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: .05em;
    }}
    .metric span {{
      display: block;
      font-size: 24px;
      font-weight: 800;
      margin-top: 4px;
    }}
    .metric small {{ display:block; color: var(--mut); margin-top: 4px; }}
    .metric.equity {{ grid-column: span 2; min-height: 132px; }}
    .metric.equity svg {{ display:block; width:100%; height:56px; margin-top:10px; }}
    .metric.equity polyline {{ fill:none; stroke:#6cb2ff; stroke-width:2.5; stroke-linecap:round; stroke-linejoin:round; }}
    .metric.equity .fill {{ fill: url(#equityFill); stroke: none; opacity: .18; }}
    .pos {{ color: var(--green); }}
    .neg {{ color: var(--red); }}
    .controls {{ padding: 16px 18px; margin: 0 0 14px; }}
    .controls-grid {{ display: grid; grid-template-columns: 1.15fr 1.15fr 1fr; gap: 14px; }}
    .control-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(8,16,28,.58);
      padding: 12px;
    }}
    .control-card h2 {{ margin: 0 0 10px; font-size: 16px; }}
    .bet-grid {{ display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:8px; }}
    .goldCol {{ width: 60px; text-align: center; }}
    label.field {{ display:flex; flex-direction:column; gap:6px; color: var(--mut); font-size:12px; }}
    input[type="number"] {{
      width: 100%;
      border: 1px solid #314765;
      border-radius: 10px;
      background: #08111d;
      color: var(--txt);
      padding: 10px 12px;
      font: inherit;
    }}
    .intervals {{ display:flex; flex-wrap:wrap; gap:8px; }}
    .chip {{
      display:inline-flex; align-items:center; gap:6px;
      padding: 6px 10px; border-radius: 999px;
      border: 1px solid #314765; background:#0a1322; color: var(--txt);
      cursor:pointer; user-select:none;
    }}
    .chip input {{ accent-color: var(--gold); }}
    .button-row {{ display:flex; gap:8px; flex-wrap:wrap; margin-top: 10px; }}
    button {{
      border:1px solid #335174; border-radius: 10px; background:#122237; color:var(--txt);
      padding:8px 12px; font:inherit; cursor:pointer;
    }}
    .summary-grid {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 14px; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid var(--line); text-align:left; white-space:nowrap; }}
    th {{ color: var(--mut); font-size:12px; }}
    .sessions {{ display:flex; flex-direction:column; gap:14px; }}
    .session-card {{ padding: 14px 16px 16px; }}
    .session-head {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; margin-bottom: 10px; }}
    .session-head h2 {{ margin:0 0 4px; font-size: 19px; }}
    .session-actions {{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
    .pill {{
      display:inline-flex; align-items:center; gap:6px;
      padding:6px 10px; border-radius:999px; border:1px solid var(--line);
      background: rgba(8,16,28,.58); color: var(--txt);
      font-size:12px;
    }}
    .pill.warn {{ color: var(--amber); }}
    .pill.ignored {{ color: var(--amber); border-color: rgba(255,184,105,.45); }}
    .session-card.ignored {{ opacity: .62; border-color: rgba(255,184,105,.35); }}
    .session-top-grid {{ display:grid; grid-template-columns: minmax(0,1.55fr) minmax(360px,.95fr); gap: 14px; align-items:start; margin-bottom: 12px; }}
    .trace-wrap {{
      border:1px solid var(--line); border-radius:12px; overflow:auto; background:#0b1323;
      min-height: 352px;
    }}
    .trace-title {{
      display:flex; align-items:center; justify-content:space-between; gap:10px;
      padding: 10px 12px; border-bottom:1px solid var(--line);
      background: linear-gradient(180deg, rgba(17,31,49,.96), rgba(11,19,35,.94));
      font-weight: 800; letter-spacing: .04em; text-transform: uppercase; font-size: 12px;
    }}
    .trace-sub {{ color: var(--mut); font-size: 11px; font-weight: 600; letter-spacing: 0; text-transform:none; }}
    .trace-svg {{ width:100%; height:auto; min-width: 1100px; display:block; }}
    .trade-summary-stack {{ display:flex; flex-direction:column; gap:10px; position: sticky; top: 12px; }}
    .decision-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 10px; margin: 0 0 12px; }}
    .decision-card {{
      border:1px solid var(--line); border-radius:12px; background: rgba(8,16,28,.60);
      padding: 12px;
    }}
    .decision-card.enter_signal {{ border-color: rgba(108,178,255,.34); }}
    .decision-card.entry_blocked {{ border-color: rgba(255,184,105,.34); }}
    .decision-head {{ display:flex; justify-content:space-between; gap:10px; font-weight:800; margin-bottom:6px; }}
    .decision-window-grid {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:8px; margin-top:8px; }}
    .decision-window {{
      border:1px solid var(--line); border-radius:10px; background: rgba(11,19,35,.72);
      padding: 8px 9px;
    }}
    .decision-window.pass {{ border-color: rgba(57,217,138,.38); }}
    .decision-window.fail {{ border-color: rgba(255,123,145,.30); }}
    .decision-title {{ font-weight:800; margin-bottom:4px; }}
    .decision-points {{
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 11px;
      color: var(--mut);
      line-height: 1.45;
      white-space: normal;
      word-break: break-word;
    }}
    .trade-card {{
      border:1px solid var(--line); border-radius:12px; background: rgba(8,16,28,.60);
      padding: 10px 12px; transition: border-color .12s ease, transform .12s ease, box-shadow .12s ease;
    }}
    .trade-card:hover, .trade-card.active {{ border-color: var(--blue); transform: translateY(-1px); box-shadow: 0 0 0 1px rgba(108,178,255,.22); }}
    .trade-card-head {{ display:flex; justify-content:space-between; gap:10px; font-weight: 800; margin-bottom: 6px; }}
    .trade-line {{ color: var(--mut); margin: 3px 0; }}
    .trade-line b {{ color: var(--txt); }}
    .tbl {{
      border:1px solid var(--line); border-radius:12px; overflow:auto; margin-top: 12px;
      max-height: 420px;
    }}
    tbody tr:hover {{ background:#162338; }}
    tbody tr.active {{ background:#22324a; }}
    tfoot td {{ position: sticky; bottom: 0; background:#101b2a; border-top: 2px solid #30455f; font-weight:800; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; }}
    .empty {{ padding:12px; color: var(--mut); }}
    .trace-group.dimmed {{ opacity:.22; }}
    .trace-group.active {{ opacity:1; filter: drop-shadow(0 0 4px rgba(255,255,255,.35)); }}
    a.compare-link {{ color: var(--blue); text-decoration: none; }}
    .sessions-toolbar {{ display:flex; align-items:center; justify-content:space-between; gap:12px; margin: 10px 0 14px; }}
    .sessions-toolbar .sub {{ margin: 0; }}
    .hover-inspector {{
      position: fixed;
      left: 16px;
      bottom: 16px;
      z-index: 9999;
      pointer-events: none;
      border: 1px solid #335174;
      border-radius: 10px;
      background: rgba(8,16,28,.96);
      color: var(--txt);
      padding: 8px 10px;
      max-width: 320px;
      box-shadow: 0 12px 28px rgba(0,0,0,.35);
      opacity: 0;
      transform: translateY(6px);
      transition: opacity .12s ease, transform .12s ease;
    }}
    .hover-inspector.visible {{ opacity: 1; transform: translateY(0); }}
    .hover-inspector .label {{ display:block; color: var(--gold); font-weight:800; margin-bottom:2px; }}
    .hover-inspector .detail {{ display:block; color: var(--mut); font-size:12px; }}
    .hover-toggle {{ display:inline-flex; align-items:center; gap:8px; }}
    @media (max-width: 1320px) {{
      .hero-grid {{ grid-template-columns: repeat(3, minmax(0,1fr)); }}
      .controls-grid, .summary-grid, .session-top-grid {{ grid-template-columns: 1fr; }}
      .trade-summary-stack {{ position: static; }}
      .bet-grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
      .sessions-toolbar {{ flex-direction: column; align-items: flex-start; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>{esc(doc["title"])}</h1>
      <div class="sub">{esc(doc["subtitle"])}</div>
      <div class="sub" style="margin-top:6px;">runId={esc(doc["run"].get("runId"))} · strategy={esc(doc["run"].get("strategyId"))} · host={esc(doc["source"].get("hostPort"))} · source={esc(doc["source"].get("runDir"))}</div>
      <div class="sub" style="margin-top:12px; font-weight:700; color:var(--txt);">{esc("Live Run Truth" if doc["run"].get("isLiveTruthRun") else "Actual Run")}</div>
      <div class="hero-grid" id="actualMetrics"></div>
      <div class="sub" style="margin-top:14px; font-weight:700; color:var(--txt);">{esc("Strategy Intent / What-If" if doc["run"].get("isLiveTruthRun") else "Modeled Run")}</div>
      <div class="hero-grid" id="modelMetrics" style="margin-top:10px;"></div>
    </section>

    <section class="controls">
      <div class="controls-grid">
        <div class="control-card">
          <h2>Base Bet Sizes</h2>
          <div class="bet-grid" id="betInputs"></div>
          <div class="button-row">
            <button id="resetBetsBtn" type="button">Reset Bets</button>
            <button id="clearIgnoresBtn" type="button">Clear Ignores</button>
          </div>
        </div>
        <div class="control-card">
          <h2>Gold Bet Sizes</h2>
          <div class="bet-grid" id="goldBetInputs"></div>
          <div class="button-row">
            <button id="resetGoldBetsBtn" type="button">Reset Gold Bets</button>
            <button id="clearGoldBtn" type="button">Clear Gold</button>
          </div>
        </div>
        <div class="control-card">
          <h2>Run Notes</h2>
          <div class="trade-line"><b>Audit now base:</b> {esc(doc["source"].get("publicBase"))}</div>
          <div class="trade-line"><b>Entry cutoff:</b> {esc(doc["controls"].get("entryCutoffSec"))}s</div>
          <div class="trade-line"><b>Trace auto-ignore:</b> if missing &gt; 30% or low resolution</div>
          <div class="trade-line"><b>Gold selection:</b> use the Gold column in the interval table below</div>
          <div class="trade-line"><b>{esc("Current live run P/L" if doc["run"].get("isLiveTruthRun") else "Current run P/L")}:</b> <span class="mono">{esc(fmt_usd(doc["run"].get("actualNetPnlUsd")))}</span></div>
          <div class="trade-line"><b>{esc("Primary truth" if doc["run"].get("isLiveTruthRun") else "Primary metrics")}:</b> {esc("real live fills and real live exits" if doc["run"].get("isLiveTruthRun") else "actual run artifacts and resolved session truth")}</div>
          <div class="trade-line"><b>{esc("Secondary model" if doc["run"].get("isLiveTruthRun") else "Secondary view")}:</b> {esc("strategy intent / what-if sizing only" if doc["run"].get("isLiveTruthRun") else "modeled sizing and interval what-if")}</div>
          <div class="trade-line"><b>Baseline parity:</b> {esc("PASS" if ((doc.get("selfTest") or {}).get("baselineParity") or {}).get("passes") else "CHECK")} · net Δ <span class="mono">{esc(fmt_usd((((doc.get("selfTest") or {}).get("baselineParity") or {}).get("netDeltaUsd")) or 0.0))}</span></div>
          <div class="trade-line"><b>Requirement 1:</b> {esc("PASS" if ((((doc.get("selfTest") or {}).get("requirements") or {}).get("defaultSelectionsMatchActual") or {}).get("passes")) else "CHECK")} · default selected gold intervals keep Modeled = Actual</div>
          <div class="trade-line"><b>Requirement 2:</b> {esc("PASS" if ((((doc.get("selfTest") or {}).get("requirements") or {}).get("gold25MatchesUnchecked25") or {}).get("passes")) else "CHECK")} · no-gold $25 baseline equals all-default-gold-selected at $25</div>
          <div class="trade-line"><b>Modeled source:</b> same {esc((((doc.get("selfTest") or {}).get("baselineParity") or {}).get("modeledTradeCount")) or 0)} trades from {esc((((doc.get("selfTest") or {}).get("baselineParity") or {}).get("modeledSessionCount")) or 0)} sessions</div>
          <div class="trade-line"><b>Build reuse:</b> reused {esc(doc["buildStats"].get("reusedSessions"))} · rebuilt {esc(doc["buildStats"].get("rebuiltSessions"))}</div>
          <div class="trade-line">
            <label class="pill hover-toggle">
              <input id="hoverNamesToggle" type="checkbox" />
              Hover Names
            </label>
            <span class="mono" style="margin-left:8px;">Hover any page element to see its UI name</span>
          </div>
          <div class="button-row">
            <a class="pill compare-link" href="{esc(doc["links"].get("runAuditUrl"))}">REFRESH RUN AUDIT</a>
            <a class="pill compare-link" href="{esc(doc["links"].get("currentSessionAuditUrl"))}" target="_blank" rel="noopener noreferrer">AUDIT CURRENT SESSION</a>
          </div>
        </div>
      </div>
    </section>

    <section class="controls">
      <div class="summary-grid">
        <div class="control-card">
          <h2>P/L By Trade</h2>
          <div class="tbl"><table><thead><tr><th>Trade</th><th>Count</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Net</th><th>Fees</th><th>P/L / Trade</th></tr></thead><tbody id="byTradeBody"></tbody></table></div>
        </div>
        <div class="control-card">
          <h2>Win % By Trade And Interval</h2>
          <div class="tbl"><table><thead><tr><th class="goldCol">Gold</th><th>Trade</th><th>Interval</th><th>Count</th><th>Win %</th><th>Net</th><th>Fees</th><th>P/L / Trade</th></tr></thead><tbody id="byIntervalBody"></tbody></table></div>
        </div>
      </div>
    </section>

    <section class="controls">
      <div class="control-card">
        <div class="sessions-toolbar">
          <div class="sub" id="sessionsSummary"></div>
          <div class="button-row">
            <button id="loadMoreSessionsBtn" type="button">Load More Sessions</button>
            <button id="collapseSessionsBtn" type="button">Collapse To Summary</button>
          </div>
        </div>
      </div>
    </section>

    <section class="sessions" id="sessions"></section>
  </div>
  <div class="hover-inspector" id="hoverInspector">
    <span class="label" id="hoverInspectorLabel"></span>
    <span class="detail" id="hoverInspectorDetail"></span>
  </div>

  <script>
  window.RUN_AUDIT_DOC = {payload};
  (() => {{
    const doc = window.RUN_AUDIT_DOC;
    const stateRunScope = `${{String(doc.source.hostPort || 'local')}}_${{String(doc.run.runNum || '0')}}_${{String(doc.run.runId || doc.run.startedAtMs || 'noid')}}`;
    const stateSchemaVersion = 10;
    const stateKey = `run_audit_state_v10_${{stateRunScope}}`;
    const legacyStateKey = `run_audit_state_${{String(doc.run.runNum || '0')}}_${{String(doc.source.hostPort || 'local')}}`;
    const sessionsEl = document.getElementById('sessions');
    const actualMetricsEl = document.getElementById('actualMetrics');
    const modelMetricsEl = document.getElementById('modelMetrics');
    const byTradeBody = document.getElementById('byTradeBody');
    const byIntervalBody = document.getElementById('byIntervalBody');
    const betInputsEl = document.getElementById('betInputs');
    const goldBetInputsEl = document.getElementById('goldBetInputs');
    const clearGoldBtn = document.getElementById('clearGoldBtn');
    const resetGoldBetsBtn = document.getElementById('resetGoldBetsBtn');
    const clearIgnoresBtn = document.getElementById('clearIgnoresBtn');
    const resetBetsBtn = document.getElementById('resetBetsBtn');
    const sessionsSummaryEl = document.getElementById('sessionsSummary');
    const loadMoreSessionsBtn = document.getElementById('loadMoreSessionsBtn');
    const collapseSessionsBtn = document.getElementById('collapseSessionsBtn');
    const hoverNamesToggle = document.getElementById('hoverNamesToggle');
    const hoverInspector = document.getElementById('hoverInspector');
    const hoverInspectorLabel = document.getElementById('hoverInspectorLabel');
    const hoverInspectorDetail = document.getElementById('hoverInspectorDetail');
    const initialVisibleSessions = Math.max(
      1,
      Math.min(doc.sessions.length || 0, {RUN_AUDIT_INITIAL_VISIBLE_SESSIONS})
    );
    const sessionLoadStep = {RUN_AUDIT_SESSION_LOAD_STEP};

    const toNum = (v, d = NaN) => {{
      const n = Number(v);
      return Number.isFinite(n) ? n : d;
    }};
    const round4 = (v) => Math.round((Number(v || 0) + 1e-12) * 10000) / 10000;
    const round6 = (v) => Math.round((Number(v || 0) + 1e-12) * 1000000) / 1000000;
    const usd = (v) => {{
      const n = Number(v || 0);
      return `${{n >= 0 ? '+' : '-'}}$${{Math.abs(n).toFixed(2)}}`;
    }};
    const usdAbs = (v) => `$${{Math.abs(Number(v || 0)).toFixed(2)}}`;
    const usdBlankZero = (v) => {{
      const n = Number(v);
      if (!Number.isFinite(n)) return '—';
      return Math.abs(n) < 0.0000005 ? '' : usd(n);
    }};
    const usdAbsBlankZero = (v) => {{
      const n = Number(v);
      if (!Number.isFinite(n)) return '—';
      return Math.abs(n) < 0.0000005 ? '' : usdAbs(n);
    }};
    const ddUsd = (v) => `-$${{Math.abs(Number(v || 0)).toFixed(2)}}`;
    const pct = (v) => Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}%` : '—';
    const px = (v) => Number.isFinite(Number(v)) ? Number(v).toFixed(3) : '—';
    const sec = (v) => Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}s` : '—';
    const fixedBlankZero = (v, digits = 4) => {{
      const n = Number(v);
      if (!Number.isFinite(n)) return '—';
      return Math.abs(n) < 0.0000005 ? '' : n.toFixed(digits);
    }};

    const takeRateTable = {json.dumps(TAKER_EFFECTIVE_RATE_TABLE)};
    function takerRate(px) {{
      const x = toNum(px, 0);
      if (!(x > 0)) return 0;
      if (x <= takeRateTable[0][0]) return takeRateTable[0][1];
      for (let i = 1; i < takeRateTable.length; i += 1) {{
        const [pxHi, rateHi] = takeRateTable[i];
        const [pxLo, rateLo] = takeRateTable[i - 1];
        if (x <= pxHi) {{
          const span = pxHi - pxLo;
          if (!(span > 0)) return rateHi;
          const t = (x - pxLo) / span;
          return rateLo + ((rateHi - rateLo) * t);
        }}
      }}
      return takeRateTable[takeRateTable.length - 1][1];
    }}
    function calcTakerFee(pxVal, sharesVal) {{
      const p = toNum(pxVal, 0);
      const s = toNum(sharesVal, 0);
      if (!(p > 0 && s > 0)) return 0;
      return round4(p * s * takerRate(p));
    }}
    function computeEntryFromBudget(budgetUsd, entryPx, feeMode) {{
      const b = toNum(budgetUsd, 0);
      const p = toNum(entryPx, 0);
      if (!(b > 0 && p > 0)) return {{ budgetUsd: 0, principalUsd: 0, entryFeeUsd: 0, shares: 0 }};
      const rate = feeMode === 'taker' ? takerRate(p) : 0;
      const principal = b / (1 + rate);
      const shares = principal / p;
      return {{
        budgetUsd: round4(b),
        principalUsd: round4(principal),
        entryFeeUsd: round4(b - principal),
        shares: round4(shares),
      }};
    }}

    function loadState() {{
      try {{
        const persistedState = localStorage.getItem(stateKey) || localStorage.getItem(legacyStateKey) || 'null';
        return JSON.parse(persistedState) || {{}};
      }} catch {{
        return {{}};
      }}
    }}
    function saveState() {{
      const payload = {{
        __schema: stateSchemaVersion,
        __scope: stateRunScope,
        betByTrade: state.betByTrade,
        goldBetByTrade: state.goldBetByTrade,
        goldSelections: Array.from(state.goldSelections),
        ignoredSlugs: Array.from(state.ignoredSlugs),
        visibleSessionCount: state.visibleSessionCount,
        hoverNamesEnabled: state.hoverNamesEnabled,
      }};
      localStorage.setItem(stateKey, JSON.stringify(payload));
    }}

    const persisted = loadState();
    const isPersistedStateCompatible = (() => {{
      try {{
        return Boolean(
          persisted
          && persisted.__schema === stateSchemaVersion
          && persisted.__scope === stateRunScope
          && typeof persisted === 'object'
        );
      }} catch {{
        return false;
      }}
    }})();
    const defaultBetUsdByTrade = {{ ...doc.controls.defaultBetUsdByTrade }};
    const defaultGoldBetUsdByTrade = {{ ...doc.controls.defaultGoldBetUsdByTrade }};
    const defaultGoldSelectionKeys = new Set(
      Array.isArray(doc?.controls?.defaultGoldSelectionKeys)
        ? doc.controls.defaultGoldSelectionKeys.map((goldKey) => String(goldKey || '').trim()).filter((goldKey) => goldKey.length > 0)
        : []
    );
    function sessionTradesForModel(session) {{
      if (Array.isArray(session?.trades) && session.trades.length > 0) return session.trades;
      if (Array.isArray(session?.modelTrades) && session.modelTrades.length > 0) return session.modelTrades;
      return [];
    }}
    const tradeNums = Array.from(new Set([
      ...Object.keys(defaultBetUsdByTrade || {{}}),
      ...Object.keys(defaultGoldBetUsdByTrade || {{}}),
      ...(doc.sessions || []).flatMap((session) => (sessionTradesForModel(session).map((trade) => String(Number(trade.tradeNum || 0))).filter((tradeNum) => Number(tradeNum) > 0))),
    ])).sort((a, b) => Number(a) - Number(b));
    const state = {{
      betByTrade: {{ ...defaultBetUsdByTrade, ...((isPersistedStateCompatible ? (persisted.betByTrade || {{}}) : {{}})) }},
      goldBetByTrade: {{ ...defaultGoldBetUsdByTrade, ...((isPersistedStateCompatible ? (persisted.goldBetByTrade || {{}}) : {{}})) }},
      goldSelections: new Set(
        Array.isArray(isPersistedStateCompatible ? (persisted.goldSelections || []) : null)
          ? (persisted.goldSelections || [])
          : Array.from(defaultGoldSelectionKeys)
      ),
      ignoredSlugs: new Set(Array.isArray(isPersistedStateCompatible ? (persisted.ignoredSlugs || []) : []) ? (isPersistedStateCompatible ? (persisted.ignoredSlugs || []) : []) : []),
      visibleSessionCount: isPersistedStateCompatible
        ? Math.max(
          initialVisibleSessions,
          Math.min(doc.sessions.length || initialVisibleSessions, Number(persisted.visibleSessionCount || 0)),
          1
        )
        : Math.min(doc.sessions.length || initialVisibleSessions, initialVisibleSessions),
      hoverNamesEnabled: isPersistedStateCompatible ? !!persisted.hoverNamesEnabled : false,
      tradeModels: {{}},
      sessionModels: new Map(),
    }};

    const validSessionSlugs = new Set(
      (doc.sessions || [])
        .map((session) => String(session.slug || '').trim())
        .filter((slug) => slug.length > 0)
    );
    const validGoldSelections = new Set();
    defaultGoldSelectionKeys.forEach((goldKey) => {{
      if (String(goldKey || '').trim()) validGoldSelections.add(String(goldKey || '').trim());
    }});
    (doc.sessions || []).forEach((session) => {{
      sessionTradesForModel(session).forEach((trade) => {{
        const tradeNum = String(Number(trade.tradeNum || 0));
        const interval = String(trade.intervalLabel || 'unknown');
        validGoldSelections.add(`${{tradeNum}}|${{interval}}`);
      }});
    }});
    state.ignoredSlugs = new Set(
      Array.from(state.ignoredSlugs).filter((slug) => validSessionSlugs.has(String(slug || '').trim()))
    );
    state.goldSelections = new Set(
      Array.from(state.goldSelections).filter((goldKey) => validGoldSelections.has(String(goldKey || '').trim()))
    );
    if ((!isPersistedStateCompatible || !Array.isArray(persisted?.goldSelections)) && defaultGoldSelectionKeys.size > 0) {{
      state.goldSelections = new Set(defaultGoldSelectionKeys);
    }}

    function resolveUiName(target) {{
      if (!(target instanceof Element)) return null;
      const mappings = [
        ['#hoverNamesToggle', 'Hover Names Toggle', 'Execution panel control'],
        ['.hero', 'Hero', 'Top run summary block'],
        ['#actualMetrics .metric', 'Actual Metric Card', 'Actual/live-truth KPI card'],
        ['#modelMetrics .metric', 'Modeled Metric Card', 'Modeled/intent KPI card'],
        ['.controls-grid > .control-card:nth-child(1)', 'Base Bet Card', 'Control panel'],
        ['.controls-grid > .control-card:nth-child(2)', 'Gold Bet Card', 'Control panel'],
        ['.controls-grid > .control-card:nth-child(3)', 'Run Notes Card', 'Execution panel / run notes'],
        ['#betInputs', 'Base Bet Grid', 'Base bet sizing inputs'],
        ['#betInputs input', 'Base Bet Input', 'Trade-level base bet input'],
        ['#goldBetInputs', 'Gold Bet Grid', 'Gold bet sizing inputs'],
        ['#goldBetInputs input', 'Gold Bet Input', 'Trade-level gold bet input'],
        ['#resetBetsBtn', 'Reset Bets Button', 'Control action'],
        ['#clearIgnoresBtn', 'Clear Ignores Button', 'Control action'],
        ['#resetGoldBetsBtn', 'Reset Gold Bets Button', 'Control action'],
        ['#clearGoldBtn', 'Clear Gold Button', 'Control action'],
        ['#byTradeBody', 'By Trade Table Body', 'Trade summary rows'],
        ['#byTradeBody tr', 'By Trade Row', 'Trade summary row'],
        ['#byIntervalBody', 'By Interval Table Body', 'Trade-interval summary rows'],
        ['#byIntervalBody tr', 'By Interval Row', 'Trade-interval summary row'],
        ['#byIntervalBody input[type="checkbox"]', 'Gold Toggle', 'Trade-interval gold selector'],
        ['.sessions-toolbar', 'Sessions Toolbar', 'Session visibility controls'],
        ['#loadMoreSessionsBtn', 'Load More Sessions Button', 'Session visibility control'],
        ['#collapseSessionsBtn', 'Collapse To Summary Button', 'Session visibility control'],
        ['.session-card', 'Session Card', 'Per-session audit block'],
        ['.session-head', 'Session Header', 'Session title and actions'],
        ['.session-head h2', 'Session Title', 'Session slug'],
        ['.ignore-toggle', 'Ignore Session Toggle', 'Session inclusion control'],
        ['.trace-wrap', 'Trace Panel', 'Raw session trace container'],
        ['.trace-svg', 'Trace Chart', 'Raw UP/DOWN line chart'],
        ['.trade-summary-stack', 'Trade Summary Stack', 'Per-session execution summary rail'],
        ['.venue-truth-card', 'Venue Truth Card', 'Venue fill truth summary'],
        ['.trade-card', 'Trade Card', 'Per-trade execution summary'],
        ['tbody tr', 'Timeline Event Row', 'Session event/timeline row'],
        ['tfoot tr', 'Session Net Summary Footer', 'Session totals row'],
        ['a.compare-link', 'Audit Action Link', 'Refresh / audit navigation'],
      ];
      for (const [selector, label, detail] of mappings) {{
        const match = target.closest(selector);
        if (match) return {{ label, detail }};
      }}
      const el = target.closest('[id], [class], section, button, input, table, tr, td, th, svg');
      if (!el) return null;
      const tag = String(el.tagName || 'element').toLowerCase();
      const idPart = el.id ? `#${{el.id}}` : '';
      const classPart = Array.from(el.classList || []).slice(0, 3).map((name) => `.${{name}}`).join('');
      return {{
        label: `Unmapped ${{tag}}`,
        detail: `DOM target ${{tag}}${{idPart}}${{classPart}}`,
      }};
    }}

    function updateHoverInspector(target) {{
      if (!hoverInspector || !hoverInspectorLabel || !hoverInspectorDetail) return;
      if (!state.hoverNamesEnabled) {{
        hoverInspector.classList.remove('visible');
        return;
      }}
      const info = resolveUiName(target);
      if (!info) {{
        hoverInspector.classList.remove('visible');
        return;
      }}
      hoverInspectorLabel.textContent = info.label;
      hoverInspectorDetail.textContent = info.detail;
      hoverInspector.classList.add('visible');
    }}

    function positionHoverInspector(clientX, clientY) {{
      if (!hoverInspector || !state.hoverNamesEnabled) return;
      const margin = 18;
      const width = hoverInspector.offsetWidth || 220;
      const height = hoverInspector.offsetHeight || 56;
      let left = clientX + margin;
      let top = clientY + margin;
      if (left + width > window.innerWidth - 8) left = Math.max(8, clientX - width - margin);
      if (top + height > window.innerHeight - 8) top = Math.max(8, clientY - height - margin);
      hoverInspector.style.left = `${{left}}px`;
      hoverInspector.style.top = `${{top}}px`;
      hoverInspector.style.bottom = 'auto';
    }}

    function metricCard(label, value, sub = '', cls = '') {{
      return `<div class="metric"><b>${{label}}</b><span class="${{cls}}">${{value}}</span>${{sub ? `<small>${{sub}}</small>` : ''}}</div>`;
    }}

    function equityCard(label, points, endBalance, sub = '', cls = '') {{
      const vals = Array.isArray(points) ? points.map((v) => toNum(v, NaN)).filter((v) => Number.isFinite(v)) : [];
      const width = 320;
      const height = 56;
      const padX = 2;
      const padY = 4;
      let chart = '<small>No equity data</small>';
      if (vals.length >= 2) {{
        const minVal = Math.min(...vals);
        const maxVal = Math.max(...vals);
        const span = Math.max(1e-9, maxVal - minVal);
        const pts = vals.map((v, idx) => {{
          const x = padX + ((idx / Math.max(1, vals.length - 1)) * (width - padX * 2));
          const y = padY + ((maxVal - v) / span) * (height - padY * 2);
          return `${{x.toFixed(2)}},${{y.toFixed(2)}}`;
        }}).join(' ');
        const areaPts = `${{padX}},${{height - padY}} ${{pts}} ${{(width - padX).toFixed(2)}},${{height - padY}}`;
        chart = `
          <svg viewBox="0 0 ${{width}} ${{height}}" preserveAspectRatio="none" aria-label="${{label}} equity curve">
            <defs>
              <linearGradient id="equityFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#6cb2ff" />
                <stop offset="100%" stop-color="#6cb2ff" stop-opacity="0" />
              </linearGradient>
            </defs>
            <polygon class="fill" points="${{areaPts}}" />
            <polyline points="${{pts}}" />
          </svg>
        `;
      }}
      return `<div class="metric equity"><b>${{label}}</b><span class="${{cls}}">${{usd(endBalance - Number(doc.run.startBalanceUsd || 0))}}</span>${{sub ? `<small>${{sub}}</small>` : ''}}${{chart}}</div>`;
    }}

    function computeMaxDrawdown(points) {{
      const arr = Array.isArray(points) ? points.map((v) => toNum(v, NaN)).filter((v) => Number.isFinite(v)) : [];
      if (!arr.length) return {{ maxDrawdownUsd: 0, maxDrawdownPct: 0 }};
      let peak = arr[0];
      let maxDd = 0;
      let peakForMax = peak > 0 ? peak : 0;
      arr.forEach((bal) => {{
        if (bal > peak) peak = bal;
        const dd = Math.max(0, peak - bal);
        if (dd > maxDd) {{
          maxDd = dd;
          peakForMax = peak;
        }}
      }});
      return {{
        maxDrawdownUsd: round6(maxDd),
        maxDrawdownPct: round6(peakForMax > 0 ? (maxDd / peakForMax) * 100 : 0),
      }};
    }}

    function renderStaticMetrics() {{
      const run = doc.run;
      const actualEquityPoints = Array.isArray(run.actualEquityPoints) ? run.actualEquityPoints : [Number(run.startBalanceUsd || 0), Number(run.endBalanceUsd || 0)];
      actualMetricsEl.innerHTML = [
        equityCard('Actual Equity Curve', actualEquityPoints, Number(run.endBalanceUsd || 0), `Start $${{Number(run.startBalanceUsd || 0).toFixed(2)}} · End $${{Number(run.endBalanceUsd || 0).toFixed(2)}}`, Number(run.actualNetPnlUsd || 0) >= 0 ? 'pos' : 'neg'),
        metricCard('Actual P/L', usd(run.actualNetPnlUsd), `Net after fees`, Number(run.actualNetPnlUsd || 0) >= 0 ? 'pos' : 'neg'),
        metricCard('Gross P/L', usd(run.actualGrossPnlUsd), `Before fees`, Number(run.actualGrossPnlUsd || 0) >= 0 ? 'pos' : 'neg'),
        metricCard('Drawdown', ddUsd(run.actualMaxDrawdownUsd), `Peak-to-trough ${{pct(run.actualMaxDrawdownPct)}}`, 'neg'),
        metricCard('P/L Per Hour', `${{usd(run.profitPerHourUsd)}}/hr`, `Hours ${{Number(run.hoursTested || 0).toFixed(2)}}`, Number(run.profitPerHourUsd || 0) >= 0 ? 'pos' : 'neg'),
        metricCard('Trades Closed', String(run.tradesClosed || 0), `Wins ${{run.wins || 0}} · Losses ${{run.losses || 0}}`),
        metricCard('Win Rate', pct(run.winRatePct), `Fill source ${{run.executionFillSource || '—'}}`, Number(run.winRatePct || 0) >= 50 ? 'pos' : 'neg'),
        metricCard('CPU Health', Number.isFinite(Number(run.serverCpuAvgPct)) ? `${{Number(run.serverCpuAvgPct).toFixed(1)}}%` : '—', `p95 ${{Number.isFinite(Number(run.serverCpuP95Pct)) ? Number(run.serverCpuP95Pct).toFixed(1) + '%' : '—'}} · max ${{Number.isFinite(Number(run.serverCpuMaxPct)) ? Number(run.serverCpuMaxPct).toFixed(1) + '%' : '—'}}`, Number(run.serverCpuAvgPct || 0) <= 10 ? 'pos' : 'neg'),
        metricCard('300s Live Gap Avg', Number.isFinite(Number(run.serverEndSnapshotLiveGapMs)) ? `${{Number(run.serverEndSnapshotLiveGapMs).toFixed(1)}}ms` : '—', `rate ${{Number.isFinite(Number(run.serverEndSnapshotEffectiveHz)) ? Number(run.serverEndSnapshotEffectiveHz).toFixed(2) + '/s' : '—'}} · sessions ${{run.serverEndSnapshotSessionCount || 0}}`, Number(run.serverEndSnapshotLiveGapMs || 0) <= 25 ? 'pos' : 'neg'),
        metricCard('Fees', usdAbs(run.actualFeesUsd), `All market-order fees included`),
      ].join('');
    }}

    function renderControls() {{
      betInputsEl.innerHTML = tradeNums.map((tradeNum) => `
        <label class="field">
          <span>Trade ${{tradeNum}} Bet</span>
          <input type="number" min="0" step="0.01" data-trade-num="${{tradeNum}}" value="${{Number(state.betByTrade[tradeNum] || 0).toFixed(2)}}" />
        </label>
      `).join('');
      goldBetInputsEl.innerHTML = tradeNums.map((tradeNum) => `
        <label class="field">
          <span>Trade ${{tradeNum}} Gold Bet</span>
          <input type="number" min="0" step="0.01" data-gold-trade-num="${{tradeNum}}" value="${{Number(state.goldBetByTrade[tradeNum] || 0).toFixed(2)}}" />
        </label>
      `).join('');
    }}

    function tradeEntryIncluded(trade, session) {{
      if (state.ignoredSlugs.has(session.slug)) return false;
      return true;
    }}

    function hasActiveModeledOverrides() {{
      if (state.ignoredSlugs.size > 0) return true;
      if (state.goldSelections.size !== defaultGoldSelectionKeys.size) return true;
      for (const goldKey of defaultGoldSelectionKeys) {{
        if (!state.goldSelections.has(goldKey)) return true;
      }}
      for (const [tradeNum, defaultBet] of Object.entries(defaultBetUsdByTrade || {{}})) {{
        if (Math.abs(toNum(state.betByTrade[tradeNum], 0) - toNum(defaultBet, 0)) > 1e-6) return true;
      }}
      for (const [tradeNum, defaultBet] of Object.entries(defaultGoldBetUsdByTrade || {{}})) {{
        if (Math.abs(toNum(state.goldBetByTrade[tradeNum], 0) - toNum(defaultBet, 0)) > 1e-6) return true;
      }}
      return false;
    }}

    function tradeGoldKey(trade) {{
      return `${{String(Number(trade?.tradeNum || 0))}}|${{String(trade?.intervalLabel || 'unknown')}}`;
    }}

    function modelTrade(trade) {{
      const tradeNum = String(Number(trade.tradeNum || 0));
      const isGold = state.goldSelections.has(tradeGoldKey(trade));
      const actualBudgetUsd = toNum(trade.actualBudgetUsd, 0);
      const targetBetUsd = toNum(
        isGold ? state.goldBetByTrade[tradeNum] : state.betByTrade[tradeNum],
        actualBudgetUsd
      );
      if (Math.abs(targetBetUsd - actualBudgetUsd) <= 1e-6) {{
        return {{
          isGold,
          betUsd: round6(actualBudgetUsd),
          shares: round6(toNum(trade.actualShares, 0)),
          grossPnlUsd: round6(toNum(trade.actualGrossPnlUsd, 0)),
          feesUsd: round6(toNum(trade.actualFeesUsd, 0)),
          netPnlUsd: round6(toNum(trade.actualNetPnlUsd, 0)),
          closeLegs: (trade.closeLegs || []).map((leg) => ({{
            ...leg,
            sharesClosed: round6(toNum(leg.actualSharesClosed, 0)),
            grossPnlUsd: round6(toNum(leg.actualGrossPnlUsd, 0)),
            feesUsd: round6(Math.max(0, toNum(leg.actualGrossPnlUsd, 0) - toNum(leg.actualNetPnlUsd, toNum(leg.actualGrossPnlUsd, 0)))),
            netPnlUsd: round6(toNum(leg.actualNetPnlUsd, 0)),
          }})),
          rowMetrics: trade.actualRowMetrics || {{}},
        }};
      }}
      const betUsd = targetBetUsd;
      const scale = actualBudgetUsd > 0 ? (betUsd / actualBudgetUsd) : 0;
      const actualRows = trade.actualRowMetrics || {{}};
      const actualEntryMetrics = (trade.actualRowMetrics || {{}})[trade.entryRowId] || {{}};
      const scaledEntryShares = round6(toNum(trade.actualShares, 0) * scale);
      const scaledEntryFees = round6(toNum(actualEntryMetrics.fees, 0) * scale);
      const closeLegsSource = Array.isArray(trade.closeLegs) ? trade.closeLegs : [];
      if (closeLegsSource.length === 0) {{
        const aggregateGross = round6(toNum(trade.actualGrossPnlUsd, 0) * scale);
        const aggregateFees = round6(toNum(trade.actualFeesUsd, 0) * scale);
        const aggregateNet = round6(toNum(trade.actualNetPnlUsd, 0) * scale);
        const rowMetrics = {{}};
        if (trade.entryRowId && scaledEntryShares > 0) {{
          rowMetrics[trade.entryRowId] = {{
            shares: scaledEntryShares,
            gross: 0,
            fees: scaledEntryFees,
            net: round6(-scaledEntryFees),
          }};
        }}
        return {{
          isGold,
          betUsd: round6(betUsd),
          shares: scaledEntryShares,
          grossPnlUsd: aggregateGross,
          feesUsd: aggregateFees,
          netPnlUsd: aggregateNet,
          closeLegs: [],
          rowMetrics,
        }};
      }}
      let remaining = scaledEntryShares;
      let gross = 0;
      let fees = scaledEntryFees;
      let net = -scaledEntryFees;
      const rowMetrics = {{}};
      if (trade.entryRowId) {{
        rowMetrics[trade.entryRowId] = {{ shares: scaledEntryShares, gross: 0, fees: scaledEntryFees, net: round6(-scaledEntryFees) }};
      }}
      const closeLegs = closeLegsSource.map((leg, idx, arr) => {{
        const actualLegShares = toNum(leg.actualSharesClosed, 0);
        let sharesClosed = round6(actualLegShares * scale);
        if (idx === arr.length - 1) sharesClosed = round6(Math.max(0, remaining));
        else sharesClosed = round6(Math.max(0, Math.min(remaining, sharesClosed)));
        remaining = Math.max(0, remaining - sharesClosed);
        const actualLegRow = leg.fillRowId ? (actualRows[leg.fillRowId] || null) : null;
        const actualGross = toNum(actualLegRow ? actualLegRow.gross : leg.actualGrossPnlUsd, 0);
        const actualNet = toNum(actualLegRow ? actualLegRow.net : leg.actualNetPnlUsd, actualGross);
        const actualFees = toNum(actualLegRow ? actualLegRow.fees : Math.max(0, actualGross - actualNet), 0);
        const legGross = round6(actualGross * scale);
        const legFees = round6(actualFees * scale);
        const legNet = round6(actualNet * scale);
        gross += legGross;
        fees += legFees;
        net += legNet;
        if (leg.fillRowId) {{
          rowMetrics[leg.fillRowId] = {{
            shares: round6(sharesClosed),
            gross: round6(legGross),
            fees: round6(legFees),
            net: round6(legNet),
          }};
        }}
        return {{
          ...leg,
          sharesClosed: round6(sharesClosed),
          grossPnlUsd: round6(legGross),
          feesUsd: round6(legFees),
          netPnlUsd: round6(legNet),
        }};
      }});
      return {{
        isGold,
        betUsd: round6(betUsd),
        shares: scaledEntryShares,
        grossPnlUsd: round6(gross),
        feesUsd: round6(fees),
        netPnlUsd: round6(net),
        closeLegs,
        rowMetrics,
      }};
    }}

    function summarizeActualByIntervalRows() {{
      const rows = [];
      const source = doc?.summaries?.byTradeIntervalAtDefaultBets || {{}};
      Object.entries(source).forEach(([tradeNum, tradeMap]) => {{
        Object.entries(tradeMap || {{}}).forEach(([intervalKey, row]) => {{
          rows.push({{
            key: `${{tradeNum}}|${{intervalKey}}`,
            tradeNum: String(tradeNum),
            interval: String(intervalKey),
            count: Number(row?.count || 0),
            wins: Number(row?.wins || 0),
            losses: Number(row?.losses || 0),
            gross: Number(row?.grossPnlUsd || 0),
            fees: Number(row?.feesUsd || 0),
            net: Number(row?.netPnlUsd || 0),
            winRatePct: Number(row?.winRatePct),
            pnlPerTradeUsd: Number(row?.pnlPerTradeUsd || 0),
          }});
        }});
      }});
      return rows.sort((a,b) => (Number(a.tradeNum) - Number(b.tradeNum)) || String(a.interval).localeCompare(String(b.interval)));
    }}

    function buildDefaultActualSessionModels() {{
      const out = new Map();
      let runningBalance = Number(doc.run.startBalanceUsd || 100);
      const equityPoints = [runningBalance];
      let totalGross = 0;
      let totalFees = 0;
      let totalNet = 0;
      const chronologicalSessions = [...(doc.sessions || [])].sort((a, b) => Number(a?.startMs || 0) - Number(b?.startMs || 0));
      chronologicalSessions.forEach((session, idx) => {{
        const slug = String(session.slug || '');
        const included = (session.includedInActualTotals !== false) && !session.forcedIgnore;
        const finalState = String(session.finalState || '').trim().toLowerCase();
        const isPendingSession = finalState === 'pending';
        const net = (included && !isPendingSession) ? Number(session.actualSessionNetPnlUsd || 0) : 0;
        const gross = (included && !isPendingSession) ? Number(session.actualSessionGrossPnlUsd || 0) : 0;
        const fees = (included && !isPendingSession) ? Number(session.actualSessionFeesUsd || 0) : 0;
        if (included) {{
          runningBalance += net;
          totalGross += gross;
          totalFees += fees;
          totalNet += net;
        }}
        equityPoints.push(runningBalance);
        out.set(slug, {{
          gross: round6(gross),
          fees: round6(fees),
          net: round6(net),
          cumulativeBalanceUsd: round6(runningBalance),
          ignored: !!session.forcedIgnore,
        }});
      }});
      return {{
        sessionModels: out,
        equityPoints,
        totalGross: round6(totalGross),
        totalFees: round6(totalFees),
        totalNet: round6(totalNet),
        endBalance: round6(runningBalance),
      }};
    }}

    function updateSessionRenderControls() {{
      const total = Array.isArray(doc.sessions) ? doc.sessions.length : 0;
      const visible = Math.min(total, Math.max(1, Number(state.visibleSessionCount || initialVisibleSessions)));
      if (sessionsSummaryEl) {{
        sessionsSummaryEl.textContent = total > visible
          ? `Showing ${{visible}} of ${{total}} sessions. Open the rest on demand to keep the page responsive.`
          : `Showing all ${{total}} session${{total === 1 ? '' : 's'}}.`;
      }}
      if (loadMoreSessionsBtn) {{
        loadMoreSessionsBtn.hidden = visible >= total;
        loadMoreSessionsBtn.disabled = visible >= total;
        loadMoreSessionsBtn.textContent = visible >= total
          ? 'All Sessions Loaded'
          : `Load ${{Math.min(sessionLoadStep, total - visible)}} More Sessions`;
      }}
      if (collapseSessionsBtn) {{
        collapseSessionsBtn.hidden = total <= initialVisibleSessions;
        collapseSessionsBtn.disabled = visible <= initialVisibleSessions;
      }}
    }}

    function renderSessions() {{
      const total = Array.isArray(doc.sessions) ? doc.sessions.length : 0;
      const visible = Math.min(total, Math.max(1, Number(state.visibleSessionCount || initialVisibleSessions)));
      const sessionsToRender = doc.sessions.slice(-visible).reverse();
      sessionsEl.innerHTML = sessionsToRender.map((session, idx) => {{
        const trace = session.trace || null;
        const cardId = `sess-${{idx + 1}}`;
        const xStart = Number(session.startMs || 0);
        const xEnd = xStart + ({SESSION_WINDOW_SEC} * 1000);
        const pad = 28, w = 1120, h = 316;
        function sx(ts) {{
          return pad + (((Number(ts || xStart) - xStart) / Math.max(1, (xEnd - xStart))) * (w - pad * 2));
        }}
        function sy(pxVal) {{
          return h - pad - (Math.max(0, Math.min(1, Number(pxVal || 0))) * (h - pad * 2));
        }}
        function buildTraceSegments(xs, ys) {{
          const segs = [];
          let cur = [];
          let prevX = null;
          let prevBucket = null;
          for (let i = 0; i < xs.length; i += 1) {{
            const rawX = xs[i];
            const rawY = ys ? ys[i] : null;
            const xNum = Number(rawX);
            const yNum = Number(rawY);
            if (!Number.isFinite(xNum) || !Number.isFinite(yNum)) {{
              if (cur.length >= 2) segs.push(cur);
              cur = [];
              prevX = null;
              prevBucket = null;
              continue;
            }}
            if (prevX != null && xNum - prevX > 1500) {{
              if (cur.length >= 2) segs.push(cur);
              cur = [];
              prevBucket = null;
            }}
            const xPx = sx(xNum);
            const yPx = sy(yNum);
            const bucket = Math.round(xPx);
            const point = `${{xPx.toFixed(2)}},${{yPx.toFixed(2)}}`;
            if (cur.length && cur[cur.length - 1].x === xNum) {{
              cur[cur.length - 1] = {{ x: xNum, bucket, p: point }};
            }} else if (cur.length && prevBucket === bucket) {{
              cur[cur.length - 1] = {{ x: xNum, bucket, p: point }};
            }} else {{
              cur.push({{ x: xNum, bucket, p: point }});
            }}
            prevX = xNum;
            prevBucket = bucket;
          }}
          if (cur.length >= 2) segs.push(cur);
          return segs.map((seg) => seg.map((pt) => pt.p).join(' '));
        }}
        let upSegments = [];
        let downSegments = [];
        if (trace && Array.isArray(trace.xMs)) {{
          upSegments = buildTraceSegments(trace.xMs, trace.up || []);
          downSegments = buildTraceSegments(trace.xMs, trace.down || []);
        }}
        const gridLines = [0,0.25,0.5,0.75,1].map((v) => {{
          const y = sy(v).toFixed(2);
          return `<line x1="${{pad}}" y1="${{y}}" x2="${{w-pad}}" y2="${{y}}" stroke="#23314f" stroke-width="1" />`;
        }}).join('');
        const exitLatencyLines = (trade) => (trade.closeLegs || []).map((leg) => `
          <div class="trade-line"><b>${{leg.exitType || 'EXIT'}}</b> Latency ${{leg.signalToFillMs != null ? `${{leg.signalToFillMs}}ms` : '—'}} · <b>Signal Px</b> ${{px(leg.signalPx)}} · <b>Fill Δ</b> ${{leg.deltaPx != null ? `${{leg.deltaPx >= 0 ? '+' : ''}}${{Number(leg.deltaPx).toFixed(4)}} (${{leg.deltaPct >= 0 ? '+' : ''}}${{Number(leg.deltaPct).toFixed(2)}}%)` : '—'}}</div>
        `).join('');
        const venueTruthLines = Array.isArray(session.venueTruth?.fills) ? session.venueTruth.fills.map((fill) => {{
          const secLabel = Number.isFinite(Number(fill?.tsMs)) ? sec((Number(fill.tsMs) - Number(session.startMs || 0)) / 1000) : '—';
          return `<div class="trade-line"><b>${{String(fill?.dir || 'FILL')}}</b> ${{String(fill?.side || '—')}} · ${{Number.isFinite(Number(fill?.shares)) ? Number(fill.shares).toFixed(4) : '—'}} @ ${{px(fill?.price)}} · <b>t</b> ${{secLabel}}</div>`;
        }}) : [];
        const venueTruthCard = session.venueTruth ? `
          <div class="trade-card venue-truth-card">
            <div class="trade-card-head">
              <div>Venue Fill Truth</div>
              <div class="mono">${{Number(session.venueTruth.fillCount || 0)}} fill(s)</div>
            </div>
            <div class="trade-line"><b>Truth Source</b> ${{session.venueTruth.truthSource || 'venue_trade_history'}}${{session.trades && session.trades.length ? ' · bot signal context also present' : ' · bot signal context missing'}}</div>
            <div class="trade-line"><b>Reason</b> ${{session.venueTruth.reconciliationReason || '—'}}${{session.venueTruth.reconciliationError ? ` · err: ${{session.venueTruth.reconciliationError}}` : ''}}</div>
            ${{venueTruthLines.join('') || '<div class="trade-line">No venue fills found</div>'}}
          </div>
        ` : '';
        const fmtDecisionPoints = (points) => {{
          const rows = Array.isArray(points) ? points : [];
          return rows.map((pt) => {{
            const ckSec = Number.isFinite(Number(pt?.checkpointMs)) ? sec(Number(pt.checkpointMs) / 1000) : '—';
            const srcSec = Number.isFinite(Number(pt?.sourceTsMs)) ? sec(Number(pt.sourceTsMs) / 1000) : '—';
            return `${{ckSec}} → ${{px(pt?.px)}} <span class="mono">(src ${{srcSec}})</span>`;
          }}).join(' · ') || '—';
        }};
        const renderDecisionWindow = (label, window, chosenSide) => {{
          if (!window) return `
            <div class="decision-window">
              <div class="decision-title">${{label}}</div>
              <div class="trade-line">No persisted window</div>
            </div>
          `;
          const passed = window.passed === true;
          const stateClass = passed ? 'pass' : 'fail';
          const stateLabel = passed ? 'PASS' : (window.failureReason || 'FAIL');
          return `
            <div class="decision-window ${{stateClass}}">
              <div class="decision-title">${{label}}${{chosenSide === label ? ' · CHOSEN' : ''}}</div>
              <div class="trade-line"><b>Status</b> ${{stateLabel}}</div>
              <div class="trade-line"><b>Gain</b> ${{window.gain != null ? Number(window.gain).toFixed(4) : '—'}} · <b>Latest</b> ${{window.latestCheckpointMs != null ? sec(Number(window.latestCheckpointMs) / 1000) : '—'}}</div>
              <div class="decision-points">${{fmtDecisionPoints(window.points)}}</div>
            </div>
          `;
        }};
        const decisionCards = (session.decisionSnapshots || []).map((decision) => {{
          const snap = decision.snapshot || {{}};
          const windows = snap.windows || {{}};
          const chosenSide = String(snap.chosenSide || '').toUpperCase();
          const sideLabel = decision.side || chosenSide || '—';
          const topLabel = decision.tradeNum ? `Trade ${{decision.tradeNum}} ${{decision.label}}` : String(decision.label || 'Decision Snapshot');
          const eventClass = String(decision.event || '').toLowerCase();
          return `
            <div class="decision-card ${{eventClass}}">
              <div class="decision-head">
                <div>${{topLabel}}</div>
                <div class="mono">${{decision.offsetSec != null ? sec(decision.offsetSec) : '—'}} · ${{sideLabel}}</div>
              </div>
              <div class="trade-line"><b>Chosen</b> ${{chosenSide || 'NO ENTRY'}}${{snap.chosenEntryPx != null ? ` @ ${{px(snap.chosenEntryPx)}}` : ''}}${{decision.entryLatencyMs != null ? ` · <b>Fill latency</b> ${{decision.entryLatencyMs}}ms` : ''}}</div>
              <div class="trade-line"><b>Quotes</b> U ${{px(snap.upBid)}} / D ${{px(snap.downBid)}} · <b>Signal Px</b> ${{px(decision.signalPx)}}${{decision.entryPx != null ? ` · <b>Entry Px</b> ${{px(decision.entryPx)}}` : ''}}</div>
              <div class="trade-line"><b>Config</b> resample ${{snap.resampleMs != null ? `${{snap.resampleMs}}ms` : '—'}} · lookback ${{snap.slopeLookbackMs != null ? `${{snap.slopeLookbackMs}}ms` : '—'}} · min samples ${{snap.minSlopeSamples != null ? snap.minSlopeSamples : '—'}} · trigger ${{px(snap.entryBreakoutThr)}}</div>
              ${{decision.reason ? `<div class="trade-line"><b>Reason</b> ${{decision.reason}}</div>` : ''}}
              <div class="trade-line"><b>Rearm Blocked</b> UP ${{snap.rearmBlocked && snap.rearmBlocked.UP ? 'yes' : 'no'}} · DOWN ${{snap.rearmBlocked && snap.rearmBlocked.DOWN ? 'yes' : 'no'}}</div>
              <div class="decision-window-grid">
                ${{renderDecisionWindow('UP', windows.UP, chosenSide)}}
                ${{renderDecisionWindow('DOWN', windows.DOWN, chosenSide)}}
              </div>
            </div>
          `;
        }}).join('') || '<div class="empty">No persisted decision snapshots for this session.</div>';
        const strategyTradeCards = (session.trades || []).map((trade) => `
          <div class="trade-card" data-trade-key="${{trade.tradeKey}}" data-session-slug="${{session.slug}}">
            <div class="trade-card-head">
              <div>Trade ${{trade.tradeNum}} · ${{trade.side}} · <span class="mono">${{sec(trade.entryOffsetSec)}}</span></div>
              <div class="mono">${{String(trade.mode || trade.executionMode || session.sessionMode || 'unknown').toUpperCase()}} · ${{trade.intervalLabel}}</div>
            </div>
            <div class="trade-line"><b>Entry</b> ${{px(trade.entryPx)}} · <b>Signal</b> ${{sec((trade.entrySignalTsMs - session.startMs) / 1000)}} · <b>Order</b> ${{sec((trade.entryOrderTsMs - session.startMs) / 1000)}} · <b>Fill</b> ${{sec((trade.entryTsMs - session.startMs) / 1000)}}</div>
            <div class="trade-line"><b>Entry Latency</b> ${{trade.entrySignalToFillMs != null ? `${{trade.entrySignalToFillMs}}ms` : '—'}} · <b>Signal Px</b> ${{px(trade.entrySignalPx)}} · <b>Fill Δ</b> ${{trade.deltaPx != null ? `${{trade.deltaPx >= 0 ? '+' : ''}}${{Number(trade.deltaPx).toFixed(4)}} (${{trade.deltaPct >= 0 ? '+' : ''}}${{Number(trade.deltaPct).toFixed(2)}}%)` : '—'}}</div>
            <div class="trade-line"><b>Order</b> <span class="mono">${{trade.entryOrderIdTail || '—'}}</span> · <b>Exec</b> ${{trade.executionMode || '—'}} / ${{trade.fillSource || '—'}}</div>
            <div class="trade-line"><b>Actual</b> <span class="${{Number(trade.actualNetPnlUsd || 0) >= 0 ? 'pos' : 'neg'}}">${{usd(trade.actualNetPnlUsd)}}</span> · <b>Gross</b> ${{usd(trade.actualGrossPnlUsd)}} · <b>Fees</b> ${{usdAbs(trade.actualFeesUsd)}}</div>
            <div class="trade-line"><b>Modeled</b> <span class="modeled-net mono" data-trade-key="${{trade.tradeKey}}">—</span> · Fees <span class="modeled-fees mono" data-trade-key="${{trade.tradeKey}}">—</span> · Bet <span class="modeled-bet mono" data-trade-key="${{trade.tradeKey}}">—</span> <span class="modeled-gold mono" data-trade-key="${{trade.tradeKey}}"></span></div>
            ${{exitLatencyLines(trade)}}
          </div>
        `).join('');
        const tradeCards = `${{venueTruthCard || ''}}${{strategyTradeCards || ''}}` || '<div class="empty">No trades found</div>';
        const endSnapshot = session.streamEndSnapshot || null;
        const thresholdPill = (gapMs) => {{
          if (!endSnapshot || !Array.isArray(endSnapshot.thresholds)) return '';
          const row = endSnapshot.thresholds.find((entry) => Number(entry.gapMs) === Number(gapMs));
          if (!row) return '';
          const pctText = Number.isFinite(Number(row.pct)) ? `${{Number(row.pct).toFixed(1)}}%` : '—';
          const countText = Number.isFinite(Number(row.count)) ? `(${{row.count}})` : '';
          return `<span class="pill"><b>&gt;${{Number(gapMs)}}ms</b> ${{pctText}} <span class="mono">${{countText}}</span></span>`;
        }};
        const endSnapshotLine = endSnapshot ? `
          <div class="session-actions" style="margin-bottom:10px;">
            <span class="pill"><b>Server Snapshot</b> ${{endSnapshot.label || '300s snapshot'}}</span>
            <span class="pill"><b>Live Gap</b> ${{Number.isFinite(Number(endSnapshot.liveGapMs)) ? `${{Math.round(Number(endSnapshot.liveGapMs))}}ms` : '—'}}</span>
            <span class="pill"><b>Avg Gap</b> ${{Number.isFinite(Number(endSnapshot.avgGapMs)) ? `${{Math.round(Number(endSnapshot.avgGapMs))}}ms` : '—'}}</span>
            <span class="pill"><b>p95</b> ${{Number.isFinite(Number(endSnapshot.p95GapMs)) ? `${{Math.round(Number(endSnapshot.p95GapMs))}}ms` : '—'}}</span>
            <span class="pill"><b>Rate</b> ${{Number.isFinite(Number(endSnapshot.effectiveHz)) ? `${{Number(endSnapshot.effectiveHz).toFixed(1)}}/s` : '—'}}</span>
            ${{thresholdPill(25)}}
            ${{thresholdPill(50)}}
            ${{thresholdPill(100)}}
          </div>
        ` : '';
        const rows = (session.timelineRows || []).map((row) => `
          <tr data-card="${{cardId}}" data-row-id="${{row.rowId}}" data-trade-key="${{row.tradeKey || (row.tradeNum ? `${{session.slug}}-t${{row.tradeNum}}` : '')}}" data-ts="${{row.tsMs || ''}}" data-px="${{Number.isFinite(Number(row.px)) ? row.px : ''}}">
            <td>${{row.tradeNum || '—'}}</td>
            <td>${{row.side || '—'}}</td>
            <td class="mono">${{sec(row.offsetSec)}}</td>
            <td>${{row.label}}</td>
            <td>${{String(row.stage || '—').toUpperCase()}}</td>
            <td>${{px(row.signalPx)}}</td>
            <td>${{px(row.intendedPx)}}</td>
            <td>${{px(row.actualFillPx ?? row.px)}}</td>
            <td>${{px(row.px)}}</td>
            <td class="mono">${{Number.isFinite(Number(row.nominalUsd)) ? usdAbsBlankZero(row.nominalUsd) : '—'}}</td>
            <td class="mono actual-shares">${{fixedBlankZero(row.sharesActual, 4)}}</td>
            <td class="mono">${{fixedBlankZero(row.sharesClosedActual, 4)}}</td>
            <td class="mono">${{fixedBlankZero(row.sharesRemainingActual, 4)}}</td>
            <td class="mono">${{Number.isFinite(Number(row.actualGrossPnlUsd)) ? usdBlankZero(row.actualGrossPnlUsd) : '—'}}</td>
            <td class="mono">${{Number.isFinite(Number(row.actualFeesUsd)) ? usdBlankZero(row.actualFeesUsd) : '—'}}</td>
            <td class="mono">${{Number.isFinite(Number(row.actualNetPnlUsd)) ? usdBlankZero(row.actualNetPnlUsd) : '—'}}</td>
            <td class="mono">${{row.signalToOrderMs != null ? `${{row.signalToOrderMs}}ms` : '—'}}</td>
            <td class="mono">${{row.signalToFillMs != null ? `${{row.signalToFillMs}}ms` : '—'}}</td>
            <td class="mono">${{row.orderIdTail || '—'}}</td>
            <td class="mono">${{[row.mode, row.executionMode, row.fillSource].filter(Boolean).join(' / ') || '—'}}</td>
            <td class="mono">${{row.note || '—'}}</td>
          </tr>
        `).join('');
        const overlays = (session.trades || []).map((trade) => {{
          const marks = [];
          const points = [];
          if (Number.isFinite(Number(trade.entryTsMs)) && Number.isFinite(Number(trade.entryPx))) {{
            points.push({{ts: trade.entryTsMs, px: trade.entryPx, kind: 'entry'}});
          }}
          (trade.closeLegs || []).forEach((leg) => {{
            if (Number.isFinite(Number(leg.fillTsMs)) && Number.isFinite(Number(leg.exitPx))) {{
              points.push({{ts: leg.fillTsMs, px: leg.exitPx, kind: 'exit'}});
            }}
          }});
          const path = points.map((p) => `${{sx(p.ts).toFixed(2)}},${{sy(p.px).toFixed(2)}}`).join(' ');
          const color = trade.tradeNum === 1 ? '#6cb2ff' : (trade.tradeNum === 2 ? '#ffcc4d' : '#39d98a');
          const group = [`<g class="trace-group" data-trade-key="${{trade.tradeKey}}">`];
          if (points.length >= 2) group.push(`<polyline fill="none" stroke="${{color}}" stroke-width="4" points="${{path}}" />`);
          points.forEach((p) => {{
            group.push(`<circle cx="${{sx(p.ts).toFixed(2)}}" cy="${{sy(p.px).toFixed(2)}}" r="4.5" fill="${{color}}" stroke="#fff" stroke-width="1.1" />`);
          }});
          group.push('</g>');
          return group.join('');
        }}).join('');
        const traceSvgMarkup = session.traceSvgFallback || `
                <svg class="trace-svg" viewBox="0 0 1120 316" data-card="${{cardId}}" data-minx="${{xStart}}" data-maxx="${{xEnd}}" data-pad="${{pad}}" data-w="1120" data-h="316">
                  <rect x="0" y="0" width="1120" height="316" fill="#0b1323" />
                  ${{gridLines}}
                  <line x1="${{pad}}" y1="${{sy(0.55).toFixed(2)}}" x2="${{1120-pad}}" y2="${{sy(0.55).toFixed(2)}}" stroke="#6cb2ff" stroke-dasharray="4 3" />
                  <line x1="${{pad}}" y1="${{sy(0.96).toFixed(2)}}" x2="${{1120-pad}}" y2="${{sy(0.96).toFixed(2)}}" stroke="#39d98a" stroke-dasharray="4 3" />
                  <line x1="${{pad}}" y1="${{sy(0.40).toFixed(2)}}" x2="${{1120-pad}}" y2="${{sy(0.40).toFixed(2)}}" stroke="#ff7b91" stroke-dasharray="4 3" />
                  ${{upSegments.map((pts) => `<polyline fill="none" stroke="#f2d14c" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" points="${{pts}}" />`).join('')}}
                  ${{downSegments.map((pts) => `<polyline fill="none" stroke="#cfd6e3" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" points="${{pts}}" />`).join('')}}
                  ${{overlays}}
                  <line id="cursor-${{cardId}}" x1="${{pad}}" y1="${{pad}}" x2="${{pad}}" y2="${{316-pad}}" stroke="#ff66c4" stroke-dasharray="5 4" opacity="0" />
                  <circle id="dot-${{cardId}}" cx="${{pad}}" cy="${{pad}}" r="5" fill="#ff66c4" opacity="0" />
                </svg>
        `;
        return `
          <section class="session-card" data-session-slug="${{session.slug}}">
            <div class="session-head">
              <div>
                <h2>${{session.slug}}</h2>
                <div class="sub">${{new Date(session.startMs).toLocaleString('en-US', {{ timeZone: 'America/Los_Angeles', hour12: true }})}} · trace ${{session.traceQuality.usable ? 'usable' : 'flagged'}} · samples ${{session.traceQuality.sampleCount}}${{session.syntheticMissing ? ' · missing from run artifacts' : ''}}</div>
              </div>
              <div class="session-actions">
                <label class="pill">
                  <input type="checkbox" class="ignore-toggle" data-slug="${{session.slug}}" ${{(state.ignoredSlugs.has(session.slug) || session.forcedIgnore) ? 'checked' : ''}} ${{session.forcedIgnore ? 'disabled' : ''}} />
                  Ignore Session
                </label>
                <span class="pill"><b>Actual</b> ${{usd(session.actualSessionNetPnlUsd)}}</span>
                <span class="pill"><b>Mode</b> ${{String(session.sessionMode || 'unknown').toUpperCase()}}</span>
                <span class="pill"><b>Final</b> ${{session.truthVerification && session.truthVerification.finalOutcomeSide ? session.truthVerification.finalOutcomeSide : 'pending'}}</span>
                <span class="pill"><b>Rounded Match</b> ${{session.truthVerification && session.truthVerification.roundedInferenceMatchesTruth === true ? 'YES' : (session.truthVerification && session.truthVerification.roundedInferenceMatchesTruth === false ? 'NO' : '—')}}</span>
                <span class="pill"><b>Included</b> <span class="session-modeled-net mono" data-slug="${{session.slug}}">—</span></span>
                <span class="pill"><b>Cumulative</b> <span class="session-cum mono" data-slug="${{session.slug}}">—</span></span>
                <a class="pill compare-link" href="${{session.auditUrl}}" target="_blank" rel="noopener noreferrer">AUDIT NOW</a>
              </div>
            </div>
            <div class="session-actions" style="margin-bottom:10px;">
              ${{(session.autoIgnoreReasons || []).map((reason) => `<span class="pill warn">${{reason}}</span>`).join('') || '<span class="pill">no auto-ignore flags</span>'}}
              ${{session.unresolvedPosition ? '<span class="pill warn">reconcile pending</span>' : ''}}
              ${{session.unresolvedPosition && session.reconcileStatus ? `<span class="pill warn">${{`reconcile:${{session.reconcileStatus}}`}}</span>` : ''}}
              ${{session.unresolvedPosition && Number.isFinite(Number(session.reconcileAgeMs)) ? `<span class="pill warn">${{`age:${{Math.round(Number(session.reconcileAgeMs) / 1000)}}s`}}</span>` : ''}}
              ${{session.includedInActualTotals === false ? '<span class="pill warn">excluded from totals</span>' : ''}}
              ${{session.truthVerification && session.truthVerification.truthStatus ? `<span class="pill">${{`truth:${{session.truthVerification.truthStatus}}`}}</span>` : ''}}
              ${{session.truthVerification && Number.isFinite(Number(session.truthVerification.truthNetPnlDeltaUsd)) ? `<span class="pill">${{`truth delta ${{usd(session.truthVerification.truthNetPnlDeltaUsd)}}`}}</span>` : ''}}
              ${{session.venueTruth && Number(session.venueTruth.fillCount || 0) > 0 ? `<span class="pill">${{`venue fills:${{Number(session.venueTruth.fillCount || 0)}}`}}</span>` : ''}}
              ${{session.venueTruth && session.venueTruth.bySide && session.venueTruth.bySide.UP && Number(session.venueTruth.bySide.UP.buyShares || 0) > 0 ? `<span class="pill">${{`venue UP buy ${{Number(session.venueTruth.bySide.UP.buyShares).toFixed(4)}} @ ${{px(session.venueTruth.bySide.UP.avgBuyPx)}}`}}</span>` : ''}}
              ${{session.venueTruth && session.venueTruth.bySide && session.venueTruth.bySide.DOWN && Number(session.venueTruth.bySide.DOWN.buyShares || 0) > 0 ? `<span class="pill">${{`venue DOWN buy ${{Number(session.venueTruth.bySide.DOWN.buyShares).toFixed(4)}} @ ${{px(session.venueTruth.bySide.DOWN.avgBuyPx)}}`}}</span>` : ''}}
            </div>
            <div class="session-top-grid">
              <div class="trace-wrap">
                <div class="trace-title">
                  <span>Trace Chart</span>
                  <span class="trace-sub">UP/DOWN low-fidelity session trace with trade overlays</span>
                </div>
                ${{traceSvgMarkup}}
              </div>
              <div class="trade-summary-stack">${{tradeCards}}</div>
            </div>
            ${{endSnapshotLine}}
            <div class="tbl">
              <table>
                <thead>
                  <tr>
                    <th>Trade</th><th>Side</th><th>t</th><th>Event</th><th>Stage</th><th>Signal Px</th><th>Intended Px</th><th>Fill Px</th><th>Row Px</th>
                    <th>Nominal</th><th>Shares</th><th>Closed</th><th>Remain</th><th>Gross</th><th>Fees</th><th>Net</th><th>Sig→Ord</th><th>Sig→Fill</th><th>Order</th><th>Exec</th><th>Note</th>
                  </tr>
                </thead>
                <tbody>${{rows}}</tbody>
                <tfoot>
                  <tr>
                    <td colspan="13">Session Net Summary</td>
                    <td class="mono session-gross" data-slug="${{session.slug}}">—</td>
                    <td class="mono session-fees" data-slug="${{session.slug}}">—</td>
                    <td class="mono session-net" data-slug="${{session.slug}}">—</td>
                    <td colspan="4"></td>
                  </tr>
                </tfoot>
              </table>
            </div>
          </section>
        `;
      }}).join('');
      bindSessionInteractions();
      updateSessionRenderControls();
    }}

    function bindSessionInteractions() {{
      document.querySelectorAll('.ignore-toggle').forEach((el) => {{
        el.addEventListener('change', (ev) => {{
          const slug = String(ev.target.getAttribute('data-slug') || '');
          if (!slug) return;
          if (ev.target.checked) state.ignoredSlugs.add(slug);
          else state.ignoredSlugs.delete(slug);
          saveState();
          recalcAndRender();
        }});
      }});

      document.querySelectorAll('.trade-card').forEach((card) => {{
        const key = String(card.getAttribute('data-trade-key') || '');
        const root = card.closest('.session-card');
        const groups = root ? root.querySelectorAll('.trace-group') : [];
        const rows = root ? root.querySelectorAll(`tbody tr[data-trade-key="${{key}}"]`) : [];
        const activate = () => {{
          groups.forEach((g) => {{
            const on = String(g.getAttribute('data-trade-key') || '') === key;
            g.classList.toggle('active', on);
            g.classList.toggle('dimmed', !on);
          }});
          rows.forEach((r) => r.classList.add('active'));
        }};
        const clear = () => {{
          groups.forEach((g) => {{ g.classList.remove('active'); g.classList.remove('dimmed'); }});
          rows.forEach((r) => r.classList.remove('active'));
        }};
        card.addEventListener('mouseenter', activate);
        card.addEventListener('mouseleave', clear);
        card.addEventListener('click', activate);
      }});

      document.querySelectorAll('tbody tr[data-card]').forEach((row) => {{
        row.addEventListener('mouseenter', () => {{
          const cardId = String(row.getAttribute('data-card') || '');
          const svg = document.querySelector(`svg[data-card="${{cardId}}"]`);
          const line = document.getElementById(`cursor-${{cardId}}`);
          const dot = document.getElementById(`dot-${{cardId}}`);
          if (!svg || !line || !dot) return;
          const minX = toNum(svg.getAttribute('data-minx'));
          const maxX = toNum(svg.getAttribute('data-maxx'));
          const pad = toNum(svg.getAttribute('data-pad'), 28);
          const w = toNum(svg.getAttribute('data-w'), 1120);
          const h = toNum(svg.getAttribute('data-h'), 316);
          const ts = toNum(row.getAttribute('data-ts'));
          const pxVal = toNum(row.getAttribute('data-px'));
          const x = pad + (((ts - minX) / Math.max(1, (maxX - minX))) * (w - pad * 2));
          line.setAttribute('x1', String(x)); line.setAttribute('x2', String(x)); line.setAttribute('opacity', '1');
          if (Number.isFinite(pxVal)) {{
            const y = h - pad - (Math.max(0, Math.min(1, pxVal)) * (h - pad * 2));
            dot.setAttribute('cx', String(x)); dot.setAttribute('cy', String(y)); dot.setAttribute('opacity', '1');
          }} else {{
            dot.setAttribute('opacity', '0');
          }}
        }});
      }});
    }}

    function recalcAndRender() {{
      if (!hasActiveModeledOverrides()) {{
        const actual = doc.run || {{}};
        const baseline = buildDefaultActualSessionModels();
        const actualEquityPoints = Array.isArray(actual.actualEquityPoints)
          ? actual.actualEquityPoints
          : (Array.isArray(baseline.equityPoints) ? baseline.equityPoints : [Number(actual.startBalanceUsd || 0), Number(actual.endBalanceUsd || 0)]);
        const actualEndBalance = Number(actualEquityPoints.length ? actualEquityPoints[actualEquityPoints.length - 1] : (actual.endBalanceUsd ?? baseline.endBalance ?? 0));
        const actualStartBalance = Number(actual.startBalanceUsd ?? (actualEquityPoints[0] ?? (Array.isArray(baseline.equityPoints) && baseline.equityPoints.length ? baseline.equityPoints[0] : 0)));
        const actualNet = Number(actual.actualNetPnlUsd ?? (actualEndBalance - actualStartBalance));
        const actualFees = Number(actual.actualFeesUsd ?? baseline.totalFees ?? 0);
        const actualWinRate = Number(actual.winRatePct);
        const actualDrawdownUsd = Number(actual.actualMaxDrawdownUsd ?? 0);
        const actualDrawdownPct = Number(actual.actualMaxDrawdownPct ?? 0);
        const actualTradesClosed = Number(actual.tradesClosed ?? 0);
        const actualWins = Number(actual.wins ?? 0);
        const actualLosses = Number(actual.losses ?? 0);
        const actualHours = Number(actual.hoursTested || 0);

        state.tradeModels = {{}};
        state.sessionModels = baseline.sessionModels;
        doc.sessions.forEach((session) => {{
          sessionTradesForModel(session).forEach((trade) => {{
            state.tradeModels[trade.tradeKey] = modelTrade(trade);
          }});
        }});

        modelMetricsEl.innerHTML = [
          equityCard('Modeled Equity Curve', actualEquityPoints, actualEndBalance, 'Matches actual until bet sizing or interval filters are changed', actualNet >= 0 ? 'pos' : 'neg'),
          metricCard('Modeled P/L', usd(actualNet), 'Exact match to actual before tuning', actualNet >= 0 ? 'pos' : 'neg'),
          metricCard('Gross P/L', usd(actualNet + actualFees), 'Before fees', (actualNet + actualFees) >= 0 ? 'pos' : 'neg'),
          metricCard('Drawdown', ddUsd(actualDrawdownUsd), `Peak-to-trough ${{pct(actualDrawdownPct)}}`, 'neg'),
          metricCard('P/L Per Hour', actualHours > 0 ? `${{usd(actualNet / actualHours)}}/hr` : '—', `Hours ${{actualHours.toFixed(2)}}`, (actualHours > 0 && actualNet / actualHours >= 0) ? 'pos' : 'neg'),
          metricCard('Modeled Win Rate', pct(actualWinRate), `Trades ${{actualTradesClosed}} · Wins ${{actualWins}} · Losses ${{actualLosses}}`, Number.isFinite(actualWinRate) && actualWinRate >= 50 ? 'pos' : 'neg'),
          metricCard('Fees', usdAbs(actualFees), 'Exact match to actual before tuning'),
        ].join('');

        const actualByTrade = doc?.summaries?.byTradeAtDefaultBets || {{}};
        byTradeBody.innerHTML = Object.keys(actualByTrade).sort((a,b)=>Number(a)-Number(b)).map((tradeKey) => {{
          const row = actualByTrade[tradeKey] || {{}};
          const count = Number(row.count || 0);
          const wins = Number(row.wins || 0);
          const losses = Number(row.losses || 0);
          const net = Number(row.netPnlUsd || 0);
          const fees = Number(row.feesUsd || 0);
          const wr = Number(row.winRatePct);
          const perTrade = Number(row.pnlPerTradeUsd || 0);
          return `<tr><td>Trade ${{tradeKey}}</td><td>${{count}}</td><td>${{wins}}</td><td>${{losses}}</td><td>${{pct(wr)}}</td><td class="${{net >= 0 ? 'pos' : 'neg'}}">${{usd(net)}}</td><td>${{usdAbs(fees)}}</td><td class="${{perTrade >= 0 ? 'pos' : 'neg'}}">${{usd(perTrade)}}</td></tr>`;
        }}).join('') || '<tr><td colspan="8" class="empty">No actual trades recorded.</td></tr>';

        const actualIntervalRows = summarizeActualByIntervalRows();
        byIntervalBody.innerHTML = actualIntervalRows.map((row) => {{
          const goldChecked = state.goldSelections.has(String(row.key || ''));
          return `<tr><td class="goldCol"><input type="checkbox" class="gold-toggle" data-gold-key="${{row.key}}" ${{goldChecked ? 'checked' : ''}} /></td><td>Trade ${{row.tradeNum}}</td><td>${{row.interval}}</td><td>${{row.count}}</td><td>${{pct(row.winRatePct)}}</td><td class="${{row.net >= 0 ? 'pos' : 'neg'}}">${{usd(row.net)}}</td><td>${{usdAbs(row.fees)}}</td><td class="${{row.pnlPerTradeUsd >= 0 ? 'pos' : 'neg'}}">${{usd(row.pnlPerTradeUsd)}}</td></tr>`;
        }}).join('') || '<tr><td colspan="8" class="empty">No interval rows for the actual baseline.</td></tr>';

        doc.sessions.forEach((session) => {{
          const slug = String(session.slug);
          const card = document.querySelector(`.session-card[data-session-slug="${{slug}}"]`);
          const model = state.sessionModels.get(slug);
          if (!card || !model) return;
          card.classList.toggle('ignored', !!model.ignored);
          const netEls = card.querySelectorAll(`.session-modeled-net[data-slug="${{slug}}"], .session-net[data-slug="${{slug}}"]`);
          netEls.forEach((el) => {{
            el.textContent = usd(model.net);
            el.classList.toggle('pos', model.net >= 0);
            el.classList.toggle('neg', model.net < 0);
          }});
          const grossEl = card.querySelector(`.session-gross[data-slug="${{slug}}"]`);
          const feesEl = card.querySelector(`.session-fees[data-slug="${{slug}}"]`);
          const cumEl = card.querySelector(`.session-cum[data-slug="${{slug}}"]`);
          if (grossEl) {{ grossEl.textContent = usdBlankZero(model.gross); grossEl.classList.toggle('pos', model.gross >= 0); grossEl.classList.toggle('neg', model.gross < 0); }}
          if (feesEl) feesEl.textContent = usdAbsBlankZero(model.fees);
          if (cumEl) cumEl.textContent = `$${{Number(model.cumulativeBalanceUsd || 0).toFixed(2)}}`;

          (session.trades || []).forEach((trade) => {{
            const tradeModel = state.tradeModels[trade.tradeKey];
            if (!tradeModel) return;
            const netEl = card.querySelector(`.modeled-net[data-trade-key="${{trade.tradeKey}}"]`);
            const feesElTrade = card.querySelector(`.modeled-fees[data-trade-key="${{trade.tradeKey}}"]`);
            const betElTrade = card.querySelector(`.modeled-bet[data-trade-key="${{trade.tradeKey}}"]`);
            const goldElTrade = card.querySelector(`.modeled-gold[data-trade-key="${{trade.tradeKey}}"]`);
            if (netEl) {{ netEl.textContent = usd(tradeModel.netPnlUsd); netEl.classList.toggle('pos', tradeModel.netPnlUsd >= 0); netEl.classList.toggle('neg', tradeModel.netPnlUsd < 0); }}
            if (feesElTrade) feesElTrade.textContent = usdAbs(tradeModel.feesUsd);
            if (betElTrade) betElTrade.textContent = usdAbs(tradeModel.betUsd);
            if (goldElTrade) goldElTrade.textContent = tradeModel.isGold ? ' GOLD' : '';
          }});
        }});

        saveState();
        return;
      }}

      if (state.ignoredSlugs.size === 0) {{
        const actual = doc.run || {{}};
        const baseline = buildDefaultActualSessionModels();
        state.tradeModels = {{}};
        state.sessionModels = new Map(
          Array.from(baseline.sessionModels.entries()).map(([slug, model]) => [slug, {{ ...model }}])
        );
        const byTrade = Object.fromEntries(
          Object.entries(doc?.summaries?.byTradeAtDefaultBets || {{}}).map(([tradeKey, row]) => [tradeKey, {{
            count: Number(row?.count || 0),
            wins: Number(row?.wins || 0),
            losses: Number(row?.losses || 0),
            gross: Number(row?.grossPnlUsd || 0),
            fees: Number(row?.feesUsd || 0),
            net: Number(row?.netPnlUsd || 0),
          }}])
        );
        const byInterval = Object.fromEntries(
          summarizeActualByIntervalRows().map((row) => [String(row.key || ''), {{
            key: String(row.key || ''),
            tradeNum: String(row.tradeNum || ''),
            interval: String(row.interval || 'unknown'),
            count: Number(row.count || 0),
            wins: Number(row.wins || 0),
            losses: Number(row.losses || 0),
            gross: Number(row.gross || 0),
            fees: Number(row.fees || 0),
            net: Number(row.net || 0),
          }}])
        );
        let totalGross = Number(baseline.totalGross || 0);
        let totalFees = Number(actual.actualFeesUsd ?? baseline.totalFees ?? 0);
        let totalNet = Number(actual.actualNetPnlUsd ?? baseline.totalNet ?? 0);
        const totalTrades = Number(actual.tradesClosed || 0);
        const totalWins = Number(actual.wins || 0);
        const totalLosses = Number(actual.losses || 0);
        const includedSessions = (doc.sessions || []).filter((session) => (session.includedInActualTotals !== false) && !session.forcedIgnore).length;
        const ignoredSessions = 0;

        doc.sessions.forEach((session) => {{
          const slug = String(session.slug || '');
          const sessionModel = state.sessionModels.get(slug) || {{
            gross: 0,
            fees: 0,
            net: 0,
            cumulativeBalanceUsd: 0,
            ignored: false,
          }};
          sessionTradesForModel(session).forEach((trade) => {{
            const model = modelTrade(trade);
            state.tradeModels[trade.tradeKey] = model;
            const actualGross = toNum(trade.actualGrossPnlUsd, 0);
            const actualFees = toNum(trade.actualFeesUsd, 0);
            const actualNet = toNum(trade.actualNetPnlUsd, 0);
            const grossDelta = Number(model.grossPnlUsd || 0) - actualGross;
            const feesDelta = Number(model.feesUsd || 0) - actualFees;
            const netDelta = Number(model.netPnlUsd || 0) - actualNet;
            if (Math.abs(grossDelta) <= 1e-9 && Math.abs(feesDelta) <= 1e-9 && Math.abs(netDelta) <= 1e-9) return;

            sessionModel.gross = round6(Number(sessionModel.gross || 0) + grossDelta);
            sessionModel.fees = round6(Number(sessionModel.fees || 0) + feesDelta);
            sessionModel.net = round6(Number(sessionModel.net || 0) + netDelta);
            totalGross += grossDelta;
            totalFees += feesDelta;
            totalNet += netDelta;

            const tradeKey = String(Number(trade.tradeNum || 0));
            const intervalKey = String(trade.intervalLabel || 'unknown');
            byTrade[tradeKey] = byTrade[tradeKey] || {{ count: 0, wins: 0, losses: 0, gross: 0, fees: 0, net: 0 }};
            byTrade[tradeKey].gross += grossDelta;
            byTrade[tradeKey].fees += feesDelta;
            byTrade[tradeKey].net += netDelta;

            const comboKey = `${{tradeKey}}|${{intervalKey}}`;
            byInterval[comboKey] = byInterval[comboKey] || {{ key: comboKey, tradeNum: tradeKey, interval: intervalKey, count: 0, wins: 0, losses: 0, gross: 0, fees: 0, net: 0 }};
            byInterval[comboKey].gross += grossDelta;
            byInterval[comboKey].fees += feesDelta;
            byInterval[comboKey].net += netDelta;
          }});
          state.sessionModels.set(slug, sessionModel);
        }});

        let runningBalance = Number(doc.run.startBalanceUsd || 100);
        const modeledEquityPoints = [runningBalance];
        (doc.sessions || []).forEach((session) => {{
          const slug = String(session.slug || '');
          const model = state.sessionModels.get(slug);
          if (!model || model.ignored) return;
          runningBalance += Number(model.net || 0);
          model.cumulativeBalanceUsd = round6(runningBalance);
          modeledEquityPoints.push(runningBalance);
        }});

        const hours = Number(doc.run.hoursTested || 0);
        const winRate = totalWins + totalLosses > 0 ? (totalWins / (totalWins + totalLosses)) * 100 : NaN;
        const modeledDrawdown = computeMaxDrawdown(modeledEquityPoints);
        const modeledEndBalance = runningBalance;
        modelMetricsEl.innerHTML = [
          equityCard('Modeled Equity Curve', modeledEquityPoints, modeledEndBalance, state.goldSelections.size === 0 ? 'Matches actual until gold intervals are selected' : `Gold selections ${{state.goldSelections.size}}`, totalNet >= 0 ? 'pos' : 'neg'),
          metricCard('Modeled P/L', usd(totalNet), `Net after fees`, totalNet >= 0 ? 'pos' : 'neg'),
          metricCard('Gross P/L', usd(totalGross), `Before fees`, totalGross >= 0 ? 'pos' : 'neg'),
          metricCard('Drawdown', ddUsd(modeledDrawdown.maxDrawdownUsd), `Peak-to-trough ${{pct(modeledDrawdown.maxDrawdownPct)}}`, 'neg'),
          metricCard('P/L Per Hour', hours > 0 ? `${{usd(totalNet / hours)}}/hr` : '—', `Hours ${{hours.toFixed(2)}}`, (hours > 0 && totalNet / hours >= 0) ? 'pos' : 'neg'),
          metricCard('Modeled Win Rate', pct(winRate), `Included sessions ${{includedSessions}} · ignored ${{ignoredSessions}}`, Number.isFinite(winRate) && winRate >= 50 ? 'pos' : 'neg'),
          metricCard('Fees', usdAbs(totalFees), state.goldSelections.size === 0 ? 'Same fee basis as actual' : 'Modeled from selected intervals'),
        ].join('');

        byTradeBody.innerHTML = Object.keys(byTrade).sort((a,b)=>Number(a)-Number(b)).map((tradeKey) => {{
          const row = byTrade[tradeKey];
          const total = row.wins + row.losses;
          const wr = total ? (row.wins / total) * 100 : NaN;
          const perTrade = row.count ? row.net / row.count : 0;
          return `<tr><td>Trade ${{tradeKey}}</td><td>${{row.count}}</td><td>${{row.wins}}</td><td>${{row.losses}}</td><td>${{pct(wr)}}</td><td class="${{row.net >= 0 ? 'pos' : 'neg'}}">${{usd(row.net)}}</td><td>${{usdAbs(row.fees)}}</td><td class="${{perTrade >= 0 ? 'pos' : 'neg'}}">${{usd(perTrade)}}</td></tr>`;
        }}).join('') || '<tr><td colspan="8" class="empty">No included trades for the current filter.</td></tr>';

        byIntervalBody.innerHTML = Object.values(byInterval)
          .sort((a,b) => (Number(a.tradeNum) - Number(b.tradeNum)) || String(a.interval).localeCompare(String(b.interval)))
          .map((row) => {{
            const total = row.wins + row.losses;
            const wr = total ? (row.wins / total) * 100 : NaN;
            const perTrade = row.count ? row.net / row.count : 0;
            const goldChecked = state.goldSelections.has(String(row.key || ''));
            return `<tr><td class="goldCol"><input type="checkbox" class="gold-toggle" data-gold-key="${{row.key}}" ${{goldChecked ? 'checked' : ''}} /></td><td>Trade ${{row.tradeNum}}</td><td>${{row.interval}}</td><td>${{row.count}}</td><td>${{pct(wr)}}</td><td class="${{row.net >= 0 ? 'pos' : 'neg'}}">${{usd(row.net)}}</td><td>${{usdAbs(row.fees)}}</td><td class="${{perTrade >= 0 ? 'pos' : 'neg'}}">${{usd(perTrade)}}</td></tr>`;
          }}).join('') || '<tr><td colspan="8" class="empty">No interval rows for the current filter.</td></tr>';

        doc.sessions.forEach((session) => {{
          const slug = String(session.slug);
          const card = document.querySelector(`.session-card[data-session-slug="${{slug}}"]`);
          const model = state.sessionModels.get(slug);
          if (!card || !model) return;
          card.classList.toggle('ignored', !!model.ignored);
          const netEls = card.querySelectorAll(`.session-modeled-net[data-slug="${{slug}}"], .session-net[data-slug="${{slug}}"]`);
          netEls.forEach((el) => {{
            el.textContent = usd(model.net);
            el.classList.toggle('pos', model.net >= 0);
            el.classList.toggle('neg', model.net < 0);
          }});
          const grossEl = card.querySelector(`.session-gross[data-slug="${{slug}}"]`);
          const feesEl = card.querySelector(`.session-fees[data-slug="${{slug}}"]`);
          const cumEl = card.querySelector(`.session-cum[data-slug="${{slug}}"]`);
          if (grossEl) {{ grossEl.textContent = usdBlankZero(model.gross); grossEl.classList.toggle('pos', model.gross >= 0); grossEl.classList.toggle('neg', model.gross < 0); }}
          if (feesEl) feesEl.textContent = usdAbsBlankZero(model.fees);
          if (cumEl) {{ cumEl.textContent = `$${{Number(model.cumulativeBalanceUsd || 0).toFixed(2)}}`; }}

          (session.trades || []).forEach((trade) => {{
            const tradeModel = state.tradeModels[trade.tradeKey];
            const netEl = card.querySelector(`.modeled-net[data-trade-key="${{trade.tradeKey}}"]`);
            const feesElTrade = card.querySelector(`.modeled-fees[data-trade-key="${{trade.tradeKey}}"]`);
            const betElTrade = card.querySelector(`.modeled-bet[data-trade-key="${{trade.tradeKey}}"]`);
            const goldElTrade = card.querySelector(`.modeled-gold[data-trade-key="${{trade.tradeKey}}"]`);
            if (netEl) {{ netEl.textContent = usd(tradeModel.netPnlUsd); netEl.classList.toggle('pos', tradeModel.netPnlUsd >= 0); netEl.classList.toggle('neg', tradeModel.netPnlUsd < 0); }}
            if (feesElTrade) feesElTrade.textContent = usdAbs(tradeModel.feesUsd);
            if (betElTrade) betElTrade.textContent = usdAbs(tradeModel.betUsd);
            if (goldElTrade) goldElTrade.textContent = tradeModel.isGold ? ' GOLD' : '';

            const rowMetrics = tradeModel.rowMetrics || {{}};
            Object.entries(rowMetrics).forEach(([rowId, metrics]) => {{
              const row = card.querySelector(`tr[data-row-id="${{rowId}}"]`);
              if (!row) return;
              const sharesEl = row.querySelector(`.modeled-shares[data-row-id="${{rowId}}"]`);
              const grossElRow = row.querySelector(`.modeled-gross[data-row-id="${{rowId}}"]`);
              const feesElRow = row.querySelector(`.modeled-fees[data-row-id="${{rowId}}"]`);
              const netElRow = row.querySelector(`.modeled-net[data-row-id="${{rowId}}"]`);
              if (sharesEl) sharesEl.textContent = Number(metrics.shares || 0).toFixed(4);
              if (grossElRow) {{ grossElRow.textContent = usdBlankZero(metrics.gross || 0); grossElRow.classList.toggle('pos', Number(metrics.gross || 0) >= 0); grossElRow.classList.toggle('neg', Number(metrics.gross || 0) < 0); }}
              if (feesElRow) feesElRow.textContent = usdAbsBlankZero(metrics.fees || 0);
              if (netElRow) {{ netElRow.textContent = usdBlankZero(metrics.net || 0); netElRow.classList.toggle('pos', Number(metrics.net || 0) >= 0); netElRow.classList.toggle('neg', Number(metrics.net || 0) < 0); }}
            }});
          }});
        }});

        saveState();
        return;
      }}

      state.tradeModels = {{}};
      state.sessionModels = new Map();
      const byTrade = {{}};
      const byInterval = {{}};
      let totalGross = 0, totalFees = 0, totalNet = 0, totalTrades = 0, totalWins = 0, totalLosses = 0;
      let includedSessions = 0, ignoredSessions = 0;
      let runningBalance = Number(doc.run.startBalanceUsd || 100);
      const modeledEquityPoints = [runningBalance];

      doc.sessions.forEach((session) => {{
        const slug = String(session.slug);
        const ignored = !!session.forcedIgnore || state.ignoredSlugs.has(slug);
        if (ignored) ignoredSessions += 1;
        let sessGross = 0, sessFees = 0, sessNet = 0;
        sessionTradesForModel(session).forEach((trade) => {{
          const model = modelTrade(trade);
          state.tradeModels[trade.tradeKey] = model;
          const included = tradeEntryIncluded(trade, session);
          if (included) {{
            const outcomePnlUsd = Number(model.netPnlUsd || 0);
            sessGross += Number(model.grossPnlUsd || 0);
            sessFees += Number(model.feesUsd || 0);
            sessNet += Number(model.netPnlUsd || 0);
            totalGross += Number(model.grossPnlUsd || 0);
            totalFees += Number(model.feesUsd || 0);
            totalNet += Number(model.netPnlUsd || 0);
            totalTrades += 1;
            if (outcomePnlUsd > 0) totalWins += 1;
            else if (outcomePnlUsd < 0) totalLosses += 1;

            const tradeKey = String(Number(trade.tradeNum || 0));
            const intervalKey = String(trade.intervalLabel || 'unknown');
            byTrade[tradeKey] = byTrade[tradeKey] || {{ count:0, wins:0, losses:0, gross:0, fees:0, net:0 }};
            byTrade[tradeKey].count += 1;
            byTrade[tradeKey].gross += Number(model.grossPnlUsd || 0);
            byTrade[tradeKey].fees += Number(model.feesUsd || 0);
            byTrade[tradeKey].net += Number(model.netPnlUsd || 0);
            if (outcomePnlUsd > 0) byTrade[tradeKey].wins += 1;
            else if (outcomePnlUsd < 0) byTrade[tradeKey].losses += 1;

            const comboKey = `${{tradeKey}}|${{intervalKey}}`;
            byInterval[comboKey] = byInterval[comboKey] || {{ key: comboKey, tradeNum: tradeKey, interval: intervalKey, count:0, wins:0, losses:0, gross:0, fees:0, net:0 }};
            byInterval[comboKey].count += 1;
            byInterval[comboKey].gross += Number(model.grossPnlUsd || 0);
            byInterval[comboKey].fees += Number(model.feesUsd || 0);
            byInterval[comboKey].net += Number(model.netPnlUsd || 0);
            if (outcomePnlUsd > 0) byInterval[comboKey].wins += 1;
            else if (outcomePnlUsd < 0) byInterval[comboKey].losses += 1;
          }}
        }});
        if (!ignored) {{
          includedSessions += 1;
          runningBalance += sessNet;
          modeledEquityPoints.push(runningBalance);
        }}
        state.sessionModels.set(slug, {{
          gross: round6(sessGross),
          fees: round6(sessFees),
          net: round6(sessNet),
          cumulativeBalanceUsd: round6(runningBalance),
          ignored,
        }});
      }});

      const hours = Number(doc.run.hoursTested || 0);
      const winRate = totalWins + totalLosses > 0 ? (totalWins / (totalWins + totalLosses)) * 100 : NaN;
      const modeledDrawdown = computeMaxDrawdown(modeledEquityPoints);
      const modeledEndBalance = Number(doc.run.startBalanceUsd || 100) + totalNet;
      modelMetricsEl.innerHTML = [
        equityCard('Modeled Equity Curve', modeledEquityPoints, modeledEndBalance, state.goldSelections.size === 0 ? 'Matches actual until gold intervals are selected' : `Gold selections ${{state.goldSelections.size}}`, totalNet >= 0 ? 'pos' : 'neg'),
        metricCard('Modeled P/L', usd(totalNet), `Net after fees`, totalNet >= 0 ? 'pos' : 'neg'),
        metricCard('Gross P/L', usd(totalGross), `Before fees`, totalGross >= 0 ? 'pos' : 'neg'),
        metricCard('Drawdown', ddUsd(modeledDrawdown.maxDrawdownUsd), `Peak-to-trough ${{pct(modeledDrawdown.maxDrawdownPct)}}`, 'neg'),
        metricCard('P/L Per Hour', hours > 0 ? `${{usd(totalNet / hours)}}/hr` : '—', `Hours ${{hours.toFixed(2)}}`, (hours > 0 && totalNet / hours >= 0) ? 'pos' : 'neg'),
        metricCard('Modeled Win Rate', pct(winRate), `Included sessions ${{includedSessions}} · ignored ${{ignoredSessions}}`, Number.isFinite(winRate) && winRate >= 50 ? 'pos' : 'neg'),
        metricCard('Fees', usdAbs(totalFees), state.goldSelections.size === 0 ? 'Same fee basis as actual' : 'Modeled from selected intervals'),
      ].join('');

      byTradeBody.innerHTML = Object.keys(byTrade).sort((a,b)=>Number(a)-Number(b)).map((tradeKey) => {{
        const row = byTrade[tradeKey];
        const total = row.wins + row.losses;
        const wr = total ? (row.wins / total) * 100 : NaN;
        const perTrade = row.count ? row.net / row.count : 0;
        return `<tr><td>Trade ${{tradeKey}}</td><td>${{row.count}}</td><td>${{row.wins}}</td><td>${{row.losses}}</td><td>${{pct(wr)}}</td><td class="${{row.net >= 0 ? 'pos' : 'neg'}}">${{usd(row.net)}}</td><td>${{usdAbs(row.fees)}}</td><td class="${{perTrade >= 0 ? 'pos' : 'neg'}}">${{usd(perTrade)}}</td></tr>`;
      }}).join('') || '<tr><td colspan="8" class="empty">No included trades for the current filter.</td></tr>';

      byIntervalBody.innerHTML = Object.values(byInterval)
        .sort((a,b) => (Number(a.tradeNum) - Number(b.tradeNum)) || String(a.interval).localeCompare(String(b.interval)))
        .map((row) => {{
          const total = row.wins + row.losses;
          const wr = total ? (row.wins / total) * 100 : NaN;
          const perTrade = row.count ? row.net / row.count : 0;
          const goldChecked = state.goldSelections.has(String(row.key || ''));
          return `<tr><td class="goldCol"><input type="checkbox" class="gold-toggle" data-gold-key="${{row.key}}" ${{goldChecked ? 'checked' : ''}} /></td><td>Trade ${{row.tradeNum}}</td><td>${{row.interval}}</td><td>${{row.count}}</td><td>${{pct(wr)}}</td><td class="${{row.net >= 0 ? 'pos' : 'neg'}}">${{usd(row.net)}}</td><td>${{usdAbs(row.fees)}}</td><td class="${{perTrade >= 0 ? 'pos' : 'neg'}}">${{usd(perTrade)}}</td></tr>`;
        }}).join('') || '<tr><td colspan="8" class="empty">No interval rows for the current filter.</td></tr>';

      doc.sessions.forEach((session) => {{
        const slug = String(session.slug);
        const card = document.querySelector(`.session-card[data-session-slug="${{slug}}"]`);
        const model = state.sessionModels.get(slug);
        if (!card || !model) return;
        card.classList.toggle('ignored', !!model.ignored);
        const netEls = card.querySelectorAll(`.session-modeled-net[data-slug="${{slug}}"], .session-net[data-slug="${{slug}}"]`);
        netEls.forEach((el) => {{
          el.textContent = usd(model.net);
          el.classList.toggle('pos', model.net >= 0);
          el.classList.toggle('neg', model.net < 0);
        }});
        const grossEl = card.querySelector(`.session-gross[data-slug="${{slug}}"]`);
        const feesEl = card.querySelector(`.session-fees[data-slug="${{slug}}"]`);
        const cumEl = card.querySelector(`.session-cum[data-slug="${{slug}}"]`);
        if (grossEl) {{ grossEl.textContent = usdBlankZero(model.gross); grossEl.classList.toggle('pos', model.gross >= 0); grossEl.classList.toggle('neg', model.gross < 0); }}
        if (feesEl) feesEl.textContent = usdAbsBlankZero(model.fees);
        if (cumEl) {{ cumEl.textContent = `$${{Number(model.cumulativeBalanceUsd || 0).toFixed(2)}}`; }}

        (session.trades || []).forEach((trade) => {{
          const tradeModel = state.tradeModels[trade.tradeKey];
          const netEl = card.querySelector(`.modeled-net[data-trade-key="${{trade.tradeKey}}"]`);
          const feesElTrade = card.querySelector(`.modeled-fees[data-trade-key="${{trade.tradeKey}}"]`);
          const betElTrade = card.querySelector(`.modeled-bet[data-trade-key="${{trade.tradeKey}}"]`);
          const goldElTrade = card.querySelector(`.modeled-gold[data-trade-key="${{trade.tradeKey}}"]`);
          if (netEl) {{ netEl.textContent = usd(tradeModel.netPnlUsd); netEl.classList.toggle('pos', tradeModel.netPnlUsd >= 0); netEl.classList.toggle('neg', tradeModel.netPnlUsd < 0); }}
          if (feesElTrade) feesElTrade.textContent = usdAbs(tradeModel.feesUsd);
          if (betElTrade) betElTrade.textContent = usdAbs(tradeModel.betUsd);
          if (goldElTrade) goldElTrade.textContent = tradeModel.isGold ? ' GOLD' : '';

          const rowMetrics = tradeModel.rowMetrics || {{}};
          Object.entries(rowMetrics).forEach(([rowId, metrics]) => {{
            const row = card.querySelector(`tr[data-row-id="${{rowId}}"]`);
            if (!row) return;
            const sharesEl = row.querySelector(`.modeled-shares[data-row-id="${{rowId}}"]`);
            const grossElRow = row.querySelector(`.modeled-gross[data-row-id="${{rowId}}"]`);
            const feesElRow = row.querySelector(`.modeled-fees[data-row-id="${{rowId}}"]`);
            const netElRow = row.querySelector(`.modeled-net[data-row-id="${{rowId}}"]`);
            if (sharesEl) sharesEl.textContent = Number(metrics.shares || 0).toFixed(4);
            if (grossElRow) {{ grossElRow.textContent = usd(metrics.gross || 0); grossElRow.classList.toggle('pos', Number(metrics.gross || 0) >= 0); grossElRow.classList.toggle('neg', Number(metrics.gross || 0) < 0); }}
            if (feesElRow) feesElRow.textContent = usdAbs(metrics.fees || 0);
            if (netElRow) {{ netElRow.textContent = usd(metrics.net || 0); netElRow.classList.toggle('pos', Number(metrics.net || 0) >= 0); netElRow.classList.toggle('neg', Number(metrics.net || 0) < 0); }}
          }});
        }});
      }});

      saveState();
    }}

    renderStaticMetrics();
    renderControls();
    renderSessions();

    if (hoverNamesToggle instanceof HTMLInputElement) {{
      hoverNamesToggle.checked = !!state.hoverNamesEnabled;
      hoverNamesToggle.addEventListener('change', () => {{
        state.hoverNamesEnabled = !!hoverNamesToggle.checked;
        saveState();
        if (!state.hoverNamesEnabled && hoverInspector) hoverInspector.classList.remove('visible');
      }});
    }}

    document.addEventListener('mousemove', (ev) => {{
      if (!state.hoverNamesEnabled) return;
      updateHoverInspector(ev.target);
      positionHoverInspector(ev.clientX, ev.clientY);
    }}, true);
    document.addEventListener('mouseleave', () => {{
      if (hoverInspector) hoverInspector.classList.remove('visible');
    }});

    betInputsEl.addEventListener('input', (ev) => {{
      const target = ev.target;
      if (!(target instanceof HTMLInputElement)) return;
      const tradeNum = String(target.getAttribute('data-trade-num') || '');
      if (!tradeNum) return;
      state.betByTrade[tradeNum] = round4(toNum(target.value, 0));
      recalcAndRender();
    }});
    goldBetInputsEl.addEventListener('input', (ev) => {{
      const target = ev.target;
      if (!(target instanceof HTMLInputElement)) return;
      const tradeNum = String(target.getAttribute('data-gold-trade-num') || '');
      if (!tradeNum) return;
      state.goldBetByTrade[tradeNum] = round4(toNum(target.value, 0));
      recalcAndRender();
    }});
    byIntervalBody.addEventListener('change', (ev) => {{
      const target = ev.target;
      if (!(target instanceof HTMLInputElement)) return;
      const goldKey = String(target.getAttribute('data-gold-key') || '');
      if (!goldKey) return;
      if (target.checked) state.goldSelections.add(goldKey);
      else state.goldSelections.delete(goldKey);
      recalcAndRender();
    }});
    clearIgnoresBtn.addEventListener('click', () => {{
      state.ignoredSlugs = new Set();
      renderSessions();
      recalcAndRender();
    }});
    loadMoreSessionsBtn.addEventListener('click', () => {{
      state.visibleSessionCount = Math.min(doc.sessions.length, Number(state.visibleSessionCount || initialVisibleSessions) + sessionLoadStep);
      renderSessions();
      recalcAndRender();
    }});
    collapseSessionsBtn.addEventListener('click', () => {{
      state.visibleSessionCount = Math.min(doc.sessions.length, initialVisibleSessions);
      renderSessions();
      recalcAndRender();
      window.scrollTo({{ top: 0, behavior: 'smooth' }});
    }});
    resetBetsBtn.addEventListener('click', () => {{
      state.betByTrade = {{ ...doc.controls.defaultBetUsdByTrade }};
      renderControls();
      recalcAndRender();
    }});
    resetGoldBetsBtn.addEventListener('click', () => {{
      state.goldBetByTrade = {{ ...doc.controls.defaultGoldBetUsdByTrade }};
      state.goldSelections = new Set(defaultGoldSelectionKeys);
      renderControls();
      recalcAndRender();
    }});
    clearGoldBtn.addEventListener('click', () => {{
      state.goldSelections = new Set();
      recalcAndRender();
    }});

    recalcAndRender();
  }})();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", type=int, required=True)
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--host-port", default=DEFAULT_HOST_PORT)
    ap.add_argument("--public-base", default=DEFAULT_PUBLIC_BASE)
    ap.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    ap.add_argument("--run-dir", default="", help="Use a local run directory instead of remote ssh files.")
    ap.add_argument("--doc-json", default="", help="Use a prebuilt audit doc JSON instead of rebuilding from source files.")
    ap.add_argument("--out-html", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--t1", type=float, default=None)
    ap.add_argument("--t2", type=float, default=None)
    ap.add_argument("--t3", type=float, default=None)
    ap.add_argument("--t4", type=float, default=None)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    cfg = SourceConfig(
        mode="local" if args.run_dir else "remote",
        host=args.host,
        key=args.key,
        host_port=str(args.host_port),
        public_base=str(args.public_base).rstrip("/"),
        remote_root=str(args.remote_root).rstrip("/"),
        run_dir=str(args.run_dir).strip() or None,
    )
    overrides = {"1": args.t1, "2": args.t2, "3": args.t3, "4": args.t4}
    doc_json_path = str(args.doc_json).strip()
    if doc_json_path:
        doc = sanitize_json_value(json.loads(Path(doc_json_path).read_text(encoding="utf-8")))
    else:
        doc = sanitize_json_value(build_doc(cfg, int(args.run), str(args.out_html), str(args.out_json), overrides))

    out_json = Path(args.out_json)
    out_html = Path(args.out_html)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    source_run_dir = str((doc.get("source") or {}).get("runDir") or "").strip()
    # When building from a remote host on a different machine, keep emitted sidecar
    # artifacts next to the requested local output files instead of trying to mirror
    # the remote absolute path on the current filesystem.
    if cfg.mode == "remote":
        run_dir = str(out_json.parent)
    else:
        run_dir = source_run_dir or str(out_json.parent)
    session_dir = run_audit_session_dir_for_run(run_dir)
    session_dir.mkdir(parents=True, exist_ok=True)
    generated_at_ms = int(time.time() * 1000)
    for session in doc.get("sessions") or []:
        slug = str(session.get("slug") or "").strip()
        if not slug:
            continue
        atomic_write_json(session_dir / f"{slug}.compact.json", {
            "schemaVersion": "1.0",
            "auditCodeVersion": AUDIT_CODE_VERSION,
            "generatedAtMs": generated_at_ms,
            "runNum": int(args.run),
            "runId": (doc.get("run") or {}).get("runId"),
            "slug": slug,
            "session": session,
        })
    atomic_write_json(out_json, doc)
    atomic_write_text(out_html, html_page(doc))
    run_state = build_run_audit_state(doc)
    state_path = run_audit_state_path_for_run(run_dir)
    atomic_write_json(state_path, run_state)

    print(json.dumps({
        "ok": True,
        "outHtml": str(out_html),
        "outJson": str(out_json),
        "runAuditState": str(state_path),
        "runNum": args.run,
        "hostPort": cfg.host_port,
        "strategyId": (doc.get("run") or {}).get("strategyId"),
        "actualNetPnlUsd": (doc.get("run") or {}).get("actualNetPnlUsd"),
        "profitPerHourUsd": (doc.get("run") or {}).get("profitPerHourUsd"),
        "sessions": len(doc.get("sessions") or []),
        "reusedSessions": doc.get("buildStats", {}).get("reusedSessions"),
        "rebuiltSessions": doc.get("buildStats", {}).get("rebuiltSessions"),
        "perfMs": (doc.get("buildStats") or {}).get("perf"),
    }, indent=2))


if __name__ == "__main__":
    main()
