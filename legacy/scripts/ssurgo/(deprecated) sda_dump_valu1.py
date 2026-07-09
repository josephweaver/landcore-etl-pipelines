#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def _normalize_columns(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        col = str(token or "").strip()
        if not col:
            continue
        key = col.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(col)
    if "mukey" not in [c.lower() for c in out]:
        out.insert(0, "mukey")
    return out


def _post_sda_query(endpoint: str, sql: str, timeout_seconds: int) -> Any:
    body = urllib.parse.urlencode(
        {
            "SERVICE": "query",
            "REQUEST": "query",
            "QUERY": sql,
            "FORMAT": "JSON+COLUMNNAME",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Accept": "application/json",
            "User-Agent": "landcore-etl/ssurgo-sda-dump",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        text = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except Exception as exc:  # noqa: BLE001
        snippet = text[:500].replace("\n", " ")
        raise RuntimeError(f"SDA response is not JSON; snippet={snippet}") from exc


def _post_with_retry(
    endpoint: str,
    sql: str,
    *,
    timeout_seconds: int,
    retries: int,
    retry_delay_seconds: float,
    verbose: bool,
) -> Any:
    last_err: Exception | None = None
    attempts = max(1, retries + 1)
    for attempt in range(1, attempts + 1):
        try:
            return _post_sda_query(endpoint, sql, timeout_seconds=timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt >= attempts:
                break
            delay = max(0.0, retry_delay_seconds) * attempt
            if verbose:
                print(f"[sda_dump_valu1] attempt={attempt} failed; retrying in {delay:.1f}s: {exc}")
            if delay > 0:
                time.sleep(delay)
    assert last_err is not None
    raise last_err


def _rows_from_sda_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        table = payload.get("Table")
        if isinstance(table, list):
            if not table:
                return []
            first = table[0]
            if isinstance(first, list):
                cols = [str(c) for c in first]
                rows: list[dict[str, Any]] = []
                for item in table[1:]:
                    if not isinstance(item, list):
                        continue
                    rows.append({cols[i]: (item[i] if i < len(item) else None) for i in range(len(cols))})
                return rows
            if isinstance(first, dict):
                return [dict(x) for x in table if isinstance(x, dict)]
        for key in ("Table1", "Rows", "Data"):
            val = payload.get(key)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return [dict(x) for x in val if isinstance(x, dict)]
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return [dict(x) for x in payload if isinstance(x, dict)]
    return []


def _safe_float(v: Any) -> Any:
    text = str(v or "").strip()
    if not text:
        return ""
    try:
        return float(text)
    except Exception:
        return text


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump SSURGO Valu1 mukey+nccpi columns from SDA.")
    ap.add_argument("--sda-endpoint", default="https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest")
    ap.add_argument("--table-name", default="valu1")
    ap.add_argument("--columns", default="mukey,nccpi3all,nccpi3soy,nccpi3corn")
    ap.add_argument("--where", default="", help="Optional SQL WHERE clause without the 'WHERE' keyword")
    ap.add_argument("--timeout-seconds", type=int, default=300)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--retry-delay-seconds", type=float, default=2.0)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    endpoint = str(args.sda_endpoint).strip()
    table_name = str(args.table_name).strip() or "valu1"
    columns = _normalize_columns(str(args.columns))
    timeout_seconds = max(1, int(args.timeout_seconds))
    where_clause = str(args.where or "").strip()

    sql = f"SELECT {','.join(columns)} FROM {table_name}"
    if where_clause:
        sql += f" WHERE {where_clause}"

    if args.verbose:
        print(f"[sda_dump_valu1] querying table={table_name} columns={columns}")

    started = time.time()
    payload = _post_with_retry(
        endpoint,
        sql,
        timeout_seconds=timeout_seconds,
        retries=max(0, int(args.retries)),
        retry_delay_seconds=max(0.0, float(args.retry_delay_seconds)),
        verbose=bool(args.verbose),
    )
    rows = _rows_from_sda_payload(payload)

    normalized_rows: list[dict[str, Any]] = []
    unique_mukeys: set[str] = set()
    for row in rows:
        out: dict[str, Any] = {}
        mk = str(row.get("mukey") or "").strip()
        if mk:
            unique_mukeys.add(mk)
        for c in columns:
            out[c] = _safe_float(row.get(c))
        normalized_rows.append(out)

    output_csv = Path(args.output_csv).expanduser().resolve()
    summary_json = Path(args.summary_json).expanduser().resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(normalized_rows)

    elapsed_seconds = round(time.time() - started, 3)
    summary = {
        "endpoint": endpoint,
        "table_name": table_name,
        "columns": columns,
        "where": where_clause,
        "row_count": int(len(normalized_rows)),
        "unique_mukey_count": int(len(unique_mukeys)),
        "elapsed_seconds": elapsed_seconds,
        "outputs": {
            "csv": output_csv.as_posix(),
            "summary_json": summary_json.as_posix(),
        },
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if args.verbose:
        print(json.dumps(summary, indent=2))
    else:
        print(f"rows={summary['row_count']} mukeys={summary['unique_mukey_count']} csv={output_csv.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
