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


def _read_unique_mukeys(path: Path, mukey_field: str) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"field mukey csv not found: {path}")
    out: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames or mukey_field not in rdr.fieldnames:
            raise ValueError(f"missing mukey field in csv: {mukey_field}")
        for row in rdr:
            mk = str((row or {}).get(mukey_field) or "").strip()
            if not mk or mk in seen:
                continue
            seen.add(mk)
            out.append(mk)
    if not out:
        raise RuntimeError("no mukey values found")
    return out


def _chunked(values: list[str], size: int):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _sql_in_list(values: list[str]) -> str:
    def esc(v: str) -> str:
        return "'" + str(v).replace("'", "''") + "'"

    return ",".join(esc(v) for v in values)


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
            "User-Agent": "landcore-etl/ssurgo-sda-valu1",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        text = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except Exception as exc:  # noqa: BLE001
        snippet = text[:500].replace("\n", " ")
        raise RuntimeError(f"SDA response is not JSON; snippet={snippet}") from exc


def _rows_from_sda_payload(payload: Any) -> list[dict[str, Any]]:
    # Common format for JSON+COLUMNNAME: {"Table":[["col1","col2"],["v1","v2"],...]}
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
                    row = {cols[i]: (item[i] if i < len(item) else None) for i in range(len(cols))}
                    rows.append(row)
                return rows
            if isinstance(first, dict):
                return [dict(x) for x in table if isinstance(x, dict)]
        # Alternate keys sometimes appear.
        for k in ("Table1", "Rows", "Data"):
            val = payload.get(k)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return [dict(x) for x in val if isinstance(x, dict)]
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return [dict(x) for x in payload if isinstance(x, dict)]
    return []


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


def _safe_float(v: Any):
    text = str(v or "").strip()
    if not text:
        return ""
    try:
        return float(text)
    except Exception:
        return text


def main() -> int:
    ap = argparse.ArgumentParser(description="Download SSURGO Valu1 NCCPI columns from SDA by mukey and join back to field map.")
    ap.add_argument("--field-mukey-long-csv", required=True, help="Input long CSV with field_id,mukey pairs")
    ap.add_argument("--field-id-field", default="tile_field_id")
    ap.add_argument("--mukey-field", default="mukey")
    ap.add_argument("--valu1-columns", default="mukey,nccpi3all,nccpi3corn,nccpi3soy")
    ap.add_argument("--sda-endpoint", default="https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest")
    ap.add_argument("--table-name", default="valu1")
    ap.add_argument("--chunk-size", type=int, default=500)
    ap.add_argument("--sleep-seconds", type=float, default=0.0)
    ap.add_argument("--timeout-seconds", type=int, default=120)
    ap.add_argument("--output-valu1-csv", required=True, help="Output CSV with one row per mukey")
    ap.add_argument("--output-field-valu1-csv", required=True, help="Output CSV joined to field_id,mukey pairs")
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    input_long = Path(args.field_mukey_long_csv).expanduser().resolve()
    output_valu1 = Path(args.output_valu1_csv).expanduser().resolve()
    output_joined = Path(args.output_field_valu1_csv).expanduser().resolve()
    summary_path = Path(args.summary_json).expanduser().resolve()
    output_valu1.parent.mkdir(parents=True, exist_ok=True)
    output_joined.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    mukeys = _read_unique_mukeys(input_long, str(args.mukey_field))
    cols = _normalize_columns(str(args.valu1_columns))
    table_name = str(args.table_name).strip() or "valu1"
    endpoint = str(args.sda_endpoint).strip()
    chunk_size = max(1, int(args.chunk_size))
    timeout_seconds = max(1, int(args.timeout_seconds))
    sleep_seconds = max(0.0, float(args.sleep_seconds))

    by_mukey: dict[str, dict[str, Any]] = {}
    request_count = 0
    for chunk in _chunked(mukeys, chunk_size):
        request_count += 1
        sql = f"SELECT {','.join(cols)} FROM {table_name} WHERE mukey IN ({_sql_in_list(chunk)})"
        payload = _post_sda_query(endpoint, sql, timeout_seconds=timeout_seconds)
        rows = _rows_from_sda_payload(payload)
        for row in rows:
            mk = str(row.get("mukey") or "").strip()
            if not mk:
                continue
            out_row: dict[str, Any] = {}
            for c in cols:
                val = row.get(c)
                out_row[c] = _safe_float(val)
            by_mukey[mk] = out_row
        if args.verbose:
            print(f"[sda_download_valu1_by_mukey] chunk={request_count} size={len(chunk)} rows={len(rows)}")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    # Write valu1-by-mukey output.
    valu1_rows = [by_mukey[k] for k in sorted(by_mukey.keys())]
    with output_valu1.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(valu1_rows)

    # Join back to field-mukey long.
    joined_rows: list[dict[str, Any]] = []
    missing_mukey = 0
    with input_long.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            raise ValueError("field-mukey-long csv has no header")
        for row in rdr:
            field_id = str((row or {}).get(str(args.field_id_field)) or "").strip()
            mk = str((row or {}).get(str(args.mukey_field)) or "").strip()
            if not field_id or not mk:
                continue
            valu1 = by_mukey.get(mk)
            if valu1 is None:
                missing_mukey += 1
                continue
            out = {str(args.field_id_field): field_id, str(args.mukey_field): mk}
            for c in cols:
                if c.lower() == str(args.mukey_field).lower():
                    continue
                out[c] = valu1.get(c, "")
            joined_rows.append(out)

    joined_fieldnames = [str(args.field_id_field), str(args.mukey_field)] + [c for c in cols if c.lower() != str(args.mukey_field).lower()]
    with output_joined.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=joined_fieldnames)
        w.writeheader()
        w.writerows(joined_rows)

    summary = {
        "endpoint": endpoint,
        "table_name": table_name,
        "columns": cols,
        "input_mukey_count": len(mukeys),
        "downloaded_mukey_count": len(by_mukey),
        "request_count": request_count,
        "joined_row_count": len(joined_rows),
        "missing_mukey_pairs": missing_mukey,
        "outputs": {
            "valu1_csv": output_valu1.as_posix(),
            "field_valu1_csv": output_joined.as_posix(),
            "summary_json": summary_path.as_posix(),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    else:
        print(
            f"mukeys_in={summary['input_mukey_count']} mukeys_out={summary['downloaded_mukey_count']} "
            f"joined_rows={summary['joined_row_count']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
