#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return str(value).strip()


def _pick(row: dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = _to_text(row.get(field))
        if value:
            return value
    return ""


def _split_tile_field_id(value: str) -> tuple[str, str]:
    text = _to_text(value)
    if "_" not in text:
        return "", ""
    left, right = text.split("_", 1)
    return left, right


def _to_annual_tillage(value: str) -> str:
    raw = _to_text(value)
    if raw == "":
        return ""
    try:
        number = float(raw)
    except ValueError as exc:
        raise ValueError(f"dominant_tillage is not numeric 0/1: {raw}") from exc
    if number not in (0.0, 1.0):
        raise ValueError(f"dominant_tillage is not numeric 0/1: {raw}")
    return str(int(number))


def _open_csv(path: Path) -> tuple[Any, csv.DictReader]:
    handle = path.open("r", encoding="utf-8-sig", newline="")
    reader = csv.DictReader(handle)
    if not reader.fieldnames:
        handle.close()
        raise RuntimeError(f"input csv has no header: {path}")
    return handle, reader


def _require_fields(path: Path, fieldnames: list[str], required: list[str]) -> None:
    missing = [field for field in required if field not in fieldnames]
    if missing:
        raise ValueError(f"input csv missing required fields {missing}: {path}")


def _year_in_range(year_text: str, *, year_start: int | None, year_end: int | None) -> bool:
    text = _to_text(year_text)
    if not text:
        return False
    try:
        year = int(text)
    except ValueError:
        return False
    if year_start is not None and year < int(year_start):
        return False
    if year_end is not None and year > int(year_end):
        return False
    return True


def _import_field_year_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    path: Path,
    value_columns: dict[str, tuple[str, ...]],
    year_start: int | None = None,
    year_end: int | None = None,
) -> int:
    handle, reader = _open_csv(path)
    try:
        fieldnames = [str(x) for x in reader.fieldnames or []]
        required = ["tile_field_ID", "year"]
        fallback_fields = [choices[0] for choices in value_columns.values()]
        _require_fields(path, fieldnames, required)
        conn.execute(
            f"""
            CREATE TABLE {table} (
              tile_field_ID TEXT NOT NULL,
              year TEXT NOT NULL,
              {", ".join(f"{column} TEXT" for column in value_columns)},
              PRIMARY KEY (tile_field_ID, year)
            )
            """
        )
        insert_columns = ["tile_field_ID", "year", *value_columns.keys()]
        insert_sql = f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({', '.join('?' for _ in insert_columns)})"
        row_count = 0
        for row in reader:
            tile_field_id = _to_text(row.get("tile_field_ID"))
            year = _to_text(row.get("year"))
            if not tile_field_id or not year:
                continue
            if not _year_in_range(year, year_start=year_start, year_end=year_end):
                continue
            values = [
                _pick(row, *value_columns[column]) for column in value_columns
            ]
            try:
                conn.execute(insert_sql, [tile_field_id, year, *values])
            except sqlite3.IntegrityError as exc:
                raise RuntimeError(f"{table} contains duplicate keys for ['tile_field_ID', 'year']: {(tile_field_id, year)}") from exc
            row_count += 1
        return row_count
    finally:
        handle.close()


def _import_field_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    path: Path,
    key_field: str,
    value_columns: dict[str, tuple[str, ...]],
) -> int:
    handle, reader = _open_csv(path)
    try:
        fieldnames = [str(x) for x in reader.fieldnames or []]
        _require_fields(path, fieldnames, [key_field])
        conn.execute(
            f"""
            CREATE TABLE {table} (
              {key_field} TEXT NOT NULL PRIMARY KEY,
              {", ".join(f"{column} TEXT" for column in value_columns)}
            )
            """
        )
        insert_columns = [key_field, *value_columns.keys()]
        insert_sql = f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({', '.join('?' for _ in insert_columns)})"
        row_count = 0
        for row in reader:
            key_value = _to_text(row.get(key_field))
            if not key_value:
                continue
            values = [_pick(row, *value_columns[column]) for column in value_columns]
            try:
                conn.execute(insert_sql, [key_value, *values])
            except sqlite3.IntegrityError as exc:
                raise RuntimeError(f"{table} contains duplicate keys for ['{key_field}']: {key_value}") from exc
            row_count += 1
        return row_count
    finally:
        handle.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a tillage MODEL_IN table from staged field-year covariates.")
    ap.add_argument("--corn-csv", required=True)
    ap.add_argument("--tillage-csv", required=True)
    ap.add_argument("--vpdmax-csv", required=True)
    ap.add_argument("--nccpi-csv", required=True)
    ap.add_argument("--field-fips-csv", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--year-start", type=int, default=None)
    ap.add_argument("--year-end", type=int, default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    corn_csv = Path(str(args.corn_csv)).expanduser().resolve()
    tillage_csv = Path(str(args.tillage_csv)).expanduser().resolve()
    vpdmax_csv = Path(str(args.vpdmax_csv)).expanduser().resolve()
    nccpi_csv = Path(str(args.nccpi_csv)).expanduser().resolve()
    field_fips_csv = Path(str(args.field_fips_csv)).expanduser().resolve()
    output_csv = Path(str(args.output_csv)).expanduser().resolve()
    summary_json = Path(str(args.summary_json)).expanduser().resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    for path in [corn_csv, tillage_csv, vpdmax_csv, nccpi_csv, field_fips_csv]:
        if not path.exists():
            raise FileNotFoundError(f"input csv not found: {path}")

    fieldnames = [
        "tile_field_ID",
        "tile_coord",
        "field_ID",
        "FIPS",
        "state",
        "county",
        "year",
        "unscaled_yield",
        "annual_tillage",
        "NCCPI",
        "tillage_0_prop",
        "tillage_1_prop",
        "vpdmax_7",
        "vpdmax_8",
        "nccpi3corn",
    ]

    with tempfile.TemporaryDirectory(prefix="tillage_model_input_", dir=str(summary_json.parent)) as temp_dir:
        db_path = Path(temp_dir) / "join.sqlite3"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = OFF")
            conn.execute("PRAGMA temp_store = FILE")
            conn.execute("PRAGMA cache_size = -200000")

            corn_row_count = _import_field_year_table(
                conn,
                table="corn",
                path=corn_csv,
                value_columns={"unscaled_yield": ("unscaled_yield", "corn_yield")},
                year_start=args.year_start,
                year_end=args.year_end,
            )
            tillage_row_count = _import_field_year_table(
                conn,
                table="tillage",
                path=tillage_csv,
                value_columns={
                    "dominant_tillage": ("dominant_tillage",),
                    "tillage_0_prop": ("tillage_0_prop",),
                    "tillage_1_prop": ("tillage_1_prop",),
                },
                year_start=args.year_start,
                year_end=args.year_end,
            )
            vpd_row_count = _import_field_year_table(
                conn,
                table="vpd",
                path=vpdmax_csv,
                value_columns={
                    "vpdmax_7": ("vpdmax_7", "vpdmax7"),
                    "vpdmax_8": ("vpdmax_8", "vpdmax8"),
                },
                year_start=args.year_start,
                year_end=args.year_end,
            )
            nccpi_row_count = _import_field_table(
                conn,
                table="nccpi",
                path=nccpi_csv,
                key_field="tile_field_id",
                value_columns={
                    "NCCPI": ("NCCPI", "nccpi3corn"),
                    "nccpi3corn": ("nccpi3corn",),
                },
            )
            field_fips_row_count = _import_field_table(
                conn,
                table="fips",
                path=field_fips_csv,
                key_field="tile_field_ID",
                value_columns={
                    "FIPS": ("FIPS", "fips_code"),
                    "state": ("STATEFP", "state"),
                    "county": ("county", "county_name_lsad"),
                },
            )
            conn.commit()

            missing_tillage = conn.execute(
                """
                SELECT COUNT(*)
                FROM corn c
                LEFT JOIN tillage t
                  ON t.tile_field_ID = c.tile_field_ID
                 AND t.year = c.year
                WHERE t.tile_field_ID IS NULL
                """
            ).fetchone()[0]
            missing_vpd = conn.execute(
                """
                SELECT COUNT(*)
                FROM corn c
                JOIN tillage t
                  ON t.tile_field_ID = c.tile_field_ID
                 AND t.year = c.year
                LEFT JOIN vpd v
                  ON v.tile_field_ID = c.tile_field_ID
                 AND v.year = c.year
                WHERE v.tile_field_ID IS NULL
                """
            ).fetchone()[0]
            missing_nccpi = conn.execute(
                """
                SELECT COUNT(*)
                FROM corn c
                JOIN tillage t
                  ON t.tile_field_ID = c.tile_field_ID
                 AND t.year = c.year
                JOIN vpd v
                  ON v.tile_field_ID = c.tile_field_ID
                 AND v.year = c.year
                LEFT JOIN nccpi n
                  ON n.tile_field_id = c.tile_field_ID
                WHERE n.tile_field_id IS NULL
                """
            ).fetchone()[0]
            missing_fips = conn.execute(
                """
                SELECT COUNT(*)
                FROM corn c
                JOIN tillage t
                  ON t.tile_field_ID = c.tile_field_ID
                 AND t.year = c.year
                JOIN vpd v
                  ON v.tile_field_ID = c.tile_field_ID
                 AND v.year = c.year
                JOIN nccpi n
                  ON n.tile_field_id = c.tile_field_ID
                LEFT JOIN fips f
                  ON f.tile_field_ID = c.tile_field_ID
                WHERE f.tile_field_ID IS NULL
                """
            ).fetchone()[0]

            missing_annual_tillage = 0
            missing_required_value = 0
            output_row_count = 0

            with output_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                cursor = conn.execute(
                    """
                    SELECT
                      c.tile_field_ID,
                      c.year,
                      c.unscaled_yield,
                      t.dominant_tillage,
                      t.tillage_0_prop,
                      t.tillage_1_prop,
                      v.vpdmax_7,
                      v.vpdmax_8,
                      n.NCCPI,
                      n.nccpi3corn,
                      f.FIPS,
                      f.state,
                      f.county
                    FROM corn c
                    JOIN tillage t
                      ON t.tile_field_ID = c.tile_field_ID
                     AND t.year = c.year
                    JOIN vpd v
                      ON v.tile_field_ID = c.tile_field_ID
                     AND v.year = c.year
                    JOIN nccpi n
                      ON n.tile_field_id = c.tile_field_ID
                    JOIN fips f
                      ON f.tile_field_ID = c.tile_field_ID
                    ORDER BY c.tile_field_ID, c.year
                    """
                )
                for raw_row in cursor:
                    (
                        tile_field_id,
                        year,
                        unscaled_yield,
                        dominant_tillage,
                        tillage_0_prop,
                        tillage_1_prop,
                        vpdmax_7,
                        vpdmax_8,
                        nccpi_value,
                        nccpi3corn,
                        fips,
                        state,
                        county,
                    ) = raw_row
                    annual_tillage = _to_annual_tillage(_to_text(dominant_tillage))
                    if not annual_tillage:
                        missing_annual_tillage += 1
                        continue
                    tile_coord, field_id = _split_tile_field_id(_to_text(tile_field_id))
                    out_row = {
                        "tile_field_ID": _to_text(tile_field_id),
                        "tile_coord": tile_coord,
                        "field_ID": field_id,
                        "FIPS": _to_text(fips),
                        "state": _to_text(state),
                        "county": _to_text(county),
                        "year": _to_text(year),
                        "unscaled_yield": _to_text(unscaled_yield),
                        "annual_tillage": annual_tillage,
                        "NCCPI": _to_text(nccpi_value),
                        "tillage_0_prop": _to_text(tillage_0_prop),
                        "tillage_1_prop": _to_text(tillage_1_prop),
                        "vpdmax_7": _to_text(vpdmax_7),
                        "vpdmax_8": _to_text(vpdmax_8),
                        "nccpi3corn": _to_text(nccpi3corn),
                    }
                    required = ["FIPS", "year", "unscaled_yield", "annual_tillage", "NCCPI", "vpdmax_7", "vpdmax_8"]
                    if any(not _to_text(out_row.get(column)) for column in required):
                        missing_required_value += 1
                        continue
                    writer.writerow(out_row)
                    output_row_count += 1

            duplicates = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                  SELECT tile_field_ID, year, COUNT(*) AS n
                  FROM corn
                  GROUP BY tile_field_ID, year
                  HAVING n > 1
                )
                """
            ).fetchone()[0]
        finally:
            conn.close()

    summary = {
        "corn_csv": corn_csv.as_posix(),
        "tillage_csv": tillage_csv.as_posix(),
        "vpdmax_csv": vpdmax_csv.as_posix(),
        "nccpi_csv": nccpi_csv.as_posix(),
        "field_fips_csv": field_fips_csv.as_posix(),
        "output_csv": output_csv.as_posix(),
        "year_start": int(args.year_start) if args.year_start is not None else None,
        "year_end": int(args.year_end) if args.year_end is not None else None,
        "corn_row_count": corn_row_count,
        "tillage_row_count": tillage_row_count,
        "vpd_row_count": vpd_row_count,
        "nccpi_row_count": nccpi_row_count,
        "field_fips_row_count": field_fips_row_count,
        "output_row_count": output_row_count,
        "duplicate_tile_field_year_rows": duplicates,
        "missing_tillage_rows": missing_tillage,
        "missing_vpd_rows": missing_vpd,
        "missing_nccpi_rows": missing_nccpi,
        "missing_fips_rows": missing_fips,
        "missing_annual_tillage_rows": missing_annual_tillage,
        "missing_required_value_rows": missing_required_value,
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.verbose:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
