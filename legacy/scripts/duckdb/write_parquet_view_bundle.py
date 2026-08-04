#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _duckdb_source_uri(source_uri: str) -> str:
    text = str(source_uri or "").strip()
    if text.startswith("gcs://"):
        return "s3://" + text[len("gcs://") :]
    return text


def write_bundle(
    *,
    table_name: str,
    dataset_id: str,
    source_uri: str,
    yaml_path: Path,
    sql_path: Path,
    object_kind: str = "view",
) -> dict:
    table = str(table_name or "").strip()
    ds = str(dataset_id or "").strip()
    source = str(source_uri or "").strip()
    if not table:
        raise ValueError("table_name is required")
    if not ds:
        raise ValueError("dataset_id is required")
    if not source:
        raise ValueError("source_uri is required")

    duckdb_uri = _duckdb_source_uri(source)
    yaml_doc = {
        "name": table,
        "dataset_id": ds,
        "object_kind": str(object_kind or "view").strip() or "view",
        "format": "parquet",
        "source_uri": source,
        "duckdb_source_uri": duckdb_uri,
    }
    sql_text = (
        f"CREATE OR REPLACE VIEW \"{table}\" AS\n"
        f"SELECT *\n"
        f"FROM read_parquet('{duckdb_uri}', union_by_name=true);\n"
    )

    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    sql_path.parent.mkdir(parents=True, exist_ok=True)

    yaml_lines = [
        f"name: {yaml_doc['name']}",
        f"dataset_id: {yaml_doc['dataset_id']}",
        f"object_kind: {yaml_doc['object_kind']}",
        "format: parquet",
        f"source_uri: {yaml_doc['source_uri']}",
        f"duckdb_source_uri: {yaml_doc['duckdb_source_uri']}",
        "",
    ]
    yaml_path.write_text("\n".join(yaml_lines), encoding="utf-8")
    sql_path.write_text(sql_text, encoding="utf-8")

    return {
        "table_name": table,
        "dataset_id": ds,
        "source_uri": source,
        "duckdb_source_uri": duckdb_uri,
        "yaml_path": yaml_path.resolve().as_posix(),
        "sql_path": sql_path.resolve().as_posix(),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Write DuckDB table/view metadata files for a Parquet dataset.")
    ap.add_argument("--table-name", required=True)
    ap.add_argument("--dataset-id", required=True)
    ap.add_argument("--source-uri", required=True)
    ap.add_argument("--yaml-path", required=True)
    ap.add_argument("--sql-path", required=True)
    ap.add_argument("--object-kind", default="view")
    ap.add_argument("--summary-json", default="")
    args = ap.parse_args(argv)

    summary = write_bundle(
        table_name=args.table_name,
        dataset_id=args.dataset_id,
        source_uri=args.source_uri,
        yaml_path=Path(args.yaml_path).expanduser().resolve(),
        sql_path=Path(args.sql_path).expanduser().resolve(),
        object_kind=args.object_kind,
    )
    summary_json = str(args.summary_json or "").strip()
    if summary_json:
        target = Path(summary_json).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
