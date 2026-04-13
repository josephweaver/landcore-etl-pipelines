#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

import duckdb
import matplotlib
import numpy as np
import pandas as pd
from scipy import stats

matplotlib.use("Agg")
from matplotlib import pyplot as plt


CORE_NUMERIC_COLUMNS = [
    "unscaled_yield",
    "annual_tillage",
    "NCCPI",
    "nccpi3corn",
    "vpdmax_7",
    "vpdmax_8",
    "tillage_0_prop",
    "tillage_1_prop",
    "tillage_na_prop",
    "classified_share",
    "high_share_among_classified",
    "low_share_among_classified",
    "year",
]

YIELD_PREDICTORS = [
    "annual_tillage",
    "NCCPI",
    "nccpi3corn",
    "vpdmax_7",
    "vpdmax_8",
    "tillage_0_prop",
    "tillage_1_prop",
    "tillage_na_prop",
    "classified_share",
    "high_share_among_classified",
    "low_share_among_classified",
    "year",
]

PLOT_PREDICTORS = [
    "vpdmax_7",
    "nccpi3corn",
    "tillage_0_prop",
    "tillage_1_prop",
    "tillage_na_prop",
    "classified_share",
    "high_share_among_classified",
]


def _resolve(path_text: str) -> Path:
    return Path(str(path_text)).expanduser().resolve()


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if math.isnan(float(value)):
            return None
        return float(value)
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return str(value)


def _safe_slug(value: str) -> str:
    out = []
    for ch in str(value):
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    return "".join(out).strip("_")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


def _profile_numeric(series: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce")
    nonnull = numeric.dropna()
    out: dict[str, Any] = {
        "row_count": int(len(series)),
        "nonnull_count": int(nonnull.shape[0]),
        "missing_count": int(series.shape[0] - nonnull.shape[0]),
        "missing_rate": float((series.shape[0] - nonnull.shape[0]) / series.shape[0]) if series.shape[0] else None,
        "distinct_count": int(nonnull.nunique()),
    }
    if nonnull.empty:
        out.update(
            {
                "mean": None,
                "std": None,
                "min": None,
                "p05": None,
                "p25": None,
                "median": None,
                "p75": None,
                "p95": None,
                "max": None,
            }
        )
        return out
    out.update(
        {
            "mean": float(nonnull.mean()),
            "std": float(nonnull.std(ddof=1)) if nonnull.shape[0] > 1 else 0.0,
            "min": float(nonnull.min()),
            "p05": float(nonnull.quantile(0.05)),
            "p25": float(nonnull.quantile(0.25)),
            "median": float(nonnull.quantile(0.50)),
            "p75": float(nonnull.quantile(0.75)),
            "p95": float(nonnull.quantile(0.95)),
            "max": float(nonnull.max()),
        }
    )
    return out


def _variable_profiles(df: pd.DataFrame, view_name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for column in CORE_NUMERIC_COLUMNS:
        if column not in df.columns:
            continue
        row = {"data_view": view_name, "variable": column}
        row.update(_profile_numeric(df[column]))
        rows.append(row)
    return pd.DataFrame(rows)


def _bivariate_stats(df: pd.DataFrame, predictors: list[str], *, view_name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    y = pd.to_numeric(df["unscaled_yield"], errors="coerce")
    for predictor in predictors:
        if predictor not in df.columns:
            continue
        x = pd.to_numeric(df[predictor], errors="coerce")
        pair = pd.DataFrame({"x": x, "y": y}).dropna()
        row: dict[str, Any] = {
            "data_view": view_name,
            "predictor": predictor,
            "pair_count": int(pair.shape[0]),
            "distinct_x_count": int(pair["x"].nunique()),
            "yield_mean": float(pair["y"].mean()) if not pair.empty else None,
            "predictor_mean": float(pair["x"].mean()) if not pair.empty else None,
            "pearson_r": None,
            "pearson_p": None,
            "spearman_r": None,
            "spearman_p": None,
            "ols_slope": None,
            "ols_intercept": None,
            "ols_r2": None,
            "yield_mean_bottom_quartile": None,
            "yield_mean_top_quartile": None,
        }
        if pair.shape[0] >= 3 and pair["x"].nunique() >= 2:
            pearson = stats.pearsonr(pair["x"], pair["y"])
            spearman = stats.spearmanr(pair["x"], pair["y"])
            lin = stats.linregress(pair["x"], pair["y"])
            row.update(
                {
                    "pearson_r": float(pearson.statistic),
                    "pearson_p": float(pearson.pvalue),
                    "spearman_r": float(spearman.statistic),
                    "spearman_p": float(spearman.pvalue),
                    "ols_slope": float(lin.slope),
                    "ols_intercept": float(lin.intercept),
                    "ols_r2": float(lin.rvalue ** 2),
                }
            )
            q1 = pair["x"].quantile(0.25)
            q3 = pair["x"].quantile(0.75)
            row["yield_mean_bottom_quartile"] = float(pair.loc[pair["x"] <= q1, "y"].mean())
            row["yield_mean_top_quartile"] = float(pair.loc[pair["x"] >= q3, "y"].mean())
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["data_view", "predictor"]).reset_index(drop=True)


def _pairwise_correlations(df: pd.DataFrame, *, view_name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for i, left in enumerate(CORE_NUMERIC_COLUMNS):
        if left not in df.columns:
            continue
        for right in CORE_NUMERIC_COLUMNS[i + 1 :]:
            if right not in df.columns:
                continue
            pair = df[[left, right]].apply(pd.to_numeric, errors="coerce").dropna()
            row: dict[str, Any] = {
                "data_view": view_name,
                "left_variable": left,
                "right_variable": right,
                "pair_count": int(pair.shape[0]),
                "pearson_r": None,
                "spearman_r": None,
            }
            if pair.shape[0] >= 3 and pair[left].nunique() >= 2 and pair[right].nunique() >= 2:
                row["pearson_r"] = float(stats.pearsonr(pair[left], pair[right]).statistic)
                row["spearman_r"] = float(stats.spearmanr(pair[left], pair[right]).statistic)
            rows.append(row)
    return pd.DataFrame(rows)


def _binned_profiles(
    df: pd.DataFrame,
    predictors: list[str],
    *,
    view_name: str,
    quantile_bins: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for predictor in predictors:
        if predictor not in df.columns:
            continue
        pair = df[[predictor, "unscaled_yield"]].apply(pd.to_numeric, errors="coerce").dropna()
        if pair.empty:
            continue
        series = pair[predictor]
        if predictor == "annual_tillage" or series.nunique() <= 2:
            grouped = pair.groupby(series)
            for bucket, group in grouped:
                rows.append(
                    {
                        "data_view": view_name,
                        "predictor": predictor,
                        "bin_label": str(bucket),
                        "bin_order": float(bucket),
                        "row_count": int(group.shape[0]),
                        "predictor_min": float(group[predictor].min()),
                        "predictor_max": float(group[predictor].max()),
                        "predictor_mean": float(group[predictor].mean()),
                        "yield_mean": float(group["unscaled_yield"].mean()),
                        "yield_median": float(group["unscaled_yield"].median()),
                    }
                )
            continue
        try:
            bins = pd.qcut(series, q=min(quantile_bins, int(series.nunique())), duplicates="drop")
        except ValueError:
            continue
        temp = pair.assign(_bin=bins)
        for order, (bucket, group) in enumerate(temp.groupby("_bin", observed=True), start=1):
            rows.append(
                {
                    "data_view": view_name,
                    "predictor": predictor,
                    "bin_label": str(bucket),
                    "bin_order": int(order),
                    "row_count": int(group.shape[0]),
                    "predictor_min": float(group[predictor].min()),
                    "predictor_max": float(group[predictor].max()),
                    "predictor_mean": float(group[predictor].mean()),
                    "yield_mean": float(group["unscaled_yield"].mean()),
                    "yield_median": float(group["unscaled_yield"].median()),
                }
            )
    return pd.DataFrame(rows)


def _annual_tillage_summary(df: pd.DataFrame, *, view_name: str) -> pd.DataFrame:
    temp = df[["annual_tillage", "unscaled_yield"]].apply(pd.to_numeric, errors="coerce").dropna()
    grouped = temp.groupby("annual_tillage")
    rows = []
    for value, group in grouped:
        rows.append(
            {
                "data_view": view_name,
                "annual_tillage": float(value),
                "row_count": int(group.shape[0]),
                "yield_mean": float(group["unscaled_yield"].mean()),
                "yield_median": float(group["unscaled_yield"].median()),
                "yield_std": float(group["unscaled_yield"].std(ddof=1)) if group.shape[0] > 1 else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("annual_tillage").reset_index(drop=True)


def _sample_for_plot(df: pd.DataFrame, columns: list[str], sample_size: int) -> pd.DataFrame:
    temp = df[columns].dropna()
    if temp.shape[0] <= sample_size:
        return temp
    return temp.sample(n=sample_size, random_state=42)


def _plot_scatter(df: pd.DataFrame, predictor: str, output_path: Path, *, sample_size: int) -> None:
    sample = _sample_for_plot(df, [predictor, "unscaled_yield"], sample_size)
    if sample.empty:
        return
    x = pd.to_numeric(sample[predictor], errors="coerce")
    y = pd.to_numeric(sample["unscaled_yield"], errors="coerce")
    valid = pd.DataFrame({"x": x, "y": y}).dropna()
    if valid.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(valid["x"], valid["y"], s=8, alpha=0.18, edgecolors="none")
    if valid.shape[0] >= 3 and valid["x"].nunique() >= 2:
        line = stats.linregress(valid["x"], valid["y"])
        xs = np.linspace(valid["x"].min(), valid["x"].max(), 100)
        ys = line.intercept + line.slope * xs
        ax.plot(xs, ys, color="crimson", linewidth=2, label=f"OLS slope={line.slope:.2f}, r={line.rvalue:.2f}")
        ax.legend(frameon=False)
    ax.set_title(f"Yield vs {predictor}")
    ax.set_xlabel(predictor)
    ax.set_ylabel("unscaled_yield")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def _plot_boxplot(df: pd.DataFrame, output_path: Path) -> None:
    temp = df[["annual_tillage", "unscaled_yield"]].apply(pd.to_numeric, errors="coerce").dropna()
    if temp.empty:
        return
    levels = sorted(temp["annual_tillage"].unique().tolist())
    data = [temp.loc[temp["annual_tillage"] == value, "unscaled_yield"].to_numpy() for value in levels]
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.boxplot(data, labels=[str(int(v)) if float(v).is_integer() else str(v) for v in levels], showfliers=False)
    ax.set_title("Yield by annual_tillage")
    ax.set_xlabel("annual_tillage")
    ax.set_ylabel("unscaled_yield")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def _plot_heatmap(df: pd.DataFrame, output_path: Path) -> None:
    temp = df[CORE_NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
    corr = temp.corr(method="spearman")
    if corr.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(corr.to_numpy(), cmap="coolwarm", vmin=-1, vmax=1)
    ax.set_xticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(corr.index)))
    ax.set_yticklabels(corr.index)
    ax.set_title("Spearman correlation heatmap (unique field-year view)")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def _build_markdown_report(
    *,
    summary: dict[str, Any],
    unique_bivariate: pd.DataFrame,
    annual_tillage_summary: pd.DataFrame,
    county_stats: pd.DataFrame,
) -> str:
    strongest = unique_bivariate.dropna(subset=["pearson_r"]).copy()
    strongest["abs_pearson_r"] = strongest["pearson_r"].abs()
    strongest = strongest.sort_values("abs_pearson_r", ascending=False).head(5)

    county_extremes = county_stats.dropna(subset=["pearson_r"]).copy()
    county_extremes["abs_pearson_r"] = county_extremes["pearson_r"].abs()
    county_extremes = county_extremes.sort_values("abs_pearson_r", ascending=False).head(8)

    lines = [
        "# County Model Input Bivariate EDA",
        "",
        "## Snapshot",
        "",
        f"- Neighborhood county files scanned: {summary['county_rows']['focal_county_count']}",
        f"- Neighborhood rows scanned: {summary['county_rows']['row_count']:,}",
        f"- Deduplicated unique field-year rows: {summary['unique_rows']['row_count']:,}",
        f"- Unique tile_field_ID count: {summary['unique_rows']['unique_tile_field_count']:,}",
        f"- Year range: {summary['unique_rows']['year_min']} to {summary['unique_rows']['year_max']}",
        f"- Average neighborhood duplication factor: {summary['duplication_factor']:.2f}",
        f"- Duplicate-key consistency violations detected: {summary['duplicate_consistency_violations']}",
        "",
        "## What This Bundle Separates",
        "",
        "- `unique field-year view`: one row per `(tile_field_ID, year)` after deduplicating neighborhood repeats.",
        "- `county neighborhood view`: the repeated per-focal-county neighborhood rows retained to study local heterogeneity.",
        "",
        "## Strongest Global Bivariate Signals With Yield",
        "",
    ]
    for _, row in strongest.iterrows():
        lines.append(
            f"- `{row['predictor']}`: Pearson r={row['pearson_r']:.3f}, Spearman r={row['spearman_r']:.3f}, "
            f"OLS slope={row['ols_slope']:.3f}, pair count={int(row['pair_count']):,}"
        )
    lines.extend(["", "## Yield By Annual Tillage", ""])
    for _, row in annual_tillage_summary.iterrows():
        lines.append(
            f"- `annual_tillage={int(row['annual_tillage'])}`: n={int(row['row_count']):,}, "
            f"mean yield={row['yield_mean']:.2f}, median yield={row['yield_median']:.2f}"
        )
    lines.extend(["", "## Strongest County-Level Pearson Signals", ""])
    for _, row in county_extremes.iterrows():
        lines.append(
            f"- focal county `{row['focal_fips']}`, predictor `{row['predictor']}`: "
            f"r={row['pearson_r']:.3f}, slope={row['ols_slope']:.3f}, n={int(row['pair_count']):,}"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- The county-neighborhood files repeat the same field-year records across neighboring focal counties, so the deduplicated view is the correct default for global EDA.",
            "- `tillage_na_prop` is derived as `1 - tillage_0_prop - tillage_1_prop` from the cropped-pixel proportions present in the county input files.",
            "- `classified_share`, `high_share_among_classified`, and `low_share_among_classified` are derived helper variables included to make the tillage composition easier to interpret.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build a bivariate EDA bundle for county neighborhood model-input CSVs."
    )
    ap.add_argument("--input-dir", required=True)
    ap.add_argument("--manifest-csv", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--report-md", required=True)
    ap.add_argument("--sqlite-path", default="")
    ap.add_argument("--sample-size", type=int, default=50000)
    ap.add_argument("--quantile-bins", type=int, default=10)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    input_dir = _resolve(args.input_dir)
    manifest_csv = _resolve(args.manifest_csv)
    output_dir = _resolve(args.output_dir)
    summary_json = _resolve(args.summary_json)
    report_md = _resolve(args.report_md)
    sqlite_path = _resolve(args.sqlite_path) if str(args.sqlite_path or "").strip() else None
    figures_dir = output_dir / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"input directory not found: {input_dir}")
    if not manifest_csv.exists():
        raise FileNotFoundError(f"manifest csv not found: {manifest_csv}")

    manifest_df = pd.read_csv(manifest_csv, dtype={"focal_fips": str})
    county_glob = input_dir.as_posix().rstrip("/") + "/*/county_data.csv"

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.register("manifest_df", manifest_df)
    if sqlite_path is not None and sqlite_path.exists():
        sqlite_conn = sqlite3.connect(str(sqlite_path))
        try:
            unique_df = pd.read_sql_query(
                """
                SELECT
                  tile_field_ID,
                  tile_coord,
                  field_ID,
                  FIPS,
                  state,
                  county,
                  year,
                  unscaled_yield,
                  annual_tillage,
                  NCCPI,
                  tillage_0_prop,
                  tillage_1_prop,
                  vpdmax_7,
                  vpdmax_8,
                  nccpi3corn
                FROM joined_output
                """,
                sqlite_conn,
            )
            county_neighbors_df = pd.read_sql_query(
                "SELECT focal_fips, neighbor_fips FROM county_neighbors",
                sqlite_conn,
            )
        finally:
            sqlite_conn.close()

        for column in [
            "year",
            "unscaled_yield",
            "annual_tillage",
            "NCCPI",
            "tillage_0_prop",
            "tillage_1_prop",
            "vpdmax_7",
            "vpdmax_8",
            "nccpi3corn",
        ]:
            unique_df[column] = pd.to_numeric(unique_df[column], errors="coerce")
        unique_df["tillage_na_prop"] = (1.0 - unique_df["tillage_0_prop"].fillna(0.0) - unique_df["tillage_1_prop"].fillna(0.0)).clip(lower=0.0)
        unique_df["classified_share"] = unique_df["tillage_0_prop"].fillna(0.0) + unique_df["tillage_1_prop"].fillna(0.0)
        denom = unique_df["classified_share"].replace({0.0: np.nan})
        unique_df["high_share_among_classified"] = unique_df["tillage_0_prop"] / denom
        unique_df["low_share_among_classified"] = unique_df["tillage_1_prop"] / denom

        con.register("unique_input_df", unique_df)
        con.register("county_neighbors_df", county_neighbors_df)
        con.execute("CREATE OR REPLACE TEMP VIEW unique_rows AS SELECT * FROM unique_input_df")
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW county_rows AS
            SELECT
              cn.focal_fips,
              u.*
            FROM unique_rows u
            JOIN county_neighbors_df cn
              ON cn.neighbor_fips = u.FIPS
            """
        )
    else:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW county_rows AS
            SELECT
              regexp_extract(filename, '[/\\\\]([0-9]{{5}})[/\\\\]county_data\\.csv$', 1) AS focal_fips,
              CAST(tile_field_ID AS VARCHAR) AS tile_field_ID,
              CAST(tile_coord AS VARCHAR) AS tile_coord,
              CAST(field_ID AS VARCHAR) AS field_ID,
              CAST(FIPS AS VARCHAR) AS FIPS,
              CAST(state AS VARCHAR) AS state,
              CAST(county AS VARCHAR) AS county,
              CAST(year AS INTEGER) AS year,
              CAST(unscaled_yield AS DOUBLE) AS unscaled_yield,
              CAST(annual_tillage AS DOUBLE) AS annual_tillage,
              CAST(NCCPI AS DOUBLE) AS NCCPI,
              CAST(tillage_0_prop AS DOUBLE) AS tillage_0_prop,
              CAST(tillage_1_prop AS DOUBLE) AS tillage_1_prop,
              GREATEST(0.0, 1.0 - COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) - COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0)) AS tillage_na_prop,
              COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) + COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0) AS classified_share,
              CASE
                WHEN (COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) + COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0)) > 0
                THEN COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) / (COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) + COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0))
                ELSE NULL
              END AS high_share_among_classified,
              CASE
                WHEN (COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) + COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0)) > 0
                THEN COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0) / (COALESCE(CAST(tillage_0_prop AS DOUBLE), 0.0) + COALESCE(CAST(tillage_1_prop AS DOUBLE), 0.0))
                ELSE NULL
              END AS low_share_among_classified,
              CAST(vpdmax_7 AS DOUBLE) AS vpdmax_7,
              CAST(vpdmax_8 AS DOUBLE) AS vpdmax_8,
              CAST(nccpi3corn AS DOUBLE) AS nccpi3corn,
              CAST(filename AS VARCHAR) AS source_path
            FROM read_csv_auto('{county_glob}', header=true, all_varchar=true, filename=true)
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW unique_rows AS
            SELECT * EXCLUDE (dedupe_rank, focal_fips, source_path)
            FROM (
              SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY tile_field_ID, year ORDER BY focal_fips, source_path) AS dedupe_rank
              FROM county_rows
            )
            WHERE dedupe_rank = 1
            """
        )

        unique_df = con.execute(
            """
            SELECT
              tile_field_ID,
              tile_coord,
              field_ID,
              FIPS,
              state,
              county,
              year,
              unscaled_yield,
              annual_tillage,
              NCCPI,
              tillage_0_prop,
              tillage_1_prop,
              tillage_na_prop,
              classified_share,
              high_share_among_classified,
              low_share_among_classified,
              vpdmax_7,
              vpdmax_8,
              nccpi3corn
            FROM unique_rows
            """
        ).df()

    county_profile_df = con.execute(
        """
        SELECT
          c.focal_fips,
          COALESCE(m.neighbor_count, NULL) AS neighbor_count,
          COUNT(*) AS row_count,
          COUNT(DISTINCT c.tile_field_ID) AS unique_tile_field_count,
          COUNT(DISTINCT CONCAT(c.tile_field_ID, '|', CAST(c.year AS VARCHAR))) AS unique_tile_field_year_count,
          COUNT(DISTINCT c.FIPS) AS source_fips_count,
          MIN(c.year) AS year_min,
          MAX(c.year) AS year_max,
          AVG(c.unscaled_yield) AS yield_mean,
          AVG(c.NCCPI) AS nccpi_mean,
          AVG(c.vpdmax_7) AS vpdmax_7_mean,
          AVG(c.vpdmax_8) AS vpdmax_8_mean,
          AVG(c.tillage_0_prop) AS tillage_0_prop_mean,
          AVG(c.tillage_1_prop) AS tillage_1_prop_mean,
          AVG(c.tillage_na_prop) AS tillage_na_prop_mean,
          AVG(c.classified_share) AS classified_share_mean
        FROM county_rows c
        LEFT JOIN manifest_df m
          ON m.focal_fips = c.focal_fips
        GROUP BY 1, 2
        ORDER BY 1
        """
    ).df()

    county_stat_queries = []
    for predictor in YIELD_PREDICTORS:
        county_stat_queries.append(
            f"""
            SELECT
              focal_fips,
              '{predictor}' AS predictor,
              COUNT(*) FILTER (WHERE {predictor} IS NOT NULL AND unscaled_yield IS NOT NULL) AS pair_count,
              CORR(unscaled_yield, {predictor}) AS pearson_r,
              REGR_SLOPE(unscaled_yield, {predictor}) AS ols_slope,
              REGR_INTERCEPT(unscaled_yield, {predictor}) AS ols_intercept,
              REGR_R2(unscaled_yield, {predictor}) AS ols_r2
            FROM county_rows
            GROUP BY focal_fips
            """
        )
    county_stats_df = con.execute("\nUNION ALL\n".join(county_stat_queries) + "\nORDER BY focal_fips, predictor").df()

    duplicate_consistency_violations = int(
        con.execute(
            """
            SELECT COUNT(*)
            FROM (
              SELECT
                tile_field_ID,
                year
              FROM county_rows
              GROUP BY tile_field_ID, year
              HAVING
                COUNT(DISTINCT unscaled_yield) > 1
                OR COUNT(DISTINCT annual_tillage) > 1
                OR COUNT(DISTINCT NCCPI) > 1
                OR COUNT(DISTINCT tillage_0_prop) > 1
                OR COUNT(DISTINCT tillage_1_prop) > 1
                OR COUNT(DISTINCT vpdmax_7) > 1
                OR COUNT(DISTINCT vpdmax_8) > 1
                OR COUNT(DISTINCT nccpi3corn) > 1
            )
            """
        ).fetchone()[0]
    )

    unique_profiles_df = _variable_profiles(unique_df, "unique_field_year")
    unique_bivariate_df = _bivariate_stats(unique_df, YIELD_PREDICTORS, view_name="unique_field_year")
    pairwise_df = _pairwise_correlations(unique_df, view_name="unique_field_year")
    binned_df = _binned_profiles(unique_df, YIELD_PREDICTORS, view_name="unique_field_year", quantile_bins=int(args.quantile_bins))
    annual_tillage_df = _annual_tillage_summary(unique_df, view_name="unique_field_year")

    unique_profiles_path = output_dir / "unique_field_year_variable_profiles.csv"
    unique_bivariate_path = output_dir / "unique_field_year_yield_bivariate_stats.csv"
    pairwise_path = output_dir / "unique_field_year_pairwise_correlations.csv"
    binned_path = output_dir / "unique_field_year_binned_yield_profiles.csv"
    annual_tillage_path = output_dir / "unique_field_year_annual_tillage_yield_summary.csv"
    county_profiles_path = output_dir / "county_neighborhood_profiles.csv"
    county_stats_path = output_dir / "county_neighborhood_yield_bivariate_stats.csv"

    unique_profiles_df.to_csv(unique_profiles_path, index=False)
    unique_bivariate_df.to_csv(unique_bivariate_path, index=False)
    pairwise_df.to_csv(pairwise_path, index=False)
    binned_df.to_csv(binned_path, index=False)
    annual_tillage_df.to_csv(annual_tillage_path, index=False)
    county_profile_df.to_csv(county_profiles_path, index=False)
    county_stats_df.to_csv(county_stats_path, index=False)

    _plot_boxplot(unique_df, figures_dir / "unique_field_year_yield_by_annual_tillage.png")
    _plot_heatmap(unique_df, figures_dir / "unique_field_year_spearman_heatmap.png")
    for predictor in PLOT_PREDICTORS:
        if predictor in unique_df.columns:
            _plot_scatter(
                unique_df,
                predictor,
                figures_dir / f"unique_field_year_yield_vs_{_safe_slug(predictor)}.png",
                sample_size=int(args.sample_size),
            )

    county_row_count = int(con.execute("SELECT COUNT(*) FROM county_rows").fetchone()[0])
    unique_row_count = int(unique_df.shape[0])
    unique_tile_field_count = int(unique_df["tile_field_ID"].nunique())
    focal_county_count = int(county_profile_df.shape[0])
    year_min = int(unique_df["year"].min()) if not unique_df.empty else None
    year_max = int(unique_df["year"].max()) if not unique_df.empty else None
    duplication_factor = float(county_row_count / unique_row_count) if unique_row_count else None

    summary = {
        "input_dir": input_dir.as_posix(),
        "manifest_csv": manifest_csv.as_posix(),
        "output_dir": output_dir.as_posix(),
        "report_md": report_md.as_posix(),
        "county_glob": county_glob,
        "county_rows": {
            "row_count": county_row_count,
            "focal_county_count": focal_county_count,
        },
        "unique_rows": {
            "row_count": unique_row_count,
            "unique_tile_field_count": unique_tile_field_count,
            "year_min": year_min,
            "year_max": year_max,
        },
        "duplication_factor": duplication_factor,
        "duplicate_consistency_violations": duplicate_consistency_violations,
        "artifacts": {
            "unique_profiles_csv": unique_profiles_path.as_posix(),
            "unique_bivariate_csv": unique_bivariate_path.as_posix(),
            "pairwise_correlations_csv": pairwise_path.as_posix(),
            "binned_profiles_csv": binned_path.as_posix(),
            "annual_tillage_summary_csv": annual_tillage_path.as_posix(),
            "county_profiles_csv": county_profiles_path.as_posix(),
            "county_stats_csv": county_stats_path.as_posix(),
            "figures_dir": figures_dir.as_posix(),
        },
    }

    report_text = _build_markdown_report(
        summary=summary,
        unique_bivariate=unique_bivariate_df,
        annual_tillage_summary=annual_tillage_df,
        county_stats=county_stats_df,
    )
    report_md.write_text(report_text, encoding="utf-8")
    _write_json(summary_json, summary)

    if args.verbose:
        print(json.dumps(summary, indent=2, default=_json_default))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
