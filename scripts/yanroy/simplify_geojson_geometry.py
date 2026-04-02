from __future__ import annotations

import argparse
import json
from pathlib import Path


def simplify_geojson_geometry(
    input_vector: Path,
    output_vector: Path,
    tolerance: float,
    preserve_topology: bool = True,
    summary_json: Path | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> dict:
    if tolerance < 0:
        raise ValueError(f"tolerance must be >= 0, got {tolerance}")

    try:
        import geopandas as gpd
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("simplify_geojson_geometry requires geopandas") from exc

    if not input_vector.exists():
        raise FileNotFoundError(f"input vector not found: {input_vector}")
    if output_vector.exists() and not overwrite:
        raise FileExistsError(f"output vector exists and overwrite not requested: {output_vector}")

    gdf = gpd.read_file(input_vector)
    input_row_count = int(len(gdf))

    if not gdf.empty:
        gdf = gdf.copy()
        gdf.geometry = gdf.geometry.simplify(tolerance=float(tolerance), preserve_topology=bool(preserve_topology))

    output_vector.parent.mkdir(parents=True, exist_ok=True)
    if output_vector.exists() and overwrite:
        output_vector.unlink()
    gdf.to_file(output_vector, driver="GeoJSON")

    result = {
        "input_vector": input_vector.resolve().as_posix(),
        "output_vector": output_vector.resolve().as_posix(),
        "row_count": input_row_count,
        "tolerance": float(tolerance),
        "preserve_topology": bool(preserve_topology),
        "empty_input": bool(gdf.empty),
    }
    if summary_json is not None:
        summary_json.parent.mkdir(parents=True, exist_ok=True)
        summary_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(result, indent=2))
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Simplify GeoJSON polygon geometry.")
    ap.add_argument("--input-vector", required=True)
    ap.add_argument("--output-vector", required=True)
    ap.add_argument("--tolerance", required=True, type=float)
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--preserve-topology", dest="preserve_topology", action="store_true")
    ap.add_argument("--no-preserve-topology", dest="preserve_topology", action="store_false")
    ap.set_defaults(preserve_topology=True)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    simplify_geojson_geometry(
        input_vector=Path(args.input_vector).expanduser().resolve(),
        output_vector=Path(args.output_vector).expanduser().resolve(),
        tolerance=float(args.tolerance),
        preserve_topology=bool(args.preserve_topology),
        summary_json=(Path(args.summary_json).expanduser().resolve() if str(args.summary_json or "").strip() else None),
        overwrite=bool(args.overwrite),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
