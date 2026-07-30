from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


WEB_MERCATOR_HALF_WORLD = 20037508.342789244
WEB_MERCATOR_WORLD = WEB_MERCATOR_HALF_WORLD * 2.0


def _tile_bounds_3857(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    tiles = 2**z
    tile_span = WEB_MERCATOR_WORLD / tiles
    minx = -WEB_MERCATOR_HALF_WORLD + (x * tile_span)
    maxx = minx + tile_span
    maxy = WEB_MERCATOR_HALF_WORLD - (y * tile_span)
    miny = maxy - tile_span
    return (minx, miny, maxx, maxy)


def _tile_range_3857(bounds: tuple[float, float, float, float], z: int) -> tuple[range, range]:
    minx, miny, maxx, maxy = bounds
    tiles = 2**z
    tile_span = WEB_MERCATOR_WORLD / tiles

    def _clamp_index(value: int) -> int:
        return max(0, min(tiles - 1, value))

    x0 = _clamp_index(int(math.floor((minx + WEB_MERCATOR_HALF_WORLD) / tile_span)))
    x1 = _clamp_index(int(math.floor((maxx + WEB_MERCATOR_HALF_WORLD) / tile_span)))
    y0 = _clamp_index(int(math.floor((WEB_MERCATOR_HALF_WORLD - maxy) / tile_span)))
    y1 = _clamp_index(int(math.floor((WEB_MERCATOR_HALF_WORLD - miny) / tile_span)))
    return range(x0, x1 + 1), range(y0, y1 + 1)


def build_google_maps_raster_tiles(
    input_raster: Path,
    output_dir: Path,
    minimum_zoom: int = 6,
    maximum_zoom: int = 14,
    tile_size: int = 256,
    nodata_value: int = 0,
    summary_json: Path | None = None,
    overwrite: bool = False,
    verbose: bool = False,
) -> dict:
    try:
        import numpy as np
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.transform import from_bounds
        from rasterio.warp import reproject
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("build_google_maps_raster_tiles requires rasterio and numpy") from exc

    if not input_raster.exists():
        raise FileNotFoundError(f"input raster not found: {input_raster}")
    if minimum_zoom < 0 or maximum_zoom < minimum_zoom:
        raise ValueError("zoom range is invalid")
    if tile_size <= 0:
        raise ValueError("tile_size must be positive")

    output_dir.mkdir(parents=True, exist_ok=True)

    tile_count = 0
    zoom_counts: dict[str, int] = {}

    with rasterio.open(input_raster) as src:
        if src.crs is None:
            raise RuntimeError("input raster missing CRS")
        crs_text = str(src.crs.to_string() if hasattr(src.crs, "to_string") else src.crs)
        if crs_text.upper() != "EPSG:3857":
            raise RuntimeError(f"input raster must be EPSG:3857 for Google Maps tiling, got {crs_text}")

        src_nodata = src.nodata if src.nodata is not None else float(nodata_value)
        bounds = tuple(float(v) for v in src.bounds)

        for z in range(int(minimum_zoom), int(maximum_zoom) + 1):
            zoom_tile_count = 0
            x_range, y_range = _tile_range_3857(bounds, z)
            for x in x_range:
                for y in y_range:
                    tile_bounds = _tile_bounds_3857(z, x, y)
                    dst = np.zeros((tile_size, tile_size), dtype="uint8")
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=dst,
                        src_transform=src.transform,
                        src_crs=src.crs,
                        src_nodata=src_nodata,
                        dst_transform=from_bounds(*tile_bounds, tile_size, tile_size),
                        dst_crs=src.crs,
                        dst_nodata=float(nodata_value),
                        resampling=Resampling.nearest,
                    )
                    if int(dst.max()) == int(nodata_value):
                        continue

                    out_path = output_dir / str(z) / str(x) / f"{y}.png"
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    if out_path.exists():
                        if overwrite:
                            out_path.unlink()
                        else:
                            continue

                    with rasterio.open(
                        out_path,
                        "w",
                        driver="PNG",
                        width=tile_size,
                        height=tile_size,
                        count=1,
                        dtype="uint8",
                    ) as dst_png:
                        dst_png.write(dst, 1)

                    tile_count += 1
                    zoom_tile_count += 1
            zoom_counts[str(z)] = zoom_tile_count
            if verbose:
                print(f"[build_google_maps_raster_tiles] zoom={z} tiles={zoom_tile_count}")

    result = {
        "input_raster": input_raster.resolve().as_posix(),
        "output_dir": output_dir.resolve().as_posix(),
        "minimum_zoom": int(minimum_zoom),
        "maximum_zoom": int(maximum_zoom),
        "tile_size": int(tile_size),
        "nodata_value": int(nodata_value),
        "tile_count": int(tile_count),
        "zoom_tile_counts": zoom_counts,
    }
    if summary_json is not None:
        summary_json.parent.mkdir(parents=True, exist_ok=True)
        summary_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if verbose:
        print(json.dumps(result, indent=2))
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build XYZ PNG raster tiles for Google Maps from an EPSG:3857 mask raster.")
    ap.add_argument("--input-raster", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--minimum-zoom", type=int, default=6)
    ap.add_argument("--maximum-zoom", type=int, default=14)
    ap.add_argument("--tile-size", type=int, default=256)
    ap.add_argument("--nodata-value", type=int, default=0)
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    build_google_maps_raster_tiles(
        input_raster=Path(args.input_raster).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        minimum_zoom=int(args.minimum_zoom),
        maximum_zoom=int(args.maximum_zoom),
        tile_size=int(args.tile_size),
        nodata_value=int(args.nodata_value),
        summary_json=(Path(args.summary_json).expanduser().resolve() if str(args.summary_json or "").strip() else None),
        overwrite=bool(args.overwrite),
        verbose=bool(args.verbose),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
