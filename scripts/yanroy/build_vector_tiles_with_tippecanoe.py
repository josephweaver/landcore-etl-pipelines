from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
from pathlib import Path


def _run_tippecanoe(cmd: list[str]) -> tuple[str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "tippecanoe failed "
            f"rc={proc.returncode} cmd={shlex.join(cmd)} "
            f"stderr={(proc.stderr or '').strip()[:2000]}"
        )
    return str(proc.stdout or ""), str(proc.stderr or "")


def build_vector_tiles_with_tippecanoe(
    input_ndjson: Path,
    output_mbtiles: Path | None,
    output_tiles_dir: Path | None,
    layer_name: str,
    minimum_zoom: int = 8,
    maximum_zoom: int = 14,
    tippecanoe_bin: str = "tippecanoe",
    detect_shared_borders: bool = True,
    coalesce_densest_as_needed: bool = True,
    drop_densest_as_needed: bool = True,
    read_parallel: bool = True,
    overwrite: bool = True,
    verbose: bool = False,
) -> dict:
    if not input_ndjson.exists():
        raise FileNotFoundError(f"input ndjson not found: {input_ndjson}")
    if not layer_name.strip():
        raise ValueError("layer_name is required")
    if output_mbtiles is None and output_tiles_dir is None:
        raise ValueError("provide at least one output target: output_mbtiles or output_tiles_dir")

    exe = shutil.which(tippecanoe_bin)
    if not exe:
        raise FileNotFoundError(
            f"tippecanoe binary not found: {tippecanoe_bin}. Install tippecanoe and ensure it is on PATH."
        )

    base = [
        exe,
        "-l",
        layer_name,
        "-Z",
        str(int(minimum_zoom)),
        "-z",
        str(int(maximum_zoom)),
        "--no-feature-limit",
        "--no-tile-size-limit",
    ]
    if read_parallel:
        base.append("--read-parallel")
    if detect_shared_borders:
        base.append("--detect-shared-borders")
    if coalesce_densest_as_needed:
        base.append("--coalesce-densest-as-needed")
    if drop_densest_as_needed:
        base.append("--drop-densest-as-needed")

    commands: list[str] = []
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    output_mbtiles_text = ""
    output_tiles_dir_text = ""
    mbtiles_size_bytes = 0
    tiles_file_count = 0

    if output_mbtiles is not None:
        output_mbtiles.parent.mkdir(parents=True, exist_ok=True)
        cmd_mbtiles = list(base) + ["-o", output_mbtiles.resolve().as_posix(), "--force", input_ndjson.resolve().as_posix()]
        out, err = _run_tippecanoe(cmd_mbtiles)
        commands.append(shlex.join(cmd_mbtiles))
        stdout_parts.append(out)
        stderr_parts.append(err)
        output_mbtiles_text = output_mbtiles.resolve().as_posix()
        mbtiles_size_bytes = int(output_mbtiles.stat().st_size) if output_mbtiles.exists() else 0

    if output_tiles_dir is not None:
        if output_tiles_dir.exists() and overwrite:
            shutil.rmtree(output_tiles_dir)
        output_tiles_dir.parent.mkdir(parents=True, exist_ok=True)
        cmd_tiles = list(base) + ["-e", output_tiles_dir.resolve().as_posix(), input_ndjson.resolve().as_posix()]
        out, err = _run_tippecanoe(cmd_tiles)
        commands.append(shlex.join(cmd_tiles))
        stdout_parts.append(out)
        stderr_parts.append(err)
        output_tiles_dir_text = output_tiles_dir.resolve().as_posix()
        if output_tiles_dir.exists():
            tiles_file_count = sum(1 for p in output_tiles_dir.rglob("*") if p.is_file())

    result = {
        "tippecanoe_bin": exe,
        "commands": commands,
        "layer_name": layer_name,
        "minimum_zoom": int(minimum_zoom),
        "maximum_zoom": int(maximum_zoom),
        "input_ndjson": input_ndjson.resolve().as_posix(),
        "output_mbtiles": output_mbtiles_text,
        "output_tiles_dir": output_tiles_dir_text,
        "stdout": "\n".join([p for p in stdout_parts if p]),
        "stderr": "\n".join([p for p in stderr_parts if p]),
        "mbtiles_size_bytes": int(mbtiles_size_bytes),
        "tiles_file_count": int(tiles_file_count),
    }
    if verbose:
        print(f"[build_vector_tiles_with_tippecanoe] output_mbtiles={result['output_mbtiles']}")
        print(f"[build_vector_tiles_with_tippecanoe] output_tiles_dir={result['output_tiles_dir']}")
        print(f"[build_vector_tiles_with_tippecanoe] mbtiles_size_bytes={result['mbtiles_size_bytes']}")
        print(f"[build_vector_tiles_with_tippecanoe] tiles_file_count={result['tiles_file_count']}")
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build vector tiles from NDJSON features using tippecanoe (MBTiles and/or XYZ dir).")
    ap.add_argument("--input-ndjson", required=True)
    ap.add_argument("--output-mbtiles", default="")
    ap.add_argument("--output-tiles-dir", default="")
    ap.add_argument("--layer-name", default="yanroy_fields")
    ap.add_argument("--minimum-zoom", type=int, default=8)
    ap.add_argument("--maximum-zoom", type=int, default=14)
    ap.add_argument("--tippecanoe-bin", default="tippecanoe")
    ap.add_argument("--no-overwrite", action="store_true")
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--verbose", action="store_true")
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[build_vector_tiles_with_tippecanoe][WARN] ignoring unknown args: {' '.join(unknown)}")

    result = build_vector_tiles_with_tippecanoe(
        input_ndjson=Path(args.input_ndjson).expanduser().resolve(),
        output_mbtiles=(Path(args.output_mbtiles).expanduser().resolve() if str(args.output_mbtiles or "").strip() else None),
        output_tiles_dir=(Path(args.output_tiles_dir).expanduser().resolve() if str(args.output_tiles_dir or "").strip() else None),
        layer_name=str(args.layer_name),
        minimum_zoom=int(args.minimum_zoom),
        maximum_zoom=int(args.maximum_zoom),
        tippecanoe_bin=str(args.tippecanoe_bin),
        overwrite=(not bool(args.no_overwrite)),
        verbose=bool(args.verbose),
    )

    summary_path = Path(str(args.summary_json or "").strip()).expanduser().resolve() if str(args.summary_json or "").strip() else None
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
