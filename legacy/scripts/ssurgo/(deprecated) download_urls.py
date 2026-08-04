#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def _read_urls(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"urls file not found: {path}")
    out: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        text = str(line).strip()
        if not text or text.startswith("#"):
            continue
        out.append(text)
    if not out:
        raise RuntimeError(f"no URLs found in file: {path}")
    return out


def _target_name(url: str) -> str:
    parsed = urllib.parse.urlparse(str(url))
    name = Path(parsed.path).name.strip()
    if not name:
        raise ValueError(f"cannot derive filename from url: {url}")
    return name


def _download_one(*, url: str, out_dir: Path, overwrite: bool, timeout_seconds: int) -> dict[str, Any]:
    filename = _target_name(url)
    out_path = out_dir / filename
    existed = out_path.exists()
    if existed and not overwrite:
        return {"url": url, "path": out_path.as_posix(), "status": "skipped_exists", "bytes": out_path.stat().st_size}

    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "User-Agent": "landcore-etl/ssurgo-downloader",
            "Accept": "*/*",
        },
    )
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        with tmp_path.open("wb") as f:
            shutil.copyfileobj(resp, f)
    tmp_path.replace(out_path)
    return {"url": url, "path": out_path.as_posix(), "status": "downloaded", "bytes": out_path.stat().st_size}


def main() -> int:
    ap = argparse.ArgumentParser(description="Download all URLs in a text file to an output directory.")
    ap.add_argument("--urls-file", required=True, help="Text file with one URL per line")
    ap.add_argument("--out-dir", required=True, help="Directory for downloaded files")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    ap.add_argument("--timeout-seconds", type=int, default=300)
    ap.add_argument("--summary-json", default="", help="Optional summary JSON output path")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    urls_file = Path(str(args.urls_file)).expanduser().resolve()
    out_dir = Path(str(args.out_dir)).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    urls = _read_urls(urls_file)
    timeout_seconds = max(10, int(args.timeout_seconds))

    results: list[dict[str, Any]] = []
    for idx, url in enumerate(urls, start=1):
        item = _download_one(
            url=str(url),
            out_dir=out_dir,
            overwrite=bool(args.overwrite),
            timeout_seconds=timeout_seconds,
        )
        results.append(item)
        if args.verbose:
            print(
                f"[download_urls] {idx}/{len(urls)} status={item['status']} "
                f"bytes={item['bytes']} path={item['path']}"
            )

    downloaded = sum(1 for x in results if x["status"] == "downloaded")
    skipped = sum(1 for x in results if x["status"] == "skipped_exists")
    summary = {
        "urls_file": urls_file.as_posix(),
        "out_dir": out_dir.as_posix(),
        "url_count": len(urls),
        "downloaded_count": downloaded,
        "skipped_exists_count": skipped,
        "results": results,
    }

    if str(args.summary_json or "").strip():
        summary_path = Path(str(args.summary_json)).expanduser().resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(
        f"url_count={summary['url_count']} downloaded={summary['downloaded_count']} "
        f"skipped_exists={summary['skipped_exists_count']} out_dir={summary['out_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
