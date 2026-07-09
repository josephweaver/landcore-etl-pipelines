#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


def _parse_states(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace(";", ",").split(","):
        s = "".join(ch for ch in str(token).strip().upper() if ch.isalpha())
        if len(s) != 2:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _load_urls_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    out: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        text = str(line).strip()
        if not text or text.startswith("#"):
            continue
        out.append(text)
    return out


def _expand_template(template: str, states: list[str]) -> list[str]:
    tpl = str(template or "").strip()
    if not tpl:
        return []
    if "{state}" not in tpl:
        raise ValueError("url_template must contain '{state}' placeholder")
    return [tpl.replace("{state}", st) for st in states]


def _select_by_state(urls: list[str], state: str) -> list[str]:
    # Prefer URLs containing _XX token (e.g., gSSURGO_IL.zip), fallback to XX anywhere.
    pat_primary = re.compile(rf"[_/\-]{re.escape(state)}([_./\-]|$)", re.IGNORECASE)
    pat_fallback = re.compile(rf"{re.escape(state)}", re.IGNORECASE)
    primary = [u for u in urls if pat_primary.search(u)]
    if primary:
        return primary
    return [u for u in urls if pat_fallback.search(u)]


def main() -> int:
    ap = argparse.ArgumentParser(description="Select/expand SSURGO state database URLs for states of interest.")
    ap.add_argument("--states", required=True, help="Comma-separated state abbreviations (e.g., IL,MN,WI)")
    ap.add_argument("--urls-file", default="", help="Path to newline-delimited URL list (optional)")
    ap.add_argument("--url-template", default="", help="URL template containing {state} placeholder (optional)")
    ap.add_argument("--output-file", required=True, help="Output file for selected URLs")
    ap.add_argument("--strict", action="store_true", help="Fail if any state has no matching URL")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    states = _parse_states(args.states)
    if not states:
        raise ValueError("no valid two-letter states parsed from --states")

    urls_file_path = Path(str(args.urls_file or "")).expanduser().resolve() if str(args.urls_file or "").strip() else None
    file_urls = _load_urls_file(urls_file_path) if urls_file_path else []
    template_urls = _expand_template(str(args.url_template or ""), states)
    pool = file_urls + template_urls
    if not pool:
        raise ValueError("no URL source provided; set --url-template and/or --urls-file with URLs")

    selected: list[str] = []
    missing: list[str] = []
    for st in states:
        matched = _select_by_state(pool, st)
        if not matched:
            missing.append(st)
            continue
        for u in matched:
            if u not in selected:
                selected.append(u)

    if args.strict and missing:
        raise RuntimeError(f"missing URL match for states: {','.join(missing)}")
    if not selected:
        raise RuntimeError("no URLs selected for requested states")

    out_path = Path(args.output_file).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(selected) + "\n", encoding="utf-8")

    if args.verbose:
        print(f"states={','.join(states)} selected={len(selected)} missing={','.join(missing) if missing else '-'}")
        print(f"output={out_path.as_posix()}")
    else:
        print(f"selected={len(selected)} missing={len(missing)} output={out_path.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
