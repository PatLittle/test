#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

import build_report
import build_bn_funnel

ROOT = Path(__file__).resolve().parents[1]

# Build the generated site directly into BN-ATI so source and published output
# live in one maintained directory without a second generated docs tree.
build_report.OUT_DIR = ROOT
build_report.OUT_SQLITE = ROOT / "data.sqlite"
build_report.OUT_HTML = ROOT / "index.html"
build_report.TEMPLATE_FILE = ROOT / "templates" / "index.html"
build_report.DOCCLOUD_CACHE = ROOT / "data" / "documentcloud_cache.jsonl"

build_bn_funnel.DOCS = ROOT
build_bn_funnel.DB_PATH = ROOT / "data.sqlite"
build_bn_funnel.OUT_JSON = ROOT / "bn-funnel.json"
build_bn_funnel.SOURCE_JS = ROOT / "templates" / "bn-funnel.js"
build_bn_funnel.OUT_JS = ROOT / "bn-funnel.js"


def inject_ui_overrides() -> None:
    source = ROOT / "templates" / "ui-overrides.css"
    target = ROOT / "ui-overrides.css"
    shutil.copy2(source, target)

    html_path = ROOT / "index.html"
    html = html_path.read_text(encoding="utf-8")
    stylesheet = '    <link rel="stylesheet" href="./ui-overrides.css" />\n'
    if 'href="./ui-overrides.css"' not in html:
        anchor = '    <link rel="stylesheet" href="./assets/app.css" />\n'
        if anchor not in html:
            raise RuntimeError("Could not find app.css link for UI overrides")
        html = html.replace(anchor, anchor + stylesheet, 1)
    html_path.write_text(html, encoding="utf-8")


def main() -> None:
    build_report.main()
    build_bn_funnel.main()
    inject_ui_overrides()
    print(f"Built consolidated site in {ROOT}", flush=True)


if __name__ == "__main__":
    main()
