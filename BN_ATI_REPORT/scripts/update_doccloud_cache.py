#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from documentcloud import DocumentCloud

ROOT = Path(__file__).resolve().parents[1]
CACHE_FILE = ROOT / "data" / "documentcloud_cache.jsonl"
QUERY = "organization:38956 created_at:[NOW-2YEAR TO NOW-1YEAR]"
PER_PAGE = 100

REQUEST_NUMBER_RE = re.compile(r"\b[A-Z]-\d{4}-\d{3,6}\b", re.IGNORECASE)


def scalar_text(value: Any) -> str:
    """Flatten list-valued DocumentCloud metadata into a single clean string."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        if not value:
            return ""
        return scalar_text(value[0])
    return str(value).strip()


def first_value(data: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = data.get(key)
        if value not in (None, "", [], ()):
            return scalar_text(value)
    return ""


def normalize_request_number(value: Any) -> str:
    candidate = scalar_text(value)
    match = REQUEST_NUMBER_RE.search(candidate)
    return match.group(0).upper() if match else ""


def load_cache() -> dict[str, dict[str, Any]]:
    if not CACHE_FILE.exists():
        return {}

    records: dict[str, dict[str, Any]] = {}
    with CACHE_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            records[str(record["documentcloud_id"])] = record
    return records


def create_client() -> DocumentCloud:
    username = os.getenv("DC_USERNAME")
    password = os.getenv("DC_PASSWORD")
    if username and password:
        return DocumentCloud(username, password)
    return DocumentCloud()


def document_to_record(document: Any) -> dict[str, Any]:
    metadata = getattr(document, "data", None)
    if not isinstance(metadata, dict):
        metadata = {}

    title = scalar_text(getattr(document, "title", ""))
    description = scalar_text(getattr(document, "description", ""))

    organization_label = first_value(
        metadata,
        (
            "owner_org",
            "organization_id",
            "organization",
            "owner_organization",
        ),
    )

    request_number = normalize_request_number(
        first_value(
            metadata,
            (
                "request_number",
                "ati_request_number",
                "request_no",
                "ati_number",
            ),
        )
    )
    if not request_number:
        request_number = normalize_request_number(f"{title} {description}")

    canonical_url = scalar_text(
        getattr(document, "canonical_url", "")
        or getattr(document, "url", "")
    )

    return {
        "documentcloud_id": scalar_text(getattr(document, "id", "")),
        # This is a source organization label, not necessarily a CKAN owner_org slug.
        "owner_org": organization_label.lower(),
        "request_number": request_number,
        "open_by_default_url": canonical_url,
        "documentcloud_title": title,
        "documentcloud_description": description,
        "documentcloud_source": scalar_text(getattr(document, "source", "")),
        "documentcloud_created_at": scalar_text(getattr(document, "created_at", "")),
        "documentcloud_updated_at": scalar_text(getattr(document, "updated_at", "")),
        "documentcloud_language": scalar_text(getattr(document, "language", "")),
        "documentcloud_metadata_json": json.dumps(
            metadata, ensure_ascii=False, sort_keys=True
        ),
        "cache_updated_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    cached = load_cache()

    print(f"DocumentCloud cache before update: {len(cached):,} records")
    print(f"Querying DocumentCloud: {QUERY}")

    client = create_client()
    documents = client.documents.search(query=QUERY, per_page=PER_PAGE)

    fetched = 0
    for document in documents:
        record = document_to_record(document)
        document_id = record["documentcloud_id"]
        if not document_id:
            continue
        cached[document_id] = record
        fetched += 1

    ordered = sorted(
        cached.values(),
        key=lambda row: (
            row.get("documentcloud_created_at", ""),
            row.get("documentcloud_id", ""),
        ),
    )

    with CACHE_FILE.open("w", encoding="utf-8", newline="\n") as handle:
        for record in ordered:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"DocumentCloud records returned this run: {fetched:,}")
    print(f"DocumentCloud cache after update: {len(ordered):,} records")
    print(f"Wrote {CACHE_FILE}")


if __name__ == "__main__":
    main()
