#!/usr/bin/env python3
"""Download and merge the English and French Burolis datasets."""

from __future__ import annotations

import csv
import json
import re
import time
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.tbs-sct.canada.ca/burolis"
EN_URL = BASE_URL
FR_URL = f"{BASE_URL}/fr"
PAGE_SIZE = 1_000
REQUEST_TIMEOUT = 120
MAX_PAGES = 1_000
OUTPUT_DIR = Path(__file__).resolve().parent / "data"
TIMESERIES_PATH = OUTPUT_DIR / "burolis_timeseries.csv"
README_PATH = Path(__file__).resolve().parent / "README.md"
TIMESERIES_FIELDS = ("date", "rows_en", "rows_fr", "change_detected")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/153.0.0.0 Safari/537.36"
)

Record = dict[str, Any]


def normalize_csv_value(value: Any) -> Any:
    """Use Git-friendly newlines inside multiline CSV string values."""
    if not isinstance(value, str):
        return value
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"[ \t]+(?=\n)", "", normalized)


def is_request_rejected(text: str) -> bool:
    """Return True when TBS responds with its HTTP-200 rejection page."""
    return "request rejected" in text.casefold()


def build_session(referer: str) -> requests.Session:
    """Create a retrying session whose cookies persist across GET and POST."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": referer,
        }
    )
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_token(session: requests.Session, url: str) -> str:
    """Open a Burolis page and extract its ASP.NET anti-forgery token."""
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    if is_request_rejected(response.text):
        raise RuntimeError(f"TBS edge protection returned 'Request Rejected' for {url}")

    soup = BeautifulSoup(response.text, "html.parser")
    token_input = soup.select_one(
        '#searchFrm input[name="__RequestVerificationToken"]'
    )
    if token_input is None or not token_input.get("value"):
        title = soup.title.get_text(" ", strip=True) if soup.title else "no title"
        raise RuntimeError(
            f"Could not find the ASP.NET anti-forgery token at {url} "
            f"(page title: {title!r})"
        )

    return str(token_input["value"])


def _record_lists(value: Any) -> Iterable[list[Record]]:
    """Yield nested lists that look like arrays of JSON records."""
    if isinstance(value, dict):
        for nested in value.values():
            yield from _record_lists(nested)
    elif isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            yield value
        for nested in value[:5]:
            yield from _record_lists(nested)


def get_records(payload: Any) -> list[Record]:
    """Find the Burolis record array in a search response."""
    if isinstance(payload, list):
        if all(isinstance(item, dict) for item in payload):
            return payload
        raise RuntimeError("The Burolis response was a list, but not a record array")

    if isinstance(payload, dict):
        for key in ("data", "results", "records", "items", "services"):
            value = payload.get(key)
            if isinstance(value, list) and all(
                isinstance(item, dict) for item in value
            ):
                return value

        candidates = list(_record_lists(payload))
        service_candidates = [
            candidate
            for candidate in candidates
            if not candidate or any("serviceId" in record for record in candidate)
        ]
        if service_candidates:
            return max(service_candidates, key=len)

    raise RuntimeError("Could not identify a record array in the Burolis response")


def _service_id(record: Record, language: str, page_number: int) -> Any:
    service_id = record.get("serviceId")
    if service_id is None:
        raise RuntimeError(
            f"{language.upper()} page {page_number} contained a record without serviceId"
        )
    return service_id


def download_language(url: str, language: str) -> list[Record]:
    """Download every result page for one Burolis language."""
    session = build_session(url)
    try:
        token = get_token(session, url)
        print(f"{language.upper()}: session established")

        records_by_id: dict[Any, Record] = {}
        previous_page_ids: tuple[str, ...] | None = None

        for page_number in range(1, MAX_PAGES + 1):
            form_data = {
                "criteria[pageNumber]": str(page_number),
                "criteria[pageSize]": str(PAGE_SIZE),
                "criteria[provinceId]": "",
                "criteria[cityId]": "",
                "criteria[institutionId]": "",
                "criteria[keywords]": "",
                "criteria[englishObligation]": "false",
                "criteria[frenchObligation]": "false",
                "criteria[bilingualObligation]": "false",
            }
            response = session.post(
                f"{url}?handler=Search",
                data=form_data,
                headers={"RequestVerificationToken": token},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            if is_request_rejected(response.text):
                raise RuntimeError(
                    "TBS edge protection returned 'Request Rejected' for "
                    f"{language.upper()} page {page_number}"
                )

            try:
                payload = response.json()
            except requests.exceptions.JSONDecodeError as exc:
                content_type = response.headers.get("Content-Type", "unknown")
                excerpt = " ".join(response.text[:200].split())
                raise RuntimeError(
                    f"Expected JSON for {language.upper()} page {page_number}; "
                    f"received {content_type}: {excerpt!r}"
                ) from exc

            page_records = get_records(payload)
            print(
                f"{language.upper()} page {page_number}: "
                f"{len(page_records):,} records"
            )
            if not page_records:
                break

            page_ids = tuple(
                str(_service_id(record, language, page_number))
                for record in page_records
            )
            if page_ids == previous_page_ids:
                raise RuntimeError(
                    f"{language.upper()} page {page_number} repeated the previous page"
                )
            previous_page_ids = page_ids

            for record in page_records:
                service_id = _service_id(record, language, page_number)
                existing = records_by_id.get(service_id)
                if existing is not None and existing != record:
                    raise RuntimeError(
                        f"Conflicting duplicate serviceId {service_id!r} in "
                        f"the {language.upper()} response"
                    )
                records_by_id[service_id] = record

            if len(page_records) < PAGE_SIZE:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError(
                f"{language.upper()} pagination exceeded the safety limit of "
                f"{MAX_PAGES:,} pages"
            )

        if not records_by_id:
            raise RuntimeError(f"Burolis returned no {language.upper()} records")

        records = sorted(
            records_by_id.values(), key=lambda record: str(record["serviceId"])
        )
        print(f"{language.upper()} total: {len(records):,}")
        return records
    finally:
        session.close()


def _records_by_id(records: list[Record], language: str) -> dict[Any, Record]:
    indexed: dict[Any, Record] = {}
    for record in records:
        service_id = record.get("serviceId")
        if service_id is None:
            raise ValueError(f"{language} record is missing serviceId: {record!r}")
        if service_id in indexed:
            raise ValueError(f"Duplicate {language} serviceId: {service_id!r}")
        indexed[service_id] = record
    return indexed


def load_saved_records(path: Path) -> list[Record] | None:
    """Load a prior JSON snapshot, or return None before the first snapshot."""
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as source:
        records = json.load(source)
    if not isinstance(records, list) or not all(
        isinstance(record, dict) for record in records
    ):
        raise RuntimeError(f"Expected a JSON record array in {path}")
    return records


def datasets_changed(
    records_en: list[Record],
    records_fr: list[Record],
    output_dir: Path = OUTPUT_DIR,
) -> bool:
    """Compare new records with the saved snapshots, independent of row order."""
    previous_en = load_saved_records(output_dir / "burolis_en.json")
    previous_fr = load_saved_records(output_dir / "burolis_fr.json")
    if previous_en is None or previous_fr is None:
        return True
    return (
        _records_by_id(previous_en, "saved EN")
        != _records_by_id(records_en, "new EN")
        or _records_by_id(previous_fr, "saved FR")
        != _records_by_id(records_fr, "new FR")
    )


def merge_languages(records_en: list[Record], records_fr: list[Record]) -> list[Record]:
    """Merge on serviceId and suffix only fields that differ anywhere."""
    en_by_id = _records_by_id(records_en, "EN")
    fr_by_id = _records_by_id(records_fr, "FR")
    all_ids = sorted(set(en_by_id) | set(fr_by_id), key=str)

    all_fields = set().union(
        *(record.keys() for record in [*records_en, *records_fr])
    )
    all_fields.discard("serviceId")

    different_fields = {
        field
        for field in all_fields
        if any(
            en_by_id.get(service_id, {}).get(field)
            != fr_by_id.get(service_id, {}).get(field)
            for service_id in all_ids
        )
    }
    identical_fields = all_fields - different_fields

    merged: list[Record] = []
    for service_id in all_ids:
        en_record = en_by_id.get(service_id, {})
        fr_record = fr_by_id.get(service_id, {})
        row: Record = {"serviceId": service_id}

        for field in sorted(identical_fields):
            row[field] = (
                en_record[field] if field in en_record else fr_record.get(field)
            )
        for field in sorted(different_fields):
            row[f"{field}_en"] = en_record.get(field)
            row[f"{field}_fr"] = fr_record.get(field)

        merged.append(row)

    print("Fields with EN/FR variants:")
    for field in sorted(different_fields):
        print(f"  {field}")
    return merged


def save_json(path: Path, records: list[Record]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output:
        json.dump(records, output, ensure_ascii=False, indent=2)
        output.write("\n")


def save_outputs(
    records_en: list[Record], records_fr: list[Record], merged: list[Record]
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_json(OUTPUT_DIR / "burolis_en.json", records_en)
    save_json(OUTPUT_DIR / "burolis_fr.json", records_fr)
    save_json(OUTPUT_DIR / "burolis_bilingual.json", merged)

    with (OUTPUT_DIR / "burolis_bilingual.jsonl").open(
        "w", encoding="utf-8", newline="\n"
    ) as output:
        for record in merged:
            output.write(json.dumps(record, ensure_ascii=False) + "\n")

    csv_frame = pd.json_normalize(merged).map(normalize_csv_value)
    csv_frame.to_csv(
        OUTPUT_DIR / "burolis_bilingual.csv",
        index=False,
        encoding="utf-8-sig",
        lineterminator="\n",
    )


def update_timeseries(
    run_date: str,
    rows_en: int,
    rows_fr: int,
    change_detected: bool,
    path: Path = TIMESERIES_PATH,
) -> None:
    """Add or update one UTC calendar-day entry in the change log."""
    datetime.strptime(run_date, "%Y-%m-%d")
    rows_by_date: dict[str, dict[str, str]] = {}

    if path.exists():
        with path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if tuple(reader.fieldnames or ()) != TIMESERIES_FIELDS:
                raise RuntimeError(
                    f"Unexpected columns in {path}; expected {TIMESERIES_FIELDS}"
                )
            for row in reader:
                datetime.strptime(row["date"], "%Y-%m-%d")
                int(row["rows_en"])
                int(row["rows_fr"])
                if row["change_detected"] not in {"0", "1"}:
                    raise RuntimeError(
                        f"Invalid change_detected value in {path}: "
                        f"{row['change_detected']!r}"
                    )
                rows_by_date[row["date"]] = row

    previous_today = rows_by_date.get(run_date)
    daily_change = int(change_detected)
    if previous_today is not None:
        daily_change |= int(previous_today["change_detected"])

    rows_by_date[run_date] = {
        "date": run_date,
        "rows_en": str(rows_en),
        "rows_fr": str(rows_fr),
        "change_detected": str(daily_change),
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(
            output, fieldnames=TIMESERIES_FIELDS, lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows_by_date[date] for date in sorted(rows_by_date))


def _chart_label(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return "(blank)"
    return str(value).strip()


def _counts(records: list[Record], field: str) -> Counter[str]:
    return Counter(_chart_label(record.get(field)) for record in records)


def _mermaid_pie(title: str, counts: Counter[str]) -> str:
    lines = ["```mermaid", "pie showData", f"    title {title}"]
    for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"    {json.dumps(label, ensure_ascii=False)} : {count}")
    lines.append("```")
    return "\n".join(lines)


def _mermaid_bar(title: str, counts: Counter[str], limit: int = 25) -> str:
    top_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[
        :limit
    ]
    labels = ", ".join(json.dumps(label, ensure_ascii=False) for label, _ in top_counts)
    values = ", ".join(str(count) for _, count in top_counts)
    maximum = max((count for _, count in top_counts), default=0)
    return "\n".join(
        [
            "```mermaid",
            "xychart-beta",
            f"    title {json.dumps(title, ensure_ascii=False)}",
            f"    x-axis [{labels}]",
            f'    y-axis "Services" 0 --> {maximum}',
            f"    bar [{values}]",
            "```",
        ]
    )


def build_readme(
    records_en: list[Record],
    records_fr: list[Record],
    merged: list[Record],
    run_date: str,
    change_detected: bool,
) -> str:
    """Build the generated project README and its Mermaid charts."""
    provision_counts = _counts(records_en, "provision")
    obligation_counts = _counts(records_en, "langObligationId")
    institution_counts = _counts(records_en, "institutionCode")
    institution_total = len(
        {
            record.get("institutionCode")
            for record in records_en
            if record.get("institutionCode") not in {None, ""}
        }
    )

    return f"""# Burolis bilingual data

This directory contains a weekly bilingual snapshot of the Treasury Board of
Canada Secretariat's [Burolis directory]({BASE_URL}). The scraper downloads all
English and French results, merges them on `serviceId`, and keeps separate
`_en` and `_fr` columns only when a field differs between the two languages.

## Current snapshot

Updated in UTC on **{run_date}**. Change detected: **{"yes" if change_detected else "no"}**.

| Measure | Count |
| --- | ---: |
| English records | {len(records_en):,} |
| French records | {len(records_fr):,} |
| Merged services | {len(merged):,} |
| Institution codes | {institution_total:,} |
| Provision values | {len(provision_counts):,} |

## Provision counts

{_mermaid_pie("Services by provision", provision_counts)}

## Language obligation ID

{_mermaid_pie("Services by language obligation ID", obligation_counts)}

## Top 25 institutions by service count

Labels use the source `institutionCode` field.

{_mermaid_bar("Top 25 institutions by service count", institution_counts)}

## Data files

- `data/burolis_en.json`
- `data/burolis_fr.json`
- `data/burolis_bilingual.json`
- `data/burolis_bilingual.jsonl`
- `data/burolis_bilingual.csv`
- `data/burolis_timeseries.csv`

The time series has one row per UTC date. `change_detected` is `1` when either
language's row count or any record content changed from the prior snapshot, and
`0` otherwise. Multiple runs on the same date update one row; once a change is
detected that day's value remains `1`.

## Run locally

```bash
python -m pip install -r burolis/requirements.txt
python burolis/scrape_burolis.py
```

The scheduled GitHub Actions workflow runs on a GitHub-hosted Ubuntu runner.
Responses containing the TBS `Request Rejected` page are treated as failures,
so rejected or incomplete responses cannot replace the saved snapshot.
"""


def write_readme(content: str, path: Path = README_PATH) -> None:
    path.write_text(content, encoding="utf-8", newline="\n")


def main() -> None:
    records_en = download_language(EN_URL, "en")
    records_fr = download_language(FR_URL, "fr")
    change_detected = datasets_changed(records_en, records_fr)
    merged = merge_languages(records_en, records_fr)
    save_outputs(records_en, records_fr, merged)
    run_date = datetime.now(timezone.utc).date().isoformat()
    update_timeseries(
        run_date,
        len(records_en),
        len(records_fr),
        change_detected,
    )
    write_readme(
        build_readme(
            records_en,
            records_fr,
            merged,
            run_date,
            change_detected,
        )
    )
    print(f"EN: {len(records_en):,}")
    print(f"FR: {len(records_fr):,}")
    print(f"Merged: {len(merged):,}")
    print(f"Change detected: {int(change_detected)}")
    print(f"Saved data in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
