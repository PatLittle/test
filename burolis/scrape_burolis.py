#!/usr/bin/env python3
"""Download and merge the English and French Burolis datasets."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Iterable
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


def main() -> None:
    records_en = download_language(EN_URL, "en")
    records_fr = download_language(FR_URL, "fr")
    merged = merge_languages(records_en, records_fr)
    save_outputs(records_en, records_fr, merged)
    print(f"EN: {len(records_en):,}")
    print(f"FR: {len(records_fr):,}")
    print(f"Merged: {len(merged):,}")
    print(f"Saved data in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
