import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[1] / "scrape_burolis.py"
SPEC = importlib.util.spec_from_file_location("scrape_burolis", SCRIPT)
assert SPEC and SPEC.loader
scraper = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = scraper
SPEC.loader.exec_module(scraper)


class MergeTests(unittest.TestCase):
    def test_suffixes_only_fields_that_differ_anywhere(self):
        records_en = [
            {"serviceId": 1, "code": "A", "name": "Office", "active": True},
            {"serviceId": 2, "code": "B", "name": "Centre", "active": True},
        ]
        records_fr = [
            {"serviceId": 1, "code": "A", "name": "Bureau", "active": True},
            {"serviceId": 2, "code": "B", "name": "Centre", "active": True},
        ]

        merged = scraper.merge_languages(records_en, records_fr)

        self.assertEqual(
            merged,
            [
                {
                    "serviceId": 1,
                    "active": True,
                    "code": "A",
                    "name_en": "Office",
                    "name_fr": "Bureau",
                },
                {
                    "serviceId": 2,
                    "active": True,
                    "code": "B",
                    "name_en": "Centre",
                    "name_fr": "Centre",
                },
            ],
        )

    def test_keeps_unmatched_language_values_in_suffixed_columns(self):
        merged = scraper.merge_languages(
            [{"serviceId": "1", "name": "Only English"}], []
        )
        self.assertEqual(
            merged,
            [{"serviceId": "1", "name_en": "Only English", "name_fr": None}],
        )


class ResponseTests(unittest.TestCase):
    def test_finds_nested_record_array(self):
        payload = {"result": {"items": [{"serviceId": 7}]}}
        self.assertEqual(scraper.get_records(payload), [{"serviceId": 7}])

    def test_rejected_get_is_an_error(self):
        response = Mock()
        response.text = "<title>Request Rejected</title>"
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.return_value = response

        with self.assertRaisesRegex(RuntimeError, "Request Rejected"):
            scraper.get_token(session, scraper.EN_URL)


class OutputTests(unittest.TestCase):
    def test_writes_all_five_outputs(self):
        en = [{"serviceId": 1, "name": "Office"}]
        fr = [{"serviceId": 1, "name": "Bureau"}]
        merged = scraper.merge_languages(en, fr)
        merged[0]["notes_en"] = "Line one \r\nLine two"
        merged[0]["notes_fr"] = "Ligne un\rLigne deux"

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(scraper, "OUTPUT_DIR", Path(temp_dir)):
                scraper.save_outputs(en, fr, merged)
            self.assertEqual(
                {path.name for path in Path(temp_dir).iterdir()},
                {
                    "burolis_en.json",
                    "burolis_fr.json",
                    "burolis_bilingual.json",
                    "burolis_bilingual.jsonl",
                    "burolis_bilingual.csv",
                },
            )
            csv_bytes = (Path(temp_dir) / "burolis_bilingual.csv").read_bytes()
            self.assertNotIn(b"\r", csv_bytes)
            self.assertNotIn(b"Line one \n", csv_bytes)


class ChangeLogTests(unittest.TestCase):
    def test_detects_content_and_row_changes_but_not_order_changes(self):
        old_en = [
            {"serviceId": 1, "name": "One"},
            {"serviceId": 2, "name": "Two"},
        ]
        old_fr = [
            {"serviceId": 1, "name": "Un"},
            {"serviceId": 2, "name": "Deux"},
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            (output_dir / "burolis_en.json").write_text(
                json.dumps(old_en), encoding="utf-8"
            )
            (output_dir / "burolis_fr.json").write_text(
                json.dumps(old_fr), encoding="utf-8"
            )

            self.assertFalse(
                scraper.datasets_changed(
                    list(reversed(old_en)), list(reversed(old_fr)), output_dir
                )
            )
            changed_fr = [*old_fr]
            changed_fr[0] = {"serviceId": 1, "name": "Un modifié"}
            self.assertTrue(
                scraper.datasets_changed(old_en, changed_fr, output_dir)
            )
            self.assertTrue(
                scraper.datasets_changed(old_en[:-1], old_fr, output_dir)
            )

    def test_keeps_one_row_per_day_and_preserves_daily_change(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "timeseries.csv"
            scraper.update_timeseries("2026-09-23", 10, 10, False, path)
            scraper.update_timeseries("2026-09-23", 11, 10, True, path)
            scraper.update_timeseries("2026-09-23", 11, 10, False, path)
            scraper.update_timeseries("2026-09-30", 11, 10, False, path)

            with path.open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))

        self.assertEqual(
            rows,
            [
                {
                    "date": "2026-09-23",
                    "rows_en": "11",
                    "rows_fr": "10",
                    "change_detected": "1",
                },
                {
                    "date": "2026-09-30",
                    "rows_en": "11",
                    "rows_fr": "10",
                    "change_detected": "0",
                },
            ],
        )

    def test_builds_requested_mermaid_charts(self):
        en = [
            {
                "serviceId": 1,
                "provision": "5-1-a",
                "langObligationId": 1,
                "institutionCode": "AAA",
            },
            {
                "serviceId": 2,
                "provision": "5-1-a",
                "langObligationId": 2,
                "institutionCode": "AAA",
            },
            {
                "serviceId": 3,
                "provision": "6-1-a",
                "langObligationId": 2,
                "institutionCode": "BBB",
            },
        ]
        readme = scraper.build_readme(en, en, en, "2026-09-23", True)

        self.assertEqual(readme.count("pie showData"), 2)
        self.assertIn("xychart-beta", readme)
        self.assertIn('"5-1-a" : 2', readme)
        self.assertIn('x-axis ["AAA", "BBB"]', readme)
        self.assertIn("Updated in UTC on **2026-09-23**", readme)


if __name__ == "__main__":
    unittest.main()
