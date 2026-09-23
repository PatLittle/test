import importlib.util
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


if __name__ == "__main__":
    unittest.main()
