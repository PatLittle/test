"""Exercise the DFRP exports and URL cache without live network requests."""

import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zipfile import ZipFile

import geopandas as gpd
from pyogrio import list_layers

from dfrp_to_geodata import build


class DfrpExportTest(unittest.TestCase):
    def test_exports_and_incremental_images(self):
        same = "https://example.test/shared.jpg"
        old = "https://example.test/old.jpg"
        new = "https://example.test/new.jpg"
        minimap = "https://example.test/map.png"

        with TemporaryDirectory() as temp:
            work = Path(temp)
            archive = work / "source.zip"
            output = work / "output"
            cache = work / "cache" / "images.sqlite3"

            def write_archive(photo_url):
                with ZipFile(archive, "w") as source:
                    source.writestr("nested/source.xml", f"""
                        <Properties>
                          <Property>
                            <Property_Number>001</Property_Number>
                            <Photos><Photo>{same}</Photo><Photo>{photo_url}</Photo></Photos>
                            <MiniMap>{minimap}</MiniMap>
                            <Parcels>
                              <Parcel Number="01">
                                <Location><Latitude>45.4</Latitude><Longitude>-75.7</Longitude></Location>
                                <Structures>
                                  <Structure><Structure_Number>02</Structure_Number></Structure>
                                </Structures>
                              </Parcel>
                              <Parcel Number="02" />
                            </Parcels>
                          </Property>
                          <Property>
                            <Property_Number>002</Property_Number>
                            <Photos><Photo>{same}</Photo></Photos>
                          </Property>
                          <Property><Property_Number>003</Property_Number></Property>
                        </Properties>
                    """)

            with patch("dfrp_to_geodata.download_image", return_value=("image/jpeg", b"photo")) as fetch:
                write_archive(old)
                self.assertTrue(build(archive, output, cache))
                self.assertCountEqual([call.args[0] for call in fetch.call_args_list], [same, old, minimap])

                fetch.reset_mock()
                self.assertFalse(build(archive, output, cache))
                fetch.assert_not_called()

                write_archive(new)
                self.assertTrue(build(archive, output, cache))
                fetch.assert_called_once_with(new)

            self.assertEqual(
                {file.name for file in output.iterdir()},
                {"dfrp_parcels.geojson", "dfrp_structures.geojson", "dfrp_parcels.parquet",
                 "dfrp_structures.parquet", "dfrp.gpkg", "dfrp_with_images.gpkg"},
            )
            for name, count in (("parcels", 4), ("structures", 1)):
                geojson = json.loads((output / f"dfrp_{name}.geojson").read_text(encoding="utf-8"))
                self.assertEqual(len(geojson["features"]), count)
                self.assertEqual(geojson["features"][0]["geometry"]["coordinates"], [-75.7, 45.4])
                parquet = gpd.read_parquet(output / f"dfrp_{name}.parquet")
                self.assertEqual(len(parquet), count)
                self.assertEqual(parquet.crs.to_epsg(), 4326)
                self.assertEqual(parquet.geometry.isna().sum(), count - 1)
                self.assertEqual(parquet.iloc[0]["parcel_id"], "001:01")

            self.assertCountEqual(list_layers(output / "dfrp.gpkg")[:, 0], ["parcels", "structures"])
            image_package = output / "dfrp_with_images.gpkg"
            self.assertCountEqual(list_layers(image_package)[:, 0],
                                  ["parcels", "structures", "dfrp_images", "dfrp_property_images"])
            with sqlite3.connect(image_package) as db:
                self.assertCountEqual(db.execute("SELECT url, content FROM dfrp_images"),
                                      [(same, b"photo"), (new, b"photo"), (minimap, b"photo")])
                self.assertEqual(db.execute("""
                    SELECT count(*) FROM dfrp_property_images AS link
                    JOIN dfrp_images AS image ON image.id = link.image_id
                    WHERE image.url = ?
                """, (same,)).fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
