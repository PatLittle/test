"""Convert the Directory of Federal Real Property ZIP to spatial layers and packages."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO, Iterable
from urllib.parse import urljoin, urlsplit
from zipfile import ZipFile, is_zipfile

import geopandas as gpd
import pandas as pd
import requests
from lxml import etree
from shapely.geometry import Point

SOURCE_URL = "https://map-carte.tbs-sct.gc.ca/dfrp-rbif/oddo/dfrp-rbif.zip"
IMAGE_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DFRP-Image-Downloader/1.0)"}
ZIP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DFRP-GeoPackage-Notebook/1.0)",
    "Accept": "application/zip, application/octet-stream, */*",
}


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = " ".join(value.split())
    return value or None


def text(parent: etree._Element, path: str) -> str | None:
    element = parent.find(path)
    return clean_text(element.text) if element is not None else None


def attr(parent: etree._Element | None, name: str) -> str | None:
    return clean_text(parent.get(name)) if parent is not None else None


def as_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def as_float(value: str | None) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def as_bool(value: str | None) -> bool | None:
    if value is None:
        return None

    normalized = value.strip().lower()

    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False

    return None


def child_code(parent: etree._Element, path: str) -> str | None:
    element = parent.find(path)
    return attr(element, "code")


def joined_text(
    parent: etree._Element,
    path: str,
    separator: str = " | ",
) -> str | None:
    values = [clean_text(element.text) for element in parent.findall(path)]
    values = [value for value in values if value]
    return separator.join(values) or None


def make_point(
    latitude: float | None,
    longitude: float | None,
) -> tuple[Point | None, bool]:
    if latitude is None or longitude is None:
        return None, False

    valid = -90 <= latitude <= 90 and -180 <= longitude <= 180

    if not valid:
        return None, False

    return Point(longitude, latitude), True


def iter_properties(source: BinaryIO) -> Iterable[etree._Element]:
    context = etree.iterparse(
        source,
        events=("end",),
        tag="Property",
        resolve_entities=False,
        no_network=True,
    )
    for _, prop in context:
        yield prop
        prop.clear()
        while prop.getprevious() is not None:
            del prop.getparent()[0]


def parse_xml_file(
    source: BinaryIO,
    source_file: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parcel_rows: list[dict[str, Any]] = []
    structure_rows: list[dict[str, Any]] = []

    for property_index, prop in enumerate(iter_properties(source), start=1):
        custodian = prop.find("Custodian")

        property_number = text(prop, "Property_Number")

        property_data: dict[str, Any] = {
            "source_file": source_file,
            "source_property_index": property_index,
            "property_number": property_number,
            "property_created_date": attr(prop, "createdDate"),
            "property_last_modified_date": attr(prop, "lastModifiedDate"),

            "custodian_code": attr(custodian, "code"),
            "custodian_name_en": text(prop, "./Custodian/Name_E"),
            "custodian_name_fr": text(prop, "./Custodian/Name_F"),
            "custodian_is_department": as_bool(
                attr(custodian, "isDepartment")
            ),
            "custodian_is_crown_corporation": as_bool(
                attr(custodian, "isCrownCorporation")
            ),
            "portfolio_last_certified_date": attr(
                custodian,
                "portfolioLastCertifiedDate",
            ),

            "official_contact_telephone": text(
                prop,
                "./Custodian/Official_Contact_Telephone",
            ),
            "official_contact_email": text(
                prop,
                "./Custodian/Official_Contact_Email",
            ),
            "official_contact_webform": text(
                prop,
                "./Custodian/Official_Contact_WebForm",
            ),

            "property_name_en": text(prop, "Property_Name_E"),
            "property_name_fr": text(prop, "Property_Name_F"),
            "property_address_en": text(prop, "Address_E"),
            "property_address_fr": text(prop, "Address_F"),

            "primary_use_code": child_code(prop, "Primary_Use_E"),
            "primary_use_en": text(prop, "Primary_Use_E"),
            "primary_use_fr": text(prop, "Primary_Use_F"),

            "interest_type_code": child_code(prop, "Interest_Type_E"),
            "interest_type_en": text(prop, "Interest_Type_E"),
            "interest_type_fr": text(prop, "Interest_Type_F"),

            "restriction_code": child_code(
                prop,
                "Restriction_on_Interest_E",
            ),
            "restriction_en": text(
                prop,
                "Restriction_on_Interest_E",
            ),
            "restriction_fr": text(
                prop,
                "Restriction_on_Interest_F",
            ),

            "photo_urls": joined_text(prop, "./Photos/Photo"),
            "minimap_url": text(prop, "MiniMap"),
        }

        parcels = prop.findall("./Parcels/Parcel")

        if not parcels:
            parcel_rows.append(
                {
                    **property_data,
                    "parcel_number": None,
                    "parcel_id": f"{property_number}:",
                    "coordinate_valid": False,
                    "geometry": None,
                }
            )
            continue

        for parcel in parcels:
            parcel_number = attr(parcel, "Number")
            parcel_id = f"{property_number}:{parcel_number or ''}"

            location = parcel.find("Location")
            interior_parking = parcel.find("./ParkingSpaces/Interior")

            latitude = as_float(text(parcel, "./Location/Latitude"))
            longitude = as_float(text(parcel, "./Location/Longitude"))
            geometry, coordinate_valid = make_point(latitude, longitude)

            parcel_data: dict[str, Any] = {
                **property_data,

                "parcel_number": parcel_number,
                "parcel_id": parcel_id,

                "land_area": as_float(text(parcel, "Land_Area")),
                "land_area_unit": attr(
                    parcel.find("Land_Area"),
                    "unitofMeasure",
                ),
                "building_count": as_int(
                    text(parcel, "Building_Count")
                ),
                "floor_area": as_float(text(parcel, "Floor_Area")),
                "floor_area_unit": attr(
                    parcel.find("Floor_Area"),
                    "unitofMeasure",
                ),

                "parking_exterior": as_int(
                    text(parcel, "./ParkingSpaces/Exterior")
                ),
                "parking_interior": as_int(
                    text(parcel, "./ParkingSpaces/Interior")
                ),
                "parking_interior_included_in_floor_area": as_bool(
                    attr(interior_parking, "includedInFloorArea")
                ),

                "location_type": attr(location, "type"),
                "sgc_code": attr(location, "sgc"),
                "federal_electoral_district_code": attr(
                    location,
                    "fed",
                ),
                "in_rural_area": as_bool(
                    attr(location, "inRuralArea")
                ),

                "province_en": text(
                    parcel,
                    "./Location/Province_E",
                ),
                "province_fr": text(
                    parcel,
                    "./Location/Province_F",
                ),
                "metro_area_name_en": text(
                    parcel,
                    "./Location/Metro_Area_Name_E",
                ),
                "metro_area_name_fr": text(
                    parcel,
                    "./Location/Metro_Area_Name_F",
                ),
                "municipality_en": text(
                    parcel,
                    "./Location/Municipality_E",
                ),
                "municipality_fr": text(
                    parcel,
                    "./Location/Municipality_F",
                ),
                "place_name_en": text(
                    parcel,
                    "./Location/Place_Name_EN",
                ),
                "place_name_fr": text(
                    parcel,
                    "./Location/Place_Name_FR",
                ),
                "city_name_en": text(
                    parcel,
                    "./Location/City_Name_E",
                ),
                "city_name_fr": text(
                    parcel,
                    "./Location/City_Name_F",
                ),

                "federal_electoral_district_en": text(
                    parcel,
                    "./Location/Federal_Electoral_District_E",
                ),
                "federal_electoral_district_fr": text(
                    parcel,
                    "./Location/Federal_Electoral_District_F",
                ),

                "country_code": child_code(
                    parcel,
                    "./Location/Country_Name_E",
                ),
                "country_name_en": text(
                    parcel,
                    "./Location/Country_Name_E",
                ),
                "country_name_fr": text(
                    parcel,
                    "./Location/Country_Name_F",
                ),

                "latitude": latitude,
                "longitude": longitude,
                "coordinate_valid": coordinate_valid,

                "positional_accuracy": as_float(
                    text(
                        parcel,
                        "./Location/Positional_Accuracy",
                    )
                ),
                "positional_accuracy_unit": attr(
                    parcel.find(
                        "./Location/Positional_Accuracy"
                    ),
                    "unitofMeasure",
                ),

                "geometry": geometry,
            }

            parcel_rows.append(parcel_data)

            for structure in parcel.findall("./Structures/Structure"):
                structure_custodian = structure.find("Custodian")

                use_types = structure.findall("./UseTypes/UseType")
                use_type_codes = [
                    attr(use_type, "code")
                    for use_type in use_types
                    if attr(use_type, "code")
                ]
                use_names_en = [
                    text(use_type, "Use_Name_E")
                    for use_type in use_types
                    if text(use_type, "Use_Name_E")
                ]
                use_names_fr = [
                    text(use_type, "Use_Name_F")
                    for use_type in use_types
                    if text(use_type, "Use_Name_F")
                ]

                structure_number = text(
                    structure,
                    "Structure_Number",
                )

                structure_rows.append(
                    {
                        "source_file": source_file,
                        "property_number": property_number,
                        "parcel_number": parcel_number,
                        "parcel_id": parcel_id,
                        "structure_number": structure_number,
                        "structure_id": (
                            f"{property_number}:"
                            f"{parcel_number or ''}:"
                            f"{structure_number or ''}"
                        ),

                        "occupancy": attr(structure, "occupancy"),
                        "structure_created_date": attr(
                            structure,
                            "createdDate",
                        ),
                        "structure_last_modified_date": attr(
                            structure,
                            "lastModifiedDate",
                        ),

                        "structure_name_en": text(
                            structure,
                            "Structure_Name_E",
                        ),
                        "structure_name_fr": text(
                            structure,
                            "Structure_Name_F",
                        ),
                        "structure_address_en": text(
                            structure,
                            "Address_E",
                        ),
                        "structure_address_fr": text(
                            structure,
                            "Address_F",
                        ),

                        "construction_year": as_int(
                            text(structure, "Construction_Year")
                        ),

                        "condition_code": child_code(
                            structure,
                            "Condition_E",
                        ),
                        "condition_en": text(
                            structure,
                            "Condition_E",
                        ),
                        "condition_fr": text(
                            structure,
                            "Condition_F",
                        ),

                        "interest_type_code": child_code(
                            structure,
                            "Interest_Type_E",
                        ),
                        "interest_type_en": text(
                            structure,
                            "Interest_Type_E",
                        ),
                        "interest_type_fr": text(
                            structure,
                            "Interest_Type_F",
                        ),

                        "floor_area": as_float(
                            text(structure, "Floor_Area")
                        ),
                        "floor_area_unit": attr(
                            structure.find("Floor_Area"),
                            "unitofMeasure",
                        ),

                        "use_type_codes": " | ".join(
                            use_type_codes
                        ) or None,
                        "use_names_en": " | ".join(
                            use_names_en
                        ) or None,
                        "use_names_fr": " | ".join(
                            use_names_fr
                        ) or None,

                        "custodian_code": attr(
                            structure_custodian,
                            "code",
                        ),
                        "custodian_name_en": text(
                            structure,
                            "./Custodian/Name_E",
                        ),
                        "custodian_name_fr": text(
                            structure,
                            "./Custodian/Name_F",
                        ),
                        "custodian_is_department": as_bool(
                            attr(
                                structure_custodian,
                                "isDepartment",
                            )
                        ),
                        "custodian_is_crown_corporation": as_bool(
                            attr(
                                structure_custodian,
                                "isCrownCorporation",
                            )
                        ),

                        "country_code": parcel_data["country_code"],
                        "country_name_en": parcel_data[
                            "country_name_en"
                        ],
                        "country_name_fr": parcel_data[
                            "country_name_fr"
                        ],
                        "city_name_en": parcel_data["city_name_en"],
                        "city_name_fr": parcel_data["city_name_fr"],
                        "latitude": latitude,
                        "longitude": longitude,
                        "coordinate_valid": coordinate_valid,

                        # Structures inherit parcel point geometry because
                        # no separate structure coordinates appear in XML.
                        "geometry": geometry,
                    }
                )

    return parcel_rows, structure_rows


def read_layers(zip_path: Path) -> dict[str, gpd.GeoDataFrame]:
    """Read XML members directly from the archive."""
    parcels, structures = [], []
    with ZipFile(zip_path) as archive:
        members = sorted(
            (member for member in archive.infolist() if member.filename.lower().endswith(".xml")),
            key=lambda member: member.filename,
        )
        for member in members:
            print(f"Reading {member.filename}")
            with archive.open(member) as source:
                parcel_rows, structure_rows = parse_xml_file(source, Path(member.filename).name)
            parcels.extend(parcel_rows)
            structures.extend(structure_rows)
    if not parcels:
        raise ValueError("The DFRP ZIP contains no Property records in its XML files")

    layers = {
        "parcels": gpd.GeoDataFrame(parcels, geometry="geometry", crs="EPSG:4326"),
        "structures": gpd.GeoDataFrame(
            structures or {
                "property_number": pd.Series(dtype="string"),
                "parcel_id": pd.Series(dtype="string"),
                "structure_id": pd.Series(dtype="string"),
                "geometry": [],
            },
            geometry="geometry",
            crs="EPSG:4326",
        ),
    }
    for name, layer in layers.items():
        # Keep integer fields nullable and retain native booleans across formats.
        for field in ("source_property_index", "building_count", "parking_exterior",
                      "parking_interior", "construction_year"):
            if field in layer:
                layer[field] = layer[field].astype("Int64")
        for field in ("coordinate_valid", "custodian_is_department",
                      "custodian_is_crown_corporation", "in_rural_area",
                      "parking_interior_included_in_floor_area"):
            if field in layer:
                layer[field] = layer[field].astype("boolean")
        print(f"{name}: {len(layer):,} rows, {layer.geometry.notna().sum():,} with coordinates")
    return layers


def get_image_links(parcels: gpd.GeoDataFrame) -> list[tuple[str, str, int, str]]:
    """Link images to the property that supplied them, even for multi-parcel properties."""
    links = set()
    for parcel in parcels[["property_number", "photo_urls", "minimap_url"]].itertuples(index=False):
        if not isinstance(parcel.property_number, str):
            continue
        photos = parcel.photo_urls.split(" | ") if isinstance(parcel.photo_urls, str) else []
        candidates = (
            ("photo", photos),
            ("minimap", [parcel.minimap_url]),
        )
        for image_type, values in candidates:
            for position, value in enumerate(values, 1):
                if isinstance(value, str) and value:
                    url = urljoin(SOURCE_URL, value)
                    parsed = urlsplit(url)
                    if parsed.scheme in {"http", "https"} and parsed.hostname:
                        links.add((parcel.property_number, image_type, position, url))
    return sorted(links)


def download_image(url: str) -> tuple[str, bytes] | None:
    try:
        with requests.get(url, headers=IMAGE_HEADERS, stream=True, timeout=(10, 30)) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if not content_type.startswith("image/"):
                raise ValueError(f"unexpected content type {content_type!r}")
            content = response.content
            if not content:
                raise ValueError("empty response")
            return content_type, content
    except (requests.RequestException, ValueError) as error:
        print(f"Image unavailable: {url} ({error})")
        return None


def refresh_image_cache(cache_path: Path, urls: set[str]) -> bool:
    """Fetch only URLs for which there is no successful cached response."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(cache_path) as cache:
        cache.execute("""
            CREATE TABLE IF NOT EXISTS images (
                url TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                content BLOB NOT NULL
            )
        """)
        cached_urls = {url for (url,) in cache.execute("SELECT url FROM images")}
        missing_urls = sorted(urls - cached_urls)
        saved = 0
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Bound the number of downloaded images waiting to be written to disk.
            for offset in range(0, len(missing_urls), 32):
                batch = missing_urls[offset:offset + 32]
                for url, image in zip(batch, executor.map(download_image, batch)):
                    if image is not None:
                        cache.execute("INSERT INTO images VALUES (?, ?, ?)", (url, *image))
                        saved += 1
                cache.commit()
    print(
        f"Images: {len(urls & cached_urls):,} reused, {saved:,} downloaded, "
        f"{len(missing_urls) - saved:,} unavailable"
    )
    return bool(saved)


def add_images(package: Path, cache_path: Path, links: list[tuple[str, str, int, str]]) -> None:
    """Embed each image once and register both related attribute tables in the GeoPackage."""
    with sqlite3.connect(package) as gpkg:
        gpkg.execute("ATTACH DATABASE ? AS cache", (str(cache_path.resolve()),))
        gpkg.execute("PRAGMA foreign_keys = ON")
        gpkg.executescript("""
            CREATE TABLE dfrp_images (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                media_type TEXT NOT NULL,
                content BLOB NOT NULL
            );
            CREATE TABLE dfrp_property_images (
                id INTEGER PRIMARY KEY,
                property_number TEXT NOT NULL,
                image_type TEXT NOT NULL,
                position INTEGER NOT NULL,
                image_id INTEGER NOT NULL REFERENCES dfrp_images(id)
            );
            CREATE INDEX dfrp_property_images_property ON dfrp_property_images(property_number);
            CREATE TEMP TABLE current_links (
                property_number TEXT, image_type TEXT, position INTEGER, url TEXT
            );
        """)
        gpkg.executemany("INSERT INTO current_links VALUES (?, ?, ?, ?)", links)
        gpkg.execute("""
            INSERT INTO dfrp_images (url, media_type, content)
            SELECT url, media_type, content FROM cache.images
            WHERE url IN (SELECT url FROM current_links)
            ORDER BY url
        """)
        gpkg.execute("""
            INSERT INTO dfrp_property_images (property_number, image_type, position, image_id)
            SELECT link.property_number, link.image_type, link.position, image.id
            FROM current_links AS link JOIN dfrp_images AS image ON image.url = link.url
            ORDER BY link.property_number, link.image_type, link.position, image.id
        """)
        gpkg.executemany("""
            INSERT INTO gpkg_contents (table_name, data_type, identifier, description)
            VALUES (?, 'attributes', ?, ?)
        """, [
            ("dfrp_images", "DFRP images", "Embedded images with original URL and media type."),
            ("dfrp_property_images", "DFRP property image links",
             "Join to parcels on property_number and to dfrp_images on image_id."),
        ])


def build(zip_path: Path, output_dir: Path, cache_path: Path) -> bool:
    layers = read_layers(zip_path)
    links = get_image_links(layers["parcels"])
    cache_updated = refresh_image_cache(cache_path, {link[3] for link in links})

    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output_dir) as stage:
        staging = Path(stage)
        plain = staging / "dfrp.gpkg"
        for name, layer in layers.items():
            layer.to_file(
                staging / f"dfrp_{name}.geojson", driver="GeoJSON", engine="pyogrio",
                index=False, RFC7946="YES",
            )
            layer.to_parquet(staging / f"dfrp_{name}.parquet", index=False)
            layer.to_file(
                plain, layer=name, driver="GPKG", engine="pyogrio", index=False,
                geometry_type="Point",
            )
        with_images = staging / "dfrp_with_images.gpkg"
        shutil.copyfile(plain, with_images)
        add_images(with_images, cache_path, links)

        for product in staging.iterdir():
            product.replace(output_dir / product.name)
    return cache_updated


def main() -> None:
    project = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-zip", type=Path, help="Use a local archive instead of downloading it.")
    parser.add_argument("--output-dir", type=Path, default=project / "geodata" / "dfrp")
    parser.add_argument("--image-cache", type=Path, default=project / ".dfrp-cache" / "images.sqlite3")
    args = parser.parse_args()

    with TemporaryDirectory() as work:
        zip_path = args.input_zip
        if zip_path is None:
            zip_path = Path(work) / "dfrp-rbif.zip"
            with requests.get(
                SOURCE_URL, headers=ZIP_HEADERS, stream=True, timeout=(15, 180),
            ) as response:
                response.raise_for_status()
                with zip_path.open("wb") as destination:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        destination.write(chunk)
                if not is_zipfile(zip_path):
                    with zip_path.open("rb") as downloaded:
                        prefix = downloaded.read(160)
                    raise ValueError(
                        f"DFRP source returned HTTP {response.status_code} from {response.url} "
                        f"with {response.headers.get('Content-Type', 'unknown content type')} "
                        f"and {zip_path.stat().st_size:,} bytes. Expected ZIP, received {prefix!r}."
                    )
        cache_updated = build(zip_path, args.output_dir, args.image_cache)
    if github_output := os.environ.get("GITHUB_OUTPUT"):
        with Path(github_output).open("a", encoding="utf-8") as output:
            print(f"cache_updated={str(cache_updated).lower()}", file=output)


if __name__ == "__main__":
    main()
