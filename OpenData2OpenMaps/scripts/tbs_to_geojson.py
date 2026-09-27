#!/usr/bin/env python3
"""Convert the Federal Contaminated Sites Inventory XML export to GeoJSON."""

from __future__ import annotations

import io
import json
import urllib3
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

ZIP_URL = "https://map-carte.tbs-sct.gc.ca/fcsi-rscf/oddo/fcsi-rscf.zip"
OUTPUT = Path(__file__).resolve().parents[1] / "geodata" / "tbs-fcsi_sites.geojson"


def clean_text(element, path):
    node = element.find(path)
    return node.text.strip() if node is not None and node.text else None


def bilingual(element, path):
    node = element.find(path)
    if node is None:
        return None, None
    return clean_text(node, "EN"), clean_text(node, "FR")


def as_number(value):
    try:
        return float(value) if value not in (None, "") else None
    except ValueError:
        return None


def download_xml():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    response = requests.get(ZIP_URL, timeout=120, verify=False)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        xml_names = [name for name in archive.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise FileNotFoundError("No XML file found inside the FCSI ZIP.")
        return archive.read(xml_names[0])


def build_geojson(xml_bytes):
    root = ET.fromstring(xml_bytes)

    orgs = {}
    for org in root.findall(".//ReportingOrganizations/ReportingOrganization"):
        code = clean_text(org, "Code")
        en, fr = bilingual(org, "Name")
        if code:
            orgs[code] = {"en": en, "fr": fr}

    features = []
    skipped = 0

    for site in root.findall(".//Sites/Site"):
        site_id = site.get("SiteIdentifier")
        org_code = site.get("ReportingOrganization")

        lat = as_number(clean_text(site, "Location/Latitude"))
        lon = as_number(clean_text(site, "Location/Longitude"))

        if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            skipped += 1
            continue

        name_en, name_fr = bilingual(site, "Name")
        status_en, status_fr = bilingual(site, "SiteStatus/Status")
        status_desc_en, status_desc_fr = bilingual(site, "SiteStatus/Description")
        class_en, class_fr = bilingual(site, "Classification/Name")
        country_en, country_fr = bilingual(site, "Location/Country")
        fed_en, fed_fr = bilingual(site, "Location/FederalElectoralDistrict")
        reason_en, reason_fr = bilingual(site, "ReasonForFederalInvolvement")
        action_en, action_fr = bilingual(site, "ActionPlan")
        info_en, info_fr = bilingual(site, "AdditionalInformation")

        management_codes, management_en, management_fr = [], [], []
        for item in site.findall(".//ManagementStrategy/ManagementType"):
            if item.get("code"):
                management_codes.append(item.get("code"))
            if clean_text(item, "EN"):
                management_en.append(clean_text(item, "EN"))
            if clean_text(item, "FR"):
                management_fr.append(clean_text(item, "FR"))

        contaminant_codes, contaminants_en, contaminants_fr = [], [], []
        medium_codes, media_en, media_fr = [], [], []

        for item in site.findall(".//ContaminationDetails/ContaminatedMedia"):
            contaminant = item.find("Contamination")
            medium = item.find("Medium")

            if contaminant is not None:
                if contaminant.get("code"):
                    contaminant_codes.append(contaminant.get("code"))
                if clean_text(contaminant, "EN"):
                    contaminants_en.append(clean_text(contaminant, "EN"))
                if clean_text(contaminant, "FR"):
                    contaminants_fr.append(clean_text(contaminant, "FR"))

            if medium is not None:
                if medium.get("code"):
                    medium_codes.append(medium.get("code"))
                if clean_text(medium, "EN"):
                    media_en.append(clean_text(medium, "EN"))
                if clean_text(medium, "FR"):
                    media_fr.append(clean_text(medium, "FR"))

        location = site.find("Location")

        properties = {
            "site_identifier": site_id,
            "reporting_organization_code": org_code,
            "reporting_organization_en": orgs.get(org_code, {}).get("en"),
            "reporting_organization_fr": orgs.get(org_code, {}).get("fr"),
            "created": site.get("Created"),
            "last_modified": site.get("LastModified"),
            "name_en": name_en,
            "name_fr": name_fr,
            "status_en": status_en,
            "status_fr": status_fr,
            "status_description_en": status_desc_en,
            "status_description_fr": status_desc_fr,
            "classification_code": clean_text(site, "Classification/Code"),
            "classification_en": class_en,
            "classification_fr": class_fr,
            "property_number": clean_text(site, "PropertyNumber"),
            "reason_for_federal_involvement_en": reason_en,
            "reason_for_federal_involvement_fr": reason_fr,
            "municipality": clean_text(site, "Location/Municipality"),
            "province": clean_text(site, "Location/Province"),
            "country_en": country_en,
            "country_fr": country_fr,
            "sgc": location.get("sgc") if location is not None else None,
            "fed": location.get("fed") if location is not None else None,
            "federal_electoral_district_en": fed_en,
            "federal_electoral_district_fr": fed_fr,
            "minimap_url": clean_text(site, "Location/MiniMapURL"),
            "management_type_codes": sorted(set(management_codes)),
            "management_types_en": sorted(set(management_en)),
            "management_types_fr": sorted(set(management_fr)),
            "contaminant_codes": sorted(set(contaminant_codes)),
            "contaminants_en": sorted(set(contaminants_en)),
            "contaminants_fr": sorted(set(contaminants_fr)),
            "medium_codes": sorted(set(medium_codes)),
            "contaminated_media_en": sorted(set(media_en)),
            "contaminated_media_fr": sorted(set(media_fr)),
            "estimated_cubic_metres": as_number(clean_text(site, "ContaminationDetails/ContaminationEstimates/CubicMetres")),
            "estimated_hectares": as_number(clean_text(site, "ContaminationDetails/ContaminationEstimates/Hectares")),
            "estimated_tons": as_number(clean_text(site, "ContaminationDetails/ContaminationEstimates/Tons")),
            "population_1km": as_number(clean_text(site, "PopulationCounts/KM1")),
            "population_5km": as_number(clean_text(site, "PopulationCounts/KM5")),
            "population_10km": as_number(clean_text(site, "PopulationCounts/KM10")),
            "population_25km": as_number(clean_text(site, "PopulationCounts/KM25")),
            "population_50km": as_number(clean_text(site, "PopulationCounts/KM50")),
            "action_plan_en": action_en,
            "action_plan_fr": action_fr,
            "additional_information_en": info_en,
            "additional_information_fr": info_fr,
            "marker-color": {
                "Suspected": "#f5a623",
                "Active": "#d71920",
                "Closed": "#4a90e2",
            }.get(status_en, "#777777"),
            "marker-size": "small",
            "marker-symbol": "circle",
            "source": ZIP_URL,
        }

        features.append({
            "type": "Feature",
            "id": site_id,
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
            "properties": properties,
        })

    print(f"Mappable sites: {len(features):,}")
    print(f"Skipped sites without valid coordinates: {skipped:,}")

    return {
        "type": "FeatureCollection",
        "name": "Federal Contaminated Sites Inventory",
        "source": ZIP_URL,
        "features": features,
    }


def main():
    xml_bytes = download_xml()
    geojson = build_geojson(xml_bytes)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(geojson, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Saved: {OUTPUT}")


if __name__ == "__main__":
    main()
