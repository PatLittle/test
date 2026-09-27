#!/usr/bin/env python3
"""Generate map-ready GeoJSON from Global Affairs Canada open data.

Outputs:
- OpenData2OpenMaps/geodata/gac_offices_service_map.geojson
- OpenData2OpenMaps/geodata/gac_travel_advisories.geojson
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from typing import Any

import requests
from shapely.geometry import Point, shape

OFFICES_URL = "https://data.international.gc.ca/travel-voyage/opendata-offices-list-alpha-eng.json"
ADVISORIES_URL = "https://data.international.gc.ca/travel-voyage/index-alpha-eng.json"

NE_BASE = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson"
COUNTRIES_URL = f"{NE_BASE}/ne_50m_admin_0_countries.geojson"
TINY_COUNTRIES_URL = f"{NE_BASE}/ne_50m_admin_0_tiny_countries.geojson"
MAP_UNITS_URL = f"{NE_BASE}/ne_50m_admin_0_map_units.geojson"

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "geodata"
OFFICES_OUT = OUTPUT_DIR / "gac_offices_service_map.geojson"
ADVISORIES_OUT = OUTPUT_DIR / "gac_travel_advisories.geojson"

RISK = {
    0: {"label_en": "Take normal security precautions", "label_fr": "Prendre des mesures de sécurité normales", "color": "#2e8540"},
    1: {"label_en": "Exercise a high degree of caution", "label_fr": "Faire preuve d'une grande prudence", "color": "#f5d328"},
    2: {"label_en": "Avoid non-essential travel", "label_fr": "Éviter tout voyage non essentiel", "color": "#f58220"},
    3: {"label_en": "Avoid all travel", "label_fr": "Éviter tout voyage", "color": "#d71920"},
}


def get_json(url: str) -> dict[str, Any]:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.json()


def clean(value: Any) -> str | None:
    if not value:
        return None
    value = re.sub(r"(?i)<br\\s*/?>", " ", str(value))
    value = re.sub(r"<[^>]+>", "", value)
    return " ".join(html.unescape(value).split()) or None


def flag(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def canonical_office_key(office: dict[str, Any], lat: float, lon: float) -> str:
    en = office.get("eng", {})
    url = clean(en.get("internet"))
    if url:
        url = re.sub(r"([?&])lang=(eng|fra)(&|$)", r"\\1", url, flags=re.I)
        return url.rstrip("?&").lower()
    return "|".join([
        f"{lat:.5f}",
        f"{lon:.5f}",
        clean(en.get("city")) or "",
        clean(en.get("type")) or "",
    ])


def build_boundaries():
    boundaries = []
    lookup = {}

    for fc in [get_json(COUNTRIES_URL), get_json(TINY_COUNTRIES_URL), get_json(MAP_UNITS_URL)]:
        for feature in fc["features"]:
            p = feature["properties"]
            geom = shape(feature["geometry"])
            record = {
                "feature": feature,
                "geom": geom,
                "name": p.get("NAME_EN") or p.get("NAME") or p.get("GEOUNIT") or p.get("ADMIN"),
            }
            boundaries.append(record)

            for field in ("ISO_A2", "ISO_A2_EH", "ISO_A3", "ISO_A3_EH", "ADM0_A3", "SOV_A3", "WB_A2", "WB_A3"):
                value = p.get(field)
                if value and value != "-99":
                    lookup[str(value).upper()] = record

            for field in ("NAME_EN", "NAME", "ADMIN", "SOVEREIGNT", "NAME_LONG", "GEOUNIT"):
                value = p.get(field)
                if value:
                    lookup[str(value).casefold()] = record

    return boundaries, lookup


def find_boundary(lookup, iso=None, name=None):
    if iso and str(iso).upper() in lookup:
        return lookup[str(iso).upper()]
    if name:
        return lookup.get(str(name).casefold())
    return None


def host_boundary(boundaries, lon, lat):
    point = Point(lon, lat)
    polygons = [b for b in boundaries if b["geom"].geom_type in {"Polygon", "MultiPolygon"}]

    for b in polygons:
        if b["geom"].covers(point):
            return b

    return min(polygons, key=lambda b: b["geom"].distance(point)) if polygons else None


def build_offices_geojson(boundaries, lookup):
    source = get_json(OFFICES_URL)["data"]
    offices = {}

    for item in source.values():
        country = item.get("country", {})
        served = {
            "iso": clean(country.get("country-iso")),
            "en": clean(country.get("eng", {}).get("name")),
            "fr": clean(country.get("fra", {}).get("name")),
        }

        for office in item.get("offices", []):
            try:
                lat, lon = float(office["lat"]), float(office["lng"])
            except (KeyError, TypeError, ValueError):
                continue

            if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
                continue

            en, fr = office.get("eng", {}), office.get("fra", {})
            key = canonical_office_key(office, lat, lon)

            if key not in offices:
                host = host_boundary(boundaries, lon, lat)
                offices[key] = {
                    "lon": lon,
                    "lat": lat,
                    "host": host,
                    "served": {},
                    "properties": {
                        "feature_type": "office",
                        "city_en": clean(en.get("city")),
                        "city_fr": clean(fr.get("city")),
                        "office_type_en": clean(en.get("type")),
                        "office_type_fr": clean(fr.get("type")),
                        "address_en": clean(en.get("address")),
                        "address_fr": clean(fr.get("address")),
                        "telephone_en": clean(en.get("tel-legacy")),
                        "telephone_fr": clean(fr.get("tel-legacy")),
                        "email_en": clean(en.get("email-1")),
                        "email_fr": clean(fr.get("email-1")),
                        "website_en": clean(en.get("internet")),
                        "website_fr": clean(fr.get("internet")),
                        "passport_services": flag(office.get("has-passport-services")),
                        "honorary_consul": flag(office.get("honorary-consul")),
                        "partner_office": flag(office.get("is-partner")),
                        "host_country": host["name"] if host else None,
                        "source": OFFICES_URL,
                    },
                }

            skey = served["iso"] or served["en"]
            if skey:
                offices[key]["served"][skey] = served

    features = []
    foreign = {}

    for office_id, office in offices.items():
        served = list(office["served"].values())
        p = office["properties"]

        p["serves_count"] = len(served)
        p["serves_en"] = [x["en"] for x in served if x["en"]]
        p["serves_fr"] = [x["fr"] for x in served if x["fr"]]
        p["serves_iso"] = [x["iso"] for x in served if x["iso"]]

        if len(served) > 1:
            p["marker-size"] = "large"
            p["marker-symbol"] = str(len(served))
            p["marker-label"] = str(len(served))
        else:
            p["marker-size"] = "medium"
            p["marker-symbol"] = "embassy"

        p["marker-color"] = (
            "#f59e0b" if p["honorary_consul"]
            else "#7c3aed" if p["partner_office"]
            else "#d71920" if p["passport_services"]
            else "#2563eb"
        )

        features.append({
            "type": "Feature",
            "id": f"office:{office_id}",
            "geometry": {"type": "Point", "coordinates": [office["lon"], office["lat"]]},
            "properties": p,
        })

        for s in served:
            target = find_boundary(lookup, s["iso"], s["en"])
            if not target:
                continue
            if office["host"] and target["feature"] is office["host"]["feature"]:
                continue
            if target["geom"].geom_type not in {"Polygon", "MultiPolygon"}:
                continue

            tp = target["feature"]["properties"]
            cid = str(tp.get("ISO_A3_EH") or tp.get("ADM0_A3") or tp.get("ISO_A3") or target["name"])
            centre = target["geom"].representative_point()

            foreign.setdefault(cid, {"boundary": target, "centre": centre, "served_by": set()})["served_by"].add(office_id)

            features.append({
                "type": "Feature",
                "id": f"service-line:{office_id}:{cid}",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[office["lon"], office["lat"]], [centre.x, centre.y]],
                },
                "properties": {
                    "feature_type": "service_line",
                    "office_city_en": p["city_en"],
                    "supported_country_en": s["en"],
                    "supported_country_fr": s["fr"],
                    "stroke": "#f59e0b",
                    "stroke-width": 2,
                    "stroke-opacity": 0.8,
                },
            })

    for cid, item in foreign.items():
        b, centre = item["boundary"], item["centre"]

        features.append({
            "type": "Feature",
            "id": f"supported-country:{cid}",
            "geometry": b["feature"]["geometry"],
            "properties": {
                "feature_type": "supported_country",
                "country_en": b["name"],
                "country_iso3": cid,
                "served_by_count": len(item["served_by"]),
                "fill": "#bdbdbd",
                "fill-opacity": 0.35,
                "stroke": "#808080",
                "stroke-width": 1,
                "stroke-opacity": 0.8,
            },
        })

        features.append({
            "type": "Feature",
            "id": f"supported-country-centre:{cid}",
            "geometry": {"type": "Point", "coordinates": [centre.x, centre.y]},
            "properties": {
                "feature_type": "supported_country_centre",
                "country_en": b["name"],
                "country_iso3": cid,
                "marker-color": "#f59e0b",
                "marker-size": "small",
                "marker-symbol": "circle",
            },
        })

    print(f"Offices: {len(offices):,}")
    print(f"Cross-border supported countries/territories: {len(foreign):,}")
    print(f"Office GeoJSON features: {len(features):,}")

    return {
        "type": "FeatureCollection",
        "name": "Global Affairs Canada offices and cross-border service areas",
        "features": features,
    }


def build_advisories_geojson(lookup):
    source = get_json(ADVISORIES_URL)["data"]
    features = []
    unmatched = []
    non_polygon = []

    for iso, a in source.items():
        try:
            state = int(a.get("advisory-state", 0))
        except (TypeError, ValueError):
            state = 0
        if state not in RISK:
            state = 0

        country_en = clean(a.get("country-eng"))
        country_fr = clean(a.get("country-fra"))
        boundary = find_boundary(lookup, a.get("country-iso") or iso, country_en)

        if not boundary:
            unmatched.append(country_en or iso)
            continue
        if boundary["geom"].geom_type not in {"Polygon", "MultiPolygon"}:
            non_polygon.append(country_en or iso)
            continue

        risk = RISK[state]
        eng, fra = a.get("eng", {}), a.get("fra", {})

        features.append({
            "type": "Feature",
            "id": f"travel-advisory:{iso}",
            "geometry": boundary["feature"]["geometry"],
            "properties": {
                "feature_type": "travel_advisory",
                "country_iso": a.get("country-iso") or iso,
                "country_en": country_en,
                "country_fr": country_fr,
                "advisory_state": state,
                "advisory_level_en": risk["label_en"],
                "advisory_level_fr": risk["label_fr"],
                "advisory_text_en": clean(eng.get("advisory-text")),
                "advisory_text_fr": clean(fra.get("advisory-text")),
                "has_advisory_warning": flag(a.get("has-advisory-warning")),
                "has_regional_advisory": flag(a.get("has-regional-advisory")),
                "date_published": (a.get("date-published") or {}).get("date"),
                "url_en": f"https://travel.gc.ca/destinations/{eng.get('url-slug')}" if eng.get("url-slug") else None,
                "url_fr": f"https://voyage.gc.ca/destinations/{fra.get('url-slug')}" if fra.get("url-slug") else None,
                "fill": risk["color"],
                "fill-opacity": 0.55,
                "stroke": "#ffffff",
                "stroke-width": 0.8,
                "stroke-opacity": 0.9,
                "source": ADVISORIES_URL,
            },
        })

    print(f"Travel advisory polygons: {len(features):,}")
    if unmatched:
        print(f"Unmatched advisory boundaries ({len(unmatched)}): " + ", ".join(sorted(unmatched)))
    if non_polygon:
        print(f"Non-polygon advisory boundaries ({len(non_polygon)}): " + ", ".join(sorted(non_polygon)))

    return {
        "type": "FeatureCollection",
        "name": "Government of Canada Travel Advice and Advisories",
        "legend": {
            str(k): {"label_en": v["label_en"], "label_fr": v["label_fr"], "color": v["color"]}
            for k, v in RISK.items()
        },
        "features": features,
    }


def write_geojson(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Saved: {path}")


def main() -> None:
    boundaries, lookup = build_boundaries()
    print(f"Boundary features loaded: {len(boundaries):,}")

    write_geojson(OFFICES_OUT, build_offices_geojson(boundaries, lookup))
    write_geojson(ADVISORIES_OUT, build_advisories_geojson(lookup))


if __name__ == "__main__":
    main()
