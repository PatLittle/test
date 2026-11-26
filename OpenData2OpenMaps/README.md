# Many Datasets would be Nice to View as as Map 

 - [Government offices that provide consular services to Canadian citizens abroad](https://open.canada.ca/data/en/dataset/6661f0f8-2fb2-46fa-9394-c033d581d531)
 - [Country Travel Advice and Advisories](https://open.canada.ca/data/en/dataset/bef2ebb3-ca9a-485f-aaff-5dc36eb89426)
 - [Federal Contaminated Sites Inventory](https://open.canada.ca/data/en/dataset/1d42f7b9-1549-40aa-8ac6-0e0302ff2902)
 - [Directory of Federal Real Property](https://map-carte.tbs-sct.gc.ca/dfrp-rbif/oddo/dfrp-rbif.zip)

## Directory of Federal Real Property downloads

Open the **dfrp-geodata** artifact for a completed run of [Update OpenData2OpenMaps GeoJSON](https://github.com/PatLittle/test/actions/workflows/update-opendata2openmaps.yml). It contains:

| Files | Contents |
| --- | --- |
| `dfrp_parcels.geojson`, `dfrp_structures.geojson` | Parcels and structures as GeoJSON |
| `dfrp_parcels.parquet`, `dfrp_structures.parquet` | The same layers as GeoParquet |
| `dfrp.gpkg` | Both layers, with source image URLs |
| `dfrp_with_images.gpkg` | Both layers plus embedded photos and minimaps |

The layers use WGS 84 parcel points, and structures inherit their parcel's coordinates. Rows without valid coordinates have null geometry. Join structures to parcels using `parcel_id`.

In the image-enabled GeoPackage, join `parcels.property_number` to `dfrp_property_images.property_number`, and `dfrp_property_images.image_id` to `dfrp_images.id`. The image table stores the original URL, media type, and binary content once per URL. The workflow reuses successful downloads across runs from its Actions cache and retries unavailable images. Only images referenced by the current source appear in the GeoPackage.

To run locally:

```sh
python -m pip install geopandas pyogrio pyarrow lxml requests
python OpenData2OpenMaps/scripts/dfrp_to_geodata.py
```

The script writes the six products to `OpenData2OpenMaps/geodata/dfrp/`. Keep its SQLite cache at `OpenData2OpenMaps/.dfrp-cache/images.sqlite3` between runs to reuse images. Use `--input-zip` to process a local copy of the source archive.
