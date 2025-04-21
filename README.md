# OSM GIS Data Pipeline

A Python pipeline to download, clean, and load OpenStreetMap (OSM) data into PostGIS/PostgreSQL.

## Features
- **Download OSM data** for any location (e.g., Lagos, Nigeria) using `osmnx`.
- **Clean and transform** geometries (e.g., buildings, roads).
- **Load into PostGIS** for spatial analysis.

## Workflow
1. `download_osm.py`: Fetch OSM data (tags: buildings, roads, amenities).  
2. `clean_osm.py`: Standardize geometries.  
3. `pipeline.py`: Orchestrate the process.  

## Setup
```bash
pip install osmnx geopandas psycopg2# osm-pipeline
```

## Config File Generator (create a starter config file)
```bash
echo '{
  "osm": {
    "location_name": "Custom Location",
    "tags": {"building": true, "highway": true}
  },
  "cleaning": {
    "target_crs": "EPSG:4326"
  },
  "postgis": {
    "user": "your_user",
    "password": "your_pass"
  }
}' > my_config.json
```
## Advanced config file settings
```bash
{
  "osm": {
    "location_name": "Ikeja, Nigeria",
    "destination_folder": "data/raw",
    "timeout": 300,
    "tags": {
      "building": true,
      "highway": true,
      "amenity": true,
      "landuse": true,
      "natural": true
    }
  },
  "cleaning": {
    "target_crs": "EPSG:4326",
    "standardize_columns": true
  },
  "postgis": {
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432,
    "database": "gis_db",
    "schema": "public",
    "table_name": "osm_data",
    "if_exists": "replace",
    "extensions": ["postgis"],
    "chunk_size": 5000,
    "geometry_type": "GEOMETRY",
    "create_index": true
  }
}
```



## All Available Arguments

| Argument         | Description                              | Example                     |
|------------------|------------------------------------------|-----------------------------|
| `--config`       | Path to config file                      | `--config config/prod.json` |
| `--location`     | Override OSM location                    | `--location "Yaba, Lagos"`  |
| `--table`        | Override PostGIS table name              | `--table lagos_roads`       |
| `--skip-download`| Skip OSM download step                   | `--skip-download`           |
| `--skip-clean`   | Skip data cleaning step                  | `--skip-clean`              |
| `--skip-db`      | Skip database operations                 | `--skip-db`                 |
| `--verbose`      | Show detailed output                     | `--verbose`                 |
| `--debug`        | Show debug traces (implies verbose)      | `--debug`                   |

## Example Usage

### Full Pipeline with All Options
```bash
python pipeline.py \
    --config custom_settings.json \
    --location "Ikeja, Nigeria" \
    --table lagos_buildings \
    --skip-download \
    --skip-clean \
    --skip-db \
    --verbose \
    --debug
```

### Download Only
```bash
python pipeline.py \
    --location "Victoria Island, Lagos" \
    --skip-clean \
    --skip-db \
    --verbose
```

### Clean existing data only
```bash
python pipeline.py \
    --skip-download \
    --skip-db \
    --config settings.json
```

### Database load only
```bash
python pipeline.py \
    --skip-download \
    --skip-clean \
    --table existing_data \
    --verbose
```
