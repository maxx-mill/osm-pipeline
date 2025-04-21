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
