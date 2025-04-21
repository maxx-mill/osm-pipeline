#!/usr/bin/env python3
"""
OSM Data Processing Pipeline

A complete workflow to:
1. Download OpenStreetMap data for a specified location
2. Clean and transform the geospatial data
3. Load it into a PostGIS database

Usage:
    python pipeline.py --config settings.json --location "Ikeja, Nigeria" --table osm_features
"""

import argparse
import json
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Dict, Any, Optional

from download_osm import OSMDownloader
from geo_cleaner import GeoCleaner
from postgres_manager import PostgreSQLDatabaseManager
from postgis_loader import PostGISLoader

# Constants
DEFAULT_CONFIG_PATH = "settings.json"

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="OSM Data Processing Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to configuration JSON file"
    )
    parser.add_argument(
        "--location",
        help="Override OSM location name from config"
    )
    parser.add_argument(
        "--table",
        help="Override PostGIS table name from config"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip the OSM download step"
    )
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help="Skip the data cleaning step"
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip database operations"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output (implies verbose)"
    )
    
    return parser.parse_args()

def setup_logging(verbose: bool = False, debug: bool = False) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log')
        ]
    )

def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load and validate configuration file."""
    try:
        with open(config_path) as f:
            config = json.load(f)
        
        # Validate required fields
        required_sections = ["osm", "cleaning", "postgis"]
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")
        
        # Set defaults for new options
        config["postgis"].setdefault("extensions", ["postgis"])
        config["postgis"].setdefault("create_index", True)
        config["postgis"].setdefault("chunk_size", None)
        config["postgis"].setdefault("geometry_type", "GEOMETRY")
        
        return config
    
    except FileNotFoundError:
        logging.error(f"Config file not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in config file {config_path}")
        sys.exit(1)
    except ValueError as e:
        logging.error(f"Configuration error: {str(e)}")
        sys.exit(1)

def ensure_directory(path: str) -> None:
    """Ensure directory exists, create if it doesn't."""
    Path(path).mkdir(parents=True, exist_ok=True)

def print_step(message: str, verbose: bool = False) -> None:
    """Print formatted step message."""
    if verbose:
        logging.info(f"\n{'='*50}")
        logging.info(f"? {message.upper()}")
        logging.info(f"{'='*50}")

def run_pipeline(config: Dict[str, Any], args: argparse.Namespace) -> None:
    """Execute the complete OSM data pipeline."""
    try:
        # Prepare paths
        location_name = args.location or config["osm"]["location_name"]
        raw_folder = config["osm"]["destination_folder"]
        ensure_directory(raw_folder)
        
        raw_path = os.path.join(raw_folder, "osm_raw.geojson")
        cleaned_path = os.path.join("data", "processed", "osm_cleaned.gpkg")
        ensure_directory(os.path.dirname(cleaned_path))

        # 1. Download OSM data (unless skipped)
        if not args.skip_download:
            print_step(f"Downloading OSM data for: {location_name}", args.verbose)
            downloader = OSMDownloader(
                location_name,
                tags=config["osm"].get("tags"),
                timeout=config["osm"].get("timeout", 300)
            )
            downloader.save(raw_path)
            logging.info(f"? OSM data saved to: {raw_path}")
        else:
            logging.info("? Skipping OSM download")

        # 2. Clean and process data (unless skipped)
        if not args.skip_clean:
            print_step("Cleaning and processing data", args.verbose)
            cleaner = GeoCleaner(raw_path)
            
            cleaner.drop_null_and_empty()
            cleaner.fix_invalid_geometries(method="make_valid")
            cleaner.reproject(config["cleaning"]["target_crs"])
            
            # Optional column standardization
            if config["cleaning"].get("standardize_columns", True):
                cleaner.standardize_columns()
                
            cleaner.save(cleaned_path)
            logging.info(f"? Cleaned data saved to: {cleaned_path}")
        else:
            logging.info("? Skipping data cleaning")

        # 3. Database operations (unless skipped)
        if not args.skip_db:
            print_step("Database operations", args.verbose)
            pg = config["postgis"]
            
            # Apply CLI overrides
            if args.table:
                pg["table_name"] = args.table

            try:
                with PostgreSQLDatabaseManager(
                    user=pg["user"],
                    password=pg["password"],
                    host=pg["host"],
                    port=pg["port"],
                    extensions=pg["extensions"],
                    min_connections=1,
                    max_connections=5
                ) as db_manager:
                    # Ensure database exists
                    db_created = db_manager.ensure_database(pg["database"])
                    if db_created and args.verbose:
                        db_manager.optimize_postgis_settings(pg["database"])
                    
                    logging.info(f"? Database verified: {pg['database']}")

                    # Load data to PostGIS
                    db_url = (
                        f"postgresql://{pg['user']}:{pg['password']}@"
                        f"{pg['host']}:{pg['port']}/{pg['database']}"
                    )
                    
                    # Get SRID from target CRS (e.g., "EPSG:4326" -> 4326)
                    srid = int(config["cleaning"]["target_crs"].split(":")[1])
                    
                    loader = PostGISLoader(
                        cleaned_path,
                        db_url,
                        chunk_size=pg["chunk_size"],
                        geometry_type=pg["geometry_type"],
                        srid=srid
                    )
                    loader.load_to_postgis(
                        table_name=pg["table_name"],
                        schema=pg["schema"],
                        if_exists=pg["if_exists"],
                        create_spatial_index=pg["create_index"]
                    )
                    logging.info(f"? Data loaded to table: {pg['schema']}.{pg['table_name']}")
                    
            except Exception as e:
                logging.error(f"? Database operations failed: {str(e)}")
                if args.debug:
                    traceback.print_exc()
                raise

        else:
            logging.info("? Skipping database operations")

        logging.info("\n?? Pipeline completed successfully!")

    except Exception as e:
        logging.error(f"\n? Pipeline failed: {str(e)}")
        if args.debug:
            traceback.print_exc()
        sys.exit(1)

def main():
    """Main entry point for the pipeline."""
    args = parse_args()
    setup_logging(verbose=args.verbose, debug=args.debug)
    
    logging.info("\n" + "="*50)
    logging.info("?? Starting OSM Data Pipeline")
    logging.info("="*50)
    
    if args.verbose:
        logging.info("\n?? Configuration:")
        logging.info(f"  - Config file: {args.config}")
        logging.info(f"  - Location: {args.location or 'from config'}")
        logging.info(f"  - Table: {args.table or 'from config'}")
        logging.info(f"  - Skip steps: {'Download' if args.skip_download else ''} "
                    f"{'Clean' if args.skip_clean else ''} {'DB' if args.skip_db else ''}")
    
    # Load configuration
    config = load_config(args.config)
    
    # Execute pipeline
    run_pipeline(config, args)

if __name__ == "__main__":
    main()