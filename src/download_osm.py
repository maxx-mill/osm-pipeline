#!/usr/bin/env python3
"""
OSM Data Downloader

Downloads OpenStreetMap data for a specified location with configurable:
- Feature types (buildings, roads, amenities)
- Geographic extent
- Download retry logic
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Union

import geopandas as gpd
import osmnx as ox
from retrying import retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default tags to download
DEFAULT_TAGS = {
    "building": True,
    "highway": True,
    "amenity": True,
    "landuse": True,
    "natural": True,
}

class OSMDownloader:
    def __init__(
        self,
        location_name: str,
        tags: Optional[Dict] = None,
        timeout: int = 300,
        max_retries: int = 3
    ):
        """
        Initialize OSM downloader.
        
        Args:
            location_name: Place name or address string
            tags: OSM tags to download (defaults to buildings, roads, amenities)
            timeout: Request timeout in seconds
            max_retries: Maximum download attempts
        """
        self.location_name = location_name
        self.tags = tags or DEFAULT_TAGS
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Configure OSMnx
        ox.settings.timeout = timeout
        ox.settings.log_console = True
        ox.settings.use_cache = True
        ox.settings.cache_folder = Path(".osmnx_cache")

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def download(self) -> gpd.GeoDataFrame:
        """
        Download OSM features with retry logic.
        
        Returns:
            GeoDataFrame with all requested features
            
        Raises:
            ValueError: If location isn't found
            TimeoutError: If download fails after retries
        """
        try:
            logger.info(f"?? Downloading OSM data for: {self.location_name}")
            logger.debug(f"Using tags: {self.tags}")
            
            start_time = time.time()
            gdf = ox.features_from_place(
                self.location_name,
                tags=self.tags,
                which_result=None  # Let OSMnx choose best result
            )
            
            duration = time.time() - start_time
            logger.info(f"? Downloaded {len(gdf)} features in {duration:.1f}s")
            return gdf

        except ox._errors.InsufficientResponseError as e:
            logger.error(f"Location not found: {self.location_name}")
            raise ValueError(f"Location not found: {self.location_name}") from e
        except Exception as e:
            logger.warning(f"Attempt failed: {str(e)}")
            raise

    def save(self, output_path: Union[str, Path]) -> None:
        """
        Download and save OSM data to file.
        
        Args:
            output_path: Output file path (.geojson or .gpkg)
            
        Raises:
            IOError: If file cannot be written
        """
        try:
            output_path = Path(output_path)
            gdf = self.download()
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Determine driver from file extension
            driver = "GeoJSON" if output_path.suffix.lower() == ".geojson" else "GPKG"
            
            gdf.to_file(output_path, driver=driver)
            logger.info(f"?? Saved {len(gdf)} features to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save OSM data: {str(e)}")
            raise IOError(f"Could not write to {output_path}") from e

def download_osm_data(
    location_name: str,
    output_path: Union[str, Path],
    tags: Optional[Dict] = None,
    **kwargs
) -> None:
    """
    Convenience function for direct downloading.
    
    Args:
        location_name: Place name to download
        output_path: File path to save results
        tags: Optional custom OSM tags
        **kwargs: Passed to OSMDownloader
    """
    OSMDownloader(location_name, tags=tags, **kwargs).save(output_path)

if __name__ == "__main__":
    # Example command-line usage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="Place name to download")
    parser.add_argument("output", help="Output file path")
    parser.add_argument("--tags", help="Custom tags JSON file")
    args = parser.parse_args()
    
    tags = DEFAULT_TAGS
    if args.tags:
        import json
        with open(args.tags) as f:
            tags = json.load(f)
    
    download_osm_data(args.location, args.output, tags=tags)
