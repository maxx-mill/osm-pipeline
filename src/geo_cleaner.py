#!/usr/bin/env python3
"""
Geospatial Data Cleaner

Handles cleaning and transformation of OSM data including:
- Null/empty geometry removal
- Geometry validation and repair
- Coordinate system transformation
- Attribute standardization
"""

import logging
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
from shapely.validation import make_valid

class GeoCleaner:
    def __init__(self, filepath: Union[str, Path]):
        """Initialize with geospatial data file.
        
        Args:
            filepath: Path to GeoJSON, GPKG, or other GIS file
        """
        try:
            self.gdf = gpd.read_file(filepath)
            self.original_crs = self.gdf.crs
            logging.info(f"Loaded {len(self.gdf)} features from {filepath}")
        except Exception as e:
            logging.error(f"Failed to load {filepath}: {str(e)}")
            raise

    def drop_null_and_empty(self) -> None:
        """Remove null or empty geometries."""
        initial_count = len(self.gdf)
        self.gdf = self.gdf[~self.gdf.geometry.is_empty & self.gdf.geometry.notnull()]
        removed = initial_count - len(self.gdf)
        logging.info(f"Removed {removed} null/empty geometries. Remaining: {len(self.gdf)}")

    def fix_invalid_geometries(self, method: str = "buffer") -> None:
        """Repair invalid geometries using specified method.
        
        Args:
            method: "buffer" (0-distance buffer) or "make_valid" (shapely.make_valid)
        """
        invalid = ~self.gdf.geometry.is_valid
        if not invalid.any():
            logging.info("No invalid geometries found")
            return

        logging.warning(f"Found {invalid.sum()} invalid geometries - repairing with {method}")

        if method == "buffer":
            self.gdf.loc[invalid, "geometry"] = (
                self.gdf[invalid].geometry.buffer(0)
        elif method == "make_valid":
            self.gdf.loc[invalid, "geometry"] = (
                self.gdf[invalid].geometry.apply(make_valid))
        else:
            raise ValueError(f"Unknown method: {method}")

        if (~self.gdf.geometry.is_valid).any():
            logging.error("Some geometries remain invalid after repair")

    def reproject(self, crs: Union[str, dict] = "EPSG:4326") -> None:
        """Transform coordinate reference system.
        
        Args:
            crs: Target CRS (EPSG code, proj string, or dict)
        """
        if self.gdf.crs == crs:
            logging.info(f"Data already in target CRS: {crs}")
            return

        try:
            self.gdf = self.gdf.to_crs(crs)
            logging.info(f"Reprojected from {self.original_crs} to {crs}")
        except Exception as e:
            logging.error(f"Reprojection failed: {str(e)}")
            raise

    def standardize_columns(self) -> None:
        """Standardize column names and data types."""
        # Convert all column names to lowercase
        self.gdf.columns = [col.lower() for col in self.gdf.columns]
        
        # Example: Convert timestamp strings to datetime
        if "timestamp" in self.gdf.columns:
            self.gdf["timestamp"] = pd.to_datetime(self.gdf["timestamp"])

    def save(self, output_path: Union[str, Path], 
             driver: str = "GPKG") -> None:
        """Save cleaned data to file.
        
        Args:
            output_path: Output file path
            driver: OGR driver name (e.g., "GPKG", "GeoJSON")
        """
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            self.gdf.to_file(output_path, driver=driver)
            logging.info(f"Saved {len(self.gdf)} features to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save {output_path}: {str(e)}")
            raise

    @property
    def is_valid(self) -> bool:
        """Check if all geometries are valid."""
        return all(self.gdf.geometry.is_valid)

    @property
    def has_empty(self) -> bool:
        """Check if any geometries are empty."""
        return any(self.gdf.geometry.is_empty)

