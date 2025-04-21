#!/usr/bin/env python3
"""
PostGIS Data Loader

Handles efficient loading of geospatial data into PostGIS with:
- Chunked loading for large datasets
- Schema validation
- Geometry type enforcement
- Comprehensive error handling
"""

import logging
import warnings
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import Engine
from geoalchemy2 import Geometry
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress unnecessary warnings
warnings.filterwarnings('ignore', message='Geometry column does not contain')

class PostGISLoader:
    def __init__(
        self,
        filepath: Union[str, Path],
        db_url: str,
        chunk_size: Optional[int] = None,
        geometry_type: str = "GEOMETRY",
        srid: int = 4326
    ):
        """
        Initialize PostGIS loader.
        
        Args:
            filepath: Path to input GeoJSON/GPKG file
            db_url: Database connection URL
            chunk_size: Number of features per batch (None for single load)
            geometry_type: PostGIS geometry type to enforce
            srid: Spatial reference system ID
        """
        self.filepath = Path(filepath)
        self.db_url = db_url
        self.chunk_size = chunk_size
        self.geometry_type = geometry_type
        self.srid = srid
        self.engine = self._create_engine()
        self.gdf = None

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        try:
            return create_engine(
                self.db_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                connect_args={
                    'connect_timeout': 10,
                    'keepalives': 1,
                    'keepalives_idle': 30,
                    'keepalives_interval': 10,
                }
            )
        except exc.SQLAlchemyError as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            raise

    def _validate_schema(self, gdf: gpd.GeoDataFrame) -> None:
        """Validate GeoDataFrame schema before loading."""
        if not isinstance(gdf, gpd.GeoDataFrame):
            raise ValueError("Input must be a GeoDataFrame")
        
        if 'geometry' not in gdf.columns:
            raise ValueError("GeoDataFrame must contain a geometry column")

    def _prepare_data(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Prepare data for PostGIS loading."""
        # Ensure consistent geometry type
        if self.geometry_type != "GEOMETRY":
            gdf['geometry'] = gdf['geometry'].apply(
                lambda geom: geom if geom.is_empty else geom.__class__.__name__.upper()
            )
        
        # Convert to WKB (Well-Known Binary)
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkb if not geom.is_empty else None)
        
        return gdf

    def load(self) -> gpd.GeoDataFrame:
        """Load data from file into memory."""
        try:
            logger.info(f"📂 Loading data from {self.filepath}")
            self.gdf = gpd.read_file(self.filepath)
            self._validate_schema(self.gdf)
            logger.info(f"✅ Loaded {len(self.gdf)} features")
            return self.gdf
        except Exception as e:
            logger.error(f"Failed to load {self.filepath}: {str(e)}")
            raise

    def load_to_postgis(
        self,
        table_name: str,
        schema: str = "public",
        if_exists: str = "replace",
        create_spatial_index: bool = True
    ) -> None:
        """
        Load data to PostGIS with optional chunking.
        
        Args:
            table_name: Target table name
            schema: Database schema
            if_exists: Behavior for existing tables ('replace', 'append', 'fail')
            create_spatial_index: Create spatial index after loading
        """
        if self.gdf is None:
            self.gdf = self.load()

        try:
            with self.engine.begin() as connection:
                # Load in chunks if specified
                if self.chunk_size:
                    total_chunks = (len(self.gdf) // self.chunk_size) + 1
                    for chunk in tqdm(
                        range(total_chunks),
                        desc=f"Loading to {schema}.{table_name}",
                        unit="chunk"
                    ):
                        chunk_start = chunk * self.chunk_size
                        chunk_end = (chunk + 1) * self.chunk_size
                        chunk_gdf = self.gdf.iloc[chunk_start:chunk_end]
                        
                        self._load_chunk(
                            chunk_gdf,
                            connection,
                            table_name,
                            schema,
                            if_exists if chunk == 0 else "append"
                        )
                else:
                    self._load_chunk(
                        self.gdf,
                        connection,
                        table_name,
                        schema,
                        if_exists
                    )

                # Create spatial index if requested
                if create_spatial_index:
                    self._create_spatial_index(connection, table_name, schema)

            logger.info(f"🎉 Successfully loaded to {schema}.{table_name}")

        except exc.SQLAlchemyError as e:
            logger.error(f"Database operation failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

    def _load_chunk(
        self,
        gdf: gpd.GeoDataFrame,
        connection,
        table_name: str,
        schema: str,
        if_exists: str
    ) -> None:
        """Load a single chunk of data to PostGIS."""
        prepared_gdf = self._prepare_data(gdf.copy())
        
        prepared_gdf.to_sql(
            name=table_name,
            con=connection,
            schema=schema,
            if_exists=if_exists,
            index=False,
            dtype={
                'geometry': Geometry(
                    geometry_type=self.geometry_type,
                    srid=self.srid
                )
            }
        )

    def _create_spatial_index(
        self,
        connection,
        table_name: str,
        schema: str
    ) -> None:
        """Create spatial index on geometry column."""
        try:
            index_name = f"idx_{table_name}_geometry"
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {schema}.{table_name} USING GIST (geometry)"
            )
            logger.info(f"Created spatial index {index_name}")
        except exc.SQLAlchemyError as e:
            logger.warning(f"Failed to create spatial index: {str(e)}")

if __name__ == "__main__":
    # Example command-line usage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", help="Input GeoJSON/GPKG file")
    parser.add_argument("db_url", help="Database connection URL")
    parser.add_argument("table_name", help="Target table name")
    parser.add_argument("--schema", default="public", help="Database schema")
    args = parser.parse_args()
    
    loader = PostGISLoader(args.filepath, args.db_url)
    loader.load_to_postgis(args.table_name, schema=args.schema)

