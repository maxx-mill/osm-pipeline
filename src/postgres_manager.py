#!/usr/bin/env python3
"""
PostgreSQL Database Manager

Handles PostgreSQL/PostGIS database creation and management with:
- Connection pooling
- Comprehensive error handling
- Extension management
- Database configuration
"""

import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.pool import ThreadedConnectionPool
from typing import Optional, List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgreSQLDatabaseManager:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int = 5432,
        min_connections: int = 1,
        max_connections: int = 5,
        extensions: Optional[List[str]] = None
    ):
        """
        Initialize database manager with connection pooling.
        
        Args:
            user: Database username
            password: Database password
            host: Database host
            port: Database port
            min_connections: Minimum connection pool size
            max_connections: Maximum connection pool size
            extensions: List of extensions to ensure exist
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.extensions = extensions or ['postgis']
        self._connection_pool = None
        
        # Initialize connection pool
        self._init_connection_pool(min_connections, max_connections)

    def _init_connection_pool(self, min_conn: int, max_conn: int) -> None:
        """Initialize threaded connection pool."""
        try:
            self._connection_pool = ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database='postgres'  # Connect to maintenance DB initially
            )
            logger.info("Initialized connection pool")
        except psycopg2.Error as e:
            logger.error(f"Failed to initialize connection pool: {str(e)}")
            raise

    def get_connection(self, database: str = 'postgres') -> psycopg2.extensions.connection:
        """Get a connection from the pool for specified database."""
        try:
            conn = self._connection_pool.getconn()
            if conn.closed:
                conn = psycopg2.connect(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=database
                )
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to get connection: {str(e)}")
            raise

    def release_connection(self, conn: psycopg2.extensions.connection) -> None:
        """Release a connection back to the pool."""
        try:
            self._connection_pool.putconn(conn)
        except Exception as e:
            logger.warning(f"Error releasing connection: {str(e)}")
            try:
                conn.close()
            except Exception:
                pass

    def ensure_database(self, db_name: str, template: str = 'template1') -> bool:
        """
        Ensure database exists with PostGIS extension.
        
        Args:
            db_name: Name of database to ensure
            template: Template database to use
            
        Returns:
            bool: True if database was created, False if it already existed
        """
        try:
            conn = self.get_connection()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # Check if database exists
            cur.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = {};").format(
                    sql.Literal(db_name)
                )
            )
            exists = cur.fetchone()

            if not exists:
                logger.info(f"Creating database '{db_name}'")
                cur.execute(
                    sql.SQL("CREATE DATABASE {} WITH TEMPLATE = {};").format(
                        sql.Identifier(db_name),
                        sql.Identifier(template)
                    )
                )
                created = True
            else:
                logger.info(f"Database '{db_name}' already exists")
                created = False

            cur.close()
            self.release_connection(conn)

            # Ensure extensions in the new database
            self._ensure_extensions(db_name)

            return created

        except psycopg2.Error as e:
            logger.error(f"Error ensuring database '{db_name}': {str(e)}")
            raise

    def _ensure_extensions(self, db_name: str) -> None:
        """Ensure required extensions exist in database."""
        try:
            conn = self.get_connection(db_name)
            cur = conn.cursor()

            for extension in self.extensions:
                cur.execute(
                    sql.SQL("CREATE EXTENSION IF NOT EXISTS {};").format(
                        sql.Identifier(extension)
                    )
                logger.info(f"Ensured extension '{extension}' in '{db_name}'")

            conn.commit()
            cur.close()
            self.release_connection(conn)

        except psycopg2.Error as e:
            logger.error(f"Error ensuring extensions: {str(e)}")
            raise

    def execute_sql(self, db_name: str, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute SQL query with parameters.
        
        Args:
            db_name: Database name
            query: SQL query to execute
            params: Dictionary of query parameters
            
        Returns:
            Query results if applicable
        """
        try:
            conn = self.get_connection(db_name)
            cur = conn.cursor()

            cur.execute(query, params or {})
            
            if cur.description:  # If query returns results
                results = cur.fetchall()
            else:
                results = None
                conn.commit()

            cur.close()
            self.release_connection(conn)
            return results

        except psycopg2.Error as e:
            logger.error(f"SQL execution failed: {str(e)}")
            raise

    def optimize_postgis_settings(self, db_name: str) -> None:
        """Configure optimal PostGIS settings for spatial workloads."""
        settings = {
            'shared_buffers': '1GB',
            'maintenance_work_mem': '256MB',
            'work_mem': '64MB',
            'effective_cache_size': '3GB',
            'random_page_cost': '1.1',
            'geqo_threshold': '12'
        }
        
        try:
            for name, value in settings.items():
                self.execute_sql(
                    db_name,
                    f"ALTER SYSTEM SET {name} = %s;",
                    (value,)
                )
            logger.info("Optimized PostGIS settings")
        except psycopg2.Error as e:
            logger.warning(f"Could not optimize settings: {str(e)}")

    def close(self) -> None:
        """Close all connections in the pool."""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("Closed all database connections")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == "__main__":
    # Example command-line usage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", required=True, help="Database username")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--host", required=True, help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--dbname", required=True, help="Database name to create")
    args = parser.parse_args()

    with PostgreSQLDatabaseManager(
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        extensions=['postgis', 'postgis_topology']
    ) as db_manager:
        db_manager.ensure_database(args.dbname)
        db_manager.optimize_postgis_settings(args.dbname)
