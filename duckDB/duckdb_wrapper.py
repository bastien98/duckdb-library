import logging
import os
from typing import List, Dict, Any, Union, Generator
import pandas as pd

import boto3
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class DuckDBWrapper:
    def __init__(self, db_file: str = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._conn = None
        self.db_file = db_file
        self.logger.info("DuckDBWrapper initialized")

    def __enter__(self):
        self.create_duckdb_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def create_duckdb_connection(self) -> None:
        """Establish a DuckDB connection."""
        if self.db_file:
            self._conn = duckdb.connect(self.db_file)
            self.logger.info(f"DuckDB connection established with database file: {self.db_file}")
        else:
            self._conn = duckdb.connect()
            self.logger.info("In-memory DuckDB connection established")

    def connect_to_s3(self) -> None:
        """Set up S3 connection for DuckDB."""

        def set_s3_credentials(aws_creds: Any) -> None:
            """Set S3 credentials in DuckDB."""
            for key, value in aws_creds.items():
                self._conn.execute(f"SET {key}='{value}'")
            self.logger.info("S3 credentials set in DuckDB")

        def install_and_load_extensions() -> None:
            """Install and load required extensions."""
            extensions = ["httpfs", "aws"]
            for ext in extensions:
                self._conn.install_extension(ext)
                self._conn.load_extension(ext)
            self.logger.info("Required extensions installed and loaded")

        if not self._conn:
            raise ConnectionError("DuckDB connection not established. Call connect() first.")
        session = boto3.session.Session()
        sts = session.client("sts")
        sts.get_caller_identity()
        aws_creds = session.get_credentials().get_frozen_credentials()
        aws_config = {
            's3_access_key_id': aws_creds.access_key,
            's3_secret_access_key': aws_creds.secret_key,
            's3_session_token': aws_creds.token,
            's3_region': session.region_name
        }

        set_s3_credentials(aws_config)
        install_and_load_extensions()

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("Database connection closed")

    def execute_query(self, query: str, params: Union[List, Dict] = None) -> List[Dict[str, Any]]:
        """Execute a query and return all results."""
        self.logger.info(f"Executing query: {query}")
        try:
            result = self._conn.execute(query, params)
            columns = [desc[0] for desc in result.description]
            data = [dict(zip(columns, row)) for row in result.fetchall()]
            self.logger.info(f"Query executed successfully. Returned {len(data)} rows.")
            return data
        except duckdb.Error as e:
            self.logger.error(f"DuckDB error executing query: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error executing query: {str(e)}")
            raise

    def execute_query_fetch_one(self, query: str, params: Union[List, Dict] = None) -> Generator[
        Dict[str, Any], None, None]:
        """Execute a query and yield results one at a time."""
        self.logger.info(f"Executing query with fetch one: {query}")
        try:
            result = self._conn.execute(query, params)
            columns = [desc[0] for desc in result.description]
            row_count = 0

            while True:
                row = result.fetchone()
                if row is None:
                    break
                row_count += 1
                yield dict(zip(columns, row))

            self.logger.info(f"Query executed successfully. Yielded {row_count} rows.")
        except duckdb.Error as e:
            self.logger.error(f"DuckDB error executing query with fetch one: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error executing query with fetch one: {str(e)}")
            raise

    def save_parquet(self, data: List[Dict[str, Any]], complete_file_name: str):
        self.logger.info(f"Saving data to Parquet file at '{complete_file_name}'")
        try:
            df = pd.DataFrame(data)
            self._conn.execute("CREATE TEMPORARY TABLE temp_table AS SELECT * FROM df")
            self._conn.execute(f"COPY temp_table TO '{complete_file_name}'")
            self._conn.execute("DROP TABLE temp_table")
            self.logger.info(f"Data saved to '{complete_file_name}' successfully.")
        except duckdb.Error as e:
            self.logger.error(f"DuckDB error saving Parquet file: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error saving Parquet file: {str(e)}")
            raise

    def read_file(self, file_path: str, file_type: str, select_columns: List[str] = None, filter_conditions: List[str] = None) -> List[Dict[str, Any]]:
        """
        Read data from a CSV or Parquet file.

        :param file_path: Path to the file (can be local or S3 path)
        :param file_type: Type of file ('csv' or 'parquet')
        :param select_columns: List of columns to select (optional)
        :param filter_conditions: List of filter conditions to apply (optional)
        :return: List of dictionaries containing the data
        """
        if file_type not in ['csv', 'parquet']:
            raise ValueError("file_type must be either 'csv' or 'parquet'")

        self.logger.info(f"Reading {file_type.upper()} file from '{file_path}'")
        try:
            if select_columns:
                columns = ", ".join(select_columns)
                query = f"SELECT {columns} FROM read_{file_type}('{file_path}')"
            else:
                query = f"SELECT * FROM read_{file_type}('{file_path}')"

            if filter_conditions:
                conditions = " AND ".join(f"({condition})" for condition in filter_conditions)
                query += f" WHERE {conditions}"

            result = self.execute_query(query)
            self.logger.info(f"Data read from '{file_path}' successfully. Returned {len(result)} rows.")
            return result
        except duckdb.Error as e:
            self.logger.error(f"DuckDB error reading {file_type.upper()} file: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error reading {file_type.upper()} file: {str(e)}")
            raise

    def create_table(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Create a table from the given data. DuckDB will infer the schema from the given data.
        """
        self.logger.info(f"Creating table '{table_name}'")
        try:
            if not data:
                raise ValueError("No data provided to create the table")

            df = pd.DataFrame(data)

            query = f"CREATE TABLE {table_name} AS SELECT * FROM df"
            self._conn.execute(query)
            self.logger.info(f"Table '{table_name}' created successfully with {len(data)} rows")
        except duckdb.Error as e:
            self.logger.error(f"DuckDB error creating table: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error creating table: {str(e)}")
            raise

    def save_table_as_parquet(self, table_name: str, file_path: str, file_name: str) -> None:
        """
        Save a DuckDB table as a Parquet file.
        :param table_name: Name of the table to be saved
        :param file_path: Directory path where the Parquet file will be saved
        :param file_name: Name of the Parquet file (without extension)
        """
        full_path = os.path.join(file_path, f"{file_name}.parquet")
        self.logger.info(f"Saving table '{table_name}' as Parquet file: {full_path}")
        try:
            query = f"COPY (SELECT * FROM {table_name}) TO '{full_path}' (FORMAT PARQUET)"
            self._conn.execute(query)
            self.logger.info(f"Table '{table_name}' successfully saved as Parquet file: {full_path}")
        except duckdb.Error as e:
            self.logger.error(f"DuckDB error saving table as Parquet: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error saving table as Parquet: {str(e)}")
            raise


