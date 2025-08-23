from typing import Dict, Any, List, Optional
import pyodbc
from datetime import datetime
from .base_loader import BaseLoader


class MSSQLLoader(BaseLoader):
    """
    MSSQL loader for loading transformed data into Microsoft SQL Server.

    This loader handles connection management, table creation, data insertion,
    and batch processing for efficient loading of ETL data.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MSSQL loader with connection details.

        Expected config format:
        {
            "server": "localhost",
            "database": "etl_target",
            "username": "sa",
            "password": "password",
            "port": 1433,
            "driver": "ODBC Driver 17 for SQL Server",
            "table_name": "processed_data",
            "batch_size": 1000,
            "create_table": True,
            "truncate_before_load": False,
            "upsert_mode": False,
            "primary_key": "recordId",
            "connection_timeout": 30,
            "command_timeout": 60,
            "trust_server_certificate": True
        }
        """
        super().__init__(config)

        # Connection parameters
        self.server = self.config.get("server", "localhost")
        self.database = self.config.get("database", "etl_target")
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.port = self.config.get("port", 1433)
        self.driver = self.config.get("driver", "ODBC Driver 17 for SQL Server")

        # Loading parameters
        self.table_name = self.config.get("table_name", "processed_data")
        self.batch_size = self.config.get("batch_size", 1000)
        self.create_table = self.config.get("create_table", True)
        self.truncate_before_load = self.config.get("truncate_before_load", False)
        self.upsert_mode = self.config.get("upsert_mode", False)
        self.primary_key = self.config.get("primary_key", "recordId")

        # Timeout settings
        self.connection_timeout = self.config.get("connection_timeout", 30)
        self.command_timeout = self.config.get("command_timeout", 60)
        self.trust_server_certificate = self.config.get("trust_server_certificate", True)

    def connect(self) -> bool:
        """
        Establish connection to MSSQL Server.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Build connection string
            connection_parts = [
                f"DRIVER={{{self.driver}}}",
                f"SERVER={self.server},{self.port}",
                f"DATABASE={self.database}",
                f"Trusted_Connection=no"
            ]

            if self.username and self.password:
                connection_parts.extend([
                    f"UID={self.username}",
                    f"PWD={self.password}"
                ])

            if self.trust_server_certificate:
                connection_parts.append("TrustServerCertificate=yes")

            connection_string = ";".join(connection_parts)

            # Establish connection
            self.connection = pyodbc.connect(
                connection_string,
                timeout=self.connection_timeout
            )

            # Set command timeout
            self.connection.timeout = self.command_timeout

            # Test connection with a simple query
            cursor = self.connection.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()

            print(f"Successfully connected to MSSQL Server")
            print(f"Server version: {version[:50]}...")

            return True

        except pyodbc.Error as e:
            print(f"Failed to connect to MSSQL Server: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error connecting to MSSQL: {e}")
            return False

    def load(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load transformed data into MSSQL Server.

        Args:
            transformed_data: List of transformed records ready for loading

        Returns:
            Dictionary with load results and statistics
        """
        if not self.connection:
            raise RuntimeError("Not connected to MSSQL Server. Call connect() first.")

        if not self.validate_input(transformed_data):
            raise ValueError("Invalid input format - expected transformed data with data and metadata")

        try:
            # Extract data for loading
            data_to_load = self._extract_data_for_loading(transformed_data)

            if not data_to_load:
                return self._create_load_result(True, 0, 0, ["No data to load"])

            # Create table if needed
            if self.create_table:
                self._create_table_if_not_exists(data_to_load[0])

            # Truncate table if requested
            if self.truncate_before_load:
                self._truncate_table()

            # Load data in batches
            total_loaded = 0
            total_errors = []

            batches = self._prepare_batch(data_to_load, self.batch_size)

            for batch_num, batch in enumerate(batches, 1):
                try:
                    loaded_count = self._load_batch(batch)
                    total_loaded += loaded_count
                    print(f"Loaded batch {batch_num}/{len(batches)}: {loaded_count} records")

                except Exception as e:
                    error_msg = f"Failed to load batch {batch_num}: {str(e)}"
                    total_errors.append(error_msg)
                    print(f"Error: {error_msg}")

            # Commit transaction
            self.connection.commit()

            success = len(total_errors) == 0
            return self._create_load_result(
                success=success,
                records_processed=len(data_to_load),
                records_loaded=total_loaded,
                errors=total_errors
            )

        except Exception as e:
            # Rollback on error
            if self.connection:
                self.connection.rollback()

            error_msg = f"Load operation failed: {str(e)}"
            return self._create_load_result(
                success=False,
                records_processed=len(transformed_data),
                records_loaded=0,
                errors=[error_msg]
            )

    def _create_table_if_not_exists(self, sample_record: Dict[str, Any]):
        """
        Create table based on sample record structure if it doesn't exist.

        Args:
            sample_record: Sample record to infer schema from
        """
        try:
            cursor = self.connection.cursor()

            # Check if table exists
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM INFORMATION_SCHEMA.TABLES
                           WHERE TABLE_NAME = ?
                           """, (self.table_name,))

            table_exists = cursor.fetchone()[0] > 0

            if not table_exists:
                # Infer column types from sample data
                columns = []
                for field_name, field_value in sample_record.items():
                    sql_type = self._infer_sql_type(field_value)
                    columns.append(f"[{field_name}] {sql_type}")

                # Add primary key constraint if specified
                if self.primary_key and self.primary_key in sample_record:
                    constraint = f", CONSTRAINT PK_{self.table_name} PRIMARY KEY ([{self.primary_key}])"
                else:
                    constraint = ""

                create_sql = f"""
                CREATE TABLE [{self.table_name}] (
                    {', '.join(columns)}
                    {constraint}
                )
                """

                cursor.execute(create_sql)
                print(f"Created table [{self.table_name}] with {len(columns)} columns")
            else:
                print(f"Table [{self.table_name}] already exists")

            cursor.close()

        except Exception as e:
            raise Exception(f"Failed to create table: {str(e)}")

    def _infer_sql_type(self, value: Any) -> str:
        """
        Infer SQL Server data type from Python value.

        Args:
            value: Python value to infer type from

        Returns:
            SQL Server type string
        """
        if value is None:
            return "NVARCHAR(MAX)"  # Default for nulls
        elif isinstance(value, bool):
            return "BIT"
        elif isinstance(value, int):
            if -2147483648 <= value <= 2147483647:
                return "INT"
            else:
                return "BIGINT"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, datetime):
            return "DATETIME2"
        elif isinstance(value, str):
            if len(value) <= 255:
                return "NVARCHAR(255)"
            else:
                return "NVARCHAR(MAX)"
        else:
            return "NVARCHAR(MAX)"  # Default for unknown types

    def _truncate_table(self):
        """Truncate the target table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE [{self.table_name}]")
            cursor.close()
            print(f"Truncated table [{self.table_name}]")
        except Exception as e:
            raise Exception(f"Failed to truncate table: {str(e)}")

    def _load_batch(self, batch: List[Dict[str, Any]]) -> int:
        """
        Load a batch of records into the database.

        Args:
            batch: List of records to load

        Returns:
            Number of records loaded
        """
        if not batch:
            return 0

        cursor = self.connection.cursor()

        try:
            # Get field names from first record
            field_names = list(batch[0].keys())

            if self.upsert_mode:
                # Use MERGE statement for upsert
                loaded_count = self._upsert_batch(cursor, batch, field_names)
            else:
                # Simple INSERT
                loaded_count = self._insert_batch(cursor, batch, field_names)

            cursor.close()
            return loaded_count

        except Exception as e:
            cursor.close()
            raise Exception(f"Batch load failed: {str(e)}")

    def _insert_batch(self, cursor, batch: List[Dict[str, Any]], field_names: List[str]) -> int:
        """
        Insert batch using simple INSERT statement.

        Args:
            cursor: Database cursor
            batch: Batch of records
            field_names: List of field names

        Returns:
            Number of records inserted
        """
        # Build INSERT statement
        columns = ', '.join([f'[{name}]' for name in field_names])
        placeholders = ', '.join(['?' for _ in field_names])

        insert_sql = f"""
        INSERT INTO [{self.table_name}] ({columns})
        VALUES ({placeholders})
        """

        # Prepare data rows
        data_rows = []
        for record in batch:
            row = [record.get(field_name) for field_name in field_names]
            data_rows.append(row)

        # Execute batch insert
        cursor.executemany(insert_sql, data_rows)
        return len(data_rows)

    def _upsert_batch(self, cursor, batch: List[Dict[str, Any]], field_names: List[str]) -> int:
        """
        Upsert batch using MERGE statement.

        Args:
            cursor: Database cursor
            batch: Batch of records
            field_names: List of field names

        Returns:
            Number of records upserted
        """
        if not self.primary_key or self.primary_key not in field_names:
            raise ValueError("Primary key must be specified and present in data for upsert mode")

        # Build VALUES clause for source data
        values_rows = []
        params = []

        for i, record in enumerate(batch):
            row_placeholders = []
            for field_name in field_names:
                row_placeholders.append('?')
                params.append(record.get(field_name))
            values_rows.append(f"({', '.join(row_placeholders)})")

        values_clause = ', '.join(values_rows)
        columns = ', '.join([f'[{name}]' for name in field_names])

        # Build MERGE statement
        update_assignments = ', '.join([
            f'TARGET.[{name}] = SOURCE.[{name}]'
            for name in field_names if name != self.primary_key
        ])

        merge_sql = f"""
        MERGE [{self.table_name}] AS TARGET
        USING (VALUES {values_clause}) AS SOURCE ({columns})
        ON TARGET.[{self.primary_key}] = SOURCE.[{self.primary_key}]
        WHEN MATCHED THEN
            UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN
            INSERT ({columns})
            VALUES ({', '.join([f'SOURCE.[{name}]' for name in field_names])});
        """

        cursor.execute(merge_sql, params)
        return len(batch)

    def disconnect(self) -> bool:
        """
        Close the MSSQL connection.

        Returns:
            True if disconnection successful, False otherwise
        """
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                print("Disconnected from MSSQL Server")
            return True
        except Exception as e:
            print(f"Error disconnecting from MSSQL: {e}")
            return False

    def get_loader_info(self) -> Dict[str, str]:
        """
        Get information about this MSSQL loader.

        Returns:
            Dictionary with loader metadata
        """
        return {
            "loader_type": "MSSQLLoader",
            "description": "Loads transformed data into Microsoft SQL Server",
            "target_destination": f"MSSQL Server: {self.server}:{self.port}/{self.database}",
            "table_name": self.table_name,
            "batch_size": str(self.batch_size),
            "upsert_mode": str(self.upsert_mode),
            "primary_key": self.primary_key or "None"
        }

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the target table.

        Returns:
            Dictionary with table information
        """
        if not self.connection:
            return {"error": "Not connected to database"}

        try:
            cursor = self.connection.cursor()

            # Get table info
            cursor.execute("""
                           SELECT COLUMN_NAME,
                                  DATA_TYPE,
                                  IS_NULLABLE,
                                  CHARACTER_MAXIMUM_LENGTH
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME = ?
                           ORDER BY ORDINAL_POSITION
                           """, (self.table_name,))

            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "max_length": row[3]
                })

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM [{self.table_name}]")
            row_count = cursor.fetchone()[0]

            cursor.close()

            return {
                "table_name": self.table_name,
                "columns": columns,
                "column_count": len(columns),
                "row_count": row_count
            }

        except Exception as e:
            return {"error": f"Failed to get table info: {str(e)}"}

    def execute_custom_sql(self, sql: str) -> Dict[str, Any]:
        """
        Execute custom SQL statement.

        Args:
            sql: SQL statement to execute

        Returns:
            Dictionary with execution results
        """
        if not self.connection:
            return {"error": "Not connected to database"}

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            if sql.strip().upper().startswith('SELECT'):
                # For SELECT statements, return data
                columns = [column[0] for column in cursor.description]
                rows = [list(row) for row in cursor.fetchall()]
                result = {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows)
                }
            else:
                # For other statements, return affected rows
                self.connection.commit()
                result = {
                    "rows_affected": cursor.rowcount,
                    "message": "SQL executed successfully"
                }

            cursor.close()
            return result

        except Exception as e:
            if self.connection:
                self.connection.rollback()
            return {"error": f"SQL execution failed: {str(e)}"}