from __future__ import annotations

import csv
import logging
import os
import tempfile
import time
from collections import namedtuple
from pathlib import Path

import duckdb
from openpyxl import load_workbook

from config import SHEETS
from functions import (
    log_error_decorator,
    log_validation_warning,
    record_input_normalization,
)


class WorkbookStore:
    """Own Excel parsing and all temporary in-memory DuckDB relations.

    Version 3.7 development replaces the PyArrow bridge with a temporary
    UTF-8 CSV file loaded through DuckDB ``COPY``. This keeps bulk loading,
    removes the PyArrow runtime dependency, and preserves the existing table
    schema and row ordering.
    """

    INTERNAL_ROW_NUMBER = "_udi_row_num"
    CSV_NULL_SENTINEL = "__UDI_NULL_VALUE_7F2B1B4D__"
    SOURCE_EXCEL_ROW_KEY = "__udi_excel_row__"

    def __init__(self, path):
        self.path = path
        self.xls = None
        self.data = {}
        self.duckdb_con = duckdb.connect(database=":memory:")
        self._known_tables = set()
        self._known_temp_tables = set()

    @staticmethod
    def _quote_identifier(identifier):
        """Safely quote a DuckDB identifier."""
        return f'"{str(identifier).replace(chr(34), chr(34) * 2)}"'

    @staticmethod
    def _quote_sql_string(value):
        """Safely quote a SQL string literal."""
        return "'" + str(value).replace("'", "''") + "'"

    def normalize_value(self, value, field_name=None):
        if value is None:
            normalized = ""
        elif isinstance(value, str):
            normalized = value.replace("\ufeff", "").replace("`", "").strip()
        elif isinstance(value, float) and value.is_integer():
            normalized = int(value)
        else:
            normalized = value

        if value is not None:
            record_input_normalization(field_name, value, normalized)
        return normalized

    def _create_empty_table(self, table_name, clean_columns):
        """Create the final DuckDB table with the legacy workbook schema."""
        quoted_table_name = self._quote_identifier(table_name)
        quoted_internal_column = self._quote_identifier(
            self.INTERNAL_ROW_NUMBER
        )

        if not clean_columns:
            self.duckdb_con.execute(
                f"CREATE OR REPLACE TABLE {quoted_table_name} "
                f"({quoted_internal_column} BIGINT)"
            )
            return

        columns_ddl = ", ".join(
            f"{self._quote_identifier(column)} VARCHAR"
            for column in clean_columns
        )
        self.duckdb_con.execute(
            f"CREATE OR REPLACE TABLE {quoted_table_name} "
            f"({columns_ddl}, {quoted_internal_column} BIGINT)"
        )

    def _write_csv_file(self, rows, clean_columns):
        """Write normalized rows to a closed temporary UTF-8 CSV file.

        ``delete=False`` is required on Windows because DuckDB must open the
        file after Python closes the writer handle.
        """
        temporary_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv",
            prefix="udi_duckdb_",
            delete=False,
        )
        temporary_path = temporary_file.name

        try:
            writer = csv.writer(
                temporary_file,
                delimiter=",",
                quotechar='"',
                escapechar=None,
                doublequote=True,
                quoting=csv.QUOTE_MINIMAL,
                lineterminator="\n",
            )

            for row_number, row in enumerate(rows, start=1):
                values = []
                for column in clean_columns:
                    value = row.get(column, "")
                    if value is None:
                        value = self.CSV_NULL_SENTINEL
                    else:
                        value = str(value)
                    values.append(value)

                values.append(row.get(self.SOURCE_EXCEL_ROW_KEY, row_number))
                writer.writerow(values)
        except Exception:
            temporary_file.close()
            self._remove_temporary_file(temporary_path)
            raise
        else:
            temporary_file.close()

        return temporary_path

    @staticmethod
    def _remove_temporary_file(path, attempts=5, delay_seconds=0.1):
        """Remove a temporary file, retrying brief Windows file locks."""
        if not path:
            return

        for attempt in range(attempts):
            try:
                os.remove(path)
                return
            except FileNotFoundError:
                return
            except OSError:
                if attempt == attempts - 1:
                    log_validation_warning(
                        "Temporary file could not be removed",
                        validation_stage="Temporary-file cleanup",
                        affected_object=path,
                        problem=(
                            "Windows kept the temporary CSV file locked after "
                            "the workbook sheet was loaded."
                        ),
                        action="The program continued without deleting this file.",
                        impact=(
                            "Generated scripts are not affected, but the temporary "
                            f"file may remain on disk: {path}"
                        ),
                    )
                    return
                time.sleep(delay_seconds)

    def _copy_csv_into_table(self, table_name, temporary_path):
        """Load a CSV file into the already-created DuckDB table."""
        quoted_table_name = self._quote_identifier(table_name)
        csv_path = Path(temporary_path).resolve().as_posix()
        quoted_csv_path = self._quote_sql_string(csv_path)
        quoted_null = self._quote_sql_string(self.CSV_NULL_SENTINEL)

        self.duckdb_con.execute(
            f"COPY {quoted_table_name} FROM {quoted_csv_path} ("
            "FORMAT CSV, "
            "HEADER FALSE, "
            "DELIMITER ',', "
            "QUOTE '\"', "
            "ESCAPE '\"', "
            f"NULLSTR {quoted_null}, "
            "AUTO_DETECT FALSE"
            ")"
        )

    def register_rows(self, table_name, rows, columns):
        """Bulk-load one normalized sheet using CSV + DuckDB ``COPY``.

        Every workbook column is stored as ``VARCHAR`` and
        ``_udi_row_num`` is a one-based ``BIGINT`` assigned after Python
        deduplication. Temporary files are removed in all success and failure
        paths.
        """
        clean_columns = [column for column in columns if column]
        self._create_empty_table(table_name, clean_columns)

        if not rows:
            self._known_tables.add(table_name.lower())
            return

        temporary_path = None
        try:
            temporary_path = self._write_csv_file(rows, clean_columns)
            self._copy_csv_into_table(table_name, temporary_path)
            self._known_tables.add(table_name.lower())
        finally:
            self._remove_temporary_file(temporary_path)

    def table_exists(self, table_name):
        return table_name.lower() in self._known_tables

    def temp_table_exists(self, table_name):
        return table_name.lower() in self._known_temp_tables

    def create_temp_table(self, table_name, select_sql, params=None):
        params = params or []
        self.duckdb_con.execute(
            f'CREATE OR REPLACE TEMP TABLE "{table_name}" AS {select_sql}',
            params,
        )
        self._known_temp_tables.add(table_name.lower())

    def drop_temp_table(self, table_name):
        self.duckdb_con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        self._known_temp_tables.discard(table_name.lower())

    def fetch_named_rows(self, query, params=None, row_type_name="DuckRow"):
        """Fetch a final prepared result once as lightweight named tuples."""
        params = params or []
        cursor = self.duckdb_con.execute(query, params)
        column_names = [description[0] for description in cursor.description]
        row_type = namedtuple(row_type_name, column_names, rename=True)
        return tuple(
            row_type(*("" if value is None else value for value in row))
            for row in cursor.fetchall()
        )

    def sql_list(
        self,
        table_name,
        column_name,
        where_clause="",
        params=None,
        distinct=False,
    ):
        params = params or []
        distinct_sql = "DISTINCT " if distinct else ""
        query = (
            f'SELECT {distinct_sql}"{column_name}" AS val '
            f'FROM "{table_name}" {where_clause}'
        )
        return [
            row[0]
            for row in self.duckdb_con.execute(query, params).fetchall()
        ]

    def sql_rows(
        self,
        table_name,
        columns,
        where_clause="",
        params=None,
        distinct=False,
    ):
        """Backward-compatible helper retained for external callers."""
        params = params or []
        distinct_sql = "DISTINCT " if distinct else ""
        columns_sql = ", ".join(f'"{column}"' for column in columns)
        return self.fetch_named_rows(
            f'SELECT {distinct_sql}{columns_sql} '
            f'FROM "{table_name}" {where_clause}',
            params,
            row_type_name="SqlRow",
        )

    @staticmethod
    def in_clause(values):
        if not values:
            return "IN (NULL)", []
        return f"IN ({','.join(['?'] * len(values))})", values

    def parse_file(self):
        self.xls = load_workbook(self.path, data_only=True, read_only=True)
        for sheet in self.xls.sheetnames:
            self.parse_sheet(sheet)

    @log_error_decorator()
    def parse_sheet(self, sheet):
        sheet_name = sheet.replace("  ", " ").replace(" ", "_").lower()
        if sheet_name not in SHEETS:
            return

        worksheet = self.xls[sheet]
        header_iterator = worksheet.iter_rows(
            min_row=1,
            max_row=1,
            values_only=True,
        )

        try:
            first_row = next(header_iterator)
        except StopIteration:
            return

        headers = [
            str(cell)
            .replace("  ", " ")
            .replace(" ", "_")
            .lower()
            .strip()
            if cell is not None
            else ""
            for cell in first_row
        ]
        active_columns = [
            (index, header)
            for index, header in enumerate(headers)
            if header
        ]
        active_header_names = [header for _, header in active_columns]

        if not active_columns:
            self.data[sheet_name] = sheet_name
            self.register_rows(sheet_name, [], [])
            return

        last_active_column = max(index for index, _ in active_columns) + 1
        data_iterator = worksheet.iter_rows(
            min_row=2,
            max_col=last_active_column,
            values_only=True,
        )

        seen = set()
        rows = []

        for excel_row_number, data_row in enumerate(data_iterator, start=2):
            row_dict = {
                column_name: self.normalize_value(
                    data_row[index] if index < len(data_row) else "",
                    field_name=column_name,
                )
                for index, column_name in active_columns
            }

            # Preserve the original deduplication key shape and first-row order.
            row_key = tuple(
                (column_name, row_dict.get(column_name, ""))
                for column_name in active_header_names
            )
            if row_key in seen:
                continue

            seen.add(row_key)
            row_dict[self.SOURCE_EXCEL_ROW_KEY] = excel_row_number
            rows.append(row_dict)

        self.data[sheet_name] = sheet_name
        self.register_rows(sheet_name, rows, active_header_names)

    @property
    def sheet_names(self):
        return self.data.keys()

    def close(self):
        if self.xls is not None:
            self.xls.close()
        self.duckdb_con.close()
