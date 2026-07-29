from __future__ import annotations

from collections import namedtuple

import duckdb
import pyarrow as pa
from openpyxl import load_workbook

from config import SHEETS
from functions import log_error_decorator


class WorkbookStore:
    """Own Excel parsing and all temporary in-memory DuckDB relations."""

    INTERNAL_ROW_NUMBER = "_udi_row_num"

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
    def normalize_value(value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value.replace("\ufeff", "").replace("`", "").strip()
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    def register_rows(self, table_name, rows, columns):
        """Bulk-load one normalized sheet into DuckDB through Apache Arrow.

        Every workbook column is stored as VARCHAR and ``_udi_row_num`` is a
        one-based BIGINT assigned after Python deduplication.
        """
        clean_columns = [column for column in columns if column]
        internal_column = self.INTERNAL_ROW_NUMBER

        quoted_table_name = self._quote_identifier(table_name)
        quoted_internal_column = self._quote_identifier(internal_column)

        if not clean_columns:
            self.duckdb_con.execute(
                f"CREATE OR REPLACE TABLE {quoted_table_name} "
                f"({quoted_internal_column} BIGINT)"
            )
            self._known_tables.add(table_name.lower())
            return

        if not rows:
            columns_ddl = ", ".join(
                f"{self._quote_identifier(column)} VARCHAR"
                for column in clean_columns
            )
            self.duckdb_con.execute(
                f"CREATE OR REPLACE TABLE {quoted_table_name} "
                f"({columns_ddl}, {quoted_internal_column} BIGINT)"
            )
            self._known_tables.add(table_name.lower())
            return

        arrow_values = {
            column: [
                None if row.get(column) is None else str(row.get(column, ""))
                for row in rows
            ]
            for column in clean_columns
        }

        arrow_arrays = {
            column: pa.array(values, type=pa.string())
            for column, values in arrow_values.items()
        }
        arrow_arrays[internal_column] = pa.array(
            range(1, len(rows) + 1),
            type=pa.int64(),
        )
        arrow_table = pa.table(arrow_arrays)

        relation_name = f"__udi_arrow_{table_name}"
        quoted_relation_name = self._quote_identifier(relation_name)
        self.duckdb_con.register(relation_name, arrow_table)

        try:
            select_columns = [
                (
                    f"CAST({self._quote_identifier(column)} AS VARCHAR) "
                    f"AS {self._quote_identifier(column)}"
                )
                for column in clean_columns
            ]
            select_columns.append(
                f"CAST({quoted_internal_column} AS BIGINT) "
                f"AS {quoted_internal_column}"
            )

            self.duckdb_con.execute(
                f"CREATE OR REPLACE TABLE {quoted_table_name} AS "
                f"SELECT {', '.join(select_columns)} "
                f"FROM {quoted_relation_name}"
            )
            self._known_tables.add(table_name.lower())
        finally:
            self.duckdb_con.unregister(relation_name)

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

        for data_row in data_iterator:
            row_dict = {
                column_name: self.normalize_value(
                    data_row[index] if index < len(data_row) else ""
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
