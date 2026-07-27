from types import SimpleNamespace

import duckdb
from openpyxl import load_workbook

from config import SHEETS
from functions import log_error_decorator


class WorkbookStore:
    """Owns Excel parsing and the temporary in-memory DuckDB tables."""

    def __init__(self, path):
        self.path = path
        self.xls = None
        self.data = {}
        self.duckdb_con = duckdb.connect(database=":memory:")
        self._known_tables = set()

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
        clean_columns = [col for col in columns if col]
        if not clean_columns:
            self.duckdb_con.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" '
                "AS SELECT * FROM (SELECT 1) WHERE 1 = 0"
            )
            self._known_tables.add(table_name.lower())
            return

        cols_ddl = ", ".join(f'"{col}" VARCHAR' for col in clean_columns)
        self.duckdb_con.execute(
            f'CREATE OR REPLACE TABLE "{table_name}" ({cols_ddl})'
        )
        self._known_tables.add(table_name.lower())

        if not rows:
            return

        insert_cols_sql = ", ".join(f'"{col}"' for col in clean_columns)
        values_sql = ", ".join(["?"] * len(clean_columns))
        insert_sql = (
            f'INSERT INTO "{table_name}" ({insert_cols_sql}) '
            f"VALUES ({values_sql})"
        )
        all_rows_values = [
            tuple(
                None if row.get(col) is None else str(row.get(col, ""))
                for col in clean_columns
            )
            for row in rows
        ]
        self.duckdb_con.executemany(insert_sql, all_rows_values)

    def table_exists(self, table_name):
        return table_name.lower() in self._known_tables

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
        params = params or []
        distinct_sql = "DISTINCT " if distinct else ""
        cols_sql = ", ".join(f'"{col}"' for col in columns)
        query = (
            f'SELECT {distinct_sql}{cols_sql} '
            f'FROM "{table_name}" {where_clause}'
        )
        cursor = self.duckdb_con.execute(query, params)
        col_names = [desc[0] for desc in cursor.description]
        return [
            SimpleNamespace(
                **{
                    key: "" if value is None else value
                    for key, value in zip(col_names, row)
                }
            )
            for row in cursor.fetchall()
        ]

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

        ws = self.xls[sheet]
        raw_rows = list(ws.iter_rows(values_only=True))
        if not raw_rows:
            return

        headers = [
            str(cell)
            .replace("  ", " ")
            .replace(" ", "_")
            .lower()
            .strip()
            if cell is not None
            else ""
            for cell in raw_rows[0]
        ]

        seen = set()
        rows = []
        for data_row in raw_rows[1:]:
            row_dict = {}
            for index, column_name in enumerate(headers):
                if not column_name:
                    continue
                value = data_row[index] if index < len(data_row) else ""
                row_dict[column_name] = self.normalize_value(value)

            row_key = tuple(
                (key, row_dict.get(key, "")) for key in headers if key
            )
            if row_key in seen:
                continue
            seen.add(row_key)
            rows.append(row_dict)

        self.data[sheet_name] = sheet_name
        self.register_rows(sheet_name, rows, headers)

    @property
    def sheet_names(self):
        return self.data.keys()

    def close(self):
        if self.xls is not None:
            self.xls.close()
        self.duckdb_con.close()
