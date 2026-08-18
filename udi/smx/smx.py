import datetime as dt
import logging
import os

from config import SHEETS, ver_no
from functions import (
    create_folder,
    log_error_decorator,
    reset_validation_log_stats,
    ValidationSummaryFilter,
)

from model_cache import RelationCache

from .model_accelerator import install_model_acceleration
from .model_initializer import ModelInitializer, build_runtime_config
from .population import ModelPopulator
from .workbook import WorkbookStore


class SMX:
    """Backward-compatible facade for the structurally refactored backend."""

    def __init__(self, path, run_id, output_path, db_prefix, source_layer0):
        install_model_acceleration()
        RelationCache.clear()
        (
            self.LAYER_TYPES,
            self.LAYERS,
            self.MAIN_DB_NAME,
            self.MAIN_DATABASE_TEMPLATE,
            self.OTHER_SCHEMAS,
            self.GRANTS,
        ) = build_runtime_config(db_prefix, source_layer0)

        self.PREFIX = db_prefix
        self.run_id = run_id
        self.path = path
        self.current_scripts_path = os.path.join(output_path)
        self.metadata_scripts = os.path.join(
            self.current_scripts_path, "metadata"
        )
        self.log_error_path = self.current_scripts_path
        self.log_file_name = f"{self.run_id}.log"

        create_folder(self.current_scripts_path)
        create_folder(self.metadata_scripts)
        self._configure_logging()

        self._source_systems = []
        self.workbook = WorkbookStore(path=self.path)
        ModelInitializer(self).initialize()
        self.model_populator = ModelPopulator(self)

    def _configure_logging(self):
        separator = "*" * 82
        reset_validation_log_stats()

        root_logger = logging.getLogger()
        for handler in tuple(root_logger.handlers):
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

        file_handler = logging.FileHandler(
            os.path.join(self.log_error_path, self.log_file_name),
            encoding="utf-8",
        )
        file_handler.addFilter(ValidationSummaryFilter())
        file_handler.setFormatter(
            logging.Formatter(
                f"[%(levelname)s] %(message)s\n{separator}\n"
            )
        )
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)

        logging.info(
            "UDI Run Report\n"
            "============================================================\n"
            "Run ID: %s\n"
            "Started: %s\n"
            "Version: %s\n"
            "Input file: %s\n"
            "Output folder: %s",
            self.run_id,
            dt.datetime.now(),
            ver_no.replace("| Build #", ""),
            self.path,
            self.current_scripts_path,
        )

    # Compatibility properties used by the existing population logic.
    @property
    def xls(self):
        return self.workbook.xls

    @property
    def data(self):
        return self.workbook.data

    @property
    def duckdb_con(self):
        return self.workbook.duckdb_con

    @property
    def _known_tables(self):
        return self.workbook._known_tables

    def _normalize_value(self, value):
        return self.workbook.normalize_value(value)

    def _register_rows(self, table_name, rows, columns):
        return self.workbook.register_rows(table_name, rows, columns)

    def _table_exists(self, table_name):
        return self.workbook.table_exists(table_name)

    def _sql_list(
        self,
        table_name,
        column_name,
        where_clause="",
        params=None,
        distinct=False,
    ):
        return self.workbook.sql_list(
            table_name,
            column_name,
            where_clause,
            params,
            distinct,
        )

    def _sql_rows(
        self,
        table_name,
        columns,
        where_clause="",
        params=None,
        distinct=False,
    ):
        return self.workbook.sql_rows(
            table_name,
            columns,
            where_clause,
            params,
            distinct,
        )

    def _in_clause(self, values):
        return self.workbook.in_clause(values)

    @log_error_decorator()
    def parse_file(self):
        self.workbook.parse_file()

        if self._table_exists("supplements"):
            self.db_engine.reserved_words = self._sql_list(
                table_name="supplements",
                column_name="reserved_words",
                where_clause='WHERE lower("reserved_words_source") = lower(?)',
                params=[self.db_engine.name],
                distinct=True,
            )

    @log_error_decorator()
    def parse_sheet(self, sheet):
        return self.workbook.parse_sheet(sheet)

    @property
    def sheet_names(self):
        return self.workbook.sheet_names

    @property
    def source_systems(self):
        return self._source_systems

    @log_error_decorator()
    def populate_model(self, source_name):
        return self.model_populator.populate(source_name)

    def close(self):
        self.workbook.close()
