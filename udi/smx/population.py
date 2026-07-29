from __future__ import annotations

import logging

from config import DS_BMAP
from functions import log_error_decorator
from model import DataSetType, DataType, LayerTable, Table

from .builders import (
    BKeyBuilder,
    BMapBuilder,
    CoreBuilder,
    SourceStgBuilder,
    SrciBuilder,
    TechnicalColumnsBuilder,
)
from .builders.base import SmxComponent
from .stage_preparer import DuckDbStagePreparer


class ModelPopulator(SmxComponent):
    """Build the in-memory model from precomputed DuckDB stage results."""

    def __init__(self, smx):
        super().__init__(smx)
        self.stage_preparer = DuckDbStagePreparer(smx)
        self.source_stg_builder = SourceStgBuilder(smx)
        self.bkey_builder = BKeyBuilder(smx)
        self.bmap_builder = BMapBuilder(smx)
        self.srci_builder = SrciBuilder(smx)
        self.core_builder = CoreBuilder(smx)
        self.technical_columns_builder = TechnicalColumnsBuilder(smx)

    def populate(self, source_name):
        self._extract_all(source_name)
        self.technical_columns_builder.build()

    @log_error_decorator()
    def _extract_all(self, source_name):
        missing_sheets = {"system", "stg_tables"} - set(self.data.keys())
        if missing_sheets:
            logging.error(
                "Make sure 'System' and 'Stg Tables' sheets are both exists! "
                f"Missing: {sorted(missing_sheets)}"
            )
            return

        prepared = self.stage_preparer.prepare(source_name)

        foundation = prepared.foundation
        keys = prepared.key_mapping
        stg_srci = prepared.stg_srci
        core = prepared.core

        if not foundation.systems or not foundation.staging_tables:
            logging.error(
                "Please make sure there are data in both 'System' "
                "and 'Stg Tables' sheets!"
            )
            return

        self._build_systems_and_data_types(foundation)
        self._configure_builders(foundation)

        self._build_table_shells(foundation, keys)
        self._build_key_mapping_definitions(keys)
        self.source_stg_builder.build_columns(stg_srci.columns)
        self.source_stg_builder.build_source_view_mappings(
            stg_srci.source_view_mappings
        )
        self.bkey_builder.build_views(keys.bkey_views)

        # The original stable workflow defines BMAP view generation but does
        # not invoke it. Keep the same output behavior for this optimization.
        # To enable later, call: self.bmap_builder.build_views(keys.bmap_views)

        self.srci_builder.build_view_mappings(
            stg_srci.srci_view_mappings
        )
        self.core_builder.build_columns(foundation.core_columns)
        self.core_builder.build_transformations(
            core.table_mappings,
            core.column_mappings_by_name,
        )
        self._validate_model(foundation.all_core_table_names)

    def _build_systems_and_data_types(self, foundation):
        self.source_stg_builder.build_systems(foundation.systems)
        self.source_stg_builder.build_data_types(foundation.data_types)

    def _configure_builders(self, foundation):
        int_data_type = DataType.get_instance(
            _key=(self.db_engine.id, "INTEGER")
        )
        vchar_data_type = DataType.get_instance(
            _key=(self.db_engine.id, "VARCHAR")
        )
        timestamp_data_type = DataType.get_instance(
            _key=(self.db_engine.id, "TIMESTAMP")
        )

        self.bkey_builder.configure_data_types(
            int_data_type,
            vchar_data_type,
            timestamp_data_type,
        )
        self.bmap_builder.configure_data_types(
            int_data_type,
            vchar_data_type,
        )
        self.core_builder.configure_tables(
            foundation.history_table_names,
            foundation.lookup_table_names,
        )

    def _build_table_shells(self, foundation, keys):
        self.core_builder.build_tables(foundation.core_tables)
        self.bkey_builder.build_tables(keys.bkey_tables)
        self.bmap_builder.build_tables(keys.bmap_tables)
        self.source_stg_builder.build_tables(foundation.staging_tables)

    def _build_key_mapping_definitions(self, keys):
        self.bkey_builder.build_datasets(keys.bkey_datasets)
        self.bkey_builder.build_domains(keys.bkey_domains)

        self.bmap_builder.build_datasets(keys.bmap_datasets)
        bmaps_data_sets = DataSetType.get_instance(_key=DS_BMAP).data_sets
        self.bmap_builder.configure_data_sets(bmaps_data_sets)
        self.bmap_builder.build_domains(keys.bmap_domains)
        self.bmap_builder.build_values(keys.bmap_values)

    def _validate_model(self, all_core_table_names):
        core_names = {str(name).upper() for name in all_core_table_names}
        invalid_history_tables = []
        invalid_lookup_tables = []

        for table in Table.get_all_instances():
            if table.table_name not in core_names:
                continue

            if table.history_table:
                if not (
                    table.key_col
                    and table.start_date_col
                    and table.end_date_col
                ):
                    invalid_history_tables.append(table.table_name)
            elif table.is_lkp:
                column_names = {column.column_name for column in table.columns}
                if (
                    f"{table.table_name}_CD" not in column_names
                    or f"{table.table_name}_DESC" not in column_names
                ):
                    invalid_lookup_tables.append(table.table_name)

            if not table.key_col and table.table_kind == "T":
                logging.error(
                    "No Primary key defined for "
                    f"'{table.schema.schema_name}.{table.table_name}' table!"
                )

        if invalid_history_tables:
            logging.error(
                f"These are invalid history tables, {invalid_history_tables}"
            )
        if invalid_lookup_tables:
            logging.error(
                f"These are invalid lookup tables, {invalid_lookup_tables}.\n"
                "CODE & DESC columns should follow the pattern "
                "<table_name>_CD & <table_name>_DESC"
            )

        for layer_table in LayerTable.get_all_instances():
            if (
                layer_table.layer.layer_name == "STG"
                and not layer_table.table.key_col
                and layer_table.table.table_kind == "T"
            ):
                logging.warning(
                    "No Primary key defined for "
                    f"'{layer_table.table.table_name}' staging table!"
                )
