import logging
from types import SimpleNamespace

from config import DS_BMAP, UNIFIED_SOURCE_SYSTEMS
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


class ModelPopulator(SmxComponent):
    def __init__(self, smx):
        super().__init__(smx)
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
        if ('system' or 'stg_tables') not in self.data.keys():
            logging.error("Make sure 'System' and 'Stg Tables' sheets are both exists!")
        else:
            _core_tables = []
            _core_tables_bkey = []
            _core_tables_bmap = []
            _source_names = None
            history_tables_lst = []
            if source_name:
                _source_names = source_name if isinstance(source_name, list) else [source_name]
                _source_names.extend(UNIFIED_SOURCE_SYSTEMS)
            if _source_names:
                system_in_clause, system_params = self._in_clause([s.lower() for s in _source_names])
                system_where = f'WHERE lower("schema") {system_in_clause}'
            else:
                system_params = []
                system_where = ''
            system_rows = self._sql_rows('system', ['schema'], system_where, system_params, distinct=True)
            stg_rows_all = self._sql_rows('stg_tables', ['schema', 'table_name', 'data_type', 'mandatory', 'natural_key', 'pk', 'key_set_name', 'key_domain_name', 'code_set_name', 'code_domain_name', 'column_name', 'column_transformation_rule', 'bkey_filter'], system_where, system_params, distinct=False)
            if not system_rows or not stg_rows_all:
                logging.error("Please make sure there are data in both 'System' and 'Stg Tables' sheets!")
            else:
                table_mapping_rows = []
                if self._table_exists('table_mapping'):
                    table_mapping_rows = self._sql_rows('table_mapping', ['target_table_name', 'source', 'main_source', 'main_source_alias', 'filter_criterion', 'mapping_name', 'join', 'historization_algorithm', 'historization_columns', 'layer'], f'WHERE lower("source") {system_in_clause} AND upper("layer") = ?' if _source_names else 'WHERE upper("layer") = ?', system_params + ['CORE'], distinct=True)
                    _core_tables = [row.target_table_name for row in table_mapping_rows if row.target_table_name]
                    history_tables_lst = list(set([row.target_table_name for row in table_mapping_rows if str(row.historization_algorithm).upper() == 'HISTORY' and row.target_table_name]))
                bkey_rows = []
                if self._table_exists('bkey'):
                    key_domain_where = (
                        f"{system_where} AND \"key_domain_name\" IS NOT NULL "
                        f"AND \"key_domain_name\" != ''"
                        if system_where
                        else "WHERE \"key_domain_name\" IS NOT NULL "
                             "AND \"key_domain_name\" != ''"
                    )
                    bkey_cursor = self.duckdb_con.execute(
                        f"""
                            SELECT DISTINCT
                                b."physical_table",
                                b."key_set_name",
                                b."key_set_id",
                                b."key_domain_id",
                                b."key_domain_name"
                            FROM "bkey" b
                            INNER JOIN (
                                SELECT DISTINCT lower("key_domain_name") AS kdn
                                FROM "stg_tables"
                                {key_domain_where}
                            ) stg
                                ON lower(b."key_domain_name") = stg.kdn
                        """,
                        system_params,
                    )
                    col_names = [d[0] for d in bkey_cursor.description]
                    bkey_rows = [
                        SimpleNamespace(**dict(zip(col_names, row)))
                        for row in bkey_cursor.fetchall()
                    ]
                    _core_tables_bkey = list(
                        set(r.key_set_name for r in bkey_rows if r.key_set_name)
                    )

                bmap_rows = []
                bmap_values_rows = []
                if self._table_exists('bmap'):
                    code_domain_where = (
                        f"{system_where} AND \"code_domain_name\" IS NOT NULL "
                        f"AND \"code_domain_name\" != ''"
                        if system_where
                        else "WHERE \"code_domain_name\" IS NOT NULL "
                             "AND \"code_domain_name\" != ''"
                    )
                    bmap_cursor = self.duckdb_con.execute(
                        f"""
                            SELECT DISTINCT
                                b."physical_table",
                                b."code_set_name",
                                b."code_set_id",
                                b."code_domain_id",
                                b."code_domain_name"
                            FROM "bmap" b
                            INNER JOIN (
                                SELECT DISTINCT lower("code_domain_name") AS cdn
                                FROM "stg_tables"
                                {code_domain_where}
                            ) stg
                                ON lower(b."code_domain_name") = stg.cdn
                        """,
                        system_params,
                    )
                    col_names = [d[0] for d in bmap_cursor.description]
                    bmap_rows = [SimpleNamespace(**dict(zip(col_names, r))) for r in bmap_cursor.fetchall()]
                    _core_tables_bmap = list(set((r.code_set_name for r in bmap_rows if r.code_set_name)))
                    if self._table_exists('bmap_values') and bmap_rows:
                        domain_ids = list(set((r.code_domain_id for r in bmap_rows if r.code_domain_id)))
                        if domain_ids:
                            id_clause, id_params = self._in_clause(domain_ids)
                            bv_cursor = self.duckdb_con.execute(f'SELECT "code_set_name", "code_domain_id", "edw_code", "source_code", "description" FROM "bmap_values" WHERE "code_domain_id" {id_clause}', id_params)
                            col_names = [d[0] for d in bv_cursor.description]
                            bmap_values_rows = [SimpleNamespace(**dict(zip(col_names, r))) for r in bv_cursor.fetchall()]
                all_core_tables = list(set(_core_tables + _core_tables_bkey + _core_tables_bmap))
                core_tables_rows = []
                if self._table_exists('core_tables') and all_core_tables:
                    ct_clause, ct_params = self._in_clause([str(v).lower() for v in all_core_tables])
                    core_tables_rows = self._sql_rows('core_tables', ['table_name', 'column_name', 'data_type', 'pk', 'mandatory', 'historization_key'], f'WHERE lower("table_name") {ct_clause}', ct_params)
                cm_by_mapping: dict = {}
                if self._table_exists('column_mapping') and table_mapping_rows:
                    mapping_names = list(set((r.mapping_name for r in table_mapping_rows if r.mapping_name)))
                    mn_clause, mn_params = self._in_clause([m.lower() for m in mapping_names])
                    cm_cursor = self.duckdb_con.execute(f'SELECT DISTINCT "mapping_name", "column_name", "mapped_to_table", "mapped_to_column", "transformation_rule", "transformation_type" FROM "column_mapping" WHERE lower("mapping_name") {mn_clause}', mn_params)
                    col_names = [d[0] for d in cm_cursor.description]
                    for row_tuple in cm_cursor.fetchall():
                        ns = SimpleNamespace(**dict(zip(col_names, row_tuple)))
                        cm_by_mapping.setdefault(ns.mapping_name.lower(), []).append(ns)
                for row in system_rows:
                    self.source_stg_builder.extract_system(row)
                for dt_val in set((r.data_type for r in stg_rows_all if r.data_type)):
                    self.source_stg_builder.extract_data_types(SimpleNamespace(data_type=dt_val))
                if core_tables_rows:
                    for dt_val in set((r.data_type for r in core_tables_rows if r.data_type)):
                        self.source_stg_builder.extract_data_types(SimpleNamespace(data_type=dt_val))
                int_data_type = DataType.get_instance(_key=(self.db_engine.id, 'INTEGER'))
                vchar_data_type = DataType.get_instance(_key=(self.db_engine.id, 'VARCHAR'))
                timestamp_data_type = DataType.get_instance(_key=(self.db_engine.id, 'TIMESTAMP'))
                self.bkey_builder.configure_data_types(int_data_type, vchar_data_type, timestamp_data_type)
                self.bmap_builder.configure_data_types(int_data_type, vchar_data_type)
                self.core_builder.configure_tables(history_tables_lst, _core_tables_bmap)
                if core_tables_rows:
                    for tname in set((r.table_name for r in core_tables_rows if r.table_name)):
                        self.core_builder.extract_core_tables(SimpleNamespace(table_name=tname))
                if bkey_rows:
                    for ptable in set((r.physical_table for r in bkey_rows if r.physical_table)):
                        self.bkey_builder.extract_bkey_tables(SimpleNamespace(physical_table=ptable))
                if bmap_rows:
                    for ptable in set((r.physical_table for r in bmap_rows if r.physical_table)):
                        self.bmap_builder.extract_bmap_tables(SimpleNamespace(physical_table=ptable))
                seen_stg = set()
                for r in stg_rows_all:
                    key = (r.schema, r.table_name)
                    if key not in seen_stg:
                        seen_stg.add(key)
                        self.source_stg_builder.extract_stg_tables(SimpleNamespace(schema=r.schema, table_name=r.table_name))
                if bkey_rows:
                    first_by_key_set_id = {}
                    for r in bkey_rows:
                        if r.key_set_id and r.key_set_id not in first_by_key_set_id:
                            first_by_key_set_id[r.key_set_id] = r
                    for r in first_by_key_set_id.values():
                        self.bkey_builder.extract_bkey_datasets(SimpleNamespace(key_set_name=r.key_set_name, key_set_id=r.key_set_id, physical_table=r.physical_table))
                    seen_domain_keys = set()
                    for r in bkey_rows:
                        if not r.key_set_id or not r.key_domain_id:
                            continue
                        dk = (r.key_set_id, r.key_domain_id)
                        if dk in seen_domain_keys:
                            continue
                        seen_domain_keys.add(dk)
                        self.bkey_builder.extract_bkey_domains(SimpleNamespace(key_set_id=r.key_set_id, key_domain_id=r.key_domain_id, key_domain_name=r.key_domain_name))
                if bmap_rows:
                    first_by_code_set_id = {}
                    for r in bmap_rows:
                        if r.code_set_id and r.code_set_id not in first_by_code_set_id:
                            first_by_code_set_id[r.code_set_id] = r
                    for r in first_by_code_set_id.values():
                        self.bmap_builder.extract_bmap_datasets(SimpleNamespace(code_set_name=r.code_set_name, code_set_id=r.code_set_id, physical_table=r.physical_table))
                bmaps_data_sets = DataSetType.get_instance(_key=DS_BMAP).data_sets
                self.bmap_builder.configure_data_sets(bmaps_data_sets)
                if bmap_rows:
                    seen_domain_keys = set()
                    for r in bmap_rows:
                        if not r.code_set_id or not r.code_domain_id:
                            continue
                        dk = (r.code_set_id, r.code_domain_id)
                        if dk in seen_domain_keys:
                            continue
                        seen_domain_keys.add(dk)
                        self.bmap_builder.extract_bmap_domains(SimpleNamespace(code_set_id=r.code_set_id, code_domain_id=r.code_domain_id, code_domain_name=r.code_domain_name))
                if bmap_values_rows:
                    for row in bmap_values_rows:
                        self.bmap_builder.extract_bmap_values(row)
                seen_cols = set()
                for r in stg_rows_all:
                    key = (r.table_name, r.column_name, r.data_type, r.mandatory, r.natural_key, r.pk, r.key_set_name, r.key_domain_name, r.code_set_name, r.code_domain_name)
                    if key not in seen_cols:
                        seen_cols.add(key)
                        self.source_stg_builder.extract_stg_srci_table_columns(SimpleNamespace(table_name=r.table_name, column_name=r.column_name, data_type=r.data_type, mandatory=r.mandatory, natural_key=r.natural_key, pk=r.pk, key_set_name=r.key_set_name, key_domain_name=r.key_domain_name, code_set_name=r.code_set_name, code_domain_name=r.code_domain_name))
                seen_src = set()
                for r in stg_rows_all:
                    key = (r.schema, r.table_name, r.natural_key, r.column_name, r.column_transformation_rule)
                    if key not in seen_src:
                        seen_src.add(key)
                        self.source_stg_builder.extract_src_view_columns(SimpleNamespace(schema=r.schema, table_name=r.table_name, natural_key=r.natural_key, column_name=r.column_name, column_transformation_rule=r.column_transformation_rule))
                seen_bkey = set()
                for r in stg_rows_all:
                    key = (r.schema, r.table_name, r.natural_key, r.column_name, r.key_set_name, r.key_domain_name, r.bkey_filter)
                    if key not in seen_bkey:
                        seen_bkey.add(key)
                        self.bkey_builder.extract_bkey_views(SimpleNamespace(schema=r.schema, table_name=r.table_name, natural_key=r.natural_key, column_name=r.column_name, key_set_name=r.key_set_name, key_domain_name=r.key_domain_name, bkey_filter=r.bkey_filter))
                seen_srci = set()
                for r in stg_rows_all:
                    key = (r.schema, r.table_name, r.column_name, r.natural_key, r.key_set_name, r.key_domain_name, r.code_set_name, r.code_domain_name)
                    if key not in seen_srci:
                        seen_srci.add(key)
                        self.srci_builder.extract_srci_view_columns(SimpleNamespace(schema=r.schema, table_name=r.table_name, column_name=r.column_name, natural_key=r.natural_key, key_set_name=r.key_set_name, key_domain_name=r.key_domain_name, code_set_name=r.code_set_name, code_domain_name=r.code_domain_name))
                if core_tables_rows:
                    for row in core_tables_rows:
                        self.core_builder.extract_core_columns(SimpleNamespace(table_name=row.table_name, column_name=row.column_name, data_type=row.data_type, pk=row.pk, mandatory=row.mandatory, historization_key=row.historization_key))
                    invalid_history_tables = []
                    invalid_lookup_tables = []
                    for table in [x for x in Table.get_all_instances() if x.table_name in all_core_tables]:
                        if table.history_table:
                            if not (table.key_col and table.start_date_col and table.end_date_col):
                                invalid_history_tables.append(table.table_name)
                        elif table.is_lkp:
                            col_names = [col.column_name for col in table.columns]
                            if f'{table.table_name}_CD' not in col_names:
                                invalid_lookup_tables.append(table.table_name)
                            elif f'{table.table_name}_DESC' not in col_names:
                                invalid_lookup_tables.append(table.table_name)
                        if not table.key_col and table.table_kind == 'T':
                            logging.error(f"No Primary key defined for '{table.schema.schema_name}.{table.table_name}' table!")
                    if invalid_history_tables:
                        logging.error(f'These are invalid history tables, {invalid_history_tables}')
                    if invalid_lookup_tables:
                        logging.error(f'These are invalid lookup tables, {invalid_lookup_tables}.\nCODE & DESC columns should follow the pattern <table_name>_CD & <table_name>_DESC')
                if table_mapping_rows:
                    for row_index, r in enumerate(table_mapping_rows):
                        row = SimpleNamespace(target_table_name=r.target_table_name, source=r.source, main_source=r.main_source, main_source_alias=r.main_source_alias, filter_criterion=r.filter_criterion, mapping_name=r.mapping_name, join=r.join, historization_algorithm=r.historization_algorithm, historization_columns=r.historization_columns, _row_index=row_index)
                        self.core_builder.extract_core_txf_views(row, cm_by_mapping.get(r.mapping_name.lower(), []))
                for table_name in [lt.table.table_name for lt in LayerTable.get_all_instances() if lt.layer.layer_name == 'STG' and (not lt.table.key_col) and (lt.table.table_kind == 'T')]:
                    logging.warning(f"No Primary key defined for '{table_name}' staging table!")