import logging

from config import CORE_PROCESS_NAME_TEMPLATE, CORE_VIEW_NAME_TEMPLATE
from functions import (
    format_processing_row,
    log_error_decorator,
    merge_multiple_spaces,
    single_quotes,
)
from model import (
    Column,
    ColumnMapping,
    DataSource,
    DataType,
    Filter,
    JoinOn,
    JoinType,
    JoinWith,
    LayerTable,
    Pipeline,
    Table,
)

from .base import SmxComponent


class CoreBuilder(SmxComponent):
    def build_tables(self, rows):
        for row in rows:
            self.extract_core_tables(row)

    def build_columns(self, rows):
        for row in rows:
            self.extract_core_columns(row)

    def build_transformations(self, rows, column_mappings_by_name):
        for row in rows:
            mapping_name = str(row.mapping_name).lower()
            self.extract_core_txf_views(
                row,
                column_mappings_by_name.get(mapping_name, ()),
            )

    def configure_tables(self, history_tables, core_bmap_tables):
        self.history_tables_lst = history_tables
        self.core_tables_bmap = core_bmap_tables

    @log_error_decorator()
    def extract_core_tables(self, row):
        history_table = True if row.table_name in self.history_tables_lst else False
        is_lkp = True if row.table_name in self.core_tables_bmap else False
        table = Table(schema_id=self.core_t_schema.id, table_name=row.table_name, table_kind='T', history_table=history_table, is_lkp=is_lkp)
        LayerTable(layer_id=self.core_layer.id, table_id=table.id)

    @log_error_decorator()
    def extract_core_columns(self, row):
        if row.column_name != '':
            core_table = Table.get_instance(_key=(self.core_t_schema.id, row.table_name))
            data_type_lst = row.data_type.split(sep='(')
            _data_type = data_type_lst[0]
            data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
            if data_type:
                if core_table.history_table:
                    is_start_date = 1 if row.historization_key.upper() == 'S' else 0
                    is_end_date = 1 if row.historization_key.upper() == 'E' else 0
                    pk = 1 if row.historization_key.upper() == 'Y' or is_start_date else 0
                else:
                    is_start_date = 0
                    is_end_date = 0
                    pk = 1 if row.pk.upper() == 'Y' else 0
                mandatory = 1 if row.mandatory.upper() == 'Y' or pk else 0
                precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
                unicode = True if core_table.is_lkp else False
                Column(table_id=core_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision, is_start_date=is_start_date, is_end_date=is_end_date, unicode=unicode)

    @log_error_decorator()
    def extract_core_txf_views(self, row, cm_rows):

        def parse_join(join_txt: str):
            join_type = None
            _join_txt = ' ' + merge_multiple_spaces(join_txt).lower() + ' '
            forbidden_query_tokens = (' select ', ' sel ', '(sel ', '(select ')
            if any(token in _join_txt for token in forbidden_query_tokens):
                logging.error(
                    'Query cannot be found in join!, processing row:\n'
                    f'{format_processing_row(row)}'
                )
            _join_txt = _join_txt.replace(' inner ', ' ').replace(' outer ', ' ').strip()
            new_input_join = 'join '
            _split = _join_txt.split(' ', 1)
            if len(_split) >= 2:
                if _split[0].lower() == 'join':
                    join_type = JoinType.get_instance(_key='ij')
                elif _split[0].lower() == 'left':
                    join_type = JoinType.get_instance(_key='lj')
                elif _split[0].lower() == 'right':
                    join_type = JoinType.get_instance(_key='rj')
                elif _split[0].lower() == 'full':
                    join_type = JoinType.get_instance(_key='fj')
                if not join_type:
                    logging.error(f'No join found!, processing row:\n{format_processing_row(row)}')
                _split = _split[1].split(' on ', 1)
                if len(_split) >= 2:
                    _split_0 = (' ' + _split[0] + ' ').replace(' join ', '')
                    _split_1 = _split[1]
                    table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
                    table_name = table__alias[0]
                    table_alias = table__alias[1] if len(table__alias) >= 2 else table_name
                    with_srci_t = Table.get_instance(_key=(self.srci_t_schema.id, table_name))
                    if not with_srci_t:
                        logging.error(f"invalid join table '{table_name}', processing row:\n{format_processing_row(row)}")
                    else:
                        with_srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, with_srci_t.id))
                        join_with = JoinWith(pipeline_id=core_pipeline.id, master_lyr_table_id=srci_lt.id, master_alias=core_pipeline.src_table_alias, join_type_id=join_type.id, with_lyr_table_id=with_srci_lt.id, with_alias=table_alias)
                        _split = _split_1.split(' join ', 1)
                        join_on = _split[0]
                        if join_on.endswith(' left'):
                            join_on = join_on.removesuffix(' left')
                            new_input_join = 'left join '
                        elif join_on.endswith(' full'):
                            join_on = join_on.removesuffix(' full')
                            new_input_join = 'full join '
                        elif join_on.endswith(' right'):
                            join_on = join_on.removesuffix(' right')
                            new_input_join = 'right join '
                        JoinOn(join_with_id=join_with.id, complete_join_on_expr=join_on)
                        if len(_split) >= 2:
                            parse_join(new_input_join + _split[1])

        @log_error_decorator()
        def column_mapping(_row):
            transformation_type = _row.transformation_type.upper()
            if transformation_type not in ('COPY', 'SQL', 'CONST'):
                logging.error(f'Transformation Type, should be one of the following COPY, SQL or CONST, processing row:\n{format_processing_row(_row)}')
            else:
                tgt_col = Column.get_instance(_key=(core_t.id, _row.column_name))
                if not tgt_col:
                    logging.error(f'Invalid Target Column Name, {_row.column_name}, processing row:\n{format_processing_row(_row)}')
                else:
                    src_col = None
                    src_table_alias = None
                    scd_type = 1
                    if row.historization_algorithm.upper() == 'INSERT':
                        scd_type = 0
                    elif row.historization_algorithm.upper() in ('UPSERT', 'UPSERTDELETE'):
                        scd_type = 1
                    elif row.historization_algorithm.upper() == 'HISTORY':
                        scd_type = 2 if _row.column_name in row.historization_columns else 1
                    else:
                        logging.error(f'Invalid historization algorithm, processing row:\n{format_processing_row(row)}')
                    scd_type = 0 if tgt_col.is_pk else scd_type
                    if transformation_type == 'COPY':
                        transformation_rule = None
                        if _row.mapped_to_column:
                            src_table_alias = _row.mapped_to_table
                            src_table = core_pipeline.get_table_by_alias(src_table_alias)
                            if src_table:
                                src_t = Table.get_instance(_key=(self.srci_t_schema.id, src_table.table_name))
                                if src_t:
                                    src_col = Column.get_instance(_key=(src_t.id, _row.mapped_to_column))
                                    if src_col is None:
                                        logging.error(f"Invalid Source Column '{_row.mapped_to_column}', processing row:\n{format_processing_row(_row)}")
                                else:
                                    logging.error(f"Invalid Table '{src_table.table_name}', processing row:\n{format_processing_row(_row)}")
                            else:
                                logging.error(f"Invalid alias '{src_table_alias}' for mapping {row.mapping_name}, processing row:\n{format_processing_row(_row)}")
                    elif transformation_type == 'CONST':
                        transformation_rule = single_quotes(_row.transformation_rule) if isinstance(_row.transformation_rule, str) and str(_row.transformation_rule).upper() != '' else None
                    else:
                        src_table_alias = _row.mapped_to_table
                        transformation_rule = _row.transformation_rule
                    ColumnMapping(pipeline_id=core_pipeline.id, tgt_col_id=tgt_col.id, src_col_id=src_col.id if src_col else None, src_table_alias=src_table_alias, col_seq=0, src_col_trx=transformation_rule if transformation_rule else None, constant_value=True if transformation_type == 'CONST' else False, scd_type=scd_type)
        core_t: Table
        ds = DataSource.get_instance(_key=row.source)
        if not ds:
            logging.error(f'Invalid source name {row.source}, processing row:\n{format_processing_row(row)}')
        else:
            main_tables_name = row.main_source.split(',')
            main_table_name = main_tables_name[0].strip()
            main_table_alias = row.main_source_alias if row.main_source_alias else main_table_name
            srci_t = Table.get_instance(_key=(self.srci_t_schema.id, main_table_name))
            if not srci_t:
                logging.error(f'invalid main table, {main_table_name}, processing row:\n{format_processing_row(row)}')
            else:
                srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, srci_t.id))
                core_t = Table.get_instance(_key=(self.core_t_schema.id, row.target_table_name))
                core_lt = LayerTable.get_instance(_key=(self.core_layer.id, core_t.id))
                if row.historization_algorithm.upper() == 'HISTORY' and (not core_t.history_table):
                    logging.error(f'History process should deal with history table only, processing row:\n{format_processing_row(row)}')
                if row.historization_algorithm.upper() != 'HISTORY' and core_t.history_table:
                    logging.error(f'Non-history process should not deal with history table, processing row:\n{format_processing_row(row)}')
                txf_process_name = CORE_PROCESS_NAME_TEMPLATE.format(mapping_name=row.mapping_name)
                txf_view_name = CORE_VIEW_NAME_TEMPLATE.format(view_name=txf_process_name)
                core_txf_v = Table(schema_id=self.txf_core_v_schema.id, table_name=txf_view_name, table_kind='V', source_id=ds.id)
                core_txf_vl = LayerTable(layer_id=self.txf_core_layer.id, table_id=core_txf_v.id)
                core_pipeline = Pipeline(src_lyr_table_id=srci_lt.id, tgt_lyr_table_id=core_lt.id, src_table_alias=main_table_alias, lyr_view_id=core_txf_vl.id, name=txf_process_name)
                if row.join:
                    parse_join(row.join)
                if row.filter_criterion:
                    Filter(pipeline_id=core_pipeline.id, filter_seq=1, complete_filter_expr=row.filter_criterion)
                if cm_rows:
                    for cm_row in cm_rows:
                        column_mapping(cm_row)
                else:
                    logging.error(f'No column mapping found for {row.mapping_name}, processing row:\n{format_processing_row(row)}')
