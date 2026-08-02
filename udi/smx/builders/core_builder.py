from config import CORE_PROCESS_NAME_TEMPLATE, CORE_VIEW_NAME_TEMPLATE
from functions import (
    log_error_decorator,
    log_validation_error,
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
                log_validation_error(
                    "Query found inside JOIN expression",
                    row=row,
                    problem=(
                        "The JOIN field contains a SELECT/SEL query. Only JOIN "
                        "clauses are allowed in this field."
                    ),
                    action="The invalid query text was not treated as a valid JOIN.",
                    impact="The affected transformation may have incomplete join logic.",
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
                    log_validation_error(
                        "Invalid JOIN syntax",
                        row=row,
                        problem="The JOIN type could not be recognized.",
                        action="The JOIN definition was not added.",
                        impact="The affected transformation may be incomplete.",
                    )
                _split = _split[1].split(' on ', 1)
                if len(_split) >= 2:
                    _split_0 = (' ' + _split[0] + ' ').replace(' join ', '')
                    _split_1 = _split[1]
                    table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
                    table_name = table__alias[0]
                    table_alias = table__alias[1] if len(table__alias) >= 2 else table_name
                    with_srci_t = Table.get_instance(_key=(self.srci_t_schema.id, table_name))
                    if not with_srci_t:
                        log_validation_error(
                            "Invalid JOIN table",
                            row=row,
                            problem=(
                                f"JOIN table '{table_name}' was not found in the "
                                "available SRCI tables."
                            ),
                            action="This JOIN was not added to the transformation.",
                            impact="The generated transformation may be incomplete.",
                        )
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
                log_validation_error(
                    "Invalid transformation type",
                    row=_row,
                    problem=(
                        f"Transformation type '{_row.transformation_type}' is not "
                        "one of: COPY, SQL, CONST."
                    ),
                    action="This column mapping was not created.",
                    impact=(
                        f"Target column '{_row.column_name}' will not receive a "
                        "generated mapping from this row."
                    ),
                )
            else:
                tgt_col = Column.get_instance(_key=(core_t.id, _row.column_name))
                if not tgt_col:
                    log_validation_error(
                        "Invalid target column",
                        row=_row,
                        problem=(
                            f"Target column '{_row.column_name}' was not found in "
                            f"core table '{row.target_table_name}'."
                        ),
                        action="This column mapping was not created.",
                        impact="The target column will have no mapping from this row.",
                    )
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
                        log_validation_error(
                            "Invalid historization algorithm",
                            row=row,
                            problem=(
                                f"Historization algorithm "
                                f"'{row.historization_algorithm}' is not supported."
                            ),
                            action="The mapping continued with the default SCD handling.",
                            impact="Historization behavior for this mapping may be incorrect.",
                        )
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
                                        log_validation_error(
                                        "Invalid source column",
                                        row=_row,
                                        problem=(
                                            f"Source column '{_row.mapped_to_column}' "
                                            f"was not found in table '{src_t.table_name}'."
                                        ),
                                        action="This column mapping was created without a source column.",
                                        impact=f"Target column '{_row.column_name}' may not be populated correctly.",
                                    )
                                else:
                                    log_validation_error(
                                    "Invalid source table",
                                    row=_row,
                                    problem=(
                                        f"Source table '{src_table.table_name}' is "
                                        "not available in the SRCI layer."
                                    ),
                                    action="This column mapping was created without a resolved source column.",
                                    impact=f"Target column '{_row.column_name}' may not be populated correctly.",
                                )
                            else:
                                log_validation_error(
                                "Invalid source-table alias",
                                row=_row,
                                problem=(
                                    f"Alias '{src_table_alias}' is not defined in "
                                    f"mapping '{row.mapping_name}'."
                                ),
                                action="This column mapping was created without a resolved source column.",
                                impact=f"Target column '{_row.column_name}' may not be populated correctly.",
                            )
                    elif transformation_type == 'CONST':
                        transformation_rule = single_quotes(_row.transformation_rule) if isinstance(_row.transformation_rule, str) and str(_row.transformation_rule).upper() != '' else None
                    else:
                        src_table_alias = _row.mapped_to_table
                        transformation_rule = _row.transformation_rule
                    ColumnMapping(pipeline_id=core_pipeline.id, tgt_col_id=tgt_col.id, src_col_id=src_col.id if src_col else None, src_table_alias=src_table_alias, col_seq=0, src_col_trx=transformation_rule if transformation_rule else None, constant_value=True if transformation_type == 'CONST' else False, scd_type=scd_type)
        core_t: Table
        ds = DataSource.get_instance(_key=row.source)
        if not ds:
            log_validation_error(
                "Invalid source name",
                row=row,
                problem=f"Source '{row.source}' is not defined in the System sheet.",
                action="The core transformation was not created.",
                impact=f"Mapping '{row.mapping_name}' is missing from the generated output.",
            )
        else:
            main_tables_name = row.main_source.split(',')
            main_table_name = main_tables_name[0].strip()
            main_table_alias = row.main_source_alias if row.main_source_alias else main_table_name
            srci_t = Table.get_instance(_key=(self.srci_t_schema.id, main_table_name))
            if not srci_t:
                log_validation_error(
                    "Invalid main source table",
                    row=row,
                    problem=(
                        f"Main source table '{main_table_name}' was not found in "
                        "the SRCI layer."
                    ),
                    action="The core transformation was not created.",
                    impact=f"Mapping '{row.mapping_name}' is missing from the generated output.",
                )
            else:
                srci_lt = LayerTable.get_instance(_key=(self.srci_layer.id, srci_t.id))
                core_t = Table.get_instance(_key=(self.core_t_schema.id, row.target_table_name))
                core_lt = LayerTable.get_instance(_key=(self.core_layer.id, core_t.id))
                if row.historization_algorithm.upper() == 'HISTORY' and (not core_t.history_table):
                    log_validation_error(
                        "History algorithm used with a non-history table",
                        row=row,
                        problem=(
                            f"Mapping '{row.mapping_name}' uses HISTORY, but target "
                            f"table '{row.target_table_name}' is not a history table."
                        ),
                        action="The program continued generating the transformation.",
                        impact="The generated historization logic may not match the table design.",
                    )
                if row.historization_algorithm.upper() != 'HISTORY' and core_t.history_table:
                    log_validation_error(
                        "Non-history algorithm used with a history table",
                        row=row,
                        problem=(
                            f"Mapping '{row.mapping_name}' uses "
                            f"'{row.historization_algorithm}', but target table "
                            f"'{row.target_table_name}' is a history table."
                        ),
                        action="The program continued generating the transformation.",
                        impact="The generated historization logic may not match the table design.",
                    )
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
                    log_validation_error(
                    "No column mappings found",
                    row=row,
                    problem=(
                        f"Mapping '{row.mapping_name}' has no matching rows in the "
                        "Column mapping sheet."
                    ),
                    action="The transformation view was created without column mappings.",
                    impact="The generated transformation is incomplete.",
                )
