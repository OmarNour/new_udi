from __future__ import annotations

from config import (
    INSERT_INTO_EDW_TABLES,
    INSERT_INTO_HISTORY,
    INSERT_INTO_PROCESS,
    INSERT_INTO_SOURCE_SYSTEMS,
    INSERT_INTO_SOURCE_SYSTEM_TABLES,
    INSERT_INTO_TRANSFORM_KEYCOL,
    LOADING_MODE,
)
from functions import WriteFile, log_error_decorator, single_quotes, time_elapsed_decorator
from model import DataSource, Layer, LayerTable, Pipeline


@log_error_decorator()
@time_elapsed_decorator
def generate_metadata_scripts(smx: SMX):
    source_sytems_file = WriteFile(smx.metadata_scripts, 'SOURCE_SYSTEMS', 'sql')
    source_system_tables_file = WriteFile(smx.metadata_scripts, 'SOURCE_SYSTEM_TABLES', 'sql')
    edw_tables_file = WriteFile(smx.metadata_scripts, 'EDW_TABLES', 'sql')
    transform_keycol_file = WriteFile(smx.metadata_scripts, 'TRANSFORM_KEYCOL', 'sql')
    history_file = WriteFile(smx.metadata_scripts, 'HISTORY', 'sql')
    process_file = WriteFile(smx.metadata_scripts, 'PROCESS', 'sql')
    for source in DataSource.get_all_instances():
        source_sytems_file.write(INSERT_INTO_SOURCE_SYSTEMS.format(meta_db=smx.meta_t_schema.schema_name, SOURCE_NAME=single_quotes(source.source_name), REJECTION_TABLE_NAME='NULL', BUSINESS_RULES_TABLE_NAME='NULL', LOADING_MODE=single_quotes(LOADING_MODE), SOURCE_LAYER=single_quotes(Layer.get_instance(_key='SRC').layer_name), DATA_SRC_CD='NULL'))
        for table in source.tables:
            if table.schema.id == smx.src_v_schema.id:
                source_system_tables_file.write(INSERT_INTO_SOURCE_SYSTEM_TABLES.format(meta_db=smx.meta_t_schema.schema_name, SOURCE_NAME=single_quotes(source.source_name), TABLE_NAME=single_quotes(table.table_name), IS_TRANSACTIONAL=1 if table.transactional_data else 0))
    for layer_table in LayerTable.get_all_instances():
        table = layer_table.table
        for pk_col in table.key_col:
            transform_keycol_file.write(INSERT_INTO_TRANSFORM_KEYCOL.format(meta_db=smx.meta_t_schema.schema_name, LAYER_NAME=single_quotes(layer_table.layer.layer_name), TABLE_NAME=single_quotes(table.table_name), KEY_COLUMN=single_quotes(pk_col.column_name)))
        if table.schema.id == smx.core_t_schema.id:
            edw_tables_file.write(INSERT_INTO_EDW_TABLES.format(meta_db=smx.meta_t_schema.schema_name, LAYER_NAME=single_quotes(layer_table.layer.layer_name), TABLE_NAME=single_quotes(table.table_name), IS_LOOKUP=1 if table.is_lkp else 0, IS_HISTORY=1 if table.history_table else 0, START_DATE_COLUMN=single_quotes(table.start_date_col.column_name) if table.history_table and table.start_date_col else 'NULL', END_DATE_COLUMN=single_quotes(table.end_date_col.column_name) if table.history_table and table.end_date_col else 'NULL'))
    for pipeline in Pipeline.get_all_instances():
        if pipeline.tgt_lyr_table:
            process_type = None
            apply_type = 'INSERT'
            key_sets = [None]
            if pipeline.lyr_view.layer.id == smx.txf_bkey_layer.id:
                process_type = 'BKEY'
                key_sets = [data_set.set_code for data_set in pipeline.tgt_lyr_table.table.surrogate_data_sets]
            elif pipeline.lyr_view.layer.id == smx.txf_core_layer.id:
                process_type = 'TXF'
                if pipeline.scd_type2_col:
                    apply_type = 'HISTORY'
                elif pipeline.scd_type1_col:
                    apply_type = 'UPSERTDELETE'
            if process_type:
                for key_set in key_sets:
                    domain_id = 'NULL'
                    key_set_id = 'NULL'
                    code_set_id = 'NULL'
                    if key_set and process_type == 'BKEY':
                        domain_id = pipeline.domain.domain_code
                        key_set_id = key_set
                    process_file.write(INSERT_INTO_PROCESS.format(meta_db=smx.meta_t_schema.schema_name, PROCESS_NAME=single_quotes(pipeline.name), SOURCE_NAME=single_quotes(pipeline.src_lyr_table.table.data_source.source_name), TGT_LAYER=single_quotes(pipeline.tgt_lyr_table.layer.layer_name), TGT_TABLE=single_quotes(pipeline.tgt_lyr_table.table.table_name), SRC_TABLE=single_quotes(pipeline.lyr_view.table.table_name), APPLY_TYPE=single_quotes(apply_type), MAIN_TABLE_NAME=single_quotes(pipeline.src_lyr_table.table.table_name), KEY_SET_ID=key_set_id, DOMAIN_ID=domain_id, CODE_SET_ID=code_set_id))
            if pipeline.tgt_lyr_table.table.history_table:
                for hist_col_mapping in pipeline.scd_type2_col:
                    history_file.write(INSERT_INTO_HISTORY.format(meta_db=smx.meta_t_schema.schema_name, PROCESS_NAME=single_quotes(pipeline.name), HISTORY_COLUMN=single_quotes(hist_col_mapping.tgt_col.column_name)))
    source_sytems_file.close()
    source_system_tables_file.close()
    transform_keycol_file.close()
    process_file.close()
    history_file.close()
    edw_tables_file.close()
