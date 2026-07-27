from __future__ import annotations

import os
from types import SimpleNamespace

from config import CORE_MODEL_FOLDER_NAME, DROP_BEFORE_CREATE, DROP_TABLE_TEMPLATE, SRC_SYSTEMS_FOLDER_NAME
from functions import WriteFile, create_folder, log_error_decorator, time_elapsed_decorator
from model import LayerTable


@log_error_decorator()
def layer_table_scripts(row):
    row.layer_table: LayerTable
    ddl = row.layer_table.table.ddl
    drop_table = ''
    table_kind = row.layer_table.table.table_kind
    if DROP_BEFORE_CREATE:
        if table_kind == 'T':
            drop_table = DROP_TABLE_TEMPLATE.format(schema_name=row.layer_table.table.schema.schema_name, table_name=row.layer_table.table.table_name) + '\n'
    if ddl:
        kind_folder = 'tables' if table_kind == 'T' else 'views'
        tables_file = WriteFile(row.out_path, kind_folder, 'sql', 'a')
        tables_file.write(drop_table + ddl)
        tables_file.write('\n')
        tables_file.close()
    dml = None
    if row.layer_table.table.is_bmap:
        dml = row.layer_table.dml
    elif row.layer_table.table.is_lkp:
        schema_name = row.layer_table.table.schema.schema_name
        table_name = row.layer_table.table.table_name
        core_lookup_ds = row.layer_table.core_lookup_ds
        if core_lookup_ds:
            src_schema = core_lookup_ds.surrogate_table.schema.schema_name
            src_table = core_lookup_ds.surrogate_table.table_name
            dml = f"insert into {schema_name}.{table_name}\n({table_name}_CD, {table_name}_DESC, PROCESS_NAME, START_TS, BATCH_ID)\nselect distinct EDW_CODE,DESCRIPTION, '{src_table}', CURRENT_TIMESTAMP(0), 0\nfrom {src_schema}.{src_table};\n"
    if dml:
        data_file = WriteFile(row.out_path, 'data', 'sql', 'a')
        data_file.write(dml)
        data_file.write('\n')
        data_file.close()


@log_error_decorator()
@time_elapsed_decorator
def generate_scripts(smx: SMX):
    source_dict: dict = {}
    core_model_path = os.path.join(smx.current_scripts_path, CORE_MODEL_FOLDER_NAME)
    src_systems_path = os.path.join(smx.current_scripts_path, SRC_SYSTEMS_FOLDER_NAME)
    for src in smx.source_systems:
        source_path = os.path.join(smx.current_scripts_path, src_systems_path, src)
        source_dict[src.upper()] = source_path
        del source_path

    def layer_table_out_path(row):
        layer_table = row.layer_table
        main_folder = source_dict[layer_table.table.data_source.source_name] if layer_table.table.data_source else core_model_path
        layer_folder_name = f'Layer_{layer_table.layer.layer_level}_{layer_table.layer.layer_name}'
        return os.path.join(smx.current_scripts_path, main_folder, layer_folder_name)
    layer_table_rows = [SimpleNamespace(layer_table=lt) for lt in LayerTable.get_all_instances()]
    for row in layer_table_rows:
        row.out_path = layer_table_out_path(row)
    for out_path in set((row.out_path for row in layer_table_rows)):
        create_folder(out_path)
    for row in layer_table_rows:
        layer_table_scripts(row)
