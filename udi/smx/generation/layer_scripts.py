from __future__ import annotations

import os
from collections import defaultdict
from types import SimpleNamespace

from config import (
    CORE_MODEL_FOLDER_NAME,
    DROP_BEFORE_CREATE,
    DROP_TABLE_TEMPLATE,
    SRC_SYSTEMS_FOLDER_NAME,
)
from functions import (
    WriteFile,
    create_folder,
    log_error_decorator,
    time_elapsed_decorator,
)
from model import LayerTable


def _render_layer_table(layer_table: LayerTable):
    """Return the table/view DDL chunk and optional data chunk once."""
    ddl = layer_table.table.ddl
    table_kind = layer_table.table.table_kind
    ddl_chunk = None

    if ddl:
        drop_table = ""
        if DROP_BEFORE_CREATE and table_kind == "T":
            drop_table = (
                DROP_TABLE_TEMPLATE.format(
                    schema_name=layer_table.table.schema.schema_name,
                    table_name=layer_table.table.table_name,
                )
                + "\n"
            )
        ddl_chunk = drop_table + ddl + "\n"

    dml = None
    if layer_table.table.is_bmap:
        dml = layer_table.dml
    elif layer_table.table.is_lkp:
        schema_name = layer_table.table.schema.schema_name
        table_name = layer_table.table.table_name
        core_lookup_ds = layer_table.core_lookup_ds
        if core_lookup_ds:
            src_schema = core_lookup_ds.surrogate_table.schema.schema_name
            src_table = core_lookup_ds.surrogate_table.table_name
            dml = (
                f"insert into {schema_name}.{table_name}\n"
                f"({table_name}_CD, {table_name}_DESC, PROCESS_NAME, "
                "START_TS, BATCH_ID)\n"
                "select distinct EDW_CODE,DESCRIPTION, "
                f"'{src_table}', CURRENT_TIMESTAMP(0), 0\n"
                f"from {src_schema}.{src_table};\n"
            )

    data_chunk = dml + "\n" if dml else None
    kind_file_name = "tables" if table_kind == "T" else "views"
    return kind_file_name, ddl_chunk, data_chunk


@log_error_decorator()
def layer_table_scripts(row):
    """Backward-compatible single-row writer used by external callers."""
    kind_file_name, ddl_chunk, data_chunk = _render_layer_table(row.layer_table)

    if ddl_chunk:
        output_file = WriteFile(row.out_path, kind_file_name, "sql", "a")
        output_file.write(ddl_chunk)
        output_file.close()

    if data_chunk:
        data_file = WriteFile(row.out_path, "data", "sql", "a")
        data_file.write(data_chunk)
        data_file.close()


@log_error_decorator()
@time_elapsed_decorator
def generate_scripts(smx):
    """Generate each physical SQL file with one open/write/close cycle."""
    source_dict: dict[str, str] = {}
    core_model_path = os.path.join(
        smx.current_scripts_path,
        CORE_MODEL_FOLDER_NAME,
    )
    src_systems_path = os.path.join(
        smx.current_scripts_path,
        SRC_SYSTEMS_FOLDER_NAME,
    )

    for source in smx.source_systems:
        source_dict[source.upper()] = os.path.join(src_systems_path, source)

    def layer_table_out_path(layer_table):
        if layer_table.table.data_source:
            main_folder = source_dict[
                layer_table.table.data_source.source_name
            ]
        else:
            main_folder = core_model_path

        layer_folder_name = (
            f"Layer_{layer_table.layer.layer_level}_"
            f"{layer_table.layer.layer_name}"
        )
        return os.path.join(main_folder, layer_folder_name)

    file_chunks: dict[tuple[str, str], list[str]] = defaultdict(list)

    for layer_table in LayerTable.get_all_instances():
        out_path = layer_table_out_path(layer_table)
        create_folder(out_path)

        kind_file_name, ddl_chunk, data_chunk = _render_layer_table(
            layer_table
        )
        if ddl_chunk:
            file_chunks[(out_path, kind_file_name)].append(ddl_chunk)
        if data_chunk:
            file_chunks[(out_path, "data")].append(data_chunk)

    for (out_path, file_name), chunks in file_chunks.items():
        output_file = WriteFile(out_path, file_name, "sql", "w")
        output_file.write("".join(chunks))
        output_file.close()
