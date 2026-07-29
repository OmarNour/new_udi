from config import CORE_TECHNICAL_COLS, STG_TECHNICAL_COLS
from functions import log_error_decorator, parse_data_type
from model import Column, DataType, Table

from .base import SmxComponent


class TechnicalColumnsBuilder(SmxComponent):
    @log_error_decorator()
    def add_technical_columns(self, tables, technical_column_objs):
        resolved_columns = []
        for technical_column in technical_column_objs:
            data_type_name, precision = parse_data_type(
                technical_column.data_type
            )
            data_type = DataType(
                db_id=self.db_engine.id,
                dt_name=data_type_name,
                _override=1,
            )
            resolved_columns.append(
                (technical_column, data_type, precision)
            )

        for table in tables:
            source_table = table.schema.id == self.src_t_schema.id
            if source_table:
                table.set_key(None)

            for technical_column, data_type, precision in resolved_columns:
                Column(
                    table_id=table.id,
                    column_name=technical_column.column_name,
                    data_type_id=data_type.id,
                    dt_precision=precision,
                    is_created_at=technical_column.is_created_at,
                    is_created_by=technical_column.is_created_by,
                    is_updated_at=technical_column.is_updated_at,
                    is_updated_by=technical_column.is_updated_by,
                    is_modification_type=(
                        technical_column.is_modification_type
                    ),
                    is_load_id=technical_column.is_load_id,
                    is_batch_id=technical_column.is_batch_id,
                    is_row_identity=technical_column.is_row_identity,
                    is_delete_flag=technical_column.is_delete_flag,
                    mandatory=technical_column.mandatory,
                    _override=1,
                    is_pk=(
                        1
                        if source_table
                        and technical_column.column_name.upper() == "REF_KEY"
                        else 0
                    ),
                )

    def build(self):
        staging_schema_ids = {
            self.src_t_schema.id,
            self.stg_t_schema.id,
            self.srci_t_schema.id,
        }
        all_tables = tuple(Table.get_all_instances())
        staging_tables = (
            table
            for table in all_tables
            if table._schema_id in staging_schema_ids
        )
        core_tables = (
            table
            for table in all_tables
            if table._schema_id == self.core_t_schema.id
        )
        self.add_technical_columns(staging_tables, STG_TECHNICAL_COLS)
        self.add_technical_columns(core_tables, CORE_TECHNICAL_COLS)
