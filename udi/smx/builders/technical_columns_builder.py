from config import CORE_TECHNICAL_COLS, STG_TECHNICAL_COLS
from functions import log_error_decorator, parse_data_type
from model import Column, DataType, Table

from .base import SmxComponent


class TechnicalColumnsBuilder(SmxComponent):
    @log_error_decorator()
    def add_technical_columns(self, tables, technical_column_objs):
        for table in tables:
            src_table = False
            if table.schema.id == self.src_t_schema.id:
                table.set_key(None)
                src_table = True
            for missing_tech_col in technical_column_objs:
                data_type_name, precision = parse_data_type(missing_tech_col.data_type)
                data_type = DataType(db_id=self.db_engine.id, dt_name=data_type_name, _override=1)
                Column(table_id=table.id, column_name=missing_tech_col.column_name, data_type_id=data_type.id, dt_precision=precision, is_created_at=missing_tech_col.is_created_at, is_created_by=missing_tech_col.is_created_by, is_updated_at=missing_tech_col.is_updated_at, is_updated_by=missing_tech_col.is_updated_by, is_modification_type=missing_tech_col.is_modification_type, is_load_id=missing_tech_col.is_load_id, is_batch_id=missing_tech_col.is_batch_id, is_row_identity=missing_tech_col.is_row_identity, is_delete_flag=missing_tech_col.is_delete_flag, mandatory=missing_tech_col.mandatory, _override=1, is_pk=1 if src_table and missing_tech_col.column_name.upper() == 'REF_KEY' else 0)

    def build(self):
        stg_tables = (
            table
            for table in Table.get_all_instances()
            if table.schema.id in (
                self.src_t_schema.id,
                self.stg_t_schema.id,
                self.srci_t_schema.id,
            )
        )
        core_tables = (
            table
            for table in Table.get_all_instances()
            if table.schema.id == self.core_t_schema.id
        )
        self.add_technical_columns(stg_tables, STG_TECHNICAL_COLS)
        self.add_technical_columns(core_tables, CORE_TECHNICAL_COLS)
