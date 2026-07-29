import logging

from functions import format_processing_row, log_error_decorator
from model import (
    Column,
    ColumnMapping,
    DataSet,
    DataSource,
    DataType,
    Domain,
    LayerTable,
    Pipeline,
    Table,
)

from .base import SmxComponent


class SourceStgBuilder(SmxComponent):
    def build_systems(self, rows):
        for row in rows:
            self.extract_system(row)

    def build_data_types(self, rows):
        for row in rows:
            self.extract_data_types(row)

    def build_tables(self, rows):
        for row in rows:
            self.extract_stg_tables(row)

    def build_columns(self, rows):
        for row in rows:
            self.extract_stg_srci_table_columns(row)

    def build_source_view_mappings(self, rows):
        for row in rows:
            self.extract_src_view_columns(row)

    @log_error_decorator()
    def extract_system(self, row):
        ds = DataSource(source_name=row.schema, source_level=1, scheduled=1)
        self._source_systems.append(ds.source_name)

    @log_error_decorator()
    def extract_data_types(self, row):
        data_type_lst = row.data_type.split(sep='(')
        DataType(db_id=self.db_engine.id, dt_name=data_type_lst[0], _override=1)

    @log_error_decorator()
    def extract_stg_tables(self, row):
        if row.table_name != '':
            ds = DataSource.get_instance(_key=row.schema)
            if ds:
                src_t = Table(schema_id=self.src_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                stg_t = Table(schema_id=self.stg_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                srci_t = Table(schema_id=self.srci_t_schema.id, table_name=row.table_name, table_kind='T', source_id=ds.id)
                src_lt = LayerTable(layer_id=self.src_layer.id, table_id=src_t.id)
                stg_lt = LayerTable(layer_id=self.stg_layer.id, table_id=stg_t.id)
                srci_lt = LayerTable(layer_id=self.srci_layer.id, table_id=srci_t.id)
                src_v = Table(schema_id=self.src_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                src_lv = LayerTable(layer_id=self.src_layer.id, table_id=src_v.id)
                Pipeline(src_lyr_table_id=src_lt.id, tgt_lyr_table_id=stg_lt.id, lyr_view_id=src_lv.id)
                stg_v = Table(schema_id=self.stg_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                stg_lv = LayerTable(layer_id=self.stg_layer.id, table_id=stg_v.id)
                Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=None, lyr_view_id=stg_lv.id, select_asterisk=True)
                srci_v = Table(schema_id=self.srci_v_schema.id, table_name=row.table_name, table_kind='V', source_id=ds.id)
                srci_vl = LayerTable(layer_id=self.srci_layer.id, table_id=srci_v.id)
                Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=srci_lt.id, lyr_view_id=srci_vl.id)
            else:
                logging.error(f' Invalid Source name, {row.schema}, processing row:\n{format_processing_row(row)}')

    @log_error_decorator()
    def extract_stg_srci_table_columns(self, row):
        if row.column_name != '':
            src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
            stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            srci_table = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
            data_type_lst = row.data_type.split(sep='(')
            _data_type = data_type_lst[0]
            data_type = DataType.get_instance(_key=(self.db_engine.id, _data_type))
            if not data_type:
                logging.error(f'Invalid data type, processing row:\n{format_processing_row(row)}')
            else:
                pk = 1 if row.pk.upper() == 'Y' else 0
                mandatory = 1 if row.pk.upper() == 'Y' else 0
                precision = data_type_lst[1].split(sep=')')[0] if len(data_type_lst) > 1 else None
                domain_id = None
                if row.natural_key == '':
                    if src_table:
                        Column(table_id=src_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)
                    if stg_table:
                        Column(table_id=stg_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision)
                else:
                    domain = None
                    domain_error_msg = f"Domain not found for {row.table_name}.{row.column_name} column, please check 'Stg tables' sheet!"
                    if row.key_set_name and row.key_domain_name:
                        data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                        if not data_set:
                            logging.error(f"invalid key set name '{row.key_set_name}', processing row:\n{format_processing_row(row)}")
                        else:
                            domain = Domain.get_by_name(data_set.id, row.key_domain_name)
                            domain_error_msg = f"Bkey Domain not found for {row.table_name}.{row.column_name} column, \n key set name:'{row.key_set_name}'\n key domain name: '{row.key_domain_name}'\n please check 'Stg tables' sheet!"
                    elif row.code_set_name and row.code_domain_name:
                        data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                        if not data_set:
                            logging.error(f"invalid code set name '{row.code_set_name}', processing row:\n{format_processing_row(row)}")
                        else:
                            domain = Domain.get_by_name(data_set.id, row.code_domain_name)
                            domain_error_msg = f"Bmap Domain not found for {row.table_name}.{row.column_name} column, \n code set name:'{row.code_set_name}'\n code domain name: '{row.code_domain_name}'\n please check 'Stg tables' sheet!"
                    if domain:
                        domain_id = domain.id
                    else:
                        logging.error(f'{domain_error_msg}, processing row:\n{format_processing_row(row)}')
                if srci_table:
                    Column(table_id=srci_table.id, column_name=row.column_name, is_pk=pk, mandatory=mandatory, data_type_id=data_type.id, dt_precision=precision, domain_id=domain_id)

    @log_error_decorator()
    def extract_src_view_columns(self, row):
        if row.natural_key == '' and row.column_name != '':
            src_table = Table.get_instance(_key=(self.src_t_schema.id, row.table_name))
            stg_table = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            src_v = Table.get_instance(_key=(self.src_v_schema.id, row.table_name))
            src_lv = LayerTable.get_instance(_key=(self.src_layer.id, src_v.id))
            pipeline = Pipeline.get_instance(_key=src_lv.id)
            src_col = Column.get_instance(_key=(src_table.id, row.column_name))
            stg_col = Column.get_instance(_key=(stg_table.id, row.column_name))
            if src_col and stg_col:
                ColumnMapping(pipeline_id=pipeline.id, col_seq=0, src_col_id=src_col.id, tgt_col_id=stg_col.id, src_col_trx=row.column_transformation_rule)
            else:
                logging.error(f"Invalid column '{row.column_name}', processing row:\n{format_processing_row(row)}")
