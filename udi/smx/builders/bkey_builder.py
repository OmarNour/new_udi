import logging

from config import BK_PROCESS_NAME_TEMPLATE, BK_VIEW_NAME_TEMPLATE
from functions import format_processing_row, log_error_decorator
from model import (
    Column,
    ColumnMapping,
    DataSet,
    DataSource,
    Domain,
    Filter,
    GroupBy,
    LayerTable,
    Pipeline,
    Table,
)

from .base import SmxComponent


class BKeyBuilder(SmxComponent):
    def configure_data_types(self, int_data_type, vchar_data_type, timestamp_data_type):
        self.int_data_type = int_data_type
        self.vchar_data_type = vchar_data_type
        self.timestamp_data_type = timestamp_data_type

    @log_error_decorator()
    def extract_bkey_tables(self, row):
        if row.physical_table != '':
            table = Table(schema_id=self.bkey_t_schema.id, table_name=row.physical_table, table_kind='T')
            LayerTable(layer_id=self.bkey_layer.id, table_id=table.id)
            Column(table_id=table.id, column_name='SOURCE_KEY', is_pk=1, mandatory=1, data_type_id=self.vchar_data_type.id, dt_precision=100, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1, data_type_id=self.int_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='EDW_KEY', is_pk=0, mandatory=1, data_type_id=self.int_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='KEY_SET_ID', is_pk=0, mandatory=1, data_type_id=self.int_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='START_DATE', is_pk=0, mandatory=1, data_type_id=self.timestamp_data_type.id, dt_precision=None, is_start_date=1, is_end_date=0)
            Column(table_id=table.id, column_name='PROCESS_NAME', is_pk=0, mandatory=1, data_type_id=self.vchar_data_type.id, dt_precision=200, is_start_date=0, is_end_date=0)

    @log_error_decorator()
    def extract_bkey_datasets(self, row):
        if row.physical_table != '':
            surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
            set_table = Table.get_instance(_key=(self.core_t_schema.id, row.key_set_name))
            ds = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
            valid_ds = True
            if ds:
                if ds.set_code != row.key_set_id:
                    valid_ds = False
                    logging.error(f"Duplicate dataset '{row.key_set_id}', processing row:\n{format_processing_row(row)}")
            if valid_ds:
                if set_table and surrogate_table:
                    DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                    surrogate_table.is_bkey = True
                else:
                    logging.error(f'Invalid set table {row.key_set_name} or surrogate table {row.physical_table}, processing row:\n{format_processing_row(row)}')

    @log_error_decorator()
    def extract_bkey_domains(self, row):
        data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
        if data_set and row.key_domain_id:
            Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)
        else:
            logging.error(f'invalid data set {row.key_set_id} or domain ID, processing row:\n{format_processing_row(row)}')

    @log_error_decorator()
    def extract_bkey_views(self, row):
        if row.natural_key != '' and row.key_domain_name != '':
            ds = DataSource.get_instance(_key=row.schema)
            if not ds:
                logging.error(f"Invalid source name '{row.schema}', processing row:\n{format_processing_row(row)}")
            else:
                data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                if not data_set:
                    logging.error(f"Invalid key set '{row.key_set_name}', processing row:\n{format_processing_row(row)}")
                else:
                    domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                    if not domain:
                        logging.error(f"Invalid key domain name '{row.key_domain_name}', processing row:\n{format_processing_row(row)}")
                    else:
                        stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                        stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))
                        srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                        srci_col = Column.get_instance(_key=(srci_t.id, row.column_name))
                        if data_set and stg_t and srci_col:
                            bk_process_name = BK_PROCESS_NAME_TEMPLATE.format(set_id=data_set.set_code, src_table_name=stg_t.table_name, column_name=srci_col.column_name, domain_id=srci_col.domain.domain_code)
                            bk_view_name = BK_VIEW_NAME_TEMPLATE.format(view_name=bk_process_name)
                            bkey_v = Table(schema_id=self.txf_bkey_v_schema.id, table_name=bk_view_name, table_kind='V', source_id=ds.id)
                            bkey_vl = LayerTable(layer_id=self.txf_bkey_layer.id, table_id=bkey_v.id)
                            bkey_pipeline = Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=domain.data_set.surrogate_table.id, lyr_view_id=bkey_vl.id, domain_id=srci_col.domain.id, name=bk_process_name)
                            if row.bkey_filter:
                                Filter(pipeline_id=bkey_pipeline.id, filter_seq=1, complete_filter_expr=row.bkey_filter)
                        else:
                            logging.error(f"Invalid key set '{row.key_set_name}', processing row:\n{format_processing_row(row)}")
