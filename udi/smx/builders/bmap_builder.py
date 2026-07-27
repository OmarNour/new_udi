import logging

from config import BMAP_PROCESS_NAME_TEMPLATE, BMAP_VIEW_NAME_TEMPLATE
from functions import format_processing_row, log_error_decorator
from model import (
    Column,
    DataSet,
    DataSource,
    Domain,
    DomainValue,
    LayerTable,
    Pipeline,
    Table,
)

from .base import SmxComponent


class BMapBuilder(SmxComponent):
    def configure_data_types(self, int_data_type, vchar_data_type):
        self.int_data_type = int_data_type
        self.vchar_data_type = vchar_data_type

    def configure_data_sets(self, bmaps_data_sets):
        self.bmaps_data_sets = bmaps_data_sets

    @log_error_decorator()
    def extract_bmap_tables(self, row):
        if row.physical_table != '':
            table = Table(schema_id=self.bmap_t_schema.id, table_name=row.physical_table, table_kind='T')
            LayerTable(layer_id=self.bmap_layer.id, table_id=table.id)
            Column(table_id=table.id, column_name='SOURCE_CODE', is_pk=1, mandatory=1, data_type_id=self.vchar_data_type.id, dt_precision=50, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='DOMAIN_ID', is_pk=1, mandatory=1, data_type_id=self.int_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='CODE_SET_ID', is_pk=1, mandatory=1, data_type_id=self.int_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='EDW_CODE', is_pk=0, mandatory=1, data_type_id=self.int_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0)
            Column(table_id=table.id, column_name='DESCRIPTION', is_pk=0, mandatory=1, data_type_id=self.vchar_data_type.id, dt_precision=None, is_start_date=0, is_end_date=0, unicode=1)

    @log_error_decorator()
    def extract_bmap_datasets(self, row):
        if row.physical_table != '':
            surrogate_table = Table.get_instance(_key=(self.bkey_t_schema.id, row.physical_table))
            set_table = Table.get_instance(_key=(self.core_t_schema.id, row.code_set_name))
            ds = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
            valid_ds = True
            if ds:
                if ds.set_code != row.code_set_id:
                    valid_ds = False
                    logging.error(f"Duplicate dataset '{row.code_set_id}', processing row:\n{format_processing_row(row)}")
            if valid_ds:
                if set_table and row.code_set_id and surrogate_table:
                    DataSet(set_type_id=self.bmap_set_type.id, set_code=row.code_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                    surrogate_table.is_bmap = True
                else:
                    logging.error(f"Invalid set table '{row.code_set_name}' or surrogate table '{row.physical_table}', processing row:\n{format_processing_row(row)}")

    @log_error_decorator()
    def extract_bmap_domains(self, row):
        data_set = DataSet.get_instance(_key=(self.bmap_set_type.id, row.code_set_id))
        if data_set and row.code_domain_id:
            Domain(data_set_id=data_set.id, domain_code=row.code_domain_id, domain_name=row.code_domain_name)
        else:
            logging.error(f'invalid data set or domain ID, processing row:\n{format_processing_row(row)}')

    @log_error_decorator()
    def extract_bmap_values(self, row):
        data_set_lst = [ds for ds in self.bmaps_data_sets if ds.set_table.table_name == row.code_set_name.upper()]
        if len(data_set_lst) > 0:
            data_set = data_set_lst[0]
            domain = Domain.get_instance(_key=(data_set.id, row.code_domain_id))
            if domain:
                DomainValue(domain_id=domain.id, source_key=row.source_code, edw_key=row.edw_code, description=row.description)

    @log_error_decorator()
    def extract_bmap_views(self, row):
        if row.code_domain_name != '':
            ds = DataSource.get_instance(_key=row.schema)
            if not ds:
                logging.error(f"Invalid source name '{row.schema}', processing row:\n{format_processing_row(row)}")
            else:
                data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                if not data_set:
                    logging.error(f"Invalid code set '{row.code_set_name}', processing row:\n{format_processing_row(row)}")
                else:
                    domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.code_domain_name)
                    if not domain:
                        logging.error(f"Invalid code domain name '{row.code_domain_name}', processing row:\n{format_processing_row(row)}")
                    else:
                        stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
                        stg_lt = LayerTable.get_instance(_key=(self.stg_layer.id, stg_t.id))
                        srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
                        srci_col = Column.get_instance(_key=(srci_t.id, row.column_name))
                        bmap_process_name = BMAP_PROCESS_NAME_TEMPLATE.format(set_id=data_set.set_code, src_table_name=stg_t.table_name, column_name=srci_col.column_name, domain_id=srci_col.domain.domain_code)
                        bmap_view_name = BMAP_VIEW_NAME_TEMPLATE.format(view_name=bmap_process_name)
                        bmap_v = Table(schema_id=self.txf_bmap_v_schema.id, table_name=bmap_view_name, table_kind='V', source_id=ds.id)
                        bmap_vl = LayerTable(layer_id=self.txf_bmap_layer.id, table_id=bmap_v.id)
                        Pipeline(src_lyr_table_id=stg_lt.id, tgt_lyr_table_id=domain.data_set.surrogate_table.id, lyr_view_id=bmap_vl.id, domain_id=srci_col.domain.id, name=bmap_process_name, distinct=True)
