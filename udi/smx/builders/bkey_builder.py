from config import BK_PROCESS_NAME_TEMPLATE, BK_VIEW_NAME_TEMPLATE
from functions import log_error_decorator, log_validation_error
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
    def build_tables(self, rows):
        for row in rows:
            self.extract_bkey_tables(row)

    def build_datasets(self, rows):
        for row in rows:
            self.extract_bkey_datasets(row)

    def build_domains(self, rows):
        for row in rows:
            self.extract_bkey_domains(row)

    def build_views(self, rows):
        for row in rows:
            self.extract_bkey_views(row)

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
                    log_validation_error(
                        "Duplicate BKEY dataset ID",
                        row=row,
                        problem=(
                            f"Dataset ID '{row.key_set_id}' is already assigned "
                            "to a different key set."
                        ),
                        action="The duplicate dataset definition was not added.",
                        impact="The affected BKEY set will not be available to dependent mappings.",
                    )
            if valid_ds:
                if set_table and surrogate_table:
                    DataSet(set_type_id=self.bkey_set_type.id, set_code=row.key_set_id, set_table_id=set_table.id, surrogate_table_id=surrogate_table.id)
                    surrogate_table.is_bkey = True
                else:
                    log_validation_error(
                    "Invalid BKEY table definition",
                    row=row,
                    problem=(
                        f"Key set table '{row.key_set_name}' or surrogate table "
                        f"'{row.physical_table}' could not be found."
                    ),
                    action="The BKEY dataset was not created.",
                    impact="Mappings that use this key set may be skipped.",
                )

    @log_error_decorator()
    def extract_bkey_domains(self, row):
        data_set = DataSet.get_instance(_key=(self.bkey_set_type.id, row.key_set_id))
        if data_set and row.key_domain_id:
            Domain(data_set_id=data_set.id, domain_code=row.key_domain_id, domain_name=row.key_domain_name)
        else:
            log_validation_error(
                "Invalid BKEY dataset or domain ID",
                row=row,
                problem=(
                    f"Dataset ID '{row.key_set_id}' was not found or domain ID "
                    f"'{row.key_domain_id}' is empty or invalid."
                ),
                action="The domain definition was not added.",
                impact="Key mappings that depend on this domain may be unavailable.",
            )

    @log_error_decorator()
    def extract_bkey_views(self, row):
        if row.natural_key != '' and row.key_domain_name != '':
            ds = DataSource.get_instance(_key=row.schema)
            if not ds:
                log_validation_error(
                    "Invalid source name",
                    row=row,
                    problem=f"Source '{row.schema}' is not defined in the System sheet.",
                    action="The BKEY transformation view was not created.",
                    impact="The affected source mapping will be missing from the generated output.",
                )
            else:
                data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                if not data_set:
                    log_validation_error(
                        "Invalid key set",
                        row=row,
                        problem=f"Key set '{row.key_set_name}' is not defined in the BKEY sheet.",
                        action="The BKEY transformation view was not created.",
                        impact="The affected key mapping will be skipped.",
                    )
                else:
                    domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                    if not domain:
                        log_validation_error(
                            "Invalid key domain",
                            row=row,
                            problem=(
                                f"Domain '{row.key_domain_name}' is not defined for "
                                f"key set '{row.key_set_name}'."
                            ),
                            action="The BKEY transformation view was not created.",
                            impact="The affected column will not receive this key-domain mapping.",
                        )
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
                            log_validation_error(
                        "Invalid key set",
                        row=row,
                        problem=f"Key set '{row.key_set_name}' is not defined in the BKEY sheet.",
                        action="The BKEY transformation view was not created.",
                        impact="The affected key mapping will be skipped.",
                    )
