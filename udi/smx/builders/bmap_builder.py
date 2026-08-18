from config import BMAP_PROCESS_NAME_TEMPLATE, BMAP_VIEW_NAME_TEMPLATE
from functions import (
    get_issue_reference,
    log_error_decorator,
    log_validation_error,
    register_issue_reference,
)
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
    def build_tables(self, rows):
        for row in rows:
            self.extract_bmap_tables(row)

    def build_datasets(self, rows):
        for row in rows:
            self.extract_bmap_datasets(row)

    def build_domains(self, rows):
        for row in rows:
            self.extract_bmap_domains(row)

    def build_values(self, rows):
        for row in rows:
            self.extract_bmap_values(row)

    def build_views(self, rows):
        for row in rows:
            self.extract_bmap_views(row)

    def configure_data_types(self, int_data_type, vchar_data_type):
        self.int_data_type = int_data_type
        self.vchar_data_type = vchar_data_type

    def configure_data_sets(self, bmaps_data_sets):
        self.bmaps_data_sets = tuple(bmaps_data_sets)
        self.bmaps_data_sets_by_table = {
            data_set.set_table.table_name: data_set
            for data_set in self.bmaps_data_sets
        }

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
        if row.physical_table == '':
            return

        surrogate_table = Table.get_instance(
            _key=(self.bmap_t_schema.id, row.physical_table)
        )
        set_table = Table.get_instance(
            _key=(self.core_t_schema.id, row.code_set_name)
        )
        existing_data_set = DataSet.get_by_name(
            self.bmap_set_type.id, row.code_set_name
        )

        if existing_data_set and existing_data_set.set_code != row.code_set_id:
            issue_id = log_validation_error(
                "Duplicate BMAP dataset ID",
                row=row,
                affected_object=row.code_set_name,
                problem=(
                    f"Dataset ID '{row.code_set_id}' is already assigned "
                    "to a different code set."
                ),
                action="The duplicate dataset definition was not added.",
                impact=(
                    "The affected BMAP code set will not be available to "
                    "dependent mappings."
                ),
            )
            register_issue_reference(
                "bmap_code_set", row.code_set_name, issue_id
            )
            register_issue_reference(
                "bmap_dataset_id", row.code_set_id, issue_id
            )
            return

        if set_table and row.code_set_id and surrogate_table:
            DataSet(
                set_type_id=self.bmap_set_type.id,
                set_code=row.code_set_id,
                set_table_id=set_table.id,
                surrogate_table_id=surrogate_table.id,
            )
            surrogate_table.is_bmap = True
            return

        missing_objects = []
        if not set_table:
            missing_objects.append(f"code set table '{row.code_set_name}'")
        if not surrogate_table:
            missing_objects.append(f"surrogate table '{row.physical_table}'")
        if not row.code_set_id:
            missing_objects.append("dataset ID")

        issue_id = log_validation_error(
            "Invalid BMAP table definition",
            row=row,
            affected_object=row.code_set_name,
            problem=(
                "The following required BMAP object(s) could not be resolved: "
                + ", ".join(missing_objects)
                + "."
            ),
            action="The BMAP dataset was not created.",
            impact="Mappings that use this code set may be skipped.",
        )
        register_issue_reference(
            "bmap_code_set", row.code_set_name, issue_id
        )
        register_issue_reference(
            "bmap_dataset_id", row.code_set_id, issue_id
        )

    @log_error_decorator()
    def extract_bmap_domains(self, row):
        data_set = DataSet.get_instance(
            _key=(self.bmap_set_type.id, row.code_set_id)
        )

        if not data_set:
            related_issue_id = get_issue_reference(
                "bmap_dataset_id", row.code_set_id
            )
            log_validation_error(
                "BMAP domain could not be created",
                row=row,
                affected_object=row.code_domain_name or row.code_domain_id,
                problem=(
                    f"Parent dataset ID '{row.code_set_id}' was not found, so "
                    f"domain ID '{row.code_domain_id}' could not be created."
                ),
                related_issue_id=related_issue_id,
                related_cause=(
                    f"The parent BMAP dataset '{row.code_set_id}' was not created."
                ) if related_issue_id else None,
                action="The domain definition was not added.",
                impact="Code mappings that depend on this domain may be unavailable.",
            )
            return

        if row.code_domain_id in (None, ''):
            log_validation_error(
                "Invalid BMAP domain ID",
                row=row,
                affected_object=row.code_domain_name or "<blank>",
                problem=(
                    f"Domain ID is empty for BMAP dataset ID "
                    f"'{row.code_set_id}'."
                ),
                action="The domain definition was not added.",
                impact="Code mappings that depend on this domain may be unavailable.",
            )
            return

        Domain(
            data_set_id=data_set.id,
            domain_code=row.code_domain_id,
            domain_name=row.code_domain_name,
        )

    @log_error_decorator()
    def extract_bmap_values(self, row):
        data_set = self.bmaps_data_sets_by_table.get(
            str(row.code_set_name).upper()
        )
        if data_set:
            domain = Domain.get_instance(
                _key=(data_set.id, row.code_domain_id)
            )
            if domain:
                DomainValue(
                    domain_id=domain.id,
                    source_key=row.source_code,
                    edw_key=row.edw_code,
                    description=row.description,
                )

    @log_error_decorator()
    def extract_bmap_views(self, row):
        if row.code_domain_name != '':
            ds = DataSource.get_instance(_key=row.schema)
            if not ds:
                log_validation_error(
                    "Invalid source name",
                    row=row,
                    problem=f"Source '{row.schema}' is not defined in the System sheet.",
                    action="The BMAP transformation view was not created.",
                    impact="The affected source mapping will be missing from the generated output.",
                )
            else:
                data_set = DataSet.get_by_name(self.bmap_set_type.id, row.code_set_name)
                if not data_set:
                    log_validation_error(
                        "Invalid code set",
                        row=row,
                        problem=f"Code set '{row.code_set_name}' is not defined in the BMAP sheet.",
                        action="The BMAP transformation view was not created.",
                        impact="The affected code-domain mapping will be skipped.",
                    )
                else:
                    domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.code_domain_name)
                    if not domain:
                        log_validation_error(
                            "Invalid code domain",
                            row=row,
                            problem=(
                                f"Domain '{row.code_domain_name}' is not defined for "
                                f"code set '{row.code_set_name}'."
                            ),
                            action="The BMAP transformation view was not created.",
                            impact="The affected column will not receive this domain mapping.",
                        )
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
