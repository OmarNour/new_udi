from config import BK_PROCESS_NAME_TEMPLATE, BK_VIEW_NAME_TEMPLATE
from functions import log_error_decorator, log_validation_error
from model import Column, ColumnMapping, DataSet, Domain, LayerTable, Pipeline, Table, GroupBy

from .base import SmxComponent


class SrciBuilder(SmxComponent):
    def build_view_mappings(self, rows):
        for row in rows:
            self.extract_srci_view_columns(row)

    @log_error_decorator()
    def extract_srci_view_columns(self, row):
        if row.column_name != '':
            stg_t = Table.get_instance(_key=(self.stg_t_schema.id, row.table_name))
            stg_t_col = None
            srci_t = Table.get_instance(_key=(self.srci_t_schema.id, row.table_name))
            srci_t_col = Column.get_instance(_key=(srci_t.id, row.column_name))
            if row.natural_key != '':
                if row.key_set_name != '':
                    data_set = DataSet.get_by_name(self.bkey_set_type.id, row.key_set_name)
                    if not data_set:
                        log_validation_error(
                            "Invalid key set",
                            row=row,
                            problem=(
                                f"Key set '{row.key_set_name}' is not defined in "
                                "the BKEY sheet."
                            ),
                            action="The SRCI key transformation mapping was not created.",
                            impact=f"Column '{row.table_name}.{row.column_name}' has no BKEY mapping.",
                        )
                    else:
                        domain = Domain.get_by_name(data_set_id=data_set.id, domain_name=row.key_domain_name)
                        if not domain:
                            log_validation_error(
                                "Invalid key domain",
                                row=row,
                                problem=(
                                    f"Key domain '{row.key_domain_name}' is not "
                                    f"defined for set '{row.key_set_name}'."
                                ),
                                action="The SRCI key transformation mapping was not created.",
                                impact=f"Column '{row.table_name}.{row.column_name}' has no BKEY mapping.",
                            )
                        else:
                            txf_process_name = BK_PROCESS_NAME_TEMPLATE.format(set_id=data_set.set_code, src_table_name=stg_t.table_name, column_name=srci_t_col.column_name, domain_id=srci_t_col.domain.domain_code)
                            txf_view_name = BK_VIEW_NAME_TEMPLATE.format(view_name=txf_process_name)
                            txf_v = Table.get_instance(_key=(self.txf_bkey_v_schema.id, txf_view_name))
                            txf_bkey_lt = LayerTable.get_instance(_key=(self.txf_bkey_layer.id, txf_v.id))
                            bkey_txf_pipeline = Pipeline.get_instance(_key=txf_bkey_lt.id)
                            source_key_col = Column.get_instance(_key=(domain.data_set.surrogate_table.id, 'source_key'))
                            ColumnMapping(pipeline_id=bkey_txf_pipeline.id, col_seq=0, src_col_id=None, tgt_col_id=source_key_col.id, src_col_trx=row.natural_key)
                            GroupBy(pipeline_id=bkey_txf_pipeline.id, col_id=source_key_col.id)
            else:
                stg_t_col = Column.get_instance(_key=(stg_t.id, row.column_name))
                if not stg_t_col:
                    log_validation_error(
                        "Invalid staging column",
                        row=row,
                        problem=(
                            f"Column '{row.column_name}' was not found in staging "
                            f"table '{row.table_name}'."
                        ),
                        action="The staging-to-SRCI mapping was not created for this column.",
                        impact="The generated SRCI view may be incomplete.",
                    )
            if not srci_t_col:
                log_validation_error(
                        "Invalid staging column",
                        row=row,
                        problem=(
                            f"Column '{row.column_name}' was not found in staging "
                            f"table '{row.table_name}'."
                        ),
                        action="The staging-to-SRCI mapping was not created for this column.",
                        impact="The generated SRCI view may be incomplete.",
                    )
            else:
                srci_v = Table.get_instance(_key=(self.srci_v_schema.id, row.table_name))
                srci_vl = LayerTable.get_instance(_key=(self.srci_layer.id, srci_v.id))
                srci_v_pipeline = Pipeline.get_instance(_key=srci_vl.id)
                ColumnMapping(pipeline_id=srci_v_pipeline.id, col_seq=0, src_col_id=stg_t_col.id if stg_t_col else None, tgt_col_id=srci_t_col.id, src_col_trx=row.natural_key if row.natural_key else None)
