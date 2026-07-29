from __future__ import annotations

from model import (
    Column,
    ColumnMapping,
    Credential,
    DataBaseEngine,
    DataSet,
    DataSetType,
    DataSource,
    DataType,
    Domain,
    DomainValue,
    Filter,
    GroupBy,
    Ip,
    JoinOn,
    JoinWith,
    Layer,
    LayerTable,
    Pipeline,
    Schema,
    Server,
    Table,
)
from model_cache import RelationCache


_INSTALLED = False


def _relation(owner, name, related_class, factory):
    return RelationCache.get(
        owner,
        name,
        related_class.count_instances(),
        factory,
    )


def install_model_acceleration() -> None:
    """Replace repeated registry scans with versioned relationship caches.

    This keeps the original model classes and public property names unchanged.
    It is installed before any SMX objects are constructed, so builders and SQL
    generation automatically use the accelerated properties.
    """
    global _INSTALLED
    if _INSTALLED:
        return

    Server.ips = property(
        lambda self: _relation(
            self,
            "ips",
            Ip,
            lambda: [ip for ip in Ip.get_all_instances() if ip._server_id == self.id],
        )
    )

    DataBaseEngine.schemas = property(
        lambda self: _relation(
            self,
            "schemas",
            Schema,
            lambda: [schema for schema in Schema.get_all_instances() if schema._db_id == self.id],
        )
    )
    DataBaseEngine.data_types = property(
        lambda self: _relation(
            self,
            "data_types",
            DataType,
            lambda: [data_type for data_type in DataType.get_all_instances() if data_type._db_id == self.id],
        )
    )
    DataBaseEngine.credentials = property(
        lambda self: _relation(
            self,
            "credentials",
            Credential,
            lambda: [credential for credential in Credential.get_all_instances() if credential._db_engine_id == self.id],
        )
    )

    Schema.tables = property(
        lambda self: _relation(
            self,
            "tables",
            Table,
            lambda: [table for table in Table.get_all_instances() if table._schema_id == self.id],
        )
    )
    Schema.kind_T_tables = property(
        lambda self: [table for table in self.tables if table.table_kind == "T"]
    )
    Schema.kind_V_tables = property(
        lambda self: [table for table in self.tables if table.table_kind == "V"]
    )
    Schema.layers = property(
        lambda self: list({layer for table in self.tables for layer in table.layers})
    )

    DataSource.tables = property(
        lambda self: _relation(
            self,
            "tables",
            Table,
            lambda: [table for table in Table.get_all_instances() if table._source_id == self.id],
        )
    )

    Table.columns = property(
        lambda self: _relation(
            self,
            "columns",
            Column,
            lambda: [column for column in Column.get_all_instances() if column._table_id == self.id],
        )
    )
    Table.surrogate_data_sets = property(
        lambda self: _relation(
            self,
            "surrogate_data_sets",
            DataSet,
            lambda: [data_set for data_set in DataSet.get_all_instances() if data_set._surrogate_table_id == self.id],
        )
    )
    Table.layers = property(
        lambda self: _relation(
            self,
            "layers",
            LayerTable,
            lambda: [layer_table.layer for layer_table in LayerTable.get_all_instances() if layer_table._table_id == self.id],
        )
    )

    def table_pipeline(table):
        def factory():
            layer_table_ids = {
                layer_table.id
                for layer_table in LayerTable.get_all_instances()
                if layer_table._table_id == table.id
            }
            for pipeline in Pipeline.get_all_instances():
                if pipeline._lyr_view_id in layer_table_ids:
                    return pipeline
            return None

        version = LayerTable.count_instances() + Pipeline.count_instances()
        return RelationCache.get(table, "pipeline", version, factory)

    Table.pipeline = property(table_pipeline)

    DataSetType.data_sets = property(
        lambda self: _relation(
            self,
            "data_sets",
            DataSet,
            lambda: [data_set for data_set in DataSet.get_all_instances() if data_set._set_type_id == self.id],
        )
    )
    DataSet.domains = property(
        lambda self: _relation(
            self,
            "domains",
            Domain,
            lambda: [domain for domain in Domain.get_all_instances() if domain._data_set_id == self.id],
        )
    )

    def data_set_by_name(cls, set_type_id: int, set_name: str):
        version = cls.count_instances()

        def factory():
            return {
                (data_set._set_type_id, data_set.set_table.table_name): data_set
                for data_set in cls.get_all_instances()
            }

        index = RelationCache.get_global("dataset_by_type_and_name", version, factory)
        return index.get((set_type_id, str(set_name).upper()))

    DataSet.get_by_name = classmethod(data_set_by_name)

    Domain.values = property(
        lambda self: _relation(
            self,
            "values",
            DomainValue,
            lambda: [value for value in DomainValue.get_all_instances() if value._domain_id == self.id],
        )
    )

    Layer.layer_tables = property(
        lambda self: _relation(
            self,
            "layer_tables",
            LayerTable,
            lambda: [layer_table for layer_table in LayerTable.get_all_instances() if layer_table._layer_id == self.id],
        )
    )

    LayerTable.src_pipelines = property(
        lambda self: _relation(
            self,
            "src_pipelines",
            Pipeline,
            lambda: [pipeline for pipeline in Pipeline.get_all_instances() if pipeline._src_lyr_table_id == self.id],
        )
    )
    LayerTable.tgt_pipelines = property(
        lambda self: _relation(
            self,
            "tgt_pipelines",
            Pipeline,
            lambda: [pipeline for pipeline in Pipeline.get_all_instances() if pipeline._tgt_lyr_table_id == self.id],
        )
    )

    Pipeline.column_mapping = property(
        lambda self: _relation(
            self,
            "column_mapping",
            ColumnMapping,
            lambda: [mapping for mapping in ColumnMapping.get_all_instances() if mapping._pipeline_id == self.id],
        )
    )
    Pipeline.group_by = property(
        lambda self: _relation(
            self,
            "group_by",
            GroupBy,
            lambda: [group_by for group_by in GroupBy.get_all_instances() if group_by._pipeline_id == self.id],
        )
    )
    Pipeline.group_by_col_names = property(
        lambda self: [group_by.column.column_name for group_by in self.group_by]
    )
    Pipeline._filters = property(
        lambda self: _relation(
            self,
            "filters",
            Filter,
            lambda: [filter_item.filter_expr for filter_item in Filter.get_all_instances() if filter_item._pipeline_id == self.id],
        )
    )
    Pipeline.join_with = property(
        lambda self: _relation(
            self,
            "join_with",
            JoinWith,
            lambda: [join for join in JoinWith.get_all_instances() if join._pipeline_id == self.id],
        )
    )

    JoinWith.join_on = property(
        lambda self: _relation(
            self,
            "join_on",
            JoinOn,
            lambda: next(
                (join_on for join_on in JoinOn.get_all_instances() if join_on._join_with_id == self.id),
                None,
            ),
        )
    )

    _INSTALLED = True
