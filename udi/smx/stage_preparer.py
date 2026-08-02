from __future__ import annotations

from collections import defaultdict

from config import UNIFIED_SOURCE_SYSTEMS

from .builders.base import SmxComponent
from .stage_data import (
    CoreStage,
    FoundationStage,
    KeyMappingStage,
    PreparedModelData,
    StgSrciStage,
)


class DuckDbStagePreparer(SmxComponent):
    """Prepare all builder inputs in DuckDB before Python model construction.

    The class performs a fixed number of set-based queries. It never queries
    DuckDB from a builder loop. Deduplication and workbook joins are performed
    here, then each final result is fetched only once.
    """

    def prepare(self, source_name) -> PreparedModelData:
        source_names = self._resolve_source_names(source_name)
        self._create_run_tables(source_names)
        self._create_definition_tables()

        foundation = self._load_foundation_stage()
        key_mapping = self._load_key_mapping_stage()
        stg_srci = self._load_stg_srci_stage()
        core = self._load_core_stage()

        return PreparedModelData(
            foundation=foundation,
            key_mapping=key_mapping,
            stg_srci=stg_srci,
            core=core,
        )

    @staticmethod
    def _resolve_source_names(source_name) -> list[str] | None:
        if not source_name:
            return None

        requested = list(source_name) if isinstance(source_name, list) else [source_name]
        # Preserve order and avoid mutating the source list supplied by the UI.
        return list(dict.fromkeys([*requested, *UNIFIED_SOURCE_SYSTEMS]))

    def _create_run_tables(self, source_names: list[str] | None) -> None:
        if source_names:
            in_clause, params = self._in_clause([str(name).lower() for name in source_names])
            source_filter = f'WHERE lower("schema") {in_clause}'
        else:
            source_filter = ""
            params = []

        self.workbook.create_temp_table(
            "udi_run_system",
            f'SELECT * FROM "system" {source_filter}',
            params,
        )
        self.workbook.create_temp_table(
            "udi_run_stg",
            f'SELECT * FROM "stg_tables" {source_filter}',
            params,
        )

        if self._table_exists("table_mapping"):
            if source_names:
                mapping_filter = (
                    f'WHERE lower("source") {in_clause} '
                    'AND upper("layer") = ?'
                )
                mapping_params = [*params, "CORE"]
            else:
                mapping_filter = 'WHERE upper("layer") = ?'
                mapping_params = ["CORE"]

            self.workbook.create_temp_table(
                "udi_run_table_mapping",
                f'SELECT * FROM "table_mapping" {mapping_filter}',
                mapping_params,
            )
        else:
            self.workbook.drop_temp_table("udi_run_table_mapping")

    def _create_definition_tables(self) -> None:
        if self._table_exists("bkey"):
            self.workbook.create_temp_table(
                "udi_run_bkey",
                """
                SELECT b.*
                FROM "bkey" b
                INNER JOIN (
                    SELECT DISTINCT lower("key_domain_name") AS domain_name
                    FROM udi_run_stg
                    WHERE coalesce("key_domain_name", '') <> ''
                ) stg
                    ON lower(b."key_domain_name") = stg.domain_name
                """,
            )
        else:
            self.workbook.drop_temp_table("udi_run_bkey")

        if self._table_exists("bmap"):
            self.workbook.create_temp_table(
                "udi_run_bmap",
                """
                SELECT b.*
                FROM "bmap" b
                INNER JOIN (
                    SELECT DISTINCT lower("code_domain_name") AS domain_name
                    FROM udi_run_stg
                    WHERE coalesce("code_domain_name", '') <> ''
                ) stg
                    ON lower(b."code_domain_name") = stg.domain_name
                """,
            )
        else:
            self.workbook.drop_temp_table("udi_run_bmap")

        required_core_queries = []
        if self.workbook.temp_table_exists("udi_run_table_mapping"):
            required_core_queries.append(
                """
                SELECT "target_table_name" AS table_name, min("_udi_row_num") AS sort_key
                FROM udi_run_table_mapping
                WHERE coalesce("target_table_name", '') <> ''
                GROUP BY "target_table_name"
                """
            )
        if self.workbook.temp_table_exists("udi_run_bkey"):
            required_core_queries.append(
                """
                SELECT "key_set_name" AS table_name, min("_udi_row_num") AS sort_key
                FROM udi_run_bkey
                WHERE coalesce("key_set_name", '') <> ''
                GROUP BY "key_set_name"
                """
            )
        if self.workbook.temp_table_exists("udi_run_bmap"):
            required_core_queries.append(
                """
                SELECT "code_set_name" AS table_name, min("_udi_row_num") AS sort_key
                FROM udi_run_bmap
                WHERE coalesce("code_set_name", '') <> ''
                GROUP BY "code_set_name"
                """
            )

        if required_core_queries:
            union_sql = " UNION ALL ".join(required_core_queries)
            self.workbook.create_temp_table(
                "udi_required_core_tables",
                f"""
                SELECT table_name, min(sort_key) AS sort_key
                FROM ({union_sql}) required
                GROUP BY table_name
                """,
            )
        else:
            self.workbook.drop_temp_table("udi_required_core_tables")

        if self._table_exists("core_tables") and self.workbook.temp_table_exists(
            "udi_required_core_tables"
        ):
            self.workbook.create_temp_table(
                "udi_run_core",
                """
                SELECT c.*
                FROM "core_tables" c
                INNER JOIN udi_required_core_tables required
                    ON lower(c."table_name") = lower(required.table_name)
                """,
            )
        else:
            self.workbook.drop_temp_table("udi_run_core")

        if (
            self._table_exists("column_mapping")
            and self.workbook.temp_table_exists("udi_run_table_mapping")
        ):
            self.workbook.create_temp_table(
                "udi_run_column_mapping",
                """
                SELECT cm.*
                FROM "column_mapping" cm
                INNER JOIN (
                    SELECT DISTINCT lower("mapping_name") AS mapping_name
                    FROM udi_run_table_mapping
                    WHERE coalesce("mapping_name", '') <> ''
                ) mappings
                    ON lower(cm."mapping_name") = mappings.mapping_name
                """,
            )
        else:
            self.workbook.drop_temp_table("udi_run_column_mapping")

        if (
            self._table_exists("bmap_values")
            and self.workbook.temp_table_exists("udi_run_bmap")
        ):
            self.workbook.create_temp_table(
                "udi_run_bmap_values",
                """
                SELECT values_table.*
                FROM "bmap_values" values_table
                INNER JOIN (
                    SELECT DISTINCT "code_domain_id"
                    FROM udi_run_bmap
                    WHERE coalesce("code_domain_id", '') <> ''
                ) domains
                    ON values_table."code_domain_id" = domains."code_domain_id"
                """,
            )
        else:
            self.workbook.drop_temp_table("udi_run_bmap_values")

    def _load_foundation_stage(self) -> FoundationStage:
        systems = self.workbook.fetch_named_rows(
            """
            SELECT "schema", min("_udi_row_num") AS source_row_number
            FROM udi_run_system
            GROUP BY "schema"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="SystemRow",
        )

        staging_tables = self.workbook.fetch_named_rows(
            """
            SELECT "schema", "table_name", min("_udi_row_num") AS source_row_number
            FROM udi_run_stg
            WHERE coalesce("table_name", '') <> ''
            GROUP BY "schema", "table_name"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="StagingTableRow",
        )

        if self.workbook.temp_table_exists("udi_run_core"):
            core_tables = self.workbook.fetch_named_rows(
                """
                SELECT "table_name", min("_udi_row_num") AS source_row_number
                FROM udi_run_core
                WHERE coalesce("table_name", '') <> ''
                GROUP BY "table_name"
                ORDER BY min("_udi_row_num")
                """,
                row_type_name="CoreTableRow",
            )
            core_columns = self.workbook.fetch_named_rows(
                """
                SELECT
                    "table_name", "column_name", "data_type", "pk",
                    "mandatory", "historization_key",
                    min("_udi_row_num") AS source_row_number
                FROM udi_run_core
                GROUP BY
                    "table_name", "column_name", "data_type", "pk",
                    "mandatory", "historization_key"
                ORDER BY min("_udi_row_num")
                """,
                row_type_name="CoreColumnRow",
            )
        else:
            core_tables = ()
            core_columns = ()

        data_type_parts = [
            """
            SELECT "data_type", min("_udi_row_num") AS sort_key, 0 AS source_order
            FROM udi_run_stg
            WHERE coalesce("data_type", '') <> ''
            GROUP BY "data_type"
            """
        ]
        if self.workbook.temp_table_exists("udi_run_core"):
            data_type_parts.append(
                """
                SELECT "data_type", min("_udi_row_num") AS sort_key, 1 AS source_order
                FROM udi_run_core
                WHERE coalesce("data_type", '') <> ''
                GROUP BY "data_type"
                """
            )

        data_types = self.workbook.fetch_named_rows(
            f"""
            SELECT "data_type"
            FROM ({' UNION ALL '.join(data_type_parts)}) data_types
            GROUP BY "data_type"
            ORDER BY min(source_order), min(sort_key)
            """,
            row_type_name="DataTypeRow",
        )

        if self.workbook.temp_table_exists("udi_run_table_mapping"):
            history_rows = self.workbook.fetch_named_rows(
                """
                SELECT "target_table_name"
                FROM udi_run_table_mapping
                WHERE upper("historization_algorithm") = 'HISTORY'
                  AND coalesce("target_table_name", '') <> ''
                GROUP BY "target_table_name"
                ORDER BY min("_udi_row_num")
                """,
                row_type_name="HistoryTableRow",
            )
            history_table_names = tuple(row.target_table_name for row in history_rows)
        else:
            history_table_names = ()

        if self.workbook.temp_table_exists("udi_run_bmap"):
            lookup_rows = self.workbook.fetch_named_rows(
                """
                SELECT "code_set_name"
                FROM udi_run_bmap
                WHERE coalesce("code_set_name", '') <> ''
                GROUP BY "code_set_name"
                ORDER BY min("_udi_row_num")
                """,
                row_type_name="LookupTableRow",
            )
            lookup_table_names = tuple(row.code_set_name for row in lookup_rows)
        else:
            lookup_table_names = ()

        if self.workbook.temp_table_exists("udi_required_core_tables"):
            all_core_rows = self.workbook.fetch_named_rows(
                """
                SELECT table_name
                FROM udi_required_core_tables
                ORDER BY sort_key
                """,
                row_type_name="RequiredCoreTableRow",
            )
            all_core_table_names = tuple(row.table_name for row in all_core_rows)
        else:
            all_core_table_names = ()

        return FoundationStage(
            systems=systems,
            data_types=data_types,
            staging_tables=staging_tables,
            core_tables=core_tables,
            core_columns=core_columns,
            history_table_names=history_table_names,
            lookup_table_names=lookup_table_names,
            all_core_table_names=all_core_table_names,
        )

    def _load_key_mapping_stage(self) -> KeyMappingStage:
        if self.workbook.temp_table_exists("udi_run_bkey"):
            bkey_tables = self.workbook.fetch_named_rows(
                """
                SELECT "physical_table", min("_udi_row_num") AS source_row_number
                FROM udi_run_bkey
                WHERE coalesce("physical_table", '') <> ''
                GROUP BY "physical_table"
                ORDER BY min("_udi_row_num")
                """,
                row_type_name="BKeyTableRow",
            )
            bkey_datasets = self.workbook.fetch_named_rows(
                """
                SELECT "key_set_name", "key_set_id", "physical_table",
                    "_udi_row_num" AS source_row_number
                FROM udi_run_bkey
                WHERE coalesce("key_set_id", '') <> ''
                QUALIFY row_number() OVER (
                    PARTITION BY "key_set_id"
                    ORDER BY "_udi_row_num"
                ) = 1
                ORDER BY "_udi_row_num"
                """,
                row_type_name="BKeyDataSetRow",
            )
            bkey_domains = self.workbook.fetch_named_rows(
                """
                SELECT "key_set_id", "key_domain_id", "key_domain_name",
                    "_udi_row_num" AS source_row_number
                FROM udi_run_bkey
                WHERE coalesce("key_set_id", '') <> ''
                  AND coalesce("key_domain_id", '') <> ''
                QUALIFY row_number() OVER (
                    PARTITION BY "key_set_id", "key_domain_id"
                    ORDER BY "_udi_row_num"
                ) = 1
                ORDER BY "_udi_row_num"
                """,
                row_type_name="BKeyDomainRow",
            )
        else:
            bkey_tables = bkey_datasets = bkey_domains = ()

        bkey_views = self.workbook.fetch_named_rows(
            """
            SELECT
                "schema", "table_name", "natural_key", "column_name",
                "key_set_name", "key_domain_name", "bkey_filter",
                min("_udi_row_num") AS source_row_number
            FROM udi_run_stg
            WHERE coalesce("natural_key", '') <> ''
              AND coalesce("key_domain_name", '') <> ''
            GROUP BY
                "schema", "table_name", "natural_key", "column_name",
                "key_set_name", "key_domain_name", "bkey_filter"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="BKeyViewRow",
        )

        if self.workbook.temp_table_exists("udi_run_bmap"):
            bmap_tables = self.workbook.fetch_named_rows(
                """
                SELECT "physical_table", min("_udi_row_num") AS source_row_number
                FROM udi_run_bmap
                WHERE coalesce("physical_table", '') <> ''
                GROUP BY "physical_table"
                ORDER BY min("_udi_row_num")
                """,
                row_type_name="BMapTableRow",
            )
            bmap_datasets = self.workbook.fetch_named_rows(
                """
                SELECT "code_set_name", "code_set_id", "physical_table",
                    "_udi_row_num" AS source_row_number
                FROM udi_run_bmap
                WHERE coalesce("code_set_id", '') <> ''
                QUALIFY row_number() OVER (
                    PARTITION BY "code_set_id"
                    ORDER BY "_udi_row_num"
                ) = 1
                ORDER BY "_udi_row_num"
                """,
                row_type_name="BMapDataSetRow",
            )
            bmap_domains = self.workbook.fetch_named_rows(
                """
                SELECT "code_set_id", "code_domain_id", "code_domain_name",
                    "_udi_row_num" AS source_row_number
                FROM udi_run_bmap
                WHERE coalesce("code_set_id", '') <> ''
                  AND coalesce("code_domain_id", '') <> ''
                QUALIFY row_number() OVER (
                    PARTITION BY "code_set_id", "code_domain_id"
                    ORDER BY "_udi_row_num"
                ) = 1
                ORDER BY "_udi_row_num"
                """,
                row_type_name="BMapDomainRow",
            )
        else:
            bmap_tables = bmap_datasets = bmap_domains = ()

        if self.workbook.temp_table_exists("udi_run_bmap_values"):
            bmap_values = self.workbook.fetch_named_rows(
                """
                SELECT
                    "code_set_name", "code_domain_id", "edw_code",
                    "source_code", "description",
                    "_udi_row_num" AS source_row_number
                FROM udi_run_bmap_values
                ORDER BY "_udi_row_num"
                """,
                row_type_name="BMapValueRow",
            )
        else:
            bmap_values = ()

        # Kept as prepared input for parity/future use. The stable original
        # population workflow defines extract_bmap_views but does not call it.
        bmap_views = self.workbook.fetch_named_rows(
            """
            SELECT
                "schema", "table_name", "column_name", "code_set_name",
                "code_domain_name", min("_udi_row_num") AS source_row_number
            FROM udi_run_stg
            WHERE coalesce("code_domain_name", '') <> ''
            GROUP BY
                "schema", "table_name", "column_name", "code_set_name",
                "code_domain_name"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="BMapViewRow",
        )

        return KeyMappingStage(
            bkey_tables=bkey_tables,
            bkey_datasets=bkey_datasets,
            bkey_domains=bkey_domains,
            bkey_views=bkey_views,
            bmap_tables=bmap_tables,
            bmap_datasets=bmap_datasets,
            bmap_domains=bmap_domains,
            bmap_values=bmap_values,
            bmap_views=bmap_views,
        )

    def _load_stg_srci_stage(self) -> StgSrciStage:
        columns = self.workbook.fetch_named_rows(
            """
            SELECT
                "table_name", "column_name", "data_type", "mandatory",
                "natural_key", "pk", "key_set_name", "key_domain_name",
                "code_set_name", "code_domain_name", min("_udi_row_num") AS source_row_number
            FROM udi_run_stg
            GROUP BY
                "table_name", "column_name", "data_type", "mandatory",
                "natural_key", "pk", "key_set_name", "key_domain_name",
                "code_set_name", "code_domain_name"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="StgSrciColumnRow",
        )

        source_view_mappings = self.workbook.fetch_named_rows(
            """
            SELECT
                "schema", "table_name", "natural_key", "column_name",
                "column_transformation_rule",
                min("_udi_row_num") AS source_row_number
            FROM udi_run_stg
            WHERE coalesce("natural_key", '') = ''
              AND coalesce("column_name", '') <> ''
            GROUP BY
                "schema", "table_name", "natural_key", "column_name",
                "column_transformation_rule"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="SourceViewMappingRow",
        )

        srci_view_mappings = self.workbook.fetch_named_rows(
            """
            SELECT
                "schema", "table_name", "column_name", "natural_key",
                "key_set_name", "key_domain_name", "code_set_name",
                "code_domain_name", min("_udi_row_num") AS source_row_number
            FROM udi_run_stg
            WHERE coalesce("column_name", '') <> ''
            GROUP BY
                "schema", "table_name", "column_name", "natural_key",
                "key_set_name", "key_domain_name", "code_set_name",
                "code_domain_name"
            ORDER BY min("_udi_row_num")
            """,
            row_type_name="SrciViewMappingRow",
        )

        return StgSrciStage(
            columns=columns,
            source_view_mappings=source_view_mappings,
            srci_view_mappings=srci_view_mappings,
        )

    def _load_core_stage(self) -> CoreStage:
        if not self.workbook.temp_table_exists("udi_run_table_mapping"):
            return CoreStage()

        table_mappings = self.workbook.fetch_named_rows(
            """
            SELECT
                "target_table_name", "source", "main_source",
                "main_source_alias", "filter_criterion", "mapping_name",
                "join", "historization_algorithm", "historization_columns",
                "layer", "_udi_row_num" AS source_row_number
            FROM udi_run_table_mapping
            QUALIFY row_number() OVER (
                PARTITION BY
                    "target_table_name", "source", "main_source",
                    "main_source_alias", "filter_criterion", "mapping_name",
                    "join", "historization_algorithm", "historization_columns",
                    "layer"
                ORDER BY "_udi_row_num"
            ) = 1
            ORDER BY "_udi_row_num"
            """,
            row_type_name="TableMappingRow",
        )

        grouped: dict[str, list] = defaultdict(list)
        if self.workbook.temp_table_exists("udi_run_column_mapping"):
            column_mappings = self.workbook.fetch_named_rows(
                """
                SELECT
                    "mapping_name", "column_name", "mapped_to_table",
                    "mapped_to_column", "transformation_rule",
                    "transformation_type",
                    "_udi_row_num" AS source_row_number
                FROM udi_run_column_mapping
                QUALIFY row_number() OVER (
                    PARTITION BY
                        "mapping_name", "column_name", "mapped_to_table",
                        "mapped_to_column", "transformation_rule",
                        "transformation_type"
                    ORDER BY "_udi_row_num"
                ) = 1
                ORDER BY "_udi_row_num"
                """,
                row_type_name="ColumnMappingRow",
            )
            for row in column_mappings:
                grouped[str(row.mapping_name).lower()].append(row)

        return CoreStage(
            table_mappings=table_mappings,
            column_mappings_by_name={
                key: tuple(value) for key, value in grouped.items()
            },
        )
