from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


Rows = tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class FoundationStage:
    systems: Rows = ()
    data_types: Rows = ()
    staging_tables: Rows = ()
    core_tables: Rows = ()
    core_columns: Rows = ()
    history_table_names: tuple[str, ...] = ()
    lookup_table_names: tuple[str, ...] = ()
    all_core_table_names: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class KeyMappingStage:
    bkey_tables: Rows = ()
    bkey_datasets: Rows = ()
    bkey_domains: Rows = ()
    bkey_views: Rows = ()
    bmap_tables: Rows = ()
    bmap_datasets: Rows = ()
    bmap_domains: Rows = ()
    bmap_values: Rows = ()
    bmap_views: Rows = ()


@dataclass(frozen=True, slots=True)
class StgSrciStage:
    columns: Rows = ()
    source_view_mappings: Rows = ()
    srci_view_mappings: Rows = ()


@dataclass(frozen=True, slots=True)
class CoreStage:
    table_mappings: Rows = ()
    column_mappings_by_name: dict[str, Rows] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PreparedModelData:
    foundation: FoundationStage
    key_mapping: KeyMappingStage
    stg_srci: StgSrciStage
    core: CoreStage
