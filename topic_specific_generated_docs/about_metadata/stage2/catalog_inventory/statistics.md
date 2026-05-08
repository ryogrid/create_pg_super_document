# Catalog Inventory: Statistics

## pg_statistic (2619) — per-column statistics

- **Identity**: 2619, header `pg_statistic.h`, no .dat.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  Oid           starelid;
  int16         staattnum;
  bool          stainherit;
  float4        stanullfrac;
  int32         stawidth;
  float4        stadistinct;
  int16         stakind1;
  int16         stakind2;
  int16         stakind3;
  int16         stakind4;
  int16         stakind5;
  Oid           staop1;       /* operator OIDs for the slots */
  /* ... staop2..staop5 ... */
  Oid           stacoll1;
  /* ... stacoll2..stacoll5 ... */
  /* float4[]  stanumbers1..5  */
  /* anyarray stavalues1..5    */
  ```

  Each `stakind` slot represents one *kind* of statistic (e.g.,
  STATISTIC_KIND_MCV, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_CORRELATION,
  STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM, ...). The 5-slot design lets
  pg_statistic store multiple kinds without separate rows.

- **Indexes**: `pg_statistic_relid_att_inh_index` (2696, unique,
  (starelid, staattnum, stainherit)).
- **Modification API**:
  - `update_attstats` (analyze.c) — bulk rewrite of pg_statistic rows for
    one relation.
  - `RemoveStatistics` (heap.c) — delete all pg_statistic for a relation
    (called by heap_drop_with_catalog).
- **Cache identifier**: `STATRELATTINH`.
- **Dependencies**: starelid → pg_class (DEPENDENCY_AUTO).
- **Bootstrap status**: no.

## pg_statistic_ext (3381) — extended statistics objects

- **Identity**: 3381, `pg_statistic_ext.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           stxrelid;
  NameData      stxname;
  Oid           stxnamespace;
  Oid           stxowner;
  /* int2[] stxkeys, char[] stxkind, text[] stxstattarget, pg_node_tree stxexprs */
  ```
  `stxkind` is an array of one-letter codes:
  - `'d'` = ndistinct (multivariate ndistinct)
  - `'f'` = functional dependencies
  - `'m'` = MCVs (multi-column most-common values)
  - `'e'` = expression statistics

- **Indexes**:
  - `pg_statistic_ext_oid_index` (3380, unique).
  - `pg_statistic_ext_name_index` (3997, unique, (stxname, stxnamespace)).
  - `pg_statistic_ext_relid_index` (3379, (stxrelid)).
- **Modification API**:
  - `CreateStatistics` (statscmds.c).
  - `RemoveStatisticsExtById`.
  - `AlterStatistics`.
- **Cache identifier**: `STATEXTOID`, `STATEXTNAMENSP`.
- **Dependencies**: stxrelid → pg_class, stxowner → pg_authid.

## pg_statistic_ext_data (3429) — computed extended stats data

- **Identity**: 3429, `pg_statistic_ext_data.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           stxoid;
  bool          stxdinherit;
  /* pg_ndistinct stxdndistinct, pg_dependencies stxddependencies,
     pg_mcv_list stxdmcv, pg_statistic[] stxdexpr */
  ```
- **Indexes**: `pg_statistic_ext_data_stxoid_inh_index` (3430, unique,
  (stxoid, stxdinherit)).
- **Modification API**:
  - `statext_store` (extended_stats.c) — written by ANALYZE.
  - `RemoveStatisticsExtById` removes both pg_statistic_ext and
    pg_statistic_ext_data rows.
- **Cache identifier**: `STATEXTDATASTXOID`.
- **Dependencies**: stxoid → pg_statistic_ext (DEPENDENCY_INTERNAL).

## ANALYZE write path

```
ANALYZE
 -> commands/analyze.c::do_analyze_rel
     -> sample tuples via TableAmRoutine::scan_analyze_next_tuple
     -> compute stats per column (compute_attribute_stats)
     -> CatalogTupleUpdate / Insert into pg_statistic
     -> compute_extension_stats (extended_stats.c)
         -> CatalogTupleUpdate into pg_statistic_ext_data
     -> heap_inplace_update_and_unlock (pg_class.relpages, reltuples,
                                        relallvisible)
```

## Use during planning

Planner's selectivity estimator (`selfuncs.c`) reads pg_statistic via
`get_attstatsslot()`, which uses `SearchSysCache3(STATRELATTINH, ...)`.
Extended stats are read via `statext_clauselist_selectivity()`.

## Cross-references

- `component_catalog_modification_apis.md` — heap_inplace_update for
  pg_class statistics.
- `catalog_inventory/core_relations.md` — pg_class.
