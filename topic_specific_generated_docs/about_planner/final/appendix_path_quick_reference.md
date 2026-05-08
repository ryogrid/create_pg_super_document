# Appendix: Path Quick Reference

This appendix is the single-page lookup table that maps every PostgreSQL
`Path` subtype to its struct definition, its constructor, its cost
function, the executor `Plan` node it ultimately becomes, and the
`createplan.c` routine that performs the conversion. It is the "Rosetta
stone" linking the three abstraction layers the optimizer manipulates:
the **algebraic Path tree** (in `pathnodes.h`), the **executable Plan
tree** (in `plannodes.h`), and the **runtime nodes** that actually
execute the query (in the executor). Use this table any time you see a
`T_*` NodeTag in EXPLAIN output, in a stack trace, or in a debug print
and you need to jump quickly to the responsible source code.

The data is sourced from `stage1/path_type_inventory.txt`, augmented by
direct re-reads of `src/include/nodes/pathnodes.h`,
`src/include/nodes/plannodes.h`, and
`src/backend/optimizer/plan/createplan.c`. Every line number reflects
the head of HEAD at the time of writing and was confirmed against the
local source tree.

---

## Path Reference Table

| NodeTag | Struct | pathnodes.h:line | Constructor (`pathnode.c` / `*path*.c`) | Cost fn (`costsize.c`) | Plan counterpart | Plan creator (`createplan.c`) | Category | Notes |
|---|---|---|---|---|---|---|---|---|
| `T_Path` (T_SeqScan)              | `Path`             | 1621 | `create_seqscan_path`              | `cost_seqscan`              | `SeqScan`              | `create_seqscan_plan`              | scan         | Plain `Path` with `pathtype = T_SeqScan`; the parent's `reltarget` is reused. |
| `T_Path` (T_SampleScan)           | `Path`             | 1621 | `create_samplescan_path`           | `cost_samplescan`           | `SampleScan`           | `create_samplescan_plan`           | scan         | Used for `TABLESAMPLE`; selectivity comes from the sampler. |
| `T_IndexPath`                     | `IndexPath`        | 1709 | `create_index_path`                | `cost_index`                | `IndexScan` / `IndexOnlyScan` | `create_indexscan_plan`     | scan / index | `pathtype` discriminates ordinary vs index-only. |
| `T_BitmapHeapPath`                | `BitmapHeapPath`   | 1784 | `create_bitmap_heap_path`          | `cost_bitmap_heap_scan`     | `BitmapHeapScan`       | `create_bitmap_scan_plan`          | scan / index | Wraps a tree of `IndexPath`/`BitmapAndPath`/`BitmapOrPath`. |
| `T_BitmapAndPath`                 | `BitmapAndPath`    | 1796 | `create_bitmap_and_path`           | `cost_bitmap_and_node`      | `BitmapAnd`            | `create_bitmap_scan_plan` (recurses) | index    | Only valid as a child of `BitmapHeapPath`. |
| `T_BitmapOrPath`                  | `BitmapOrPath`     | 1809 | `create_bitmap_or_path`            | `cost_bitmap_or_node`       | `BitmapOr`             | `create_bitmap_scan_plan` (recurses) | index    | Only valid as a child of `BitmapHeapPath`. |
| `T_TidPath`                       | `TidPath`          | 1823 | `create_tidscan_path`              | `cost_tidscan`              | `TidScan`              | `create_tidscan_plan`              | scan         | OR-list of `CTID = const`; created by `create_tidscan_paths`. |
| `T_TidRangePath`                  | `TidRangePath`     | 1835 | `create_tidrangescan_path`         | `cost_tidrangescan`         | `TidRangeScan`         | `create_tidrangescan_plan`         | scan         | AND-list of `CTID <op> const`; introduced for range scans on TID. |
| `T_SubqueryScanPath`              | `SubqueryScanPath` | 1849 | `create_subqueryscan_path`         | `cost_subqueryscan`         | `SubqueryScan`         | `create_subqueryscan_plan`         | scan         | Sub-query has its own `PlannerInfo`; subpath belongs to that root. |
| `T_ForeignPath`                   | `ForeignPath`      | 1869 | `create_foreignscan_path` / `create_foreign_join_path` / `create_foreign_upper_path` | inline (FDW computes) | `ForeignScan`        | `create_foreignscan_plan`          | scan / FDW   | Cost is filled by the FDW callback; `GetForeignPlan` builds the executor node. |
| `T_CustomPath`                    | `CustomPath`       | 1905 | extension callback `PlanCustomPath` | inline (extension)         | `CustomScan`           | `create_customscan_plan`           | scan / extension | Path is built and costed by the extension; core just dispatches. |
| `T_Path` (T_FunctionScan)         | `Path`             | 1621 | `create_functionscan_path`         | `cost_functionscan`         | `FunctionScan`         | `create_functionscan_plan`         | scan         | Plain `Path`; one row per SRF tuple. |
| `T_Path` (T_TableFuncScan)        | `Path`             | 1621 | `create_tablefuncscan_path`        | `cost_tablefuncscan`        | `TableFuncScan`        | `create_tablefuncscan_plan`        | scan         | XMLTABLE / JSON_TABLE. |
| `T_Path` (T_ValuesScan)           | `Path`             | 1621 | `create_valuesscan_path`           | `cost_valuesscan`           | `ValuesScan`           | `create_valuesscan_plan`           | scan         | `VALUES (...)`-typed RTE. |
| `T_Path` (T_CteScan)              | `Path`             | 1621 | `create_ctescan_path`              | `cost_ctescan`              | `CteScan`              | `create_ctescan_plan`              | scan         | Reads from a sibling CTE worktable. |
| `T_Path` (T_NamedTuplestoreScan)  | `Path`             | 1621 | `create_namedtuplestorescan_path`  | `cost_namedtuplestorescan`  | `NamedTuplestoreScan`  | `create_namedtuplestorescan_plan`  | scan         | Trigger transition tables. |
| `T_Path` (T_Result, RTE_RESULT)   | `Path`             | 1621 | `create_resultscan_path`           | `cost_resultscan`           | `Result`               | `create_resultscan_plan`           | scan         | Used when an entire FROM clause folds to constants. |
| `T_Path` (T_WorkTableScan)        | `Path`             | 1621 | `create_worktablescan_path`        | `cost_seqscan` (approx.)    | `WorkTableScan`        | `create_worktablescan_plan`        | scan         | Recursive-CTE recursive term. |
| `T_AppendPath`                    | `AppendPath`       | 1931 | `create_append_path`               | `cost_append`               | `Append`               | `create_append_plan`               | append       | Inheritance / partitioning / UNION ALL; `IS_DUMMY_APPEND` marks empty rels. |
| `T_MergeAppendPath`               | `MergeAppendPath`  | 1955 | `create_merge_append_path`         | `cost_merge_append`         | `MergeAppend`          | `create_merge_append_plan`         | append       | Preserves total order across children. |
| `T_GroupResultPath`               | `GroupResultPath`  | 1969 | `create_group_result_path`         | inline                      | `Result`               | `create_group_result_plan`         | upper        | Degenerate `GROUP BY` producing exactly one row. |
| `T_MaterialPath`                  | `MaterialPath`     | 1981 | `create_material_path`             | `cost_material`             | `Material`             | `create_material_plan`             | helper       | Used to cache an inner side that lacks mark/restore. |
| `T_MemoizePath`                   | `MemoizePath`      | 1992 | `create_memoize_path`              | `cost_memoize_rescan`       | `Memoize`              | `create_memoize_plan`              | helper       | Caches parameterized inner-side results in nestloops. |
| `T_UniquePath`                    | `UniquePath`       | 2027 | `create_unique_path`               | inline                      | `Unique` or `Agg(HASHED)` | `create_unique_plan`             | upper        | `umethod` chooses NOOP / SORT / HASH; HASH builds an `Agg`. |
| `T_GatherPath`                    | `GatherPath`       | 2041 | `create_gather_path`               | `cost_gather`               | `Gather`               | `create_gather_plan`               | parallel     | Adds the leader-side overhead to a partial path. |
| `T_GatherMergePath`               | `GatherMergePath`  | 2053 | `create_gather_merge_path`         | `cost_gather_merge`         | `GatherMerge`          | `create_gather_merge_plan`         | parallel     | Preserves sort order across workers. |
| `T_NestPath`                      | `NestPath`         | 2092 | `create_nestloop_path`             | `initial_cost_nestloop` / `final_cost_nestloop` | `NestLoop`     | `create_nestloop_plan`             | join         | Wraps a `JoinPath`; supports parameterized inner. |
| `T_MergePath`                     | `MergePath`        | 2132 | `create_mergejoin_path`            | `initial_cost_mergejoin` / `final_cost_mergejoin` | `MergeJoin` | `create_mergejoin_plan`           | join         | May implicitly add inner `Sort` and inner `Material`. |
| `T_HashPath`                      | `HashPath`         | 2151 | `create_hashjoin_path`             | `initial_cost_hashjoin` / `final_cost_hashjoin` | `HashJoin` (with inner `Hash`) | `create_hashjoin_plan` | join | Inner side becomes the build side; `num_batches` drives spill. |
| `T_ProjectionPath`                | `ProjectionPath`   | 2173 | `create_projection_path` / `create_set_projection_path` | inline | `Result` (or merged into child) | `create_projection_plan` | helper | `dummypp` = true means projection is folded into the input plan. |
| `T_ProjectSetPath`                | `ProjectSetPath`   | 2185 | `create_set_projection_path`       | inline                      | `ProjectSet`           | `create_project_set_plan`          | helper       | Required when the targetlist contains set-returning functions. |
| `T_SortPath`                      | `SortPath`         | 2199 | `create_sort_path`                 | `cost_sort`                 | `Sort`                 | `create_sort_plan`                 | upper        | Cannot project; targetlist must match input. |
| `T_IncrementalSortPath`           | `IncrementalSortPath` | 2211 | `create_incremental_sort_path`  | `cost_incremental_sort`     | `IncrementalSort`      | `create_incrementalsort_plan`      | upper        | Inherits from `SortPath`; uses presorted leading columns. |
| `T_GroupPath`                     | `GroupPath`        | 2225 | `create_group_path`                | `cost_group`                | `Group`                | `create_group_plan`                | upper        | Sort-based `GROUP BY` (input must already be sorted). |
| `T_UpperUniquePath`               | `UpperUniquePath`  | 2239 | `create_upper_unique_path`         | inline                      | `Unique`               | `create_upper_unique_plan`         | upper        | Used by `DISTINCT` over already-sorted input. |
| `T_AggPath`                       | `AggPath`          | 2253 | `create_agg_path`                  | `cost_agg`                  | `Agg`                  | `create_agg_plan`                  | upper        | `aggstrategy` selects PLAIN / SORTED / HASHED / MIXED. |
| `T_GroupingSetsPath`              | `GroupingSetsPath` | 2295 | `create_groupingsets_path`         | `cost_agg` (per rollup)     | `Agg` (chained)        | `create_groupingsets_plan`         | upper        | Builds one `Agg` per rollup with a `Sort`/`HashAgg` input. |
| `T_MinMaxAggPath`                 | `MinMaxAggPath`    | 2308 | `create_minmaxagg_path`            | inline (`build_minmax_path`) | `Result`              | `create_minmaxagg_plan`            | upper        | Optimization that uses an indexable `LIMIT 1` per aggregate. |
| `T_WindowAggPath`                 | `WindowAggPath`    | 2318 | `create_windowagg_path`            | `cost_windowagg`            | `WindowAgg`            | `create_windowagg_plan`            | upper        | One node per `WindowClause`. |
| `T_SetOpPath`                     | `SetOpPath`        | 2332 | `create_setop_path`                | inline                      | `SetOp`                | `create_setop_plan`                | upper        | INTERSECT / EXCEPT (UNION uses `Append`). |
| `T_RecursiveUnionPath`            | `RecursiveUnionPath` | 2347 | `create_recursiveunion_path`     | `cost_recursive_union`      | `RecursiveUnion`       | `create_recursiveunion_plan`       | upper        | Recursive CTE driver; uses `wtParam` for the worktable. |
| `T_LockRowsPath`                  | `LockRowsPath`     | 2360 | `create_lockrows_path`             | inline                      | `LockRows`             | `create_lockrows_plan`             | DML helper   | `SELECT ... FOR UPDATE/SHARE`. |
| `T_ModifyTablePath`               | `ModifyTablePath`  | 2375 | `create_modifytable_path`          | inline                      | `ModifyTable`          | `create_modifytable_plan`          | DML          | INSERT / UPDATE / DELETE / MERGE; cost is mostly the subpath. |
| `T_LimitPath`                     | `LimitPath`        | 2400 | `create_limit_path`                | inline (uses parent rows)   | `Limit`                | `create_limit_plan`                | upper        | Honors `LimitOption` (FETCH FIRST WITH TIES). |

### Helper structures (not Path subtypes; included for navigation)

| NodeTag | Struct | pathnodes.h:line | Constructor | Notes |
|---|---|---|---|---|
| `T_PathKey`        | `PathKey`        | 1463 | `make_canonical_pathkey` / `make_pathkey_from_sortinfo` / `make_pathkey_from_sortop` | Sort-order encoding; references an `EquivalenceClass`. |
| (none)             | `PathTarget`     | 1528 | `create_pathtarget` / `make_pathtarget_from_tlist` | Per-Path output column list with cost+width. |
| `T_ParamPathInfo`  | `ParamPathInfo`  | 1575 | `get_baserel_parampathinfo` / `get_joinrel_parampathinfo` / `get_appendrel_parampathinfo` | Records outer-rel parameterization for a path. |
| (none)             | `JoinPathExtraData` | 3230 | populated inline by `add_paths_to_joinrel` | Per-join scratch shared by all `try_*_path` helpers. |

---

## How to read this table

- **NodeTag**: The runtime tag stored in `Path.type` (set by the
  `makeNode()` constructor). Plain `Path` is reused for several scan
  node types — those rows show the discriminating `pathtype` in
  parentheses. Always compare against `path->type` (struct identity)
  rather than `path->pathtype` (executor-flavor) when navigating Path
  code; compare against `path->pathtype` when matching to a
  `plannodes.h` tag.
- **Struct / pathnodes.h:line**: The line in
  `src/include/nodes/pathnodes.h` where the typedef begins. All Path
  subtypes embed `Path` as their first member (`JoinPath` is itself
  embedded by `NestPath`/`MergePath`/`HashPath`); so the upcast macro
  `&xxxPath->path` always yields the generic `Path *`.
- **Constructor**: The factory in `pathnode.c` (or for index/scan paths,
  in `path/indxpath.c` / `path/tidpath.c`). Constructors compute the
  cost in two ways: either by calling a dedicated `cost_*` function
  (column "Cost fn"), or **inline** (`startup_cost`/`total_cost` are
  assigned directly inside the constructor — common for cheap or
  trivially-derivable paths).
- **Cost fn**: The function in `costsize.c` that fills
  `path->startup_cost`, `path->total_cost`, and `path->rows`. Some join
  paths split this into `initial_cost_*` (a cheap pre-screen used by
  `try_*_path`) and `final_cost_*` (the full estimate, called from the
  constructor).
- **Plan counterpart**: The `Plan` node tag emitted into the executable
  tree. For some Path types this is a 1:1 mapping; for others
  (`MergePath` ➜ `MergeJoin` plus implicit `Sort`/`Material`,
  `UniquePath` ➜ `Unique` or `Agg(HASHED)`) several plan nodes can be
  produced from a single Path.
- **Plan creator**: The static function in `createplan.c` that
  consumes the Path and produces the `Plan`. Dispatch happens in
  `create_plan_recurse` (createplan.c:389) via a switch on
  `path->pathtype`.
- **Category**: A coarse grouping that mirrors the `path_catalog/`
  layout in Stage 2 (`scan_paths.md`, `join_paths.md`, `upper_paths.md`,
  `parallel_paths.md`, `append_and_partition_paths.md`,
  `modify_paths.md`).

### Cross-references to module documents

- Scan paths → [`./06_base_relation_paths.md`](./06_base_relation_paths.md), [`./07_index_paths.md`](./07_index_paths.md)
- Join paths → [`./08_join_paths_and_search.md`](./08_join_paths_and_search.md)
- Upper paths (sort/agg/limit) → [`./16_plan_creation_and_setrefs.md`](./16_plan_creation_and_setrefs.md)
- Parallel paths → [`./14_parallel_planning.md`](./14_parallel_planning.md)
- Append / partitionwise → [`./13_inheritance_and_partitioning.md`](./13_inheritance_and_partitioning.md)
- DML paths → [`./16_plan_creation_and_setrefs.md`](./16_plan_creation_and_setrefs.md)
- Plan creation → [`./16_plan_creation_and_setrefs.md`](./16_plan_creation_and_setrefs.md)
- Cost model → [`./09_cost_model_and_selectivity.md`](./09_cost_model_and_selectivity.md)

### Notes and gotchas

1. Plain `Path` covers many scan NodeTags. Always inspect
   `path->pathtype`, not just `IsA(path, Path)`.
2. `BitmapAndPath`/`BitmapOrPath` only exist nested inside a
   `BitmapHeapPath`; they have no standalone plan counterpart.
3. `IncrementalSortPath` *embeds* `SortPath` (not just `Path`), so
   `IsA(path, SortPath)` is **false** for an `IncrementalSortPath`.
4. `ModifyTablePath` cost is overwhelmingly its subpath cost; the
   ModifyTable node itself only adds per-tuple write overhead computed
   inline by `create_modifytable_path`.
5. `ForeignPath`/`CustomPath` cost is determined by the FDW or
   extension that produced the path. Core code never re-costs them.
6. Sort variants: a free-standing `Sort` is created by `create_sort_path`,
   but mergejoin can introduce `Sort` nodes implicitly via
   `MergePath.outersortkeys` / `innersortkeys`.
