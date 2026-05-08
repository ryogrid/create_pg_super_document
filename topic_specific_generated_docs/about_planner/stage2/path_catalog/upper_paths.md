# Path Catalog: Upper-Relation Paths

This catalog documents the Path subtypes produced for upper relations — that is, the post-join layers that perform sorting, grouping, aggregation, set operations, projection, materialization, memoization, and limiting. These are constructed by `grouping_planner()` and its helpers in `src/backend/optimizer/plan/planner.c`, attached to `RelOptInfo`s of `reloptkind = RELOPT_UPPER_REL`.

The naming convention matters: `UpperUniquePath` and `UniquePath` both compile down to `Unique` plans, but they exist for different use cases (the former is for DISTINCT/distinct-aggregate at the top of the plan; the latter is for semi-join unique-ification of the inner side).

---

## SortPath (T_SortPath)

**Identity**: struct `SortPath` defined at `src/include/nodes/pathnodes.h:2199`.

```c
typedef struct SortPath
{
    Path        path;
    Path       *subpath;
} SortPath;
```

**Purpose**: Represents an explicit Sort step. Sort keys are exactly `path.pathkeys`.

**Constructor**: `create_sort_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *pathkeys, double limit_tuples)` at `src/backend/optimizer/util/pathnode.c:3000`.
   - Cost computation: inline `cost_sort(&pathnode->path, root, pathkeys, subpath->total_cost, subpath->rows, subpath->pathtarget->width, 0.0, work_mem, limit_tuples)`.

**Cost function**: `cost_sort()` at `src/backend/optimizer/path/costsize.c:2124`.
   - Formula summary: in-memory quicksort if input fits in `work_mem`, otherwise external merge sort. CPU cost is `2.0 * cpu_operator_cost * N * log2(N)` baseline. Disk cost added for external sort.
   - GUC dependencies: `work_mem`, `cpu_operator_cost`, `seq_page_cost`, `random_page_cost`.

**Pathkey behavior**: Output pathkeys = the requested sort order.

**Parameterization**: No (`param_info = NULL` always; sorts are above any join layer).

**Parallel-aware**: No directly, but `parallel_safe` propagates from the subpath. The subpath may be a partial path that workers each sort.

**Plan counterpart**: `create_sort_plan()` at `src/backend/optimizer/plan/createplan.c:2181` produces `Sort` (`plannodes.h:931`).

**When chosen**: When upstream operations need a particular ordering and no equivalent ordering is freely available.

**Example SQL**: `SELECT * FROM t ORDER BY x;` → `Sort  Sort Key: x -> Seq Scan on t`

---

## IncrementalSortPath (T_IncrementalSortPath)

**Identity**: struct `IncrementalSortPath` defined at `src/include/nodes/pathnodes.h:2211`. Embeds `SortPath` as its first member.

```c
typedef struct IncrementalSortPath
{
    SortPath    spath;
    int         nPresortedCols;
} IncrementalSortPath;
```

**Purpose**: Represents an incremental sort: input is already sorted by some prefix of the desired pathkeys; the executor groups by that prefix and sorts each group fully.

**Constructor**: `create_incremental_sort_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *pathkeys, int presorted_keys, double limit_tuples)` at `src/backend/optimizer/util/pathnode.c:2951`.
   - Cost computation: inline `cost_incremental_sort()`.

**Cost function**: `cost_incremental_sort()` at `src/backend/optimizer/path/costsize.c:1986`. Estimates the cost as `cost_sort(group_size)` per group, multiplied by the number of groups; smaller groups → much less work.

**Pathkey behavior**: Output pathkeys = full pathkey list.

**Parameterization**: No.

**Parallel-aware**: Same as SortPath.

**Plan counterpart**: `create_incrementalsort_plan()` at `src/backend/optimizer/plan/createplan.c:2215` produces `IncrementalSort` (`plannodes.h:955`).

**When chosen**: When input pathkeys form a prefix of the desired order. Cheaper than a full Sort because each group is small. Often appears under MergeAppend or above an indexed scan that produces partial ordering.

**Example SQL**: `SELECT * FROM t ORDER BY a, b;` (with index on a alone) → `Incremental Sort  Sort Key: a, b  Presorted Key: a`

---

## AggPath (T_AggPath)

**Identity**: struct `AggPath` defined at `src/include/nodes/pathnodes.h:2253`.

```c
typedef struct AggPath
{
    Path        path;
    Path       *subpath;
    AggStrategy aggstrategy;        /* AGG_PLAIN/SORTED/HASHED/MIXED */
    AggSplit    aggsplit;           /* splitting mode for partial aggregation */
    Cardinality numGroups;
    uint64      transitionSpace;
    List       *groupClause;
    List       *qual;               /* HAVING quals */
} AggPath;
```

**Purpose**: Represents grouping aggregation — `GROUP BY` plus aggregate functions, or a plain whole-table aggregate. Strategy is one of AGG_PLAIN (single group), AGG_SORTED (input must be presorted), AGG_HASHED, or AGG_MIXED (a combination, used inside GROUPING SETS only).

**Constructor**: `create_agg_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target, AggStrategy aggstrategy, AggSplit aggsplit, List *groupClause, List *qual, const AggClauseCosts *aggcosts, double numGroups)` at `src/backend/optimizer/util/pathnode.c:3155`.
   - Cost computation: inline `cost_agg()`.

**Cost function**: `cost_agg()` at `src/backend/optimizer/path/costsize.c:2650`.
   - Formula summary: For AGG_HASHED, includes hash table construction cost (`numGroups * transitionSpace`); for AGG_SORTED, requires presorted input. Adds aggregate transition function costs from `aggcosts`.

**Pathkey behavior**: For `AGG_SORTED`, preserves input pathkeys (truncated to `num_groupby_pathkeys`). Otherwise `NIL`.

**Parameterization**: No.

**Parallel-aware**: No directly, but `aggsplit` controls partial-aggregate behavior. A Gather or GatherMerge above may finalize partials.

**Plan counterpart**: `create_agg_plan()` at `src/backend/optimizer/plan/createplan.c:2309` produces `Agg` (`plannodes.h:996`).

**When chosen**: For any `GROUP BY` (when not GROUPING SETS) and any aggregate query without grouping. Multiple AggPaths (sorted vs. hashed) may compete.

**Example SQL**: `SELECT a, count(*) FROM t GROUP BY a;` → `HashAggregate Group Key: a -> Seq Scan on t`

---

## GroupingSetsPath (T_GroupingSetsPath)

**Identity**: struct `GroupingSetsPath` defined at `src/include/nodes/pathnodes.h:2295`.

```c
typedef struct GroupingSetsPath
{
    Path        path;
    Path       *subpath;
    AggStrategy aggstrategy;
    List       *rollups;            /* list of RollupData */
    List       *qual;
    uint64      transitionSpace;
} GroupingSetsPath;
```

**Purpose**: Represents GROUPING SETS / CUBE / ROLLUP — generates an Agg plan node with a chain of additional Agg nodes (one per rollup) in its `chain` field.

**Constructor**: `create_groupingsets_path(...)` at `src/backend/optimizer/util/pathnode.c:3237`. Iterates over the rollups list and accumulates costs of each rollup (with cost_agg for AGG_HASHED rollups, cost_sort + cost_agg for AGG_SORTED rollups).

**Cost function**: `cost_agg()` (`costsize.c:2650`), called once per rollup; results summed.

**Pathkey behavior**: Single-rollup AGG_SORTED preserves `group_pathkeys`; otherwise `NIL`.

**Parameterization**: Inherited from subpath.

**Parallel-aware**: No.

**Plan counterpart**: `create_groupingsets_plan()` at `src/backend/optimizer/plan/createplan.c:2393`. Builds the topmost `Agg` node with a `chain` of subsidiary `Agg` (and `Sort`) nodes; sets up `root->grouping_map` for setrefs.c to fix GroupingFunc nodes.

**When chosen**: For any query containing `GROUPING SETS`, `CUBE`, or `ROLLUP`.

**Example SQL**: `SELECT a, b, count(*) FROM t GROUP BY ROLLUP(a, b);` → `GroupAggregate -> ...`

---

## MinMaxAggPath (T_MinMaxAggPath)

**Identity**: struct `MinMaxAggPath` defined at `src/include/nodes/pathnodes.h:2308`.

```c
typedef struct MinMaxAggPath
{
    Path        path;
    List       *mmaggregates;       /* list of MinMaxAggInfo */
    List       *quals;              /* HAVING quals */
} MinMaxAggPath;
```

**Purpose**: Represents `MIN(col)` / `MAX(col)` evaluated by reading the first/last index entry — much cheaper than a full table aggregate when an index covers the column.

**Constructor**: `create_minmaxagg_path(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, List *mmaggregates, List *quals)` at `src/backend/optimizer/util/pathnode.c:3397`.
   - Cost computation: inline. Sums `mminfo->pathcost` from each `MinMaxAggInfo` (already computed by `build_minmax_path`) + tlist eval cost.

**Cost function**: None. Each `MinMaxAggInfo->path` is a pre-built single-row index scan whose cost is already known.

**Pathkey behavior**: Always `NIL` (single row).

**Parameterization**: No.

**Parallel-aware**: `parallel_safe = true` if all subpaths are parallel-safe; the Result node itself isn't parallelizable but the safety flag matters for outer-query usage.

**Plan counterpart**: `create_minmaxagg_plan()` at `src/backend/optimizer/plan/createplan.c:2551` produces a `Result` plan, with each min/max aggregate evaluated by a separate `InitPlan` (created via `SS_make_initplan_from_plan`). `setrefs.c` later replaces `Aggref` references in the surrounding plan with `Param` references to the InitPlan results (driven by `root->minmax_aggs`).

**When chosen**: For queries like `SELECT min(x) FROM t` where there's a usable B-tree index on x.

**Example SQL**: `SELECT min(id) FROM big;` → `Result  InitPlan 1: Limit -> Index Only Scan ... ORDER BY id LIMIT 1`

---

## WindowAggPath (T_WindowAggPath)

**Identity**: struct `WindowAggPath` defined at `src/include/nodes/pathnodes.h:2318`.

```c
typedef struct WindowAggPath
{
    Path        path;
    Path       *subpath;
    WindowClause *winclause;
    List       *qual;
    List       *runCondition;
    bool        topwindow;
} WindowAggPath;
```

**Purpose**: Represents window-function evaluation for one window clause. Multi-window queries chain WindowAggPaths.

**Constructor**: `create_windowagg_path(...)` at `src/backend/optimizer/util/pathnode.c:3485`.
   - Cost computation: inline `cost_windowagg()`.

**Cost function**: `cost_windowagg()` at `src/backend/optimizer/path/costsize.c:3068`. Per-tuple cost includes evaluating each window function plus the cost of buffering frame contents in tuplestore.

**Pathkey behavior**: Preserves input ordering.

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: `create_windowagg_plan()` at `src/backend/optimizer/plan/createplan.c:2617` produces `WindowAgg` (`plannodes.h:1038`).

**When chosen**: Any query with window functions.

**Example SQL**: `SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM t;` → `WindowAgg -> Sort by a, b -> Seq Scan on t`

---

## UniquePath (T_UniquePath)

**Identity**: struct `UniquePath` defined at `src/include/nodes/pathnodes.h:2027`.

```c
typedef struct UniquePath
{
    Path        path;
    Path       *subpath;
    UniquePathMethod umethod;       /* NOOP / HASH / SORT */
    List       *in_operators;       /* IN equality operators */
    List       *uniq_exprs;         /* expressions to make unique */
} UniquePath;
```

**Purpose**: Represents unique-ification of an inner subpath, used for converting semi-joins to inner joins on a unique inner side. The `umethod` field selects between (a) NOOP (input is already unique), (b) HASH (compile to HashAgg), and (c) SORT (compile to Sort+Unique).

**Constructor**: `create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, SpecialJoinInfo *sjinfo)` at `src/backend/optimizer/util/pathnode.c:1654`.
   - Cost computation: inline. Computes both sort-based and hash-based costs, picks `umethod` accordingly.
   - Caches result in `rel->cheapest_unique_path` since it may be invoked multiple times with the same inputs.

**Cost function**: None — costs computed inline using `cost_sort` and `cost_agg` dummies.

**Pathkey behavior**: Set conservatively (typically input pathkeys for SORT mode, NIL for HASH).

**Parameterization**: Same as input subpath.

**Parallel-aware**: No.

**Plan counterpart**: `create_unique_plan()` at `src/backend/optimizer/plan/createplan.c:1721` produces either:
   - `subplan` directly (UNIQUE_PATH_NOOP),
   - an `Agg` with `AGG_HASHED` (UNIQUE_PATH_HASH), or
   - a `Sort` + `Unique` (UNIQUE_PATH_SORT).

**When chosen**: When the join planner converts an `IN`/semi-join to an inner-join by first unique-ifying the inner side. See `add_paths_to_joinrel`'s handling of JOIN_UNIQUE_INNER/OUTER.

**Example SQL**: `SELECT * FROM a WHERE a.x IN (SELECT b.x FROM b);` (when planner picks unique-ification) — the inner `b` rel gets a UniquePath above its scan path before join.

---

## SetOpPath (T_SetOpPath)

**Identity**: struct `SetOpPath` defined at `src/include/nodes/pathnodes.h:2332`.

```c
typedef struct SetOpPath
{
    Path        path;
    Path       *subpath;
    SetOpCmd    cmd;                /* INTERSECT / EXCEPT, ALL or distinct */
    SetOpStrategy strategy;         /* SETOP_SORTED / SETOP_HASHED */
    List       *distinctList;
    AttrNumber  flagColIdx;
    int         firstFlag;
    Cardinality numGroups;
} SetOpPath;
```

**Purpose**: Implements `INTERSECT`/`EXCEPT` (with or without ALL). UNION is implemented via Append + (HashAgg or Sort+Unique) without a SetOp node.

**Constructor**: `create_setop_path(...)` at `src/backend/optimizer/util/pathnode.c:3555`.
   - Cost computation: inline. Adds `cpu_operator_cost * subpath->rows * numCols` for comparisons.

**Cost function**: None — inline.

**Pathkey behavior**: For SETOP_SORTED, preserves input pathkeys; for SETOP_HASHED, NIL.

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: `create_setop_plan()` at `src/backend/optimizer/plan/createplan.c:2720` produces `SetOp` (`plannodes.h:1217`).

**When chosen**: Whenever `INTERSECT` or `EXCEPT` appears in the query.

**Example SQL**: `SELECT a FROM t1 INTERSECT SELECT b FROM t2;` → `HashSetOp Intersect -> Append -> Seq Scan t1 + Seq Scan t2`

---

## RecursiveUnionPath (T_RecursiveUnionPath)

**Identity**: struct `RecursiveUnionPath` defined at `src/include/nodes/pathnodes.h:2347`.

```c
typedef struct RecursiveUnionPath
{
    Path        path;
    Path       *leftpath;           /* non-recursive term */
    Path       *rightpath;          /* recursive term */
    List       *distinctList;
    int         wtParam;
    Cardinality numGroups;
} RecursiveUnionPath;
```

**Purpose**: Represents `WITH RECURSIVE x AS (non_recursive_term UNION [ALL] recursive_term)`. Iterates the recursive term, accumulating tuples until no new ones are produced, optionally deduplicating.

**Constructor**: `create_recursiveunion_path(...)` at `src/backend/optimizer/util/pathnode.c:3617`.
   - Cost computation: inline `cost_recursive_union()`.

**Cost function**: `cost_recursive_union()` at `src/backend/optimizer/path/costsize.c:1813`. Cost of left + 10× the cost of right (heuristic for typical iteration counts) + dedup cost.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: `create_recursiveunion_plan()` at `src/backend/optimizer/plan/createplan.c:2756` produces `RecursiveUnion` (`plannodes.h:325`).

**When chosen**: For any `WITH RECURSIVE` CTE.

**Example SQL**: `WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<10) SELECT * FROM r;` → `CTE Scan -> RecursiveUnion -> Result + WorkTable Scan`

---

## LimitPath (T_LimitPath)

**Identity**: struct `LimitPath` defined at `src/include/nodes/pathnodes.h:2400`.

```c
typedef struct LimitPath
{
    Path        path;
    Path       *subpath;
    Node       *limitOffset;
    Node       *limitCount;
    LimitOption limitOption;        /* WITH TIES or exact number */
} LimitPath;
```

**Purpose**: Represents `LIMIT`/`OFFSET`/`FETCH FIRST ... WITH TIES`.

**Constructor**: `create_limit_path(...)` at `src/backend/optimizer/util/pathnode.c:3826`.
   - Cost computation: inline. Calls `adjust_limit_rows_costs()` to scale rows and proportionally compute startup/total cost.

**Cost function**: None — uses `adjust_limit_rows_costs()` (`pathnode.c:3881`).

**Pathkey behavior**: Inherits from subpath.

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: `create_limit_plan()` at `src/backend/optimizer/plan/createplan.c:2856` produces `Limit` (`plannodes.h:1270`). For WITH TIES, also extracts unique-key columns from the parse's sortClause.

**Example SQL**: `SELECT * FROM t LIMIT 10;` → `Limit -> Seq Scan on t`

---

## ProjectionPath (T_ProjectionPath)

**Identity**: struct `ProjectionPath` defined at `src/include/nodes/pathnodes.h:2173`.

```c
typedef struct ProjectionPath
{
    Path        path;
    Path       *subpath;
    bool        dummypp;            /* true if no separate Result needed */
} ProjectionPath;
```

**Purpose**: Represents a tlist computation step. The `dummypp` flag tells the plan creator whether a separate `Result` node is needed or whether the subpath can absorb the projection.

**Constructor**: `create_projection_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target)` at `src/backend/optimizer/util/pathnode.c:2685`. Also `create_set_projection_path()` at line 2882 for set-returning-function tlists (which produces a ProjectSetPath, not ProjectionPath; named confusingly).

**Cost function**: None — inline. Computes cost based on whether a separate Result node will be needed.

**Pathkey behavior**: Inherited from subpath.

**Parameterization**: No (`param_info = NULL` always — projections are above any join).

**Parallel-aware**: As subpath, AND-ed with `is_parallel_safe(target->exprs)`.

**Plan counterpart**: `create_projection_plan()` at `src/backend/optimizer/plan/createplan.c:2019`. May produce a `Result` node, or simply replace the subplan's targetlist when projection-capable. The decision is rechecked at plan time because earlier flags (CP_EXACT_TLIST etc.) may force the issue.

**Example SQL**: Whenever a Path's pathtarget differs from its subpath's pathtarget — e.g., `SELECT x+1 FROM t;` if the +1 evaluation can't fold into the SeqScan.

---

## ProjectSetPath (T_ProjectSetPath)

**Identity**: struct `ProjectSetPath` defined at `src/include/nodes/pathnodes.h:2185`.

```c
typedef struct ProjectSetPath
{
    Path        path;
    Path       *subpath;
} ProjectSetPath;
```

**Purpose**: Tlist evaluation that includes set-returning functions in the SELECT list (`SELECT generate_series(1,10), x FROM t`). Always requires a separate executor node.

**Constructor**: `create_set_projection_path()` at `src/backend/optimizer/util/pathnode.c:2882`.

**Cost function**: None — inline. Per-row cost is `cpu_tuple_cost + tlist_cost.per_tuple` per output tuple, where `tlist_rows` factors in the SRF expansion.

**Pathkey behavior**: Inherited from subpath (XXX comment in source flags this as questionable for SRFs).

**Parameterization**: No.

**Parallel-aware**: As subpath, AND-ed with target safety.

**Plan counterpart**: `create_project_set_plan()` at `src/backend/optimizer/plan/createplan.c:1613` produces `ProjectSet` (`plannodes.h:208`).

**Example SQL**: `SELECT id, generate_series(1,3) FROM t;` → `ProjectSet -> Seq Scan on t`

---

## MaterialPath (T_MaterialPath)

**Identity**: struct `MaterialPath` defined at `src/include/nodes/pathnodes.h:1981`.

```c
typedef struct MaterialPath
{
    Path        path;
    Path       *subpath;
} MaterialPath;
```

**Purpose**: Represents a Material node — a tuplestore-backed cache that allows mark/restore and avoids re-executing an expensive subpath on rescan.

**Constructor**: `create_material_path(RelOptInfo *rel, Path *subpath)` at `src/backend/optimizer/util/pathnode.c:1566`.
   - Cost computation: inline `cost_material(&pathnode->path, subpath->startup_cost, subpath->total_cost, subpath->rows, subpath->pathtarget->width)`.

**Cost function**: `cost_material()` at `src/backend/optimizer/path/costsize.c:2453`. Storage cost based on `work_mem` overflow to disk; per-tuple read cost.

**Pathkey behavior**: Inherits subpath pathkeys.

**Parameterization**: Inherits from subpath.

**Parallel-aware**: As subpath.

**Plan counterpart**: `create_material_plan()` at `src/backend/optimizer/plan/createplan.c:1639` produces `Material` (`plannodes.h:880`).

**When chosen**: As inner side of certain mergejoins (forced via `final_cost_mergejoin`'s `materialize_inner`), or to enable mark/restore on input that can't natively support it, or when the planner expects many rescans of an expensive subplan.

**Example SQL**: `SELECT * FROM t1, t2 WHERE t1.x = t2.x;` (with mergejoin and inner needs materializing) → `Merge Join -> Sort -> Material -> Sort`

---

## MemoizePath (T_MemoizePath)

**Identity**: struct `MemoizePath` defined at `src/include/nodes/pathnodes.h:1992`.

```c
typedef struct MemoizePath
{
    Path        path;
    Path       *subpath;
    List       *hash_operators;
    List       *param_exprs;
    bool        singlerow;
    bool        binary_mode;
    Cardinality calls;
    uint32      est_entries;
} MemoizePath;
```

**Purpose**: Represents a Memoize node — caches results of a parameterized inner subpath keyed by the bound parameter values, so repeated nestloop scans with the same key avoid rescanning.

**Constructor**: `create_memoize_path(...)` at `src/backend/optimizer/util/pathnode.c:1598`.
   - Cost computation: inline. Initial cost is `subpath cost + cpu_tuple_cost`; the more sophisticated rescan-cost analysis happens later in `cost_memoize_rescan()`.

**Cost function**: `cost_memoize_rescan()` at `src/backend/optimizer/path/costsize.c:2509`. Models hit ratio based on number of distinct parameter values vs. expected calls; sets `est_entries` based on `work_mem` capacity.

**Pathkey behavior**: Inherited from subpath.

**Parameterization**: Yes — that's the whole point.

**Parallel-aware**: No (`parallel_aware = false`).

**Plan counterpart**: `create_memoize_plan()` at `src/backend/optimizer/plan/createplan.c:1667` produces `Memoize` (`plannodes.h:889`).

**When chosen**: Inner side of nestloops where the parameter values repeat enough to make caching worthwhile. Considered automatically by `add_paths_to_joinrel` when the parameterization patterns suggest reuse.

**Example SQL**: `SELECT * FROM big_outer JOIN small_inner ON outer.k = inner.k;` (with few distinct k's in outer) → `Nested Loop -> Seq Scan big_outer -> Memoize -> Index Scan small_inner`

---

## GroupResultPath (T_GroupResultPath)

**Identity**: struct `GroupResultPath` defined at `src/include/nodes/pathnodes.h:1969`.

```c
typedef struct GroupResultPath
{
    Path        path;
    List       *quals;              /* HAVING clauses (bare expressions) */
} GroupResultPath;
```

**Purpose**: Represents the degenerate grouping case where we know we should produce exactly one row (`SELECT count(*) FROM t WHERE false;` — query is empty but the aggregate yields one row anyway). The HAVING qual filters that single row.

**Constructor**: `create_group_result_path(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, List *havingqual)` at `src/backend/optimizer/util/pathnode.c:1518`.
   - Cost computation: inline. Always 1 row; cost is `cpu_tuple_cost + target eval cost + qual eval cost`.

**Cost function**: None — inline (cannot quite reuse `cost_resultscan` because the quals aren't baserestrictinfo).

**Pathkey behavior**: Always `NIL` (single row).

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: `create_group_result_plan()` at `src/backend/optimizer/plan/createplan.c:1588` produces `Result` (`plannodes.h:196`). Discriminated from other Result-producing paths in `create_plan_recurse` by `IsA(best_path, GroupResultPath)`.

**When chosen**: For aggregate queries on empty FROM (`SELECT count(*) FROM t WHERE false`) or all-rows-eliminated queries that still need to produce the empty-aggregate result.

**Example SQL**: `SELECT count(*) FROM t WHERE 1=0;` → `Result  Filter: ...`

---

## UpperUniquePath (T_UpperUniquePath)

**Identity**: struct `UpperUniquePath` defined at `src/include/nodes/pathnodes.h:2239`.

```c
typedef struct UpperUniquePath
{
    Path        path;
    Path       *subpath;
    int         numkeys;
} UpperUniquePath;
```

**Purpose**: Represents adjacent-duplicate elimination on presorted input (used for `SELECT DISTINCT` with a Sort-based plan).

**Constructor**: `create_upper_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, int numCols, double numGroups)` at `src/backend/optimizer/util/pathnode.c:3103`.
   - Cost computation: inline. `total_cost = subpath->total_cost + cpu_operator_cost * subpath->rows * numCols`.

**Cost function**: None — inline.

**Pathkey behavior**: Inherits subpath pathkeys.

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: `create_upper_unique_plan()` at `src/backend/optimizer/plan/createplan.c:2281` produces `Unique` (`plannodes.h:1112`). Discriminated from `UniquePath` via `IsA(best_path, UpperUniquePath)` in dispatch.

**When chosen**: For `SELECT DISTINCT` when the plan is sort-based (HashAgg-based DISTINCT uses AggPath instead).

**Example SQL**: `SELECT DISTINCT a FROM t ORDER BY a;` → `Unique -> Sort -> Seq Scan on t`

---

## GroupPath (T_GroupPath)

**Identity**: struct `GroupPath` defined at `src/include/nodes/pathnodes.h:2225`.

```c
typedef struct GroupPath
{
    Path        path;
    Path       *subpath;
    List       *groupClause;
    List       *qual;               /* HAVING quals */
} GroupPath;
```

**Purpose**: Represents grouping-without-aggregation on presorted input — i.e., a `GROUP BY` query that only produces one row per group with no aggregate functions, equivalent to `SELECT DISTINCT ON (...)`-like semantics.

**Constructor**: `create_group_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *groupClause, List *qual, double numGroups)` at `src/backend/optimizer/util/pathnode.c:3044`.
   - Cost computation: inline `cost_group()` plus tlist eval cost.

**Cost function**: `cost_group()` at `src/backend/optimizer/path/costsize.c:3163`.

**Pathkey behavior**: Preserves subpath pathkeys (Group requires sorted input).

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: `create_group_plan()` at `src/backend/optimizer/plan/createplan.c:2242` produces `Group` (`plannodes.h:967`).

**When chosen**: Rarely standalone — usually superseded by AggPath. Used when grouping the output of an already-sorted input and not actually aggregating.

**Example SQL**: `SELECT a FROM t GROUP BY a;` (without aggregates) when planner picks sort-based grouping → `Group -> Sort -> Seq Scan on t`
