# Path Catalog: Scan Paths

This catalog documents Path subtypes that represent base-relation scans (and a few helpers that participate in scan plan trees). All entries reference the canonical sources in `src/include/nodes/pathnodes.h`, the constructors in `src/backend/optimizer/util/pathnode.c`, the cost functions in `src/backend/optimizer/path/costsize.c`, and the plan creators in `src/backend/optimizer/plan/createplan.c`.

The plain `Path` struct itself is the most polymorphic Path: it is reused for nine different `pathtype` discriminators (T_SeqScan, T_SampleScan, T_FunctionScan, T_TableFuncScan, T_ValuesScan, T_CteScan, T_NamedTuplestoreScan, T_WorkTableScan, and T_Result). Each appears as its own entry below because the constructors, cost functions, and plan creators differ.

---

## Path (T_SeqScan)

**Identity**: struct `Path` defined at `src/include/nodes/pathnodes.h:1621`.

```c
typedef struct Path
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    NodeTag     type;
    NodeTag     pathtype;       /* tag identifying scan/join method */
    RelOptInfo *parent;         /* the relation this path can build */
    PathTarget *pathtarget;     /* list of Vars/Exprs, cost, width */
    ParamPathInfo *param_info;  /* parameterization info, or NULL */
    bool        parallel_aware; /* engage parallel-aware logic? */
    bool        parallel_safe;  /* OK to use as part of parallel plan? */
    int         parallel_workers;
    Cardinality rows;
    Cost        startup_cost;
    Cost        total_cost;
    List       *pathkeys;       /* sort ordering of path's output */
} Path;
```

**Purpose**: Represents a sequential scan of a heap relation. This is the universal fallback access method for any baserel.

**Constructor**: `create_seqscan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer, int parallel_workers)` at `src/backend/optimizer/util/pathnode.c:927`.
   - Allocation: `makeNode(Path)` then sets `pathtype = T_SeqScan`.
   - Cost computation: invokes `cost_seqscan(pathnode, root, rel, pathnode->param_info)` inline.

**Cost function**: `cost_seqscan()` at `src/backend/optimizer/path/costsize.c:284`.
   - Formula summary: `disk_run_cost = spc_seq_page_cost * baserel->pages` plus `cpu_run_cost = (cpu_tuple_cost + qual_cost.per_tuple) * tuples`. With parallel workers, run cost is divided by `get_parallel_divisor(path)`.
   - GUC dependencies: `seq_page_cost` (per-tablespace), `cpu_tuple_cost`, `parallel_tuple_cost`, `min_parallel_table_scan_size`.

**Pathkey behavior**: Always `NIL` — sequential scans produce unordered output.

**Parameterization**: Yes via `required_outer`; in that case, additional movable join clauses are accounted for in `param_info->ppi_clauses` and the row estimate reflects their selectivity.

**Parallel-aware**: Yes when `parallel_workers > 0`. The same struct is used; only `parallel_aware`/`parallel_workers` differ. Parallel SeqScan splits heap blocks across workers via `ParallelTableScanDesc`.

**Plan counterpart**: `create_seqscan_plan()` at `src/backend/optimizer/plan/createplan.c:2917` produces `SeqScan` (defined at `src/include/nodes/plannodes.h:396`).

**When chosen**: When no usable index exists, or when expected to read most pages (so seq I/O wins over random I/O), or when the table is tiny enough that random vs. sequential is irrelevant.

**Example SQL**: `SELECT * FROM t WHERE x > 0;` → `Seq Scan on t  (cost=0.00..123.45 rows=10000 width=8)`

---

## Path (T_SampleScan)

**Identity**: same `Path` struct (`pathnodes.h:1621`), discriminated by `pathtype = T_SampleScan`.

**Purpose**: Represents `TABLESAMPLE` scans (BERNOULLI, SYSTEM, or extension-provided sampling methods).

**Constructor**: `create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)` at `src/backend/optimizer/util/pathnode.c:952`.
   - Allocation: `makeNode(Path)`.
   - Cost computation: inline call to `cost_samplescan()`.

**Cost function**: `cost_samplescan()` at `src/backend/optimizer/path/costsize.c:361`. Cost is determined by the tablesample method's `SampleScanGetSampleSize` callback plus per-tuple CPU cost.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer` (parameterization through join clauses).

**Parallel-aware**: No (`parallel_workers = 0`, `parallel_aware = false` always).

**Plan counterpart**: `create_samplescan_plan()` at `src/backend/optimizer/plan/createplan.c:2955` produces `SampleScan` at `src/include/nodes/plannodes.h:405` (which embeds a `TableSampleClause *tablesample`).

**When chosen**: Whenever the FROM-clause RTE has a non-NULL `tablesample` clause; this is the only access method considered.

**Example SQL**: `SELECT * FROM t TABLESAMPLE BERNOULLI(10);` → `Sample Scan on t  (cost=...)`

---

## IndexPath (T_IndexPath)

**Identity**: struct `IndexPath` defined at `src/include/nodes/pathnodes.h:1709`.

```c
typedef struct IndexPath
{
    Path        path;
    IndexOptInfo *indexinfo;
    List       *indexclauses;       /* List of IndexClause nodes */
    List       *indexorderbys;      /* ORDER BY exprs usable by amcanorderbyop */
    List       *indexorderbycols;   /* index column numbers for indexorderbys */
    ScanDirection indexscandir;     /* Forward or Backward */
    Cost        indextotalcost;
    Selectivity indexselectivity;
} IndexPath;
```

**Purpose**: Represents either a regular index scan (`pathtype = T_IndexScan`) or an index-only scan (`pathtype = T_IndexOnlyScan`). The same struct is also reused as a child of BitmapHeapPath, where it corresponds to a BitmapIndexScan executor node.

**Constructor**: `create_index_path(PlannerInfo *root, IndexOptInfo *index, List *indexclauses, List *indexorderbys, List *indexorderbycols, List *pathkeys, ScanDirection indexscandir, bool indexonly, Relids required_outer, double loop_count, bool partial_path)` at `src/backend/optimizer/util/pathnode.c:993`.
   - Allocation: `makeNode(IndexPath)`; `pathtype` set from the `indexonly` flag.
   - Cost computation: inline call to `cost_index(pathnode, root, loop_count, partial_path)`.

**Cost function**: `cost_index()` at `src/backend/optimizer/path/costsize.c:549`.
   - Formula summary: combines the index AM's `amcostestimate` callback (which yields `indexStartupCost`, `indexTotalCost`, `indexSelectivity`, `indexCorrelation`) with heap fetch cost. Index-only scans skip heap fetches when the visibility map is good. Cache effects across `loop_count` repetitions are modeled with the Mackert-Lohman formula.
   - GUC dependencies: `random_page_cost`, `seq_page_cost`, `cpu_index_tuple_cost`, `cpu_operator_cost`, `effective_cache_size`.

**Pathkey behavior**: Set by caller from `build_index_pathkeys()`. For `amcanorder` indexes, an ascending scan yields ASC pathkeys; backward scan yields DESC pathkeys with reversed nulls-first. ORDER BY operator support (`amcanorderbyop`) is handled via `indexorderbys`.

**Parameterization**: Yes — the canonical use case. Inner-side IndexPaths are typically parameterized by outer-rel Vars to feed nestloop joins.

**Parallel-aware**: Yes when `partial_path = true`. The constructor still produces an IndexPath, but the executor invokes parallel B-tree scan logic.

**Plan counterpart**: `create_indexscan_plan()` at `src/backend/optimizer/plan/createplan.c:3006` produces either `IndexScan` (`plannodes.h:449`) or `IndexOnlyScan` (`plannodes.h:492`), driven by the `indexonly` argument from the dispatch in `create_scan_plan`.

**When chosen**: When the index's `indexselectivity` plus heap-fetch cost beats SeqScan, or when sort ordering matches a useful PathKey list (avoiding a Sort), or when index-only coverage avoids heap I/O.

**Example SQL**: `SELECT * FROM t WHERE id = 42;` → `Index Scan using t_pkey on t  (cost=0.29..8.30 rows=1 width=...)`

---

## BitmapHeapPath (T_BitmapHeapPath)

**Identity**: struct `BitmapHeapPath` defined at `src/include/nodes/pathnodes.h:1784`.

```c
typedef struct BitmapHeapPath
{
    Path        path;
    Path       *bitmapqual;     /* IndexPath, BitmapAndPath, BitmapOrPath */
} BitmapHeapPath;
```

**Purpose**: Represents a heap scan driven by a TID bitmap built from one or more index scans, optionally combined by AND/OR. The bitmap is built first (random index access), sorted into heap order, then the heap is scanned in physical order (sequential-ish access).

**Constructor**: `create_bitmap_heap_path(PlannerInfo *root, RelOptInfo *rel, Path *bitmapqual, Relids required_outer, double loop_count, int parallel_degree)` at `src/backend/optimizer/util/pathnode.c:1042`.
   - Allocation: `makeNode(BitmapHeapPath)`.
   - Cost computation: inline call to `cost_bitmap_heap_scan(&pathnode->path, root, rel, pathnode->path.param_info, bitmapqual, loop_count)`.

**Cost function**: `cost_bitmap_heap_scan()` at `src/backend/optimizer/path/costsize.c:1013`.
   - Formula summary: index access cost from `cost_bitmap_tree_node(bitmapqual)` plus heap page cost interpolated between `seq_page_cost` and `random_page_cost` based on the fraction of pages touched (`pages_fetched / baserel->pages`).
   - GUC dependencies: `seq_page_cost`, `random_page_cost`, `effective_cache_size`, `cpu_tuple_cost`, `cpu_operator_cost`.

**Pathkey behavior**: Always `NIL` — heap is scanned in physical order, so any index ordering is destroyed.

**Parameterization**: Yes; inherits parameterization from the bitmapqual subtree.

**Parallel-aware**: Yes when `parallel_degree > 0`; bitmap is built into a shared bitmap and workers fetch heap pages in parallel.

**Plan counterpart**: `create_bitmap_scan_plan()` at `src/backend/optimizer/plan/createplan.c:3202` produces `BitmapHeapScan` (`plannodes.h:538`).

**When chosen**: When several restrictive indexable conditions can be combined (especially with OR), or when one indexscan would touch too many pages in random order — bitmap conversion to physical order amortizes the cost.

**Example SQL**: `SELECT * FROM t WHERE a = 1 OR b = 2;` → `Bitmap Heap Scan on t -> BitmapOr -> BitmapIndexScan(a) + BitmapIndexScan(b)`

---

## BitmapAndPath (T_BitmapAndPath)

**Identity**: struct `BitmapAndPath` defined at `src/include/nodes/pathnodes.h:1796`.

```c
typedef struct BitmapAndPath
{
    Path        path;
    List       *bitmapquals;        /* IndexPaths and BitmapOrPaths */
    Selectivity bitmapselectivity;
} BitmapAndPath;
```

**Purpose**: Represents the intersection of multiple bitmap-producing subpaths. Always appears under a BitmapHeapPath (or under another Bitmap{And,Or}Path), never standalone.

**Constructor**: `create_bitmap_and_path(PlannerInfo *root, RelOptInfo *rel, List *bitmapquals)` at `src/backend/optimizer/util/pathnode.c:1075`.
   - Allocation: `makeNode(BitmapAndPath)`.
   - Cost computation: inline call to `cost_bitmap_and_node(pathnode, root)`.

**Cost function**: `cost_bitmap_and_node()` at `src/backend/optimizer/path/costsize.c:1157`. Sums child costs and multiplies their selectivities (with damping).

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Computed as the union of all child paths' `PATH_REQ_OUTER`.

**Parallel-aware**: `parallel_aware = false` (the BitmapHeap above is the parallel-aware node); `parallel_safe` is inherited from `rel->consider_parallel`.

**Plan counterpart**: Produced as a `BitmapAnd` plan (`plannodes.h:356`) inside `create_bitmap_subplan()`, recursively invoked from `create_bitmap_scan_plan()`. There is no top-level `create_bitmap_and_plan` — `create_bitmap_subplan` recurses on `IsA(bitmapqual, BitmapAndPath)`.

**When chosen**: When two or more indexable AND conditions exist on the same relation; combining bitmaps via AND can be cheaper than relying on a single index plus filter.

**Example SQL**: `SELECT * FROM t WHERE a = 1 AND b = 2;` (with separate indexes on a and b) → `BitmapAnd` under `BitmapHeapScan`.

---

## BitmapOrPath (T_BitmapOrPath)

**Identity**: struct `BitmapOrPath` defined at `src/include/nodes/pathnodes.h:1809`.

```c
typedef struct BitmapOrPath
{
    Path        path;
    List       *bitmapquals;        /* IndexPaths and BitmapAndPaths */
    Selectivity bitmapselectivity;
} BitmapOrPath;
```

**Purpose**: Represents the union of multiple bitmap-producing subpaths. Always appears under a BitmapHeapPath (or another Bitmap{And,Or}Path).

**Constructor**: `create_bitmap_or_path(PlannerInfo *root, RelOptInfo *rel, List *bitmapquals)` at `src/backend/optimizer/util/pathnode.c:1127`. Symmetric to `create_bitmap_and_path` except it uses `cost_bitmap_or_node`.

**Cost function**: `cost_bitmap_or_node()` at `src/backend/optimizer/path/costsize.c:1201`. Sums child costs; combined selectivity is `1 - prod(1 - child_selectivity_i)` with damping.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Union of children's required_outer.

**Parallel-aware**: Same rules as BitmapAndPath.

**Plan counterpart**: `BitmapOr` plan (`plannodes.h:370`) produced by recursive `create_bitmap_subplan()`. As an optimization, single-element BitmapOrPaths collapse to just their lone child during plan generation.

**When chosen**: For `WHERE a=1 OR b=2` style conditions where each disjunct is independently indexable.

**Example SQL**: `SELECT * FROM t WHERE a=1 OR b=2;` → `BitmapOr` under `BitmapHeapScan`.

---

## TidPath (T_TidPath)

**Identity**: struct `TidPath` defined at `src/include/nodes/pathnodes.h:1823`.

```c
typedef struct TidPath
{
    Path        path;
    List       *tidquals;           /* CTID = constant or CTID = ANY(...) */
} TidPath;
```

**Purpose**: Represents direct-by-TID access — used for `WHERE ctid = '(0,1)'`, `WHERE ctid = ANY(...)`, and `WHERE CURRENT OF cursor` scans.

**Constructor**: `create_tidscan_path(PlannerInfo *root, RelOptInfo *rel, List *tidquals, Relids required_outer)` at `src/backend/optimizer/util/pathnode.c:1179`.
   - Allocation: `makeNode(TidPath)`.
   - Cost computation: inline call to `cost_tidscan(&pathnode->path, root, rel, tidquals, pathnode->path.param_info)`.

**Cost function**: `cost_tidscan()` at `src/backend/optimizer/path/costsize.c:1249`. Cost is `random_page_cost * ntids` plus per-tuple CPU cost; ntids is derived from the tidquals (constants vs. ANY arrays).

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: `create_tidscan_plan()` at `src/backend/optimizer/plan/createplan.c:3540` produces `TidScan` (`plannodes.h:552`).

**When chosen**: Generated by `create_tidscan_paths()` from `tidpath.c` whenever the WHERE clause contains TID-equality conditions.

**Example SQL**: `SELECT * FROM t WHERE ctid = '(0,1)';` → `Tid Scan on t  (cost=0.00..4.01 rows=1 ...)`

---

## TidRangePath (T_TidRangePath)

**Identity**: struct `TidRangePath` defined at `src/include/nodes/pathnodes.h:1835`.

```c
typedef struct TidRangePath
{
    Path        path;
    List       *tidrangequals;      /* CTID relop pseudoconstant (>,>=,<,<=) */
} TidRangePath;
```

**Purpose**: Represents a contiguous-TID-range scan — useful for queries like `WHERE ctid > '(100,0)' AND ctid < '(200,0)'`.

**Constructor**: `create_tidrangescan_path()` at `src/backend/optimizer/util/pathnode.c:1208`.

**Cost function**: `cost_tidrangescan()` at `src/backend/optimizer/path/costsize.c:1357`. Estimates `seq_page_cost * pages_in_range` (since the range is read sequentially) plus per-tuple cost.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: `create_tidrangescan_plan()` at `src/backend/optimizer/plan/createplan.c:3637` produces `TidRangeScan` (`plannodes.h:565`).

**When chosen**: When a query restricts CTID to a contiguous range (typical for chunked-table-scan tools).

**Example SQL**: `SELECT * FROM t WHERE ctid >= '(0,0)' AND ctid < '(1000,0)';` → `Tid Range Scan on t`

---

## SubqueryScanPath (T_SubqueryScanPath)

**Identity**: struct `SubqueryScanPath` defined at `src/include/nodes/pathnodes.h:1849`.

```c
typedef struct SubqueryScanPath
{
    Path        path;
    Path       *subpath;            /* path representing subquery execution */
} SubqueryScanPath;
```

**Purpose**: Represents the scan side of an unflattened subquery RTE. The subpath comes from a different planning domain (a recursive call to `subquery_planner` produced it), so the SubqueryScanPath provides the binding between outer and inner planner contexts.

**Constructor**: `create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, bool trivial_pathtarget, List *pathkeys, Relids required_outer)` at `src/backend/optimizer/util/pathnode.c:2016`.
   - Cost computation: inline call to `cost_subqueryscan(pathnode, root, rel, pathnode->path.param_info, trivial_pathtarget)`.

**Cost function**: `cost_subqueryscan()` at `src/backend/optimizer/path/costsize.c:1451`. Adds `cpu_tuple_cost + qual_cost.per_tuple` per row to the subpath's cost; if `trivial_pathtarget = false`, also adds tlist eval cost.

**Pathkey behavior**: Inherited from `pathkeys` argument (typically derived from the subpath's own pathkeys).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: `create_subqueryscan_plan()` at `src/backend/optimizer/plan/createplan.c:3702` produces `SubqueryScan` (`plannodes.h:598`). Notably, this is the only place `create_plan` (not `create_plan_recurse`) is invoked for the subpath, because the subroot is a separate planning context.

**When chosen**: For any subquery that wasn't pulled up into the parent query by `pull_up_subqueries` (e.g., subqueries with LIMIT, set operations, or aggregates that block flattening).

**Example SQL**: `SELECT * FROM (SELECT * FROM t LIMIT 10) sq;` → `Subquery Scan on sq  -> Limit -> Seq Scan on t`

---

## ForeignPath (T_ForeignPath)

**Identity**: struct `ForeignPath` defined at `src/include/nodes/pathnodes.h:1869`.

```c
typedef struct ForeignPath
{
    Path        path;
    Path       *fdw_outerpath;
    List       *fdw_restrictinfo;
    List       *fdw_private;
} ForeignPath;
```

**Purpose**: Represents a scan/join/upper-relation step performed by a Foreign Data Wrapper (postgres_fdw, file_fdw, etc.). The FDW supplies costs directly.

**Constructors** (all at `pathnode.c`):
   - `create_foreignscan_path()` (line 2235) — for foreign base relations.
   - `create_foreign_join_path()` (line 2281) — for FDW-pushed joins.
   - `create_foreign_upper_path()` (line 2333) — for FDW-pushed aggregates/sorts.

**Cost function**: None — cost is supplied directly by the FDW's `GetForeignPaths` / `GetForeignJoinPaths` / `GetForeignUpperPaths` callbacks.

**Pathkey behavior**: Whatever the FDW asserts (a remote ORDER BY can give a sorted ForeignPath).

**Parameterization**: Yes for base scans; foreign joins do not currently support parameterization (`elog(ERROR)` if attempted).

**Parallel-aware**: As declared by the FDW via `parallel_safe` flag.

**Plan counterpart**: `create_foreignscan_plan()` at `src/backend/optimizer/plan/createplan.c:4122` produces `ForeignScan` (`plannodes.h:707`). The actual plan struct is built by the FDW's `GetForeignPlan` callback; the core code wraps it.

**When chosen**: Whenever the relation's `fdwroutine` is set; FDW provides paths via `GetForeignPaths`. Core never generates ForeignPaths itself.

**Example SQL**: `SELECT * FROM remote_t;` (foreign table) → `Foreign Scan on remote_t`

---

## CustomPath (T_CustomPath)

**Identity**: struct `CustomPath` defined at `src/include/nodes/pathnodes.h:1905`.

```c
typedef struct CustomPath
{
    Path        path;
    uint32      flags;              /* CUSTOMPATH_* flags */
    List       *custom_paths;       /* child Path nodes, if any */
    List       *custom_restrictinfo;
    List       *custom_private;
    const struct CustomPathMethods *methods;
} CustomPath;
```

**Purpose**: Extension-supplied scan or join path. Extensions may install paths via `set_rel_pathlist_hook` (base relations) or `set_join_pathlist_hook` (join relations).

**Constructor**: No core constructor. Extensions allocate a CustomPath (or a struct that embeds it as the first member) and fill in fields manually, including all cost/row estimates.

**Cost function**: None in core; extension supplies costs directly.

**Pathkey behavior**: Whatever the extension declares.

**Parameterization**: Optional; extensions handle this themselves.

**Parallel-aware**: As declared by the extension.

**Plan counterpart**: `create_customscan_plan()` at `src/backend/optimizer/plan/createplan.c:4277` produces `CustomScan` (`plannodes.h:739`) by invoking `methods->PlanCustomPath()`.

**When chosen**: Only when extensions inject CustomPaths.

**Example**: pg_strom, citus, timescaledb extensions install custom paths.

---

## Path (T_FunctionScan)

**Identity**: same `Path` struct, `pathtype = T_FunctionScan`.

**Purpose**: Scan of a set-returning function in FROM (`SELECT * FROM generate_series(1,10)`).

**Constructor**: `create_functionscan_path(PlannerInfo *root, RelOptInfo *rel, List *pathkeys, Relids required_outer)` at `src/backend/optimizer/util/pathnode.c:2046`.

**Cost function**: `cost_functionscan()` at `src/backend/optimizer/path/costsize.c:1531`. Uses the function's declared rowcount estimate (default 1000 unless overridden) times `cpu_tuple_cost + qual_cost.per_tuple`.

**Pathkey behavior**: Caller-supplied (some functions like `generate_series` return ordered output; this is detected via `set_function_size_estimates` and friends).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: `create_functionscan_plan()` at `src/backend/optimizer/plan/createplan.c:3761` → `FunctionScan` (`plannodes.h:609`).

**Example SQL**: `SELECT * FROM generate_series(1,1000);` → `Function Scan on generate_series`

---

## Path (T_TableFuncScan)

**Identity**: `Path`, `pathtype = T_TableFuncScan`.

**Purpose**: Scan of a `TableFunc` (e.g., XMLTABLE, JSON_TABLE).

**Constructor**: `create_tablefuncscan_path()` at `src/backend/optimizer/util/pathnode.c:2072`.

**Cost function**: `cost_tablefuncscan()` at `src/backend/optimizer/path/costsize.c:1592`.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes (the table function expressions can reference outer Vars).

**Parallel-aware**: No.

**Plan counterpart**: `create_tablefuncscan_plan()` at `src/backend/optimizer/plan/createplan.c:3804` → `TableFuncScan` (`plannodes.h:630`).

**Example SQL**: `SELECT * FROM XMLTABLE(...)` → `Table Function Scan on xmltable`

---

## Path (T_ValuesScan)

**Identity**: `Path`, `pathtype = T_ValuesScan`.

**Purpose**: Scan of an inline VALUES list.

**Constructor**: `create_valuesscan_path()` at `src/backend/optimizer/util/pathnode.c:2098`.

**Cost function**: `cost_valuesscan()` at `src/backend/optimizer/path/costsize.c:1648`. Per-row CPU cost times list length.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes.

**Parallel-aware**: No.

**Plan counterpart**: `create_valuesscan_plan()` at `src/backend/optimizer/plan/createplan.c:3847` → `ValuesScan` (`plannodes.h:620`).

**Example SQL**: `SELECT * FROM (VALUES (1),(2),(3)) v(x);` → `Values Scan on v`

---

## Path (T_CteScan)

**Identity**: `Path`, `pathtype = T_CteScan`.

**Purpose**: Scan of a CTE (common table expression) that has been materialized as a separate subplan in the planner.

**Constructor**: `create_ctescan_path()` at `src/backend/optimizer/util/pathnode.c:2124`.

**Cost function**: `cost_ctescan()` at `src/backend/optimizer/path/costsize.c:1698`. Per-row read cost from the tuplestore that the CTE's InitPlan will materialize.

**Pathkey behavior**: Caller-supplied.

**Parameterization**: Yes.

**Parallel-aware**: No.

**Plan counterpart**: `create_ctescan_plan()` at `src/backend/optimizer/plan/createplan.c:3891` → `CteScan` (`plannodes.h:640`). Notably, this plan creator looks up the CTE's `plan_id` and `cte_param_id` from `cteroot->init_plans`.

**Example SQL**: `WITH x AS MATERIALIZED (SELECT ...) SELECT * FROM x;` → `CTE Scan on x`

---

## Path (T_NamedTuplestoreScan)

**Identity**: `Path`, `pathtype = T_NamedTuplestoreScan`.

**Purpose**: Scan of a transition tuplestore in trigger or AFTER-statement contexts (e.g., the OLD/NEW/transition tables).

**Constructor**: `create_namedtuplestorescan_path()` at `src/backend/optimizer/util/pathnode.c:2150`.

**Cost function**: `cost_namedtuplestorescan()` at `src/backend/optimizer/path/costsize.c:1739`.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes.

**Parallel-aware**: No.

**Plan counterpart**: `create_namedtuplestorescan_plan()` at `src/backend/optimizer/plan/createplan.c:3986` → `NamedTuplestoreScan` (`plannodes.h:651`).

**Example SQL**: `CREATE TRIGGER ... REFERENCING OLD TABLE AS old_t ...; SELECT * FROM old_t;` (inside the trigger).

---

## Path (T_Result, plain RTE_RESULT)

**Identity**: `Path`, `pathtype = T_Result`. Used for an RTE_RESULT relation (e.g., a FROM-less `SELECT 1`).

**Note**: Distinct from GroupResultPath (which is also pathtype = T_Result but a different struct). The dispatch in `create_plan_recurse` distinguishes them via `IsA(best_path, ProjectionPath/MinMaxAggPath/GroupResultPath)` checks before falling through to `create_scan_plan`.

**Constructor**: `create_resultscan_path()` at `src/backend/optimizer/util/pathnode.c:2176`.

**Cost function**: `cost_resultscan()` at `src/backend/optimizer/path/costsize.c:1776`. Returns essentially trivial cost (single-row).

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: `create_resultscan_plan()` at `src/backend/optimizer/plan/createplan.c:4025` → `Result` (`plannodes.h:196`).

**Example SQL**: `SELECT 1;` → `Result  (cost=0.00..0.01 rows=1)`

---

## Path (T_WorkTableScan)

**Identity**: `Path`, `pathtype = T_WorkTableScan`.

**Purpose**: The "work table" scan that reads each iteration's intermediate output inside a recursive CTE.

**Constructor**: `create_worktablescan_path()` at `src/backend/optimizer/util/pathnode.c:2202`.

**Cost function**: Reuses `cost_ctescan()` at `costsize.c:1698`.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: `create_worktablescan_plan()` at `src/backend/optimizer/plan/createplan.c:4062` → `WorkTableScan` (`plannodes.h:661`). Unique twist: this scan looks up the worktable param ID from one level *below* the level where the CTE comes from (the level containing the RecursiveUnion).

**Example SQL**: Inside `WITH RECURSIVE r AS (... UNION ALL SELECT ... FROM r ...)` — the `FROM r` reference becomes a WorkTableScan.
