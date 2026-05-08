# 18. Path Catalog

Prerequisites: [08 Base relation paths](08_base_relation_paths.md), [09 Join paths and search](09_join_paths_and_search.md), [10 Cost model and selectivity](10_cost_model_and_selectivity.md), [11 RestrictInfo and clause utilities](11_restrictinfo_and_clause_utils.md).

This is the consolidated catalog of every Path subtype core PostgreSQL emits. Each entry uses the standardized template:

- **Identity** (struct definition, `pathnodes.h` line)
- **Purpose** (what concept of execution it represents)
- **Constructor** (which `create_*_path` produces it)
- **Cost function** (which `cost_*` quantifies it, plus GUC dependencies)
- **Pathkey behavior** (does the path preserve / produce sort order)
- **Parameterization** (does it carry `param_info`)
- **Parallel-aware** (when the same struct admits a parallel-aware variant)
- **Plan counterpart** (which `create_*_plan` and `Plan` subtype it becomes — cross-link to [Module 19](19_plan_creator_catalog.md))
- **When chosen** (the cost-model situations that select it)
- **Example SQL** (the simplest demonstration in EXPLAIN form)

Catalog organization mirrors the Stage 2 source layout:

- [Scan paths](#scan-paths) — base-relation access methods.
- [Join paths](#join-paths) — the three join algorithms.
- [Upper paths](#upper-paths) — sort/group/agg/limit/project.
- [Append and partition paths](#append-and-partition-paths) — child unioning.
- [Parallel paths](#parallel-paths) — Gather/GatherMerge and parallel-aware variants.
- [Modify paths](#modify-paths) — DML and row locking.

Cross-references: every entry links to the corresponding plan creator in [Module 19](19_plan_creator_catalog.md).

---

## Scan paths

These represent base-relation access methods. The plain `Path` struct is the most polymorphic type: it is reused for nine different `pathtype` discriminators (T_SeqScan, T_SampleScan, T_FunctionScan, T_TableFuncScan, T_ValuesScan, T_CteScan, T_NamedTuplestoreScan, T_WorkTableScan, and T_Result). Each appears as its own entry below because the constructors, cost functions, and plan creators differ.

### Path (T_SeqScan)

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

**Purpose**: Sequential scan of a heap relation. The universal fallback access method for any baserel.

**Constructor**: `create_seqscan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer, int parallel_workers)` at `src/backend/optimizer/util/pathnode.c:927`. Allocation: `makeNode(Path)` then sets `pathtype = T_SeqScan`. Cost computation: invokes `cost_seqscan(pathnode, root, rel, pathnode->param_info)` inline.

**Cost function**: `cost_seqscan()` at `src/backend/optimizer/path/costsize.c:284`. Formula: `disk_run_cost = spc_seq_page_cost * baserel->pages` plus `cpu_run_cost = (cpu_tuple_cost + qual_cost.per_tuple) * tuples`. With parallel workers, run cost is divided by `get_parallel_divisor(path)`. GUC dependencies: `seq_page_cost` (per-tablespace), `cpu_tuple_cost`, `parallel_tuple_cost`, `min_parallel_table_scan_size`.

**Pathkey behavior**: Always `NIL` — sequential scans produce unordered output.

**Parameterization**: Yes via `required_outer`; in that case, additional movable join clauses are accounted for in `param_info->ppi_clauses` and the row estimate reflects their selectivity.

**Parallel-aware**: Yes when `parallel_workers > 0`. The same struct is used; only `parallel_aware`/`parallel_workers` differ. Parallel SeqScan splits heap blocks across workers via `ParallelTableScanDesc`.

**Plan counterpart**: [`create_seqscan_plan`](19_plan_creator_catalog.md#create_seqscan_plan) at `createplan.c:2917` produces `SeqScan` (`plannodes.h:396`).

**When chosen**: When no usable index exists, or when expected to read most pages (so seq I/O wins over random I/O), or when the table is tiny enough that random vs. sequential is irrelevant.

**Example SQL**: `SELECT * FROM t WHERE x > 0;` → `Seq Scan on t  (cost=0.00..123.45 rows=10000 width=8)`

### Path (T_SampleScan)

**Identity**: same `Path` struct (`pathnodes.h:1621`), discriminated by `pathtype = T_SampleScan`.

**Purpose**: `TABLESAMPLE` scans (BERNOULLI, SYSTEM, or extension-provided sampling methods).

**Constructor**: `create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)` at `pathnode.c:952`.

**Cost function**: `cost_samplescan()` at `costsize.c:361`. Cost is determined by the tablesample method's `SampleScanGetSampleSize` callback plus per-tuple CPU cost.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No (`parallel_workers = 0`, `parallel_aware = false` always).

**Plan counterpart**: [`create_samplescan_plan`](19_plan_creator_catalog.md#create_samplescan_plan) at `createplan.c:2955` produces `SampleScan` at `plannodes.h:405` (which embeds a `TableSampleClause *tablesample`).

**When chosen**: Whenever the FROM-clause RTE has a non-NULL `tablesample` clause; this is the only access method considered.

**Example SQL**: `SELECT * FROM t TABLESAMPLE BERNOULLI(10);` → `Sample Scan on t  (cost=...)`

### IndexPath (T_IndexPath)

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

**Purpose**: Either a regular index scan (`pathtype = T_IndexScan`) or an index-only scan (`pathtype = T_IndexOnlyScan`). The same struct is reused as a child of BitmapHeapPath, where it corresponds to a BitmapIndexScan executor node.

**Constructor**: `create_index_path(PlannerInfo *root, IndexOptInfo *index, List *indexclauses, List *indexorderbys, List *indexorderbycols, List *pathkeys, ScanDirection indexscandir, bool indexonly, Relids required_outer, double loop_count, bool partial_path)` at `pathnode.c:993`. `pathtype` set from the `indexonly` flag. Cost computation: inline `cost_index(pathnode, root, loop_count, partial_path)`.

**Cost function**: `cost_index()` at `costsize.c:549`. Combines the index AM's `amcostestimate` callback (which yields `indexStartupCost`, `indexTotalCost`, `indexSelectivity`, `indexCorrelation`) with heap fetch cost. Index-only scans skip heap fetches when the visibility map is good. Cache effects across `loop_count` repetitions are modeled with the Mackert-Lohman formula. GUC dependencies: `random_page_cost`, `seq_page_cost`, `cpu_index_tuple_cost`, `cpu_operator_cost`, `effective_cache_size`.

**Pathkey behavior**: Set by caller from `build_index_pathkeys()`. For `amcanorder` indexes, an ascending scan yields ASC pathkeys; backward scan yields DESC pathkeys with reversed nulls-first. ORDER BY operator support (`amcanorderbyop`) is handled via `indexorderbys`.

**Parameterization**: Yes — the canonical use case. Inner-side IndexPaths are typically parameterized by outer-rel Vars to feed nestloop joins.

**Parallel-aware**: Yes when `partial_path = true`. The constructor still produces an IndexPath, but the executor invokes parallel B-tree scan logic.

**Plan counterpart**: [`create_indexscan_plan`](19_plan_creator_catalog.md#create_indexscan_plan) at `createplan.c:3006` produces either `IndexScan` (`plannodes.h:449`) or `IndexOnlyScan` (`plannodes.h:492`), driven by the `indexonly` argument.

**When chosen**: When the index's `indexselectivity` plus heap-fetch cost beats SeqScan, or when sort ordering matches a useful pathkey list (avoiding a Sort), or when index-only coverage avoids heap I/O.

**Example SQL**: `SELECT * FROM t WHERE id = 42;` → `Index Scan using t_pkey on t  (cost=0.29..8.30 rows=1 width=...)`

### BitmapHeapPath (T_BitmapHeapPath)

**Identity**: struct `BitmapHeapPath` defined at `src/include/nodes/pathnodes.h:1784`.

```c
typedef struct BitmapHeapPath
{
    Path        path;
    Path       *bitmapqual;     /* IndexPath, BitmapAndPath, BitmapOrPath */
} BitmapHeapPath;
```

**Purpose**: Heap scan driven by a TID bitmap built from one or more index scans, optionally combined by AND/OR. The bitmap is built first (random index access), sorted into heap order, then the heap is scanned in physical order (sequential-ish access).

**Constructor**: `create_bitmap_heap_path(PlannerInfo *root, RelOptInfo *rel, Path *bitmapqual, Relids required_outer, double loop_count, int parallel_degree)` at `pathnode.c:1042`. Cost computation: inline `cost_bitmap_heap_scan(&pathnode->path, root, rel, pathnode->path.param_info, bitmapqual, loop_count)`.

**Cost function**: `cost_bitmap_heap_scan()` at `costsize.c:1013`. Index access cost from `cost_bitmap_tree_node(bitmapqual)` plus heap page cost interpolated between `seq_page_cost` and `random_page_cost` based on the fraction of pages touched (`pages_fetched / baserel->pages`). GUC dependencies: `seq_page_cost`, `random_page_cost`, `effective_cache_size`, `cpu_tuple_cost`, `cpu_operator_cost`.

**Pathkey behavior**: Always `NIL` — heap is scanned in physical order, so any index ordering is destroyed.

**Parameterization**: Yes; inherits parameterization from the bitmapqual subtree.

**Parallel-aware**: Yes when `parallel_degree > 0`; bitmap is built into a shared bitmap and workers fetch heap pages in parallel.

**Plan counterpart**: [`create_bitmap_scan_plan`](19_plan_creator_catalog.md#create_bitmap_scan_plan) at `createplan.c:3202` produces `BitmapHeapScan` (`plannodes.h:538`).

**When chosen**: When several restrictive indexable conditions can be combined (especially with OR), or when one indexscan would touch too many pages in random order — bitmap conversion to physical order amortizes the cost.

**Example SQL**: `SELECT * FROM t WHERE a = 1 OR b = 2;` → `Bitmap Heap Scan on t -> BitmapOr -> BitmapIndexScan(a) + BitmapIndexScan(b)`

### BitmapAndPath (T_BitmapAndPath)

**Identity**: struct `BitmapAndPath` defined at `src/include/nodes/pathnodes.h:1796`.

```c
typedef struct BitmapAndPath
{
    Path        path;
    List       *bitmapquals;        /* IndexPaths and BitmapOrPaths */
    Selectivity bitmapselectivity;
} BitmapAndPath;
```

**Purpose**: Intersection of multiple bitmap-producing subpaths. Always appears under a BitmapHeapPath (or under another Bitmap{And,Or}Path), never standalone.

**Constructor**: `create_bitmap_and_path(PlannerInfo *root, RelOptInfo *rel, List *bitmapquals)` at `pathnode.c:1075`.

**Cost function**: `cost_bitmap_and_node()` at `costsize.c:1157`. Sums child costs and multiplies their selectivities (with damping).

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Computed as the union of all child paths' `PATH_REQ_OUTER`.

**Parallel-aware**: `parallel_aware = false`; `parallel_safe` is inherited from `rel->consider_parallel`.

**Plan counterpart**: Produced as a `BitmapAnd` plan (`plannodes.h:356`) inside `create_bitmap_subplan`, recursively invoked from [`create_bitmap_scan_plan`](19_plan_creator_catalog.md#create_bitmap_scan_plan). There is no top-level `create_bitmap_and_plan` — `create_bitmap_subplan` recurses on `IsA(bitmapqual, BitmapAndPath)`.

**When chosen**: When two or more indexable AND conditions exist on the same relation; combining bitmaps via AND can be cheaper than relying on a single index plus filter.

**Example SQL**: `SELECT * FROM t WHERE a = 1 AND b = 2;` (with separate indexes) → `BitmapAnd` under `BitmapHeapScan`.

### BitmapOrPath (T_BitmapOrPath)

**Identity**: struct `BitmapOrPath` defined at `src/include/nodes/pathnodes.h:1809`.

```c
typedef struct BitmapOrPath
{
    Path        path;
    List       *bitmapquals;        /* IndexPaths and BitmapAndPaths */
    Selectivity bitmapselectivity;
} BitmapOrPath;
```

**Purpose**: Union of multiple bitmap-producing subpaths.

**Constructor**: `create_bitmap_or_path(PlannerInfo *root, RelOptInfo *rel, List *bitmapquals)` at `pathnode.c:1127`. Symmetric to BitmapAndPath except it uses `cost_bitmap_or_node`.

**Cost function**: `cost_bitmap_or_node()` at `costsize.c:1201`. Sums child costs; combined selectivity is `1 - prod(1 - child_selectivity_i)` with damping.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Union of children's required_outer.

**Parallel-aware**: Same rules as BitmapAndPath.

**Plan counterpart**: `BitmapOr` plan (`plannodes.h:370`) produced by recursive `create_bitmap_subplan`. As an optimization, single-element BitmapOrPaths collapse to just their lone child during plan generation.

**When chosen**: For `WHERE a=1 OR b=2` style conditions where each disjunct is independently indexable.

**Example SQL**: `SELECT * FROM t WHERE a=1 OR b=2;` → `BitmapOr` under `BitmapHeapScan`.

### TidPath (T_TidPath)

**Identity**: struct `TidPath` defined at `src/include/nodes/pathnodes.h:1823`.

```c
typedef struct TidPath
{
    Path        path;
    List       *tidquals;           /* CTID = constant or CTID = ANY(...) */
} TidPath;
```

**Purpose**: Direct-by-TID access — used for `WHERE ctid = '(0,1)'`, `WHERE ctid = ANY(...)`, and `WHERE CURRENT OF cursor` scans.

**Constructor**: `create_tidscan_path(PlannerInfo *root, RelOptInfo *rel, List *tidquals, Relids required_outer)` at `pathnode.c:1179`.

**Cost function**: `cost_tidscan()` at `costsize.c:1249`. Cost is `random_page_cost * ntids` plus per-tuple CPU cost; ntids is derived from the tidquals.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: [`create_tidscan_plan`](19_plan_creator_catalog.md#create_tidscan_plan) at `createplan.c:3540` produces `TidScan` (`plannodes.h:552`).

**When chosen**: Generated by `create_tidscan_paths()` from `tidpath.c` whenever the WHERE clause contains TID-equality conditions.

**Example SQL**: `SELECT * FROM t WHERE ctid = '(0,1)';` → `Tid Scan on t  (cost=0.00..4.01 rows=1 ...)`

### TidRangePath (T_TidRangePath)

**Identity**: struct `TidRangePath` defined at `src/include/nodes/pathnodes.h:1835`.

```c
typedef struct TidRangePath
{
    Path        path;
    List       *tidrangequals;      /* CTID relop pseudoconstant (>,>=,<,<=) */
} TidRangePath;
```

**Purpose**: Contiguous-TID-range scan — useful for queries like `WHERE ctid > '(100,0)' AND ctid < '(200,0)'`.

**Constructor**: `create_tidrangescan_path()` at `pathnode.c:1208`.

**Cost function**: `cost_tidrangescan()` at `costsize.c:1357`. Estimates `seq_page_cost * pages_in_range` plus per-tuple cost.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: [`create_tidrangescan_plan`](19_plan_creator_catalog.md#create_tidrangescan_plan) at `createplan.c:3637` produces `TidRangeScan` (`plannodes.h:565`).

**When chosen**: When a query restricts CTID to a contiguous range (typical for chunked-table-scan tools).

**Example SQL**: `SELECT * FROM t WHERE ctid >= '(0,0)' AND ctid < '(1000,0)';` → `Tid Range Scan on t`

### SubqueryScanPath (T_SubqueryScanPath)

**Identity**: struct `SubqueryScanPath` defined at `src/include/nodes/pathnodes.h:1849`.

```c
typedef struct SubqueryScanPath
{
    Path        path;
    Path       *subpath;            /* path representing subquery execution */
} SubqueryScanPath;
```

**Purpose**: The scan side of an unflattened subquery RTE. The subpath comes from a different planning domain (a recursive call to `subquery_planner` produced it), so the SubqueryScanPath provides the binding between outer and inner planner contexts.

**Constructor**: `create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, bool trivial_pathtarget, List *pathkeys, Relids required_outer)` at `pathnode.c:2016`.

**Cost function**: `cost_subqueryscan()` at `costsize.c:1451`. Adds `cpu_tuple_cost + qual_cost.per_tuple` per row to the subpath's cost; if `trivial_pathtarget = false`, also adds tlist eval cost.

**Pathkey behavior**: Inherited from `pathkeys` argument (typically derived from the subpath's own pathkeys).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: [`create_subqueryscan_plan`](19_plan_creator_catalog.md#create_subqueryscan_plan) at `createplan.c:3702` produces `SubqueryScan` (`plannodes.h:598`). Notably, this is the only place `create_plan` (not `create_plan_recurse`) is invoked for the subpath, because the subroot is a separate planning context.

**When chosen**: For any subquery that was not pulled up into the parent query by `pull_up_subqueries` (e.g., subqueries with LIMIT, set operations, or aggregates that block flattening).

**Example SQL**: `SELECT * FROM (SELECT * FROM t LIMIT 10) sq;` → `Subquery Scan on sq  -> Limit -> Seq Scan on t`

### ForeignPath (T_ForeignPath)

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

**Purpose**: Scan, join, or upper-relation step performed by a Foreign Data Wrapper (postgres_fdw, file_fdw, etc.). The FDW supplies costs directly.

**Constructors** (all in `pathnode.c`):

- `create_foreignscan_path()` at line 2235 — for foreign base relations.
- `create_foreign_join_path()` at line 2281 — for FDW-pushed joins.
- `create_foreign_upper_path()` at line 2333 — for FDW-pushed aggregates/sorts.

**Cost function**: None — cost is supplied directly by the FDW's `GetForeignPaths` / `GetForeignJoinPaths` / `GetForeignUpperPaths` callbacks.

**Pathkey behavior**: Whatever the FDW asserts (a remote ORDER BY can give a sorted ForeignPath).

**Parameterization**: Yes for base scans; foreign joins do not currently support parameterization (`elog(ERROR)` if attempted).

**Parallel-aware**: As declared by the FDW via `parallel_safe` flag.

**Plan counterpart**: [`create_foreignscan_plan`](19_plan_creator_catalog.md#create_foreignscan_plan) at `createplan.c:4122` produces `ForeignScan` (`plannodes.h:707`). The actual plan struct is built by the FDW's `GetForeignPlan` callback; the core code wraps it.

**When chosen**: Whenever the relation's `fdwroutine` is set; FDW provides paths via `GetForeignPaths`. Core never generates ForeignPaths itself.

**Example SQL**: `SELECT * FROM remote_t;` (foreign table) → `Foreign Scan on remote_t`

### CustomPath (T_CustomPath)

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

**Purpose**: Extension-supplied scan or join path. Extensions install paths via `set_rel_pathlist_hook` (base relations) or `set_join_pathlist_hook` (join relations).

**Constructor**: No core constructor. Extensions allocate a CustomPath (or a struct that embeds it as the first member) and fill in fields manually, including all cost/row estimates.

**Cost function**: None in core; extension supplies costs directly.

**Pathkey behavior**: Whatever the extension declares.

**Parameterization**: Optional; extensions handle this themselves.

**Parallel-aware**: As declared by the extension.

**Plan counterpart**: [`create_customscan_plan`](19_plan_creator_catalog.md#create_customscan_plan) at `createplan.c:4277` produces `CustomScan` (`plannodes.h:739`) by invoking `methods->PlanCustomPath()`.

**When chosen**: Only when extensions inject CustomPaths.

**Example**: pg_strom, citus, timescaledb extensions install custom paths.

### Path (T_FunctionScan)

**Identity**: same `Path` struct, `pathtype = T_FunctionScan`.

**Purpose**: Scan of a set-returning function in FROM (`SELECT * FROM generate_series(1,10)`).

**Constructor**: `create_functionscan_path(PlannerInfo *root, RelOptInfo *rel, List *pathkeys, Relids required_outer)` at `pathnode.c:2046`.

**Cost function**: `cost_functionscan()` at `costsize.c:1531`. Uses the function's declared rowcount estimate (default 1000 unless overridden) times `cpu_tuple_cost + qual_cost.per_tuple`.

**Pathkey behavior**: Caller-supplied (some functions like `generate_series` return ordered output).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: [`create_functionscan_plan`](19_plan_creator_catalog.md#create_functionscan_plan) at `createplan.c:3761` → `FunctionScan` (`plannodes.h:609`).

**Example SQL**: `SELECT * FROM generate_series(1,1000);` → `Function Scan on generate_series`

### Path (T_TableFuncScan)

**Identity**: `Path`, `pathtype = T_TableFuncScan`.

**Purpose**: Scan of a `TableFunc` (e.g., XMLTABLE, JSON_TABLE).

**Constructor**: `create_tablefuncscan_path()` at `pathnode.c:2072`.

**Cost function**: `cost_tablefuncscan()` at `costsize.c:1592`.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes (the table function expressions can reference outer Vars).

**Parallel-aware**: No.

**Plan counterpart**: [`create_tablefuncscan_plan`](19_plan_creator_catalog.md#create_tablefuncscan_plan) at `createplan.c:3804` → `TableFuncScan` (`plannodes.h:630`).

**Example SQL**: `SELECT * FROM XMLTABLE(...)` → `Table Function Scan on xmltable`

### Path (T_ValuesScan)

**Identity**: `Path`, `pathtype = T_ValuesScan`.

**Purpose**: Scan of an inline VALUES list.

**Constructor**: `create_valuesscan_path()` at `pathnode.c:2098`.

**Cost function**: `cost_valuesscan()` at `costsize.c:1648`. Per-row CPU cost times list length.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes.

**Parallel-aware**: No.

**Plan counterpart**: [`create_valuesscan_plan`](19_plan_creator_catalog.md#create_valuesscan_plan) at `createplan.c:3847` → `ValuesScan` (`plannodes.h:620`).

**Example SQL**: `SELECT * FROM (VALUES (1),(2),(3)) v(x);` → `Values Scan on v`

### Path (T_CteScan)

**Identity**: `Path`, `pathtype = T_CteScan`.

**Purpose**: Scan of a CTE that has been materialized as a separate subplan.

**Constructor**: `create_ctescan_path()` at `pathnode.c:2124`.

**Cost function**: `cost_ctescan()` at `costsize.c:1698`. Per-row read cost from the tuplestore that the CTE's InitPlan will materialize.

**Pathkey behavior**: Caller-supplied.

**Parameterization**: Yes.

**Parallel-aware**: No.

**Plan counterpart**: [`create_ctescan_plan`](19_plan_creator_catalog.md#create_ctescan_plan) at `createplan.c:3891` → `CteScan` (`plannodes.h:640`). Notably, this plan creator looks up the CTE's `plan_id` and `cte_param_id` from `cteroot->init_plans`.

**Example SQL**: `WITH x AS MATERIALIZED (SELECT ...) SELECT * FROM x;` → `CTE Scan on x`

### Path (T_NamedTuplestoreScan)

**Identity**: `Path`, `pathtype = T_NamedTuplestoreScan`.

**Purpose**: Scan of a transition tuplestore in trigger or AFTER-statement contexts.

**Constructor**: `create_namedtuplestorescan_path()` at `pathnode.c:2150`.

**Cost function**: `cost_namedtuplestorescan()` at `costsize.c:1739`.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes.

**Parallel-aware**: No.

**Plan counterpart**: [`create_namedtuplestorescan_plan`](19_plan_creator_catalog.md#create_namedtuplestorescan_plan) at `createplan.c:3986` → `NamedTuplestoreScan` (`plannodes.h:651`).

**Example SQL**: `CREATE TRIGGER ... REFERENCING OLD TABLE AS old_t ...; SELECT * FROM old_t;` (inside the trigger).

### Path (T_Result, plain RTE_RESULT)

**Identity**: `Path`, `pathtype = T_Result`. Used for an RTE_RESULT relation (e.g., a FROM-less `SELECT 1`).

**Note**: Distinct from GroupResultPath (which is also pathtype = T_Result but a different struct). The dispatch in `create_plan_recurse` distinguishes them via `IsA(best_path, ProjectionPath/MinMaxAggPath/GroupResultPath)` checks before falling through to `create_scan_plan`.

**Constructor**: `create_resultscan_path()` at `pathnode.c:2176`.

**Cost function**: `cost_resultscan()` at `costsize.c:1776`. Returns essentially trivial cost (single-row).

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: [`create_resultscan_plan`](19_plan_creator_catalog.md#create_resultscan_plan) at `createplan.c:4025` → `Result` (`plannodes.h:196`).

**Example SQL**: `SELECT 1;` → `Result  (cost=0.00..0.01 rows=1)`

### Path (T_WorkTableScan)

**Identity**: `Path`, `pathtype = T_WorkTableScan`.

**Purpose**: The work-table scan that reads each iteration's intermediate output inside a recursive CTE.

**Constructor**: `create_worktablescan_path()` at `pathnode.c:2202`.

**Cost function**: Reuses `cost_ctescan()` at `costsize.c:1698`.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: No.

**Plan counterpart**: [`create_worktablescan_plan`](19_plan_creator_catalog.md#create_worktablescan_plan) at `createplan.c:4062` → `WorkTableScan` (`plannodes.h:661`). Unique twist: this scan looks up the worktable param ID from one level *below* the level where the CTE comes from (the level containing the RecursiveUnion).

**Example SQL**: Inside `WITH RECURSIVE r AS (... UNION ALL SELECT ... FROM r ...)` — the `FROM r` reference becomes a WorkTableScan.

---

## Join paths

The three Path subtypes that represent join methods. All three derive from the abstract `JoinPath` struct (`pathnodes.h:2065`), which carries the join type, inner-unique flag, outer/inner subpaths, and the join restriction clauses:

```c
typedef struct JoinPath
{
    pg_node_attr(abstract)
    Path        path;
    JoinType    jointype;
    bool        inner_unique;       /* outer matches at most one inner */
    Path       *outerjoinpath;
    Path       *innerjoinpath;
    List       *joinrestrictinfo;   /* RestrictInfos to apply at join */
} JoinPath;
```

Join paths are produced from `add_paths_to_joinrel()` in `joinpath.c` for every viable join method on each pair of relations. The `inner_unique` flag is critical to runtime semantics: when set, the executor stops searching the inner side after the first match.

### NestPath (T_NestPath)

**Identity**: struct `NestPath` defined at `src/include/nodes/pathnodes.h:2092`.

```c
typedef struct NestPath
{
    JoinPath    jpath;
} NestPath;
```

(No fields beyond the abstract `JoinPath` — nested-loop joins need no extra metadata.)

**Purpose**: Nested-loop join. For each outer tuple, the inner subpath is rescanned (or, when parameterized, re-executed with the outer tuple's values bound to nestloop Params).

**Constructor**: `create_nestloop_path(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype, JoinCostWorkspace *workspace, JoinPathExtraData *extra, Path *outer_path, Path *inner_path, List *restrict_clauses, List *pathkeys, Relids required_outer)` at `pathnode.c:2457`. Cost computation: `final_cost_nestloop(root, pathnode, workspace, extra)` at the end. Two-stage cost model uses `initial_cost_nestloop` to short-circuit clearly-uncompetitive paths before populating the full path. Side effect: drops `restrict_clauses` already enforced inside the inner parameterized path (matched by `rinfo_serial`) so they are not re-evaluated at the join.

**Cost function**: `initial_cost_nestloop()` (`costsize.c:3233`) + `final_cost_nestloop()` (`costsize.c:3308`). Formula: `outer_path->total_cost + outer_rows * inner_rescan_cost + cpu_per_tuple * outer*inner`. Inner rescan cost is computed by `cost_rescan()` and accounts for materialization and parameter changes. For semi/anti joins with `inner_unique`, the formula uses `outer_rows * inner_path->total_cost` only on a fraction of outer rows.

**Pathkey behavior**: Caller-supplied; usually equals the outer path's pathkeys.

**Parameterization**: The classic case — inner path is typically parameterized by the outer rel, enabling indexed inner lookups.

**Parallel-aware**: No (the NestPath itself is not parallel-aware), but `parallel_safe` propagates from outer/inner subpaths.

**Plan counterpart**: [`create_nestloop_plan`](19_plan_creator_catalog.md#create_nestloop_plan) at `createplan.c:4348` produces `NestLoop` (`plannodes.h:807`).

**When chosen**: When the inner side is small or a parameterized index lookup makes per-outer-tuple inner scan cheap; when no equijoin clauses make hash/merge applicable; for FOR EACH ROW semantics like LATERAL.

**Example SQL**: `SELECT * FROM small s JOIN big b ON b.id = s.fk;` (with index on b.id) → `Nested Loop -> Seq Scan on s -> Index Scan on big using big_pkey`

### MergePath (T_MergePath)

**Identity**: struct `MergePath` defined at `src/include/nodes/pathnodes.h:2132`.

```c
typedef struct MergePath
{
    JoinPath    jpath;
    List       *path_mergeclauses;  /* join clauses used for merge */
    List       *outersortkeys;      /* keys for explicit sort, if any */
    List       *innersortkeys;      /* keys for explicit sort, if any */
    bool        skip_mark_restore;  /* can executor skip mark/restore? */
    bool        materialize_inner;  /* add Materialize to inner? */
} MergePath;
```

**Purpose**: Merge-join over two presorted (or to-be-sorted) input streams. A single MergePath may compile down to up to four executor nodes: MergeJoin, an outer Sort, an inner Sort, and an inner Material — combined in `create_mergejoin_plan`.

**Constructor**: `create_mergejoin_path(...)` at `pathnode.c:2553`. Cost computation: `final_cost_mergejoin(root, pathnode, workspace, extra)` decides whether `skip_mark_restore` and `materialize_inner` apply.

**Cost function**: `initial_cost_mergejoin()` (`costsize.c:3514`) + `final_cost_mergejoin()` (`costsize.c:3745`). Outer_path cost + (outer Sort if `outersortkeys != NIL`) + inner_path cost + (inner Sort if needed) + (inner Material if needed) + per-comparison CPU cost across both rescanned streams. Accounts for early termination when one side runs out.

**Pathkey behavior**: Output pathkeys equal the outer path's mergeclause pathkeys (with caveats from `truncate_useless_pathkeys`).

**Parameterization**: Yes — but unusual; typically MergePath is for non-parameterized joins.

**Parallel-aware**: No directly; `parallel_safe` is the AND of its inputs.

**Plan counterpart**: [`create_mergejoin_plan`](19_plan_creator_catalog.md#create_mergejoin_plan) at `createplan.c:4440` produces `MergeJoin` (`plannodes.h:833`), possibly with synthetic `Sort` nodes (via `make_sort_from_pathkeys`) and a `Material` node injected when `materialize_inner` is set.

**When chosen**: When both inputs are already sorted (or cheaply sortable) on the join keys, especially for large equijoin-driven joins. Also for full outer joins.

**Example SQL**: `SELECT * FROM a JOIN b ON a.k = b.k` with both sides indexed on k → `Merge Join -> Index Scan a -> Index Scan b`

### HashPath (T_HashPath)

**Identity**: struct `HashPath` defined at `src/include/nodes/pathnodes.h:2151`.

```c
typedef struct HashPath
{
    JoinPath    jpath;
    List       *path_hashclauses;   /* join clauses used for hashing */
    int         num_batches;        /* number of batches expected */
    Cardinality inner_rows_total;   /* total inner rows expected */
} HashPath;
```

**Purpose**: Hash-join. Inner side is built into a hash table; outer side is probed.

**Constructor**: `create_hashjoin_path(...)` at `pathnode.c:2619`. Accepts a `parallel_hash` flag that selects Parallel Hash (shared hash table built collaboratively by workers). Cost computation: `final_cost_hashjoin(root, pathnode, workspace, extra)` fills `num_batches`.

**Cost function**: `initial_cost_hashjoin()` (`costsize.c:4073`) + `final_cost_hashjoin()` (`costsize.c:4181`). Cost of building the inner hash table (`inner_path->total_cost + cost of hashing each row`) plus cost of probing (`outer_rows * (hash_cost + match_check_cost)`). Multi-batch overhead added when the hash table will not fit in `work_mem`.

**Pathkey behavior**: Always `NIL` — a hashjoin's output ordering is unpredictable, especially with batching.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: Yes when `parallel_hash = true`. The hash table becomes a shared DSM region built collaboratively by all workers participating in the join.

**Plan counterpart**: [`create_hashjoin_plan`](19_plan_creator_catalog.md#create_hashjoin_plan) at `createplan.c:4747` produces `HashJoin` (`plannodes.h:862`). Critically, this creator also synthesizes a `Hash` plan node (`plannodes.h:1197`) wrapping the inner subplan — Hash is the only Plan node never directly produced from a Path; it appears solely as a child of HashJoin.

**When chosen**: For large equijoins where neither side is presorted, when one side fits comfortably in `work_mem`, or when the outer side is so much larger than the inner that hashing the inner is clearly cheapest.

**Example SQL**: `SELECT * FROM big_a JOIN big_b ON a.k = b.k;` (both unsorted, inner small enough) → `Hash Join -> Seq Scan big_a -> Hash -> Seq Scan big_b`

---

## Upper paths

The Path subtypes produced for upper relations — the post-join layers that perform sorting, grouping, aggregation, set operations, projection, materialization, memoization, and limiting. Constructed by `grouping_planner()` and its helpers in `src/backend/optimizer/plan/planner.c`, attached to RelOptInfos of `reloptkind = RELOPT_UPPER_REL`.

`UpperUniquePath` and `UniquePath` both compile down to `Unique` plans, but exist for different use cases.

### SortPath (T_SortPath)

**Identity**: struct `SortPath` defined at `src/include/nodes/pathnodes.h:2199`.

```c
typedef struct SortPath
{
    Path        path;
    Path       *subpath;
} SortPath;
```

**Purpose**: Explicit Sort step. Sort keys are exactly `path.pathkeys`.

**Constructor**: `create_sort_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *pathkeys, double limit_tuples)` at `pathnode.c:3000`. Cost: inline `cost_sort(&pathnode->path, root, pathkeys, subpath->total_cost, subpath->rows, subpath->pathtarget->width, 0.0, work_mem, limit_tuples)`.

**Cost function**: `cost_sort()` at `costsize.c:2124`. In-memory quicksort if input fits in `work_mem`; otherwise external merge sort. Baseline CPU cost is `2.0 * cpu_operator_cost * N * log2(N)`. Disk cost added for external sort. GUC dependencies: `work_mem`, `cpu_operator_cost`, `seq_page_cost`, `random_page_cost`.

**Pathkey behavior**: Output pathkeys = the requested sort order.

**Parameterization**: No (`param_info = NULL` always; sorts are above any join layer).

**Parallel-aware**: No directly, but `parallel_safe` propagates from the subpath.

**Plan counterpart**: [`create_sort_plan`](19_plan_creator_catalog.md#create_sort_plan) at `createplan.c:2181` produces `Sort` (`plannodes.h:931`).

**When chosen**: When upstream operations need a particular ordering and no equivalent ordering is freely available.

**Example SQL**: `SELECT * FROM t ORDER BY x;` → `Sort  Sort Key: x -> Seq Scan on t`

### IncrementalSortPath (T_IncrementalSortPath)

**Identity**: struct `IncrementalSortPath` defined at `src/include/nodes/pathnodes.h:2211`. Embeds `SortPath` as its first member.

```c
typedef struct IncrementalSortPath
{
    SortPath    spath;
    int         nPresortedCols;
} IncrementalSortPath;
```

**Purpose**: Incremental sort: input is already sorted by some prefix of the desired pathkeys; the executor groups by that prefix and sorts each group fully.

**Constructor**: `create_incremental_sort_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *pathkeys, int presorted_keys, double limit_tuples)` at `pathnode.c:2951`.

**Cost function**: `cost_incremental_sort()` at `costsize.c:1986`. Estimates the cost as `cost_sort(group_size)` per group, multiplied by the number of groups; smaller groups → much less work.

**Pathkey behavior**: Output pathkeys = full pathkey list.

**Parameterization**: No.

**Parallel-aware**: Same as SortPath.

**Plan counterpart**: [`create_incrementalsort_plan`](19_plan_creator_catalog.md#create_incrementalsort_plan) at `createplan.c:2215` produces `IncrementalSort` (`plannodes.h:955`).

**When chosen**: When input pathkeys form a prefix of the desired order. Cheaper than a full Sort because each group is small.

**Example SQL**: `SELECT * FROM t ORDER BY a, b;` (with index on a alone) → `Incremental Sort  Sort Key: a, b  Presorted Key: a`

### AggPath (T_AggPath)

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

**Purpose**: Grouping aggregation — `GROUP BY` plus aggregate functions, or a plain whole-table aggregate. Strategy is one of AGG_PLAIN (single group), AGG_SORTED (input must be presorted), AGG_HASHED, or AGG_MIXED (used inside GROUPING SETS only).

**Constructor**: `create_agg_path(...)` at `pathnode.c:3155`.

**Cost function**: `cost_agg()` at `costsize.c:2650`. For AGG_HASHED, includes hash table construction cost (`numGroups * transitionSpace`); for AGG_SORTED, requires presorted input. Adds aggregate transition function costs from `aggcosts`.

**Pathkey behavior**: For `AGG_SORTED`, preserves input pathkeys (truncated to `num_groupby_pathkeys`). Otherwise `NIL`.

**Parameterization**: No.

**Parallel-aware**: No directly, but `aggsplit` controls partial-aggregate behavior.

**Plan counterpart**: [`create_agg_plan`](19_plan_creator_catalog.md#create_agg_plan) at `createplan.c:2309` produces `Agg` (`plannodes.h:996`).

**When chosen**: For any `GROUP BY` (when not GROUPING SETS) and any aggregate query without grouping. Multiple AggPaths (sorted vs. hashed) may compete.

**Example SQL**: `SELECT a, count(*) FROM t GROUP BY a;` → `HashAggregate Group Key: a -> Seq Scan on t`

### GroupingSetsPath (T_GroupingSetsPath)

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

**Purpose**: GROUPING SETS / CUBE / ROLLUP — generates an Agg plan node with a chain of additional Agg nodes (one per rollup) in its `chain` field.

**Constructor**: `create_groupingsets_path(...)` at `pathnode.c:3237`. Iterates over the rollups list and accumulates costs of each rollup.

**Cost function**: `cost_agg()` (`costsize.c:2650`), called once per rollup; results summed.

**Pathkey behavior**: Single-rollup AGG_SORTED preserves `group_pathkeys`; otherwise `NIL`.

**Parameterization**: Inherited from subpath.

**Parallel-aware**: No.

**Plan counterpart**: [`create_groupingsets_plan`](19_plan_creator_catalog.md#create_groupingsets_plan) at `createplan.c:2393`. Builds the topmost `Agg` node with a `chain` of subsidiary `Agg` (and `Sort`) nodes; sets up `root->grouping_map` for setrefs.c to fix GroupingFunc nodes.

**When chosen**: For any query containing `GROUPING SETS`, `CUBE`, or `ROLLUP`.

**Example SQL**: `SELECT a, b, count(*) FROM t GROUP BY ROLLUP(a, b);` → `GroupAggregate -> ...`

### MinMaxAggPath (T_MinMaxAggPath)

**Identity**: struct `MinMaxAggPath` defined at `src/include/nodes/pathnodes.h:2308`.

```c
typedef struct MinMaxAggPath
{
    Path        path;
    List       *mmaggregates;       /* list of MinMaxAggInfo */
    List       *quals;              /* HAVING quals */
} MinMaxAggPath;
```

**Purpose**: `MIN(col)` / `MAX(col)` evaluated by reading the first/last index entry — much cheaper than a full table aggregate when an index covers the column.

**Constructor**: `create_minmaxagg_path(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, List *mmaggregates, List *quals)` at `pathnode.c:3397`. Sums `mminfo->pathcost` from each `MinMaxAggInfo` plus tlist eval cost.

**Cost function**: None. Each `MinMaxAggInfo->path` is a pre-built single-row index scan whose cost is already known.

**Pathkey behavior**: Always `NIL` (single row).

**Parameterization**: No.

**Parallel-aware**: `parallel_safe = true` if all subpaths are parallel-safe.

**Plan counterpart**: [`create_minmaxagg_plan`](19_plan_creator_catalog.md#create_minmaxagg_plan) at `createplan.c:2551` produces a `Result` plan, with each min/max aggregate evaluated by a separate `InitPlan`.

**When chosen**: For queries like `SELECT min(x) FROM t` where there is a usable B-tree index on x.

**Example SQL**: `SELECT min(id) FROM big;` → `Result  InitPlan 1: Limit -> Index Only Scan ... ORDER BY id LIMIT 1`

### WindowAggPath (T_WindowAggPath)

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

**Purpose**: Window-function evaluation for one window clause. Multi-window queries chain WindowAggPaths.

**Constructor**: `create_windowagg_path(...)` at `pathnode.c:3485`.

**Cost function**: `cost_windowagg()` at `costsize.c:3068`. Per-tuple cost includes evaluating each window function plus the cost of buffering frame contents in tuplestore.

**Pathkey behavior**: Preserves input ordering.

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: [`create_windowagg_plan`](19_plan_creator_catalog.md#create_windowagg_plan) at `createplan.c:2617` produces `WindowAgg` (`plannodes.h:1038`).

**When chosen**: Any query with window functions.

**Example SQL**: `SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM t;` → `WindowAgg -> Sort by a, b -> Seq Scan on t`

### UniquePath (T_UniquePath)

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

**Purpose**: Unique-ification of an inner subpath, used for converting semi-joins to inner joins on a unique inner side. `umethod` selects between NOOP (input is already unique), HASH (compile to HashAgg), and SORT (compile to Sort+Unique).

**Constructor**: `create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, SpecialJoinInfo *sjinfo)` at `pathnode.c:1654`. Caches result in `rel->cheapest_unique_path` since it may be invoked multiple times with the same inputs.

**Cost function**: None — costs computed inline using `cost_sort` and `cost_agg` dummies.

**Pathkey behavior**: Set conservatively (typically input pathkeys for SORT mode, NIL for HASH).

**Parameterization**: Same as input subpath.

**Parallel-aware**: No.

**Plan counterpart**: [`create_unique_plan`](19_plan_creator_catalog.md#create_unique_plan) at `createplan.c:1721` produces either: `subplan` directly (UNIQUE_PATH_NOOP), an `Agg` with `AGG_HASHED` (UNIQUE_PATH_HASH), or a `Sort` + `Unique` (UNIQUE_PATH_SORT).

**When chosen**: When the join planner converts an `IN`/semi-join to an inner-join by first unique-ifying the inner side.

**Example SQL**: `SELECT * FROM a WHERE a.x IN (SELECT b.x FROM b);` (when planner picks unique-ification).

### SetOpPath (T_SetOpPath)

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

**Purpose**: `INTERSECT`/`EXCEPT` (with or without ALL). UNION is implemented via Append + (HashAgg or Sort+Unique) without a SetOp node.

**Constructor**: `create_setop_path(...)` at `pathnode.c:3555`.

**Cost function**: None — inline. Adds `cpu_operator_cost * subpath->rows * numCols`.

**Pathkey behavior**: For SETOP_SORTED, preserves input pathkeys; for SETOP_HASHED, NIL.

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: [`create_setop_plan`](19_plan_creator_catalog.md#create_setop_plan) at `createplan.c:2720` produces `SetOp` (`plannodes.h:1217`).

**When chosen**: Whenever `INTERSECT` or `EXCEPT` appears in the query.

**Example SQL**: `SELECT a FROM t1 INTERSECT SELECT b FROM t2;` → `HashSetOp Intersect -> Append -> Seq Scan t1 + Seq Scan t2`

### RecursiveUnionPath (T_RecursiveUnionPath)

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

**Purpose**: `WITH RECURSIVE x AS (non_recursive_term UNION [ALL] recursive_term)`. Iterates the recursive term, accumulating tuples until no new ones are produced, optionally deduplicating.

**Constructor**: `create_recursiveunion_path(...)` at `pathnode.c:3617`.

**Cost function**: `cost_recursive_union()` at `costsize.c:1813`. Cost of left + 10× the cost of right (heuristic for typical iteration counts) + dedup cost.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: [`create_recursiveunion_plan`](19_plan_creator_catalog.md#create_recursiveunion_plan) at `createplan.c:2756` produces `RecursiveUnion` (`plannodes.h:325`).

**When chosen**: For any `WITH RECURSIVE` CTE.

**Example SQL**: `WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<10) SELECT * FROM r;` → `CTE Scan -> RecursiveUnion -> Result + WorkTable Scan`

### LimitPath (T_LimitPath)

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

**Purpose**: `LIMIT`/`OFFSET`/`FETCH FIRST ... WITH TIES`.

**Constructor**: `create_limit_path(...)` at `pathnode.c:3826`. Calls `adjust_limit_rows_costs()` to scale rows and proportionally compute startup/total cost.

**Cost function**: None — uses `adjust_limit_rows_costs()` (`pathnode.c:3881`).

**Pathkey behavior**: Inherits from subpath.

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: [`create_limit_plan`](19_plan_creator_catalog.md#create_limit_plan) at `createplan.c:2856` produces `Limit` (`plannodes.h:1270`). For WITH TIES, also extracts unique-key columns from the parse's sortClause.

**Example SQL**: `SELECT * FROM t LIMIT 10;` → `Limit -> Seq Scan on t`

### ProjectionPath (T_ProjectionPath)

**Identity**: struct `ProjectionPath` defined at `src/include/nodes/pathnodes.h:2173`.

```c
typedef struct ProjectionPath
{
    Path        path;
    Path       *subpath;
    bool        dummypp;            /* true if no separate Result needed */
} ProjectionPath;
```

**Purpose**: A tlist computation step. The `dummypp` flag tells the plan creator whether a separate `Result` node is needed or whether the subpath can absorb the projection.

**Constructor**: `create_projection_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target)` at `pathnode.c:2685`. Also `create_set_projection_path()` at line 2882 for set-returning-function tlists (which produces a ProjectSetPath, not ProjectionPath; named confusingly).

**Cost function**: None — inline.

**Pathkey behavior**: Inherited from subpath.

**Parameterization**: No (`param_info = NULL` always).

**Parallel-aware**: As subpath, AND-ed with `is_parallel_safe(target->exprs)`.

**Plan counterpart**: [`create_projection_plan`](19_plan_creator_catalog.md#create_projection_plan) at `createplan.c:2019`. May produce a `Result` node, or simply replace the subplan's targetlist when projection-capable.

**Example SQL**: Whenever a Path's pathtarget differs from its subpath's pathtarget — e.g., `SELECT x+1 FROM t;` if the +1 evaluation cannot fold into the SeqScan.

### ProjectSetPath (T_ProjectSetPath)

**Identity**: struct `ProjectSetPath` defined at `src/include/nodes/pathnodes.h:2185`.

```c
typedef struct ProjectSetPath
{
    Path        path;
    Path       *subpath;
} ProjectSetPath;
```

**Purpose**: Tlist evaluation that includes set-returning functions in the SELECT list. Always requires a separate executor node.

**Constructor**: `create_set_projection_path()` at `pathnode.c:2882`.

**Cost function**: None — inline. Per-row cost is `cpu_tuple_cost + tlist_cost.per_tuple` per output tuple, where `tlist_rows` factors in the SRF expansion.

**Pathkey behavior**: Inherited from subpath.

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: [`create_project_set_plan`](19_plan_creator_catalog.md#create_project_set_plan) at `createplan.c:1613` produces `ProjectSet` (`plannodes.h:208`).

**Example SQL**: `SELECT id, generate_series(1,3) FROM t;` → `ProjectSet -> Seq Scan on t`

### MaterialPath (T_MaterialPath)

**Identity**: struct `MaterialPath` defined at `src/include/nodes/pathnodes.h:1981`.

```c
typedef struct MaterialPath
{
    Path        path;
    Path       *subpath;
} MaterialPath;
```

**Purpose**: Material node — a tuplestore-backed cache that allows mark/restore and avoids re-executing an expensive subpath on rescan.

**Constructor**: `create_material_path(RelOptInfo *rel, Path *subpath)` at `pathnode.c:1566`.

**Cost function**: `cost_material()` at `costsize.c:2453`. Storage cost based on `work_mem` overflow to disk; per-tuple read cost.

**Pathkey behavior**: Inherits subpath pathkeys.

**Parameterization**: Inherits from subpath.

**Parallel-aware**: As subpath.

**Plan counterpart**: [`create_material_plan`](19_plan_creator_catalog.md#create_material_plan) at `createplan.c:1639` produces `Material` (`plannodes.h:880`).

**When chosen**: As inner side of certain mergejoins (forced via `final_cost_mergejoin`'s `materialize_inner`), to enable mark/restore on input that cannot natively support it, or when the planner expects many rescans of an expensive subplan.

**Example SQL**: `SELECT * FROM t1, t2 WHERE t1.x = t2.x;` (with mergejoin and inner needing materializing) → `Merge Join -> Sort -> Material -> Sort`

### MemoizePath (T_MemoizePath)

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

**Purpose**: Memoize node — caches results of a parameterized inner subpath keyed by the bound parameter values, so repeated nestloop scans with the same key avoid rescanning.

**Constructor**: `create_memoize_path(...)` at `pathnode.c:1598`. Initial cost is `subpath cost + cpu_tuple_cost`; the more sophisticated rescan-cost analysis happens later in `cost_memoize_rescan()`.

**Cost function**: `cost_memoize_rescan()` at `costsize.c:2509`. Models hit ratio based on number of distinct parameter values vs. expected calls; sets `est_entries` based on `work_mem` capacity.

**Pathkey behavior**: Inherited from subpath.

**Parameterization**: Yes — that is the whole point.

**Parallel-aware**: No (`parallel_aware = false`).

**Plan counterpart**: [`create_memoize_plan`](19_plan_creator_catalog.md#create_memoize_plan) at `createplan.c:1667` produces `Memoize` (`plannodes.h:889`).

**When chosen**: Inner side of nestloops where the parameter values repeat enough to make caching worthwhile. Considered automatically by `add_paths_to_joinrel`.

**Example SQL**: `SELECT * FROM big_outer JOIN small_inner ON outer.k = inner.k;` (with few distinct k's in outer) → `Nested Loop -> Seq Scan big_outer -> Memoize -> Index Scan small_inner`

### GroupResultPath (T_GroupResultPath)

**Identity**: struct `GroupResultPath` defined at `src/include/nodes/pathnodes.h:1969`.

```c
typedef struct GroupResultPath
{
    Path        path;
    List       *quals;              /* HAVING clauses (bare expressions) */
} GroupResultPath;
```

**Purpose**: The degenerate grouping case where we know we should produce exactly one row (`SELECT count(*) FROM t WHERE false;` — query is empty but the aggregate yields one row anyway). The HAVING qual filters that single row.

**Constructor**: `create_group_result_path(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, List *havingqual)` at `pathnode.c:1518`.

**Cost function**: None — inline (cannot quite reuse `cost_resultscan` because the quals are not baserestrictinfo).

**Pathkey behavior**: Always `NIL` (single row).

**Parameterization**: No.

**Parallel-aware**: No.

**Plan counterpart**: [`create_group_result_plan`](19_plan_creator_catalog.md#create_group_result_plan) at `createplan.c:1588` produces `Result` (`plannodes.h:196`). Discriminated from other Result-producing paths by `IsA(best_path, GroupResultPath)`.

**When chosen**: For aggregate queries on empty FROM (`SELECT count(*) FROM t WHERE false`) or all-rows-eliminated queries that still need to produce the empty-aggregate result.

**Example SQL**: `SELECT count(*) FROM t WHERE 1=0;` → `Result  Filter: ...`

### UpperUniquePath (T_UpperUniquePath)

**Identity**: struct `UpperUniquePath` defined at `src/include/nodes/pathnodes.h:2239`.

```c
typedef struct UpperUniquePath
{
    Path        path;
    Path       *subpath;
    int         numkeys;
} UpperUniquePath;
```

**Purpose**: Adjacent-duplicate elimination on presorted input (used for `SELECT DISTINCT` with a Sort-based plan).

**Constructor**: `create_upper_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, int numCols, double numGroups)` at `pathnode.c:3103`. `total_cost = subpath->total_cost + cpu_operator_cost * subpath->rows * numCols`.

**Cost function**: None — inline.

**Pathkey behavior**: Inherits subpath pathkeys.

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: [`create_upper_unique_plan`](19_plan_creator_catalog.md#create_upper_unique_plan) at `createplan.c:2281` produces `Unique` (`plannodes.h:1112`). Discriminated from `UniquePath` via `IsA(best_path, UpperUniquePath)` in dispatch.

**When chosen**: For `SELECT DISTINCT` when the plan is sort-based (HashAgg-based DISTINCT uses AggPath instead).

**Example SQL**: `SELECT DISTINCT a FROM t ORDER BY a;` → `Unique -> Sort -> Seq Scan on t`

### GroupPath (T_GroupPath)

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

**Purpose**: Grouping-without-aggregation on presorted input — a `GROUP BY` query that only produces one row per group with no aggregate functions.

**Constructor**: `create_group_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *groupClause, List *qual, double numGroups)` at `pathnode.c:3044`.

**Cost function**: `cost_group()` at `costsize.c:3163`.

**Pathkey behavior**: Preserves subpath pathkeys (Group requires sorted input).

**Parameterization**: No.

**Parallel-aware**: As subpath.

**Plan counterpart**: [`create_group_plan`](19_plan_creator_catalog.md#create_group_plan) at `createplan.c:2242` produces `Group` (`plannodes.h:967`).

**When chosen**: Rarely standalone — usually superseded by AggPath. Used when grouping the output of an already-sorted input and not actually aggregating.

**Example SQL**: `SELECT a FROM t GROUP BY a;` (without aggregates) when planner picks sort-based grouping → `Group -> Sort -> Seq Scan on t`

---

## Append and partition paths

The two Path subtypes that combine the outputs of multiple subpaths into a single relation: `AppendPath` and `MergeAppendPath`. Both are also the substrate for partition-wise plans (each partition's scan becomes a child path), and for inheritance hierarchies.

### AppendPath (T_AppendPath)

**Identity**: struct `AppendPath` defined at `src/include/nodes/pathnodes.h:1931`.

```c
typedef struct AppendPath
{
    Path        path;
    List       *subpaths;           /* list of component Paths */
    int         first_partial_path; /* index of first partial subpath */
    Cardinality limit_tuples;       /* hard limit on output tuples, or -1 */
} AppendPath;
```

**Purpose**: Append plan — concatenating tuples from several subpaths, in arbitrary order. Used for:

1. Partitioned-table or inheritance-hierarchy scans (one subpath per partition/child).
2. UNION ALL of subqueries.
3. Empty-result placeholders (an AppendPath with `subpaths == NIL` represents a provably-empty relation; macros `IS_DUMMY_APPEND` and `IS_DUMMY_REL` test for this).
4. Parallel Append: combines partial and non-partial subpaths under a single shared-state coordinator.

**Constructor**: `create_append_path(PlannerInfo *root, RelOptInfo *rel, List *subpaths, List *partial_subpaths, List *pathkeys, Relids required_outer, int parallel_workers, bool parallel_aware, double rows)` at `pathnode.c:1244`. Notable behaviors:

- For parallel-aware Append, sorts non-partial paths by descending total cost and partial paths by descending startup cost (so workers pick expensive jobs first while leader picks cheapest startup).
- Sets `first_partial_path = list_length(subpaths)` then concatenates partial paths after non-partial.
- Special-cases single-child Append: cost equals child's cost (planner will collapse the Append in setrefs.c).
- Applies query-wide LIMIT to `limit_tuples` if the rel covers the whole query.

**Cost function**: `cost_append()` at `costsize.c:2231`. For non-parallel: `startup_cost = first_subpath->startup_cost`, `total_cost = sum(subpath->total_cost)`. For parallel: estimates per-worker share of work, accounts for non-partial paths each running on a single worker.

**Pathkey behavior**: Caller-supplied. Append generally does not preserve ordering, but for partition-wise plans where each child is sorted compatibly and pathkeys are inherited, an ordered Append is meaningful.

**Parameterization**: Yes. The constructor checks that all subpaths have the same `PATH_REQ_OUTER` and uses `get_baserel_parampathinfo` (for baserels) or `get_appendrel_parampathinfo` (for joinrels and partition trees) to set `param_info`.

**Parallel-aware**: Yes when `parallel_aware = true`. Both leader and workers participate; non-partial children run start-to-finish on a single worker each, partial children are split across multiple workers.

**Plan counterpart**: [`create_append_plan`](19_plan_creator_catalog.md#create_append_plan) at `createplan.c:1217` produces `Append` (`plannodes.h:265`). Includes `apprelids` (RTIs of the appendrels), `nasyncplans` count for foreign-table async execution, `first_partial_plan`, and `part_prune_info` for runtime partition pruning.

**When chosen**: Always used for partitioned tables and inheritance hierarchies that are not proven to need ordering preserved. Also for UNION ALL.

**Example SQL**:
```sql
SELECT * FROM partitioned_table;
-- Append
--   -> Seq Scan on partition_1
--   -> Seq Scan on partition_2
--   -> Seq Scan on partition_3
```

### MergeAppendPath (T_MergeAppendPath)

**Identity**: struct `MergeAppendPath` defined at `src/include/nodes/pathnodes.h:1955`.

```c
typedef struct MergeAppendPath
{
    Path        path;
    List       *subpaths;           /* list of component Paths */
    Cardinality limit_tuples;       /* hard limit on output tuples, or -1 */
} MergeAppendPath;
```

**Purpose**: MergeAppend plan — k-way merge of presorted subpaths, preserving overall sort order. Used when:

1. Querying a partitioned table with `ORDER BY` matching a partition-key prefix where each partition can be scanned in order.
2. Querying an inheritance hierarchy with ordered output requirements.

**Constructor**: `create_merge_append_path(PlannerInfo *root, RelOptInfo *rel, List *subpaths, List *pathkeys, Relids required_outer)` at `pathnode.c:1415`. For each subpath, if its pathkeys do not match the desired pathkeys, computes the cost of inserting a Sort node above it (using a dummy `cost_sort` call) and accumulates that into total cost. Special-cases single-subpath: degenerates into the child's own cost.

**Cost function**: `cost_merge_append()` at `costsize.c:2404`. Input costs (with implicit Sort costs included from constructor) + heap-based merge cost (`2.0 * cpu_operator_cost * N * log2(num_subpaths)`) over all output tuples. GUC dependencies: `cpu_operator_cost`, `cpu_tuple_cost`.

**Pathkey behavior**: Output pathkeys = the desired sort order (caller-supplied). Each subpath must produce output ordered by these pathkeys (Sort nodes are inserted at plan time to enforce this).

**Parameterization**: Limited — the constructor uses `get_appendrel_parampathinfo` (no `get_baserel_parampathinfo` fallback), and there is an assert in `create_merge_append_plan` that `param_info == NULL`. Currently the planner does not generate parameterized MergeAppend.

**Parallel-aware**: No (`parallel_aware = false` always; `parallel_workers = 0`). MergeAppend's heap-based merge is not parallelizable.

**Plan counterpart**: [`create_merge_append_plan`](19_plan_creator_catalog.md#create_merge_append_plan) at `createplan.c:1438` produces `MergeAppend` (`plannodes.h:287`). Plan generation also runs `prepare_sort_from_pathkeys` to set `sortColIdx`/`sortOperators`/`collations`/`nullsFirst` arrays for the executor's heap, and inserts explicit Sort nodes for unordered children. Like Append, supports `part_prune_info` for runtime partition pruning.

**When chosen**: When (a) querying a partitioned table with ORDER BY where each partition can be cheaply produced in order (common case: indexed scans on each partition), and (b) the alternative (Append + Sort over the whole result) is more expensive.

**Example SQL**:
```sql
SELECT * FROM partitioned_t ORDER BY partition_key;
-- Merge Append
--   Sort Key: partition_key
--   -> Index Scan partition_1 (ordered)
--   -> Index Scan partition_2 (ordered)
--   -> Index Scan partition_3 (ordered)
```

### Partition-wise variants

There are no separate "PartitionwiseAppendPath" types — partition-wise plans are constructed using ordinary AppendPath or MergeAppendPath whose subpaths are themselves base relation paths (partition-wise scans), join paths (partition-wise join), or aggregate paths (partition-wise aggregation). The relevant facilities:

- **Partition-wise scan**: each leaf partition becomes a child relation of the partitioned table's RelOptInfo, and `set_append_rel_pathlist()` calls `add_paths_to_append_rel()` to build an AppendPath over each partition's cheapest path.
- **Partition-wise join**: when `enable_partitionwise_join = on` and the partitioning of two joined tables matches, `try_partitionwise_join()` builds joinrels for each pair of matching partitions, then an AppendPath aggregates them.
- **Partition-wise aggregation**: when `enable_partitionwise_aggregate = on`, partial aggregates are computed per partition and then combined via an AppendPath plus a Finalize Agg.
- **Runtime partition pruning** is encoded as `PartitionPruneInfo` attached to the resulting `Append` or `MergeAppend` plan. See [Module 13](13_inheritance_and_partitioning.md) and [Module 20.12](20_deep_dives.md#2012-partition-pruning-at-plan-vs-execution-time).

---

## Parallel paths

The two Path subtypes whose sole purpose is to coordinate parallel execution: `GatherPath` and `GatherMergePath`. Plus the parallel-aware variants of other Path types.

PostgreSQL's parallelism uses two related concepts:

- **Partial path**: a path whose subpath can be split across multiple workers, each producing a fraction of the total rows. Lives in `RelOptInfo->partial_pathlist`. `parallel_workers > 0` indicates the suggested degree of parallelism.
- **Parallel-aware path**: a path whose plan node uses parallel-execution coordination (shared memory, atomic counters, etc.). Marked by `parallel_aware = true`.

A non-parallel-aware path can still appear inside a parallel plan — it just runs entirely on one worker. The Gather/GatherMerge layer is what converts a partial path back into a regular (non-partial) path.

### GatherPath (T_GatherPath)

**Identity**: struct `GatherPath` defined at `src/include/nodes/pathnodes.h:2041`.

```c
typedef struct GatherPath
{
    Path        path;
    Path       *subpath;            /* path for each worker */
    bool        single_copy;        /* don't execute path more than once */
    int         num_workers;        /* number of workers sought to help */
} GatherPath;
```

**Purpose**: Gather plan — the parallel leader collects tuples from all workers running `subpath`, in arbitrary order. After Gather, the result is no longer parallel and the rest of the plan runs in the leader.

**Constructor**: `create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target, Relids required_outer, double *rows)` at `pathnode.c:1972`. Subpath-driven worker count: `num_workers = subpath->parallel_workers`. If subpath has zero workers (degenerate single-copy case), `num_workers` is set to 1 and `single_copy = true`.

**Cost function**: `cost_gather()` at `costsize.c:436`. `subpath cost + parallel_setup_cost + parallel_tuple_cost * subpath->rows`. Each tuple shipped through the worker→leader queue costs `parallel_tuple_cost`. GUC dependencies: `parallel_setup_cost`, `parallel_tuple_cost`.

**Pathkey behavior**: Always `NIL` — Gather destroys ordering. Special exception: when `subpath->parallel_workers == 0`, `single_copy = true` and `pathkeys` are inherited.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: The Gather node is `parallel_aware = false` (it is the coordinator, not a worker). `parallel_safe = false` because Gather cannot be inside another parallel section. `parallel_workers = 0` post-Gather.

**Plan counterpart**: [`create_gather_plan`](19_plan_creator_catalog.md#create_gather_plan) at `createplan.c:1920` produces `Gather` (`plannodes.h:1140`). Sets `root->glob->parallelModeNeeded = true`. Pushes projection to the worker via `CP_EXACT_TLIST`.

**When chosen**: As the topmost layer of a parallel scan/join when ordering does not matter or when a Sort above will reimpose order.

**Example SQL**: `SELECT count(*) FROM big WHERE x > 0;` → `Finalize Aggregate -> Gather (Workers=4) -> Partial Aggregate -> Parallel Seq Scan on big`

### GatherMergePath (T_GatherMergePath)

**Identity**: struct `GatherMergePath` defined at `src/include/nodes/pathnodes.h:2053`.

```c
typedef struct GatherMergePath
{
    Path        path;
    Path       *subpath;            /* path for each worker */
    int         num_workers;        /* number of workers sought to help */
} GatherMergePath;
```

**Purpose**: Like Gather, but preserves the common sort order of the worker outputs. Each worker produces tuples in the same order, and the leader merges them via a binary heap.

**Constructor**: `create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target, List *pathkeys, Relids required_outer, double *rows)` at `pathnode.c:1881`. Asserts `subpath->parallel_safe` and `pathkeys != NIL`. For unordered subpaths, includes the cost of sorting each worker's output via a dummy `cost_sort()` call.

**Cost function**: `cost_gather_merge()` at `costsize.c:474`. Subpath costs + per-tuple comparison cost for merge `cpu_operator_cost * 2.0 * N * log2(num_workers + 1)` + parallel-tuple-cost overhead (1.05× standard parallel_tuple_cost as a heuristic).

**Pathkey behavior**: Output pathkeys = caller-supplied (and must be a prefix of subpath pathkeys, or the constructor pre-pays for sorting).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: GatherMerge itself is the coordinator; `parallel_aware = false`.

**Plan counterpart**: [`create_gather_merge_plan`](19_plan_creator_catalog.md#create_gather_merge_plan) at `createplan.c:1958` produces `GatherMerge` (`plannodes.h:1155`). Calls `prepare_sort_from_pathkeys()` to set up sort columns. Asserts at plan time that the subpath actually produces sufficiently sorted output.

**When chosen**: Whenever the query needs ordered output and the producing subpath is parallel-safe and either already ordered or cheaply sortable per-worker.

**Example SQL**: `SELECT * FROM big ORDER BY id;` (with parallel index scan possible) → `Gather Merge (Workers=4) Sort Key: id -> Parallel Index Scan using big_pkey`

### Parallel-aware variants of other Path types

The catalog rows for SeqScan, IndexScan, BitmapHeapPath, HashPath, and AppendPath all admit parallel-aware variants. There is no separate struct — instead, the same Path struct is allocated with `parallel_aware = true` and `parallel_workers > 0`. The constructors decide based on their arguments:

- **`create_seqscan_path(..., int parallel_workers)`** — `parallel_aware = (parallel_workers > 0)`. The cost model in `cost_seqscan` divides run cost by `get_parallel_divisor(path)`. Plan is still `SeqScan`, executed via `nodeSeqscan.c`'s parallel-aware code paths.
- **`create_index_path(..., bool partial_path)`** — when `partial_path = true`, the IndexPath is parallel-aware. Currently implemented for B-tree indexes only.
- **`create_bitmap_heap_path(..., int parallel_degree)`** — `parallel_aware = (parallel_degree > 0)`. The BitmapAnd/BitmapOr/BitmapIndex subtree is built once into a shared bitmap; workers then scan heap pages from that shared bitmap concurrently. Note that BitmapAndPath and BitmapOrPath themselves are *not* parallel-aware.
- **`create_hashjoin_path(..., bool parallel_hash)`** — when `parallel_hash = true`, the inner side is built into a shared hash table that all workers participate in constructing and probing. The Hash plan node (built inside `create_hashjoin_plan`) gets `parallel_aware = true` and receives `rows_total`.
- **`create_append_path(..., bool parallel_aware)`** — Parallel Append. The AppendPath includes both partial and non-partial subpaths; non-partial children each run on a single worker, partial children are split across multiple workers.

#### Parallel-safety propagation

Every Path constructor sets `parallel_safe` based on rules that depend on the path type:

- For scans: `parallel_safe = rel->consider_parallel`.
- For joins: `parallel_safe = joinrel->consider_parallel && outer->parallel_safe && inner->parallel_safe`.
- For upper-relation paths (Sort, Agg, etc.): `parallel_safe = rel->consider_parallel && subpath->parallel_safe`, AND-ed with `is_parallel_safe()` checks on tlist and qual expressions.
- ModifyTable paths and LockRows paths force `parallel_safe = false`.

A partial path can be wrapped by Gather only if its tree is entirely parallel-safe; the parallel-aware nodes within that tree then coordinate via shared memory.

---

## Modify paths

The two Path subtypes that introduce data-modifying or row-locking semantics: `ModifyTablePath` (the wrapper for INSERT/UPDATE/DELETE/MERGE) and `LockRowsPath` (the wrapper for `SELECT ... FOR UPDATE/SHARE`). Both sit at the top of their respective plan trees.

### ModifyTablePath (T_ModifyTablePath)

**Identity**: struct `ModifyTablePath` defined at `src/include/nodes/pathnodes.h:2375`.

```c
typedef struct ModifyTablePath
{
    Path        path;
    Path       *subpath;            /* Path producing source data */
    CmdType     operation;          /* INSERT, UPDATE, DELETE, or MERGE */
    bool        canSetTag;          /* do we set the command tag/es_processed? */
    Index       nominalRelation;    /* parent RT index for use of EXPLAIN */
    Index       rootRelation;       /* root RT index, if partitioned/inherited */
    bool        partColsUpdated;    /* some part key in hierarchy updated? */
    List       *resultRelations;    /* integer list of RT indexes */
    List       *updateColnosLists;  /* per-target-table update_colnos lists */
    List       *withCheckOptionLists;
    List       *returningLists;     /* per-target-table RETURNING tlists */
    List       *rowMarks;           /* PlanRowMarks (non-locking only) */
    OnConflictExpr *onconflict;     /* ON CONFLICT clause, or NULL */
    int         epqParam;           /* EvalPlanQual re-eval param */
    List       *mergeActionLists;   /* per-target-table MERGE action lists */
    List       *mergeJoinConditions;/* per-target-table MERGE join conditions */
} ModifyTablePath;
```

**Purpose**: The wrapper path for any data-modifying statement. The `subpath` produces the rows that will drive the modification (for INSERT, this is typically a Result with VALUES or a SELECT subquery; for UPDATE/DELETE/MERGE, this is the qualified source tuples with implicit ctid-or-equivalent identifiers).

**Constructor**: `create_modifytable_path(...)` at `pathnode.c:3725`. Validation asserts: update lists match result-relations count; WithCheckOption / Returning lists either NIL or match result-relations count. Cost computation: inline. Cost = `subpath->startup_cost / subpath->total_cost` (no per-row write overhead is charged — comment says "would only be window dressing since ModifyTable is always top-level"). Row count: when `returningLists != NIL`, `path.rows = subpath->rows`; otherwise `path.rows = 0`.

**Cost function**: None — inline.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: No (`param_info = NULL` always).

**Parallel-aware**: `parallel_safe = false` always — DML cannot currently be parallelized.

**Plan counterpart**: [`create_modifytable_plan`](19_plan_creator_catalog.md#create_modifytable_plan) at `createplan.c:2815` produces `ModifyTable` (`plannodes.h:229`). Notable features:

- Calls `create_plan_recurse(root, subpath, CP_EXACT_TLIST)` so the subplan produces exactly the columns expected by the modification.
- Calls `apply_tlist_labeling(subplan->targetlist, root->processed_tlist)` so resname/resjunk labels match the parser's expectation.
- Delegates to `make_modifytable()` which itself calls `expand_inherited_targets()` and may construct one or more `ForeignScan` direct-modify plans for foreign target tables.
- Threads ON CONFLICT info, MERGE action lists, returning lists, and PlanRowMarks into the plan node.

**When chosen**: Always for any DML statement. Always sole path on the topmost RelOptInfo for INSERT/UPDATE/DELETE/MERGE.

**Example SQL**:
```sql
UPDATE big SET v = v + 1 WHERE k > 100;
-- ModifyTable (Update on big)
--   -> Seq Scan on big
--      Filter: (k > 100)
```

### LockRowsPath (T_LockRowsPath)

**Identity**: struct `LockRowsPath` defined at `src/include/nodes/pathnodes.h:2360`.

```c
typedef struct LockRowsPath
{
    Path        path;
    Path       *subpath;
    List       *rowMarks;           /* a list of PlanRowMark's */
    int         epqParam;           /* ID of Param for EvalPlanQual re-eval */
} LockRowsPath;
```

**Purpose**: `SELECT ... FOR UPDATE` / `FOR NO KEY UPDATE` / `FOR SHARE` / `FOR KEY SHARE`. Acquires row locks on the target tuples and (if a concurrent update happens) re-evaluates the query against the new tuple via EvalPlanQual.

**Constructor**: `create_lockrows_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *rowMarks, int epqParam)` at `pathnode.c:3662`. `total_cost = subpath->total_cost + cpu_tuple_cost * subpath->rows`. The comment notes this is a stab in the dark — actual lock cost is hard to estimate.

**Cost function**: None — inline.

**Pathkey behavior**: Always `NIL`. The comment explains: "result cannot be assumed sorted, since locking might cause the sort key columns to be replaced with new values" (when EvalPlanQual re-evaluates against an updated row).

**Parameterization**: No.

**Parallel-aware**: `parallel_safe = false`.

**Plan counterpart**: [`create_lockrows_plan`](19_plan_creator_catalog.md#create_lockrows_plan) at `createplan.c:2792` produces `LockRows` (`plannodes.h:1256`). Trivial: just calls `create_plan_recurse` on the subpath, then `make_lockrows()` to wrap.

**When chosen**: Whenever the query has a `FOR UPDATE/SHARE` clause (set in `root->parse->rowMarks` and propagated to LockRowsPath in `grouping_planner`).

**Example SQL**:
```sql
SELECT * FROM accounts WHERE id = 42 FOR UPDATE;
-- LockRows
--   -> Index Scan using accounts_pkey on accounts
--        Index Cond: (id = 42)
```

---

## Cross-references

- Plan creators: [Module 19 Plan creator catalog](19_plan_creator_catalog.md).
- The Path/Plan correspondence diagram: [Module 16](16_plan_creation_and_setrefs.md).
- The cost-model formulas behind every `cost_*` function: [Module 10 Cost model and selectivity](10_cost_model_and_selectivity.md).
- The `add_path` Pareto-dominance rule that picks among competing Paths: [Module 20.1](20_deep_dives.md#201-add_path-pareto-dominance-test-and-the-cost_diff_fuzz_factor).
- ParamPathInfo and parameterized paths: [Module 20.9](20_deep_dives.md#209-parameterized-paths-and-parampathinfo).
- The Memoize decision rule: [Module 20.14](20_deep_dives.md#2014-memoize-node-decision-and-cost).

Next: [19 Plan creator catalog](19_plan_creator_catalog.md).
