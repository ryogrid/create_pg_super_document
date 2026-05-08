# Path Catalog: Parallel Paths

This file documents the two Path subtypes whose sole purpose is to coordinate parallel execution: `GatherPath` and `GatherMergePath`. It also describes how parallel-awareness propagates through other Path types (parallel-aware SeqScan, IndexScan, BitmapHeapScan, Hash, and Append).

## Conceptual Overview

PostgreSQL's parallelism uses two concepts that map onto Path attributes:

- **Partial path**: a path whose subpath can be split across multiple workers, each producing a fraction of the total rows. Partial paths live in `RelOptInfo->partial_pathlist`. Their `parallel_workers > 0` indicates the suggested degree of parallelism.
- **Parallel-aware path**: a path whose plan node uses parallel-execution coordination (shared memory, atomic counters, etc.). Marked by `parallel_aware = true`.

A non-parallel-aware path can still appear inside a parallel plan — it just runs entirely on one worker. The Gather/GatherMerge layer is what converts a partial path back into a regular (non-partial) path that the rest of the plan can consume.

---

## GatherPath (T_GatherPath)

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

**Purpose**: Represents a Gather plan — the parallel leader collects tuples from all workers running `subpath`, in arbitrary order. After Gather, the result is "no longer parallel" and the rest of the plan runs in the leader.

**Constructor**: `create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target, Relids required_outer, double *rows)` at `src/backend/optimizer/util/pathnode.c:1972`.
   - Allocation: `makeNode(GatherPath)`.
   - Cost computation: inline `cost_gather()`.
   - Subpath-driven worker count: `num_workers = subpath->parallel_workers`. If subpath has zero workers (degenerate single-copy case), `num_workers` is set to 1 and `single_copy = true`.

**Cost function**: `cost_gather()` at `src/backend/optimizer/path/costsize.c:436`.
   - Formula summary: `subpath cost + parallel_setup_cost + parallel_tuple_cost * subpath->rows`. Each tuple shipped through the worker→leader queue costs `parallel_tuple_cost`.
   - GUC dependencies: `parallel_setup_cost`, `parallel_tuple_cost`.

**Pathkey behavior**: Always `NIL` — Gather destroys ordering (workers return tuples in nondeterministic interleaving). Special exception: when `subpath->parallel_workers == 0`, `single_copy = true` and `pathkeys` are inherited (degenerate single-worker case).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: The Gather node is `parallel_aware = false` (it's the coordinator, not a worker). `parallel_safe = false` because Gather can't be inside another parallel section. `parallel_workers = 0` post-Gather.

**Plan counterpart**: `create_gather_plan()` at `src/backend/optimizer/plan/createplan.c:1920` produces `Gather` (`plannodes.h:1140`). Sets `root->glob->parallelModeNeeded = true`. Pushes projection to the worker via `CP_EXACT_TLIST`. Assigns a special exec param via `assign_special_exec_param(root)` for runtime parameter coordination.

**When chosen**: As the topmost layer of a parallel scan/join when ordering doesn't matter or when a Sort above will reimpose order.

**Example SQL**: `SELECT count(*) FROM big WHERE x > 0;` → `Finalize Aggregate -> Gather (Workers=4) -> Partial Aggregate -> Parallel Seq Scan on big`

---

## GatherMergePath (T_GatherMergePath)

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

**Constructor**: `create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target, List *pathkeys, Relids required_outer, double *rows)` at `src/backend/optimizer/util/pathnode.c:1881`.
   - Asserts `subpath->parallel_safe` and `pathkeys != NIL`.
   - Allocation: `makeNode(GatherMergePath)`.
   - For unordered subpaths, includes the cost of sorting each worker's output via a dummy `cost_sort()` call.
   - Cost computation: `cost_gather_merge()`.

**Cost function**: `cost_gather_merge()` at `src/backend/optimizer/path/costsize.c:474`.
   - Formula summary: subpath costs + per-tuple comparison cost for merge `cpu_operator_cost * 2.0 * N * log2(num_workers + 1)` + parallel-tuple-cost overhead (1.05× standard parallel_tuple_cost as a heuristic).

**Pathkey behavior**: Output pathkeys = caller-supplied (and must be a prefix of subpath pathkeys, or the constructor pre-pays for sorting).

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: GatherMerge itself is the coordinator; `parallel_aware = false`.

**Plan counterpart**: `create_gather_merge_plan()` at `src/backend/optimizer/plan/createplan.c:1958` produces `GatherMerge` (`plannodes.h:1155`). Calls `prepare_sort_from_pathkeys()` to set up sort columns. Asserts at plan time that the subpath actually produces sufficiently sorted output (it relies on explicit Sorts having been inserted at path generation time).

**When chosen**: Whenever the query needs ordered output and the producing subpath is parallel-safe and either already ordered or cheaply sortable per-worker.

**Example SQL**: `SELECT * FROM big ORDER BY id;` (with parallel index scan possible) → `Gather Merge (Workers=4) Sort Key: id -> Parallel Index Scan using big_pkey`

---

## Parallel-aware Variants of Other Path Types

The Path subtype catalog rows for SeqScan, IndexScan, BitmapHeapPath, HashPath, and AppendPath all admit parallel-aware variants. There is no separate struct for these — instead, the same Path struct is allocated with `parallel_aware = true` and `parallel_workers > 0`. The constructors decide based on their arguments:

- **`create_seqscan_path(..., int parallel_workers)`** — `parallel_aware = (parallel_workers > 0)`. The cost model in `cost_seqscan` divides run cost by `get_parallel_divisor(path)`, an effective worker count that approximates leader-also-participates math. The plan is still `SeqScan`, executed via `nodeSeqscan.c`'s parallel-aware code paths.

- **`create_index_path(..., bool partial_path)`** — when `partial_path = true`, the IndexPath is parallel-aware. Currently implemented for B-tree indexes only (other AMs may not support parallel scan).

- **`create_bitmap_heap_path(..., int parallel_degree)`** — `parallel_aware = (parallel_degree > 0)`. The BitmapAnd/BitmapOr/BitmapIndex subtree is built once into a shared bitmap (via `bitmap_subplan_mark_shared` in `create_bitmap_scan_plan`); workers then scan heap pages from that shared bitmap concurrently. Note that BitmapAndPath and BitmapOrPath themselves are *not* parallel-aware — only the topmost BitmapHeapPath is.

- **`create_hashjoin_path(..., bool parallel_hash)`** — when `parallel_hash = true`, the inner side is built into a shared hash table that all workers participate in constructing and probing. The Hash plan node (built inside `create_hashjoin_plan`) gets `parallel_aware = true` and receives `rows_total` for sizing the shared hash table.

- **`create_append_path(..., bool parallel_aware)`** — Parallel Append. The AppendPath includes both partial and non-partial subpaths; non-partial children each run on a single worker, partial children are split across multiple workers. The constructor sorts non-partial paths by total cost descending and partial paths by startup cost descending so workers and leader pick optimally.

### Partial-path Semantics

A path is "partial" if a worker running it produces a fraction (typically `1/parallel_workers`) of the total tuples. Partial paths populate `RelOptInfo->partial_pathlist`. The relationship to parallel_aware:

- All partial paths must be parallel-safe (`parallel_safe = true`).
- A partial path is typically also parallel-aware (e.g., parallel SeqScan), but the topmost partial path under a Gather doesn't have to be — non-parallel-aware paths inside the partial-path tree just run on one worker.
- `set_baserel_partial_paths()` and `set_join_partial_paths()` populate `partial_pathlist`; `generate_useful_gather_paths()` consumes it to wrap each partial path in a GatherPath or GatherMergePath, producing regular paths for `pathlist`.

### Parallel-safety Propagation

Every Path constructor sets `parallel_safe` based on rules that depend on the path type:

- For scans, `parallel_safe = rel->consider_parallel`. `set_rel_consider_parallel()` (`allpaths.c`) determines this once per RelOptInfo, ruling out paths involving unsafe RTEs (e.g., temp tables, CTEs, certain functions).
- For joins, `parallel_safe = joinrel->consider_parallel && outer->parallel_safe && inner->parallel_safe`.
- For upper-relation paths (Sort, Agg, etc.), `parallel_safe = rel->consider_parallel && subpath->parallel_safe`, AND-ed with `is_parallel_safe()` checks on tlist and qual expressions when applicable.
- ModifyTable paths and LockRows paths force `parallel_safe = false` because writes can't be parallelized.

The end result: a partial path can be wrapped by Gather only if its tree is entirely parallel-safe; the parallel-aware nodes within that tree then coordinate via shared memory.
