# Component: Parallel Planning

> Stage 2 documentation for **PARALLEL_PLANNING**.
> Sources:
> - `src/backend/optimizer/path/allpaths.c`: `compute_parallel_worker`,
>   `set_rel_consider_parallel`, `create_plain_partial_paths`,
>   `create_partial_bitmap_paths`, `generate_useful_gather_paths`.
> - `src/backend/optimizer/path/costsize.c`: `cost_gather`,
>   `cost_gather_merge`, parallel divisors.
> - `src/backend/optimizer/util/clauses.c`: `is_parallel_safe`,
>   `max_parallel_hazard`, `max_parallel_hazard_walker`.
> - `src/backend/optimizer/util/pathnode.c`: `add_partial_path`,
>   `add_partial_path_precheck`, `create_gather_path`,
>   `create_gather_merge_path`.
> - `src/backend/optimizer/path/joinpath.c`: `try_partial_*_path`.
>
> Diagram: `diagrams/09_parallel_path_gen.mermaid`.

## 1. Why this exists

Parallel query splits a table scan / join / aggregate among multiple
backends. The planner needs to:

1. Decide whether parallelism is **safe** (no parallel-unsafe
   functions, no temp tables, etc.).
2. Decide the **worker count** per rel.
3. Build **partial paths** that produce a partition of the result.
4. Insert a **Gather** or **GatherMerge** node to combine them.
5. Cost the parallel option fairly so the cheapest plan wins.

---

## 2. Symbol table

| Symbol                                | File:line                                       | Importance | Tier |
|---------------------------------------|-------------------------------------------------|------------|------|
| `add_partial_path`                    | `src/backend/optimizer/util/pathnode.c:747`     | 0.78 | 1 |
| `add_partial_path_precheck`           | `src/backend/optimizer/util/pathnode.c`         | 0.55 | 2 |
| `compute_parallel_worker`             | `src/backend/optimizer/path/allpaths.c`         | 0.55 | 2 |
| `set_rel_consider_parallel`           | `src/backend/optimizer/path/allpaths.c`         | 0.50 | 2 |
| `create_plain_partial_paths`          | `src/backend/optimizer/path/allpaths.c`         | 0.50 | 2 |
| `create_partial_bitmap_paths`         | `src/backend/optimizer/path/allpaths.c`         | 0.45 | 3 |
| `generate_gather_paths`               | `src/backend/optimizer/path/allpaths.c`         | 0.55 | 2 |
| `generate_useful_gather_paths`        | `src/backend/optimizer/path/allpaths.c`         | 0.55 | 2 |
| `cost_gather`                         | `src/backend/optimizer/path/costsize.c`         | 0.50 | 2 |
| `cost_gather_merge`                   | `src/backend/optimizer/path/costsize.c`         | 0.45 | 3 |
| `max_parallel_hazard`                 | `src/backend/optimizer/util/clauses.c`          | 0.50 | 2 |
| `max_parallel_hazard_walker`          | `src/backend/optimizer/util/clauses.c`          | 0.40 | 3 |
| `is_parallel_safe`                    | `src/backend/optimizer/util/clauses.c`          | 0.45 | 3 |
| `try_partial_nestloop_path`           | `src/backend/optimizer/path/joinpath.c`         | 0.65 | 2 |
| `try_partial_mergejoin_path`          | `src/backend/optimizer/path/joinpath.c`         | 0.65 | 2 |
| `try_partial_hashjoin_path`           | `src/backend/optimizer/path/joinpath.c`         | 0.65 | 2 |
| `create_gather_path`                  | `src/backend/optimizer/util/pathnode.c`         | 0.50 | 2 |
| `create_gather_merge_path`            | `src/backend/optimizer/util/pathnode.c`         | 0.45 | 3 |

---

## 3. Parallel safety classification

`pg_proc.proparallel`:
- `s` (PROPARALLEL_SAFE): callable from a worker.
- `r` (PROPARALLEL_RESTRICTED): callable from leader only.
- `u` (PROPARALLEL_UNSAFE): can't run in parallel mode.

### 3.1 `max_parallel_hazard`
```c
char max_parallel_hazard(Query *parse);
```
Walks the entire query tree once at `standard_planner` start and
returns the worst hazard level encountered. The return value seeds
`glob->maxParallelHazard` and `glob->parallelModeOK`.

Hazards include: parallel-unsafe functions, temp tables, RowMarks
(SELECT FOR UPDATE), modifying CTE, user-defined aggregates without
combinefunc, foreign tables (FDW must opt in via
`IsForeignScanParallelSafe`), Vars referencing the leader's
`upper_params`, etc.

### 3.2 `is_parallel_safe(root, expr)`
Per-expression check used during path generation (e.g. when deciding
whether a qual can be pushed into a partial scan). Calls
`max_parallel_hazard_walker` on the expression with the appropriate
hazard threshold (UNSAFE for "must not appear", RESTRICTED for "OK
in leader paths only").

### 3.3 `set_rel_consider_parallel`
Per-rel decision. For RTE_RELATION it checks: not temp, no
parallel-unsafe quals, FDW says OK if foreign. For RTE_SUBQUERY it
checks recursively that the sub-Plan is parallel-safe. Sets
`rel->consider_parallel = true` on success.

---

## 4. `compute_parallel_worker`

```c
int compute_parallel_worker(RelOptInfo *rel, double heap_pages,
                             double index_pages, int max_workers);
```
Source: `src/backend/optimizer/path/allpaths.c`.

Logic (paraphrased):
- If `rel->rel_parallel_workers >= 0`, use that explicit setting
  (per-table `parallel_workers` reloption).
- Else compute `parallel_workers = log2(rel->pages /
  min_parallel_table_scan_size) + 1`. (Doubles per 2x-larger table.)
- Apply the same log2 step for `index_pages` against
  `min_parallel_index_scan_size`.
- Cap at `max_parallel_workers_per_gather` (and at
  `max_workers` argument).

Defaults:
- `min_parallel_table_scan_size`: 8 MB.
- `min_parallel_index_scan_size`: 512 KB.
- `max_parallel_workers_per_gather`: 2.

A return of 0 means "don't bother with parallel".

---

## 5. Partial paths

### 5.1 `create_plain_partial_paths`
For a base RTE_RELATION rel:
1. Compute `parallel_workers` via `compute_parallel_worker`.
2. If > 0: emit a parallel-aware `SeqScan` partial path via
   `create_seqscan_path` with that worker count.

### 5.2 `create_partial_bitmap_paths`
For a base rel, called from `create_index_paths` after a useful
`bitmapqual` has been chosen. Emits a partial `BitmapHeapScan` path.
Index nodes inside the bitmap don't need to be parallel-aware (one
worker scans the bitmap; the others share the page reads via parallel
table scan).

### 5.3 `add_partial_path` semantics (recap)
Source: `src/backend/optimizer/util/pathnode.c:747`.

Simpler than `add_path`:
- No parameterization considered (parameterized partial paths are
  unsupported because parallel workers can't synchronize parameter
  values).
- Compares only pathkeys + total_cost (no startup cost, no rows —
  every partial path produces the same overall row count).
- pfree dominated paths immediately (no IndexPath exception).

The `partial_pathlist` is kept sorted by total_cost so cheapest is
first.

### 5.4 Parallel join partial paths
`try_partial_nestloop_path` / `try_partial_mergejoin_path` /
`try_partial_hashjoin_path` (joinpath.c):
- For nestloop: outer must be partial (workers split outer rows);
  inner can be regular.
- For mergejoin: outer partial, inner regular; inner is rescanned
  per worker.
- For hashjoin: outer partial, inner can be parallel-aware (Parallel
  Hash) when `enable_parallel_hash`.

These call `initial_cost_*join`, then `add_partial_path_precheck`,
then `create_*_path`. The resulting paths land in `joinrel->partial_pathlist`.

---

## 6. Gather insertion

### 6.1 `generate_gather_paths`
For each Path in `rel->partial_pathlist`:
- Build a `GatherPath` (cost_gather):
  - `total_cost = subpath->total_cost + parallel_setup_cost +
    parallel_tuple_cost × subpath->rows`.
  - Output is `unsorted` (Gather doesn't preserve order).
  - `rows` adjusted by the parallel divisor.
- `add_path(rel, GatherPath)`.

### 6.2 `generate_useful_gather_paths`
Extends the above by also building `GatherMergePath`s where useful:
- For each interesting set of pathkeys (`root->query_pathkeys`,
  pathkeys requested by upper rels), find a partial path whose
  pathkeys cover the prefix; if found, build a
  `GatherMergePath` that preserves the per-worker sort order via a
  binary heap merge.
- `cost_gather_merge` adds `log2(num_workers)` per-tuple CPU on top
  of `cost_gather` to account for the heap maintenance.

### 6.3 When `generate_useful_gather_paths` is called
- After `set_rel_pathlist` for each base rel **except**:
  - inheritance children (one Gather per child blows the worker pool),
  - the topmost scan/join rel (deferred to `apply_scanjoin_target_to_paths`
    in grouping_planner so we can use the final tlist for projection).
- After `join_search_one_level` for each non-topmost joinrel.
- After GEQO's `merge_clump` (geqo_eval.c) for non-topmost joinrels.
- For the top scan/join rel: from `apply_scanjoin_target_to_paths` in
  grouping_planner — using `false` for `override_rows` initially, then
  `true` for upper rels.

---

## 7. Parallel aggregate

When the GROUP BY (or scalar agg) is parallel-safe and has a
combinefunc, the planner builds:
- A partial Agg path with `aggsplit = AGGSPLIT_INITIAL_SERIAL`. Each
  worker computes per-worker partial aggregates.
- A Gather/GatherMerge wrapper.
- A final Agg with `aggsplit = AGGSPLIT_FINAL_DESERIAL` that combines
  partials.

`create_grouping_paths` (planner.c) does this when the
`UPPERREL_GROUP_AGG` rel has `partial_pathlist` and the aggregates'
combinefuncs are parallel-safe.

---

## 8. Parallel hash join

Source: `src/backend/executor/nodeHashjoin.c` (executor). Planner
side: `try_partial_hashjoin_path` builds a HashPath with
`parallel_aware = true` on the inner Hash node. Inner side is built
in shared memory; all workers probe in parallel.

`enable_parallel_hash` controls whether this is considered.

---

## 9. Parallel append

Source: planner emits an `AppendPath` with `parallel_aware = true` and
worker-level subpaths. `enable_parallel_append` controls. The
executor coordinates which workers scan which subpaths via shared
state, automatically loading the "next" subpath as soon as a worker
is free (no lockstep).

`add_paths_to_append_rel` (allpaths.c) builds the parallel Append
path when at least some children have partial paths.

---

## 10. Parallel-mode flag and final plan

After `create_plan` builds the top plan, `standard_planner` (planner.c):
```c
if (top_plan->parallel_safe && glob->parallelModeOK
    && debug_parallel_query == DEBUG_PARALLEL_REGRESS) {
    /* wrap top_plan in a Gather for testing */
}

/* SS_finalize_plan computes parallel_safe propagation up the tree */
SS_finalize_plan(root, top_plan);

/* glob->parallelModeNeeded becomes true if any Gather/GatherMerge
   was actually emitted */
```

`glob->parallelModeNeeded` is what the executor checks to decide
whether to enter parallel mode (acquire ExecParallel state, set up
shared memory, etc.).

---

## 11. Cost-model intuition

- **Setup cost dominates** for very small queries: `parallel_setup_cost
  = 1000.0` discourages parallelism unless the workload is large.
- **Per-tuple shipping cost**: `parallel_tuple_cost = 0.1`. Each
  tuple sent worker→leader costs 100x a per-tuple operator. This
  prefers plans that ship few tuples (post-filter, post-aggregate).
- The **parallel divisor** in worker-side cost is ~ `parallel_workers
  + leader_contribution`. Leader contribution is fractional (default
  0.5 — leader does some work alongside workers).

---

## 12. Cross-references

- Worker safety classification (volatile / proparallel checks):
  `component_restrictinfo_and_clause_utils.md`
- AppendPath details: `component_inheritance_and_partitioning.md`
- Cost equations: `component_cost_model_and_selectivity.md`
- Plan-final fixups touching `parallelModeNeeded`:
  `component_plan_creation_and_setrefs.md`
- Diagram: `diagrams/09_parallel_path_gen.mermaid`.
