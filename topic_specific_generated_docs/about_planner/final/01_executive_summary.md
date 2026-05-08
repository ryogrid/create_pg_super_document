# 01. Executive Summary

Prerequisites: none — this module is the entry point. If you want
context first, see [index.md](./index.md).

---

## 1. Where the planner sits

```
SQL text
   │
   ▼
parser ── parse tree (RawStmt)
   │
   ▼
analyzer ── Query tree
   │
   ▼
rewriter ── Query tree (post-rule expansion)
   │
   ▼
PLANNER ── PlannedStmt
   │
   ▼
executor ── tuples
```

The planner's input is a post-rewrite `Query` tree. Its output is a
`PlannedStmt` whose `planTree` is a `Plan` node (root of a Plan tree)
plus auxiliary structures: `subplans`, `rtable`, `paramExecTypes`,
`relationOids`, `invalItems`, etc. The executor reads `PlannedStmt`
to set up its run-time state and iterate the plan.

The planner is invoked from `pg_plan_query()` in
`src/backend/tcop/postgres.c`, which in turn is called from prepared
statement preparation, `EXPLAIN`, immediate execution of
`PreparedStmt`s, and any code path that turns a `Query` into a
runnable plan. See [03_lifecycle_and_entry_points.md](./03_lifecycle_and_entry_points.md)
for a full call graph.

---

## 2. Path / Plan duality — the central design idea

The planner works in two stages on each query:

1. **Path stage**: enumerate access strategies as `Path` objects.
   Paths share substructure (an inner `Path` may appear under many
   join `Path`s; a base-rel `Path` may be referenced by multiple
   parameterized join wrappers). Paths carry costs but no execution
   state. Paths are organized per `RelOptInfo` (one rel per
   `RelOptInfo`; pathlists per rel). Pareto-style pruning via
   `add_path` (`src/backend/optimizer/util/pathnode.c:420`) keeps the
   pathlist small.
2. **Plan stage**: walk the cheapest surviving `Path` and convert it
   into a `Plan` tree via `create_plan` / `create_plan_recurse`
   (`src/backend/optimizer/plan/createplan.c:338, 389`). At this
   point sharing is broken — every `Plan` node is a fresh,
   self-contained executor instruction. Then `set_plan_references`
   (`src/backend/optimizer/plan/setrefs.c:287`) flattens the rangetable
   and rewires Vars to point at the executor's flat tuple slots.

This split is fundamental for performance:

- During search, sharing means a base-rel `Path` is built once and
  referenced N times under N different join shapes. Building each
  `Plan` node only once at the *end* avoids the combinatorial
  explosion of premature plan construction.
- After search, the executor needs a tree where every node has its
  own slots, lists, and var references. `create_plan` materializes
  exactly that.

See [18_path_catalog.md](./18_path_catalog.md) for all 32 `Path`
subtypes and [19_plan_creator_catalog.md](./19_plan_creator_catalog.md)
for the matching `create_*_plan` functions.

---

## 3. DP versus GEQO — why two join-search strategies

For *N* base rels, the number of possible join orderings (left-deep +
bushy, no cartesian filter) grows as
`O(3^N − 2^(N+1) + 1)` joinrels in the dynamic-programming lattice
(see `src/backend/optimizer/README`, "Optimizer Functions"). At
`N = 12` that's ≈ 530k joinrels — still tractable; at `N = 13` it's
≈ 1.6M, which starts to dominate planning time.

PostgreSQL therefore offers two strategies:

- **Standard DP search** (`standard_join_search`,
  `src/backend/optimizer/path/allpaths.c:3411`): exhaustive, optimal
  with respect to the cost model, used when
  `levels_needed < geqo_threshold` (default 12).
- **GEQO** (`geqo`, `src/backend/optimizer/geqo/geqo_main.c`): a
  genetic algorithm that explores a sample of the search space.
  Used when `levels_needed >= geqo_threshold && enable_geqo`. Plans
  may be sub-optimal but are produced in O(`pool_size *
  num_generations`) time.

Both are pluggable: extensions install a `join_search_hook` to
substitute their own algorithm. See [17_hooks_and_extensibility.md](./17_hooks_and_extensibility.md).

DP is preferred because its cost model is identical to the
single-rel one (every `make_join_rel` produces a `RelOptInfo` whose
pathlist is pruned by the same `add_path` rules). GEQO falls back
because for very large *N* the planner cannot afford exhaustive
search; below threshold, the per-query `create_plan` overhead is
negligible compared to the search itself.

See [08_join_paths_and_search.md](./08_join_paths_and_search.md) for
the full DP walkthrough and [15_geqo.md](./15_geqo.md) for GEQO
details.

---

## 4. Key trade-offs

### 4.1 Planning time vs plan quality

The defaults (`from_collapse_limit = 8`, `join_collapse_limit = 8`,
`geqo_threshold = 12`) bias toward fast planning. Raising these
allows the DP search to consider strictly more orderings but
quadruples-or-worse the planning time. For OLAP workloads with long
queries, raising both collapse limits to 20+ and `geqo_threshold` to
20+ can pay off; for OLTP it usually doesn't. See
[appendix_guc_parameters.md](./appendix_guc_parameters.md).

### 4.2 Exhaustive vs heuristic

DP enumerates *every* legal join ordering at *every* level; this is
exhaustive only with respect to the structure (DP, no cartesian
fallback). GEQO is heuristic by definition — it samples. The cost
model itself is *also* heuristic, so even DP's "optimal" plan is
only optimal under the cost model's view of reality.

### 4.3 Cost-model accuracy vs statistics overhead

The planner consults `pg_statistic` (refreshed by `ANALYZE`) for
selectivity estimates. More statistics targets (`default_statistics_target`,
per-column overrides) produce better estimates but bigger
`pg_statistic` rows and slower `ANALYZE`. Extended statistics
(`CREATE STATISTICS`) capture column correlations that single-column
stats miss, but cost extra. See
[09_cost_model_and_selectivity.md](./09_cost_model_and_selectivity.md).

### 4.4 Parallel vs serial

Parallel paths add an Amdahl-like overhead (`parallel_setup_cost +
parallel_tuple_cost * out_rows`). They win for big scans and
expensive joins, lose for small queries. The planner builds both a
serial pathlist and a `partial_pathlist`, and Gather-wraps the
latter only when cost-effective. See [14_parallel_planning.md](./14_parallel_planning.md).

### 4.5 Path memory vs path richness

Every kept Path costs memory. The Pareto fuzz factor
(`STD_FUZZ_FACTOR = 1.01`, `pathnode.c:47`) trims paths that differ
by < 1% on cost. Without this, near-tied paths would proliferate
through the join lattice; with it, plan choice becomes stable across
platforms (where float roundoff differs).

---

## 5. Where to read next

The next stop depends on intent:

- **Pipeline overview**: [02_architecture_overview.md](./02_architecture_overview.md) shows the full call graph as a Mermaid flowchart.
- **Entry-point details**: [03_lifecycle_and_entry_points.md](./03_lifecycle_and_entry_points.md) walks `planner` → `standard_planner` → `subquery_planner` → `query_planner` → `grouping_planner` line by line.
- **A specific area**: jump using the Reference route in [index.md](./index.md#1-reading-paths).

---

Next: [02_architecture_overview.md](./02_architecture_overview.md)
