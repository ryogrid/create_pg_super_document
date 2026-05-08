# 09. Cost Model and Selectivity

Prerequisites: [08_join_paths_and_search.md](./08_join_paths_and_search.md).

This module covers how every `Path`'s `startup_cost` and
`total_cost` are computed, and how row counts are derived from
`pg_statistic`. Source files:

- `src/backend/optimizer/path/costsize.c` (~6500 lines): all `cost_*`
  and `initial_cost_` / `final_cost_` pairs.
- `src/backend/optimizer/path/clausesel.c`:
  `clauselist_selectivity`.
- `src/backend/utils/adt/selfuncs.c`: per-operator selectivity
  functions and the variable-statistics helpers.
- `src/include/optimizer/cost.h`: GUC variable declarations and
  `DEFAULT_*` macros.

---

## 1. Why this exists

PostgreSQL's planner is **cost-based**: every `Path` carries a
`startup_cost` and `total_cost`, both in arbitrary "units" defined
by the GUCs in `costsize.c`. The cost model has to compare:

- Different access methods (seqscan, indexscan, bitmap, etc.).
- Different join orderings and join methods.
- Sort vs. hash for grouping/distinct.
- Partial-path + Gather vs. plain.

For comparisons to be meaningful, costs must be:

1. **Decomposable**: the cost of a join is built from costs of
   inputs.
2. **Stable**: small input changes shouldn't flip plan choice for
   equivalent expressions (hence `STD_FUZZ_FACTOR` in `add_path` —
   see [08_join_paths_and_search.md](./08_join_paths_and_search.md#13-add_path-pareto-dominance-test)).
3. **Tunable via GUCs**: users can correct for hardware mismatches.

The cost model's accuracy is dominated by **selectivity
estimation**: how many rows survive a given clause? Selectivity
feeds row counts, which feed every downstream cost calculation.

---

## 2. GUCs and default constants

Source: `src/backend/optimizer/path/costsize.c:119-154` and
`src/include/optimizer/cost.h`.

| GUC                          | Default | Macro                              | Meaning |
|------------------------------|---------|------------------------------------|---------|
| `seq_page_cost`              | 1.0     | `DEFAULT_SEQ_PAGE_COST`            | Sequential page fetch unit. |
| `random_page_cost`           | 4.0     | `DEFAULT_RANDOM_PAGE_COST`         | Random page fetch (used by index/bitmap). |
| `cpu_tuple_cost`             | 0.01    | `DEFAULT_CPU_TUPLE_COST`           | Per-tuple CPU overhead. |
| `cpu_index_tuple_cost`       | 0.005   | `DEFAULT_CPU_INDEX_TUPLE_COST`     | Per index-tuple CPU. |
| `cpu_operator_cost`          | 0.0025  | `DEFAULT_CPU_OPERATOR_COST`        | Per operator/function call. |
| `parallel_tuple_cost`        | 0.1     | `DEFAULT_PARALLEL_TUPLE_COST`      | Cost to pass one tuple from worker to leader. |
| `parallel_setup_cost`        | 1000.0  | `DEFAULT_PARALLEL_SETUP_COST`      | Fixed startup for launching workers. |
| `recursive_worktable_factor` | 10.0    | `DEFAULT_RECURSIVE_WORKTABLE_FACTOR` | Multiplier for recursive CTE work-table size estimate. |
| `effective_cache_size`       | 524288 pages = 4 GiB | `DEFAULT_EFFECTIVE_CACHE_SIZE` | Used in `cost_index` Mackert-Lohman correction. |

### 2.1 Disable knobs (`bool` flags)

| GUC | Default |
|-----|---------|
| `enable_seqscan` | true |
| `enable_indexscan` | true |
| `enable_indexonlyscan` | true |
| `enable_bitmapscan` | true |
| `enable_tidscan` | true |
| `enable_sort` | true |
| `enable_incremental_sort` | true |
| `enable_hashagg` | true |
| `enable_nestloop` | true |
| `enable_material` | true |
| `enable_memoize` | true |
| `enable_mergejoin` | true |
| `enable_hashjoin` | true |
| `enable_gathermerge` | true |
| `enable_partitionwise_join` | false |
| `enable_partitionwise_aggregate` | false |
| `enable_parallel_append` | true |
| `enable_parallel_hash` | true |
| `enable_partition_pruning` | true |
| `enable_presorted_aggregate` | true |
| `enable_async_append` | true |

When `enable_xxx = false`, the corresponding `cost_*` function adds
`disable_cost = 1.0e10` (`costsize.c:130`) to make those paths
overwhelmingly expensive — but they may still be selected if no
alternative exists (e.g. `enable_seqscan = false` and a table has
no suitable index).

### 2.2 Cost intuition

- Setting `random_page_cost = seq_page_cost` simulates "fully
  cached" behaviour and biases the planner toward index scans.
- Lowering `cpu_tuple_cost` makes seqscans relatively cheaper than
  index scans for large tables.
- Raising `effective_cache_size` reduces estimated random I/O for
  index scans (Mackert-Lohman page-fetch model in
  `index_pages_fetched`, costsize.c).
- Raising `parallel_setup_cost` discourages parallelism for cheap
  queries.

The full GUC reference is in [appendix_guc_parameters.md](./appendix_guc_parameters.md).

---

## 3. Cost-function inventory

### 3.1 Scan / unsorted paths

| Function | Path subtype | Notes |
|----------|--------------|-------|
| `cost_seqscan` (costsize.c:284) | `Path` (T_SeqScan) | Tier 1. Per-tuple + per-page. |
| `cost_samplescan` | `Path` (T_SampleScan) | Pro-rated by sample fraction. |
| `cost_index` (costsize.c:549) | `IndexPath` | Tier 1. Includes Mackert-Lohman. |
| `cost_bitmap_heap_scan` | `BitmapHeapPath` | Includes recheck cost. |
| `cost_bitmap_and_node` / `cost_bitmap_or_node` | `BitmapAndPath` / `BitmapOrPath` | Multiplies / unions selectivity. |
| `cost_tidscan` / `cost_tidrangescan` | `TidPath` / `TidRangePath` | random-page per TID. |
| `cost_subqueryscan` | `SubqueryScanPath` | Adds projection eval over subpath. |
| `cost_functionscan` / `cost_tablefuncscan` | `Path` | Per-row CPU cost. |
| `cost_valuesscan` | `Path` | Trivial. |
| `cost_ctescan` / `cost_namedtuplestorescan` / `cost_resultscan` | `Path` | Trivial. |
| `cost_recursive_union` | `RecursiveUnionPath` | Iterations × inner cost. |
| `cost_append` | `AppendPath` | Sum of children + per-tuple overhead. |
| `cost_merge_append` | `MergeAppendPath` | Sum + log2(N) heap maintenance. |

### 3.2 Sort / aggregation

| Function | Notes |
|----------|-------|
| `cost_sort` | Quicksort if fits work_mem else external merge. |
| `cost_incremental_sort` | Sorts only the unsorted tail per group. |
| `cost_agg` | Hash vs. group-agg paths. |
| `cost_group` | Pre-sorted grouping. |
| `cost_windowagg` | Per-frame eval cost. |
| `cost_material` | Spill to disk if > work_mem. |
| `cost_memoize_rescan` | Cache hit estimation. |

### 3.3 Join two-phase costing

For each join method, costsize.c provides an **`initial_cost_*`** /
**`final_cost_*`** pair:

| Pair | Path subtype |
|------|--------------|
| `initial_cost_nestloop` / `final_cost_nestloop` | `NestPath` |
| `initial_cost_mergejoin` / `final_cost_mergejoin` | `MergePath` |
| `initial_cost_hashjoin` / `final_cost_hashjoin` | `HashPath` |

**Why two phases?** `initial_cost_*` returns a cheap **lower
bound** (no per-clause selectivity, no bucket-distribution
analysis). It populates a `JoinCostWorkspace` that `try_*_path`
passes to `add_path_precheck`. If the precheck rejects the
candidate, the expensive `final_cost_*` is never run. For surviving
candidates, `create_*_path` calls `final_cost_*` to fill the actual
`Path` cost.

### 3.4 Parallel

| Function | Notes |
|----------|-------|
| `cost_gather` | Adds `parallel_setup_cost` + per-tuple `parallel_tuple_cost`. |
| `cost_gather_merge` | Same plus log2(workers) per-tuple heap-maintenance. |
| `compute_parallel_worker` (allpaths.c) | Picks worker count via log2(pages / min_parallel_table_scan_size). |

---

## 4. `cost_seqscan` deep dive

```c
void cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
                  ParamPathInfo *param_info);
```

Source: `src/backend/optimizer/path/costsize.c:284`.

Body (essentials):

```c
Cost     cpu_per_tuple;
Cost     run_cost = 0;
double   spc_seq_page_cost;

/* Per-tablespace seq_page_cost override (cf. ALTER TABLESPACE) */
get_tablespace_page_costs(baserel->reltablespace,
                          NULL, &spc_seq_page_cost);
run_cost += spc_seq_page_cost * baserel->pages;

/* Adjust rows / qpquals based on parameterization */
if (param_info) {
    path->rows = param_info->ppi_rows;
    qpquals = param_info->ppi_clauses;
} else {
    path->rows = baserel->rows;
    qpquals = baserel->baserestrictinfo;
}

cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
run_cost += cpu_per_tuple * baserel->tuples;

/* Parallel adjustments */
if (path->parallel_workers > 0) {
    double parallel_divisor = get_parallel_divisor(path);
    path->rows = clamp_row_est(path->rows / parallel_divisor);
    run_cost /= parallel_divisor;
}

path->startup_cost = startup_cost;
path->total_cost = startup_cost + run_cost;
```

Key points:

- I/O = `pages × seq_page_cost`. **Pages**, not tuples. Wider rows
  cost more per tuple via `pages` (the table is bigger), not via
  `cpu_tuple_cost`.
- CPU = `tuples × (cpu_tuple_cost + qpqual_cost.per_tuple)`.
  `qpqual_cost` is precomputed via `cost_qual_eval` and represents
  the per-tuple cost of evaluating the rel's restriction clauses
  (operator costs, function calls, etc.).
- `parallel_divisor` is < `parallel_workers` because the leader
  also does work. `get_parallel_divisor` returns
  `parallel_workers + leader_contribution(parallel_workers)`.

---

## 5. `cost_index` deep dive

```c
void cost_index(IndexPath *path, PlannerInfo *root, double loop_count,
                bool partial_path);
```

Source: `src/backend/optimizer/path/costsize.c:549`.

Outline:

1. Calls `amcostestimate` (per-AM hook, e.g. `btcostestimate`)
   which fills `indexStartupCost`, `indexTotalCost`,
   `indexSelectivity`, `indexCorrelation`.
2. `tuples_fetched = clamp(indexSelectivity × baserel->tuples, ...)`.
3. **Heap-page-fetch estimation** via `index_pages_fetched`
   (Mackert-Lohman model from "An Effective Buffer Pool Management
   Technique"):
   - Considers `effective_cache_size`, `baserel->pages`, and
     `tuples_fetched` to estimate how many distinct pages will be
     touched.
   - For correlated indexes (`indexCorrelation`), pages are partly
     sequential → blends `seq_page_cost` and `random_page_cost`.
4. **Repeated-fetch correction** via `loop_count`: if the index is
   used inside a nestloop with a known loop count, charge less per
   fetch (cache hits are likely).
5. CPU = `tuples_fetched × cpu_tuple_cost +` qpqual cost.
6. Index-only scan optimization: if all needed columns come from
   the index, skip the heap read for the visible-page fraction
   (`baserel->allvisfrac`).

---

## 6. Join cost: nestloop / mergejoin / hashjoin

The `12_join_cost_decomposition.mermaid` diagram in
[08_join_paths_and_search.md](./08_join_paths_and_search.md#32-per-method-cost-decomposition)
shows the full breakdown. Key formulas (paraphrased; check the
source for exact constants):

### 6.1 Nestloop

- **Startup**: `outer_path->startup + inner_path->startup`.
- **Run**: `outer_path->run + outer_rows × inner_rescan_cost +
  outer_rows × per-clause CPU`.
- For `inner_unique` SEMI/ANTI: short-circuit on first match
  (`semifactors.outer_match_frac` reduces `outer_rows`
  effectively).
- For `parallel_aware`: divisions by `parallel_divisor`.

### 6.2 Mergejoin

- **Startup**: outer/inner sorts (cost_sort if needed) + setup.
- **Run**: cost of streaming both sides + duplicate inner re-scan
  (potentially via Material).
- Uses **`MergeScanSelCache`** in each RestrictInfo to cache the
  fraction of each side that participates in the matching range
  (`leftstartsel`, `leftendsel`, ...).

### 6.3 Hashjoin

- **Startup**: build phase: `inner_path->total + cpu_operator_cost
  × inner_rows` (one hash compute per inner row).
- **Run**: probe phase: `outer_path->run + cpu_operator_cost ×
  outer_rows × num_hashclauses + cpu_tuple_cost × out_rows`.
- **Batch penalty**: when `nbatch > 1`, charge `2 × seq_page_cost ×
  (inner_pages + outer_pages) × (nbatch - 1)`. See
  `final_cost_hashjoin`.
- Bucket-distribution adjustment: `estimate_hash_bucket_stats`
  looks up `pg_statistic` MCV info on the inner-side hash key; if
  the most-common-value frequency is high, hash collisions inflate
  cost (worst case becomes nestloop-like). The MCV freq feeds
  `right_bucketsize` in the RestrictInfo; left_bucketsize for the
  outer.

---

## 7. Selectivity estimation

### 7.1 `clauselist_selectivity`

```c
Selectivity
clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
                       JoinType jointype, SpecialJoinInfo *sjinfo);
```

Source: `src/backend/optimizer/path/clausesel.c:100`.

- For each clause, calls `clause_selectivity_ext` which dispatches
  to the operator's cost function via `pg_operator.oprrest` /
  `oprjoin`.
- Multiplies independent-event selectivities. Calls into
  `dependencies_clauselist_selectivity`
  (`statext_clauselist_selectivity`) if the rel has extended
  statistics (`CREATE STATISTICS`) to handle correlated columns
  more accurately.
- Handles OR clauses via `clauselist_selectivity_or` (probability
  inclusion-exclusion) and individual `extract_or_clause` results.

### 7.2 `selfuncs.c` building blocks

- **`examine_variable`** — given an expression, find the
  underlying `pg_statistic` row (or MCV/histogram of a base-rel
  Var). Handles `VariableStatData.rel`,
  `VariableStatData.statsTuple`, `VariableStatData.atttype`, etc.
- **MCV (most-common values)** — `pg_statistic.stavalues1` /
  `stanumbers1`. Used for equality selectivity (look up exact freq)
  and for OR/IN selectivity.
- **Histograms** — `pg_statistic.stavalues2` / `stanumbers2`.
  Equal-population buckets used by `<`, `>`, `BETWEEN` for the
  non-MCV part of the distribution.
- **n_distinct** — `pg_statistic.stadistinct`. Negative values mean
  "fraction of total rows".
- **`get_variable_numdistinct`** — combines stadistinct, stadistinct
  override on the column, or fallback heuristics.
- **`get_attstatsslot`** — fetches a specific stakind slot's
  values.

### 7.3 Specific operator selectivity helpers

| Helper | Use |
|--------|-----|
| `eqsel`, `neqsel` | `=`, `<>` selectivity. MCV-driven. |
| `scalarltsel`, `scalarlesel`, `scalargtsel`, `scalargesel` | range comparisons. |
| `eqjoinsel` | join `=` selectivity = MCV cross-product + ndistinct fallback. |
| `prefixsel` | LIKE 'foo%' style. |
| `patternsel` | regex-style. |
| `boolvarsel` | bare boolean column. |
| `nulltestsel` | IS NULL / IS NOT NULL via stanullfrac. |
| `var_eq_const` | the workhorse for `var = const`. |

### 7.4 `pg_statistic` slot kinds

| stakind | Meaning |
|---------|---------|
| `STATISTIC_KIND_MCV` | Most common values. |
| `STATISTIC_KIND_HISTOGRAM` | Equi-population histogram. |
| `STATISTIC_KIND_CORRELATION` | Physical-vs-logical correlation. |
| `STATISTIC_KIND_MCELEM` | MCEs for arrays. |
| `STATISTIC_KIND_DECHIST` | Distinct-elements histogram for arrays. |
| `STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM` | Range-type lengths. |
| `STATISTIC_KIND_BOUNDS_HISTOGRAM` | Range-type bounds. |

Extended statistics (`pg_statistic_ext`) provide:

- `STATS_EXT_NDISTINCT` (multivariate ndistinct)
- `STATS_EXT_DEPENDENCIES` (functional dependencies)
- `STATS_EXT_MCV` (multivariate MCV)

---

## 8. Cost-flow walkthrough: EXPLAIN → cost numbers

For a query like:

```sql
SELECT * FROM orders o JOIN customers c ON o.cid = c.id
WHERE c.country = 'JP';
```

The cost flow at the join level is:

1. **Base rel sizes**:
   - `o`: `set_baserel_size_estimates` → `rows = c.tuples ×
     selectivity(restrictinfo)`. No restriction, so `o.rows =
     o.tuples`.
   - `c`: selectivity of `country = 'JP'` from MCV/histogram.
2. **Base rel paths**:
   - `o`: `cost_seqscan` (no useful index).
   - `c`: `cost_index` over an index on `country`, or
     `cost_bitmap_heap_scan` if the qual matches multiple bitmaps,
     or `cost_seqscan` if no index.
3. **Join level (level 2)**: `add_paths_to_joinrel(o, c,
   JOIN_INNER, ...)`.
   - Mergeclause selection: `select_mergejoin_clauses` finds `c.id
     = o.cid` (in an EC).
   - `sort_inner_and_outer`: builds MergePath with cost via
     `initial_cost_mergejoin` then `final_cost_mergejoin`.
   - `match_unsorted_outer`: tries nestloop pairs and merge with
     one-side sort.
   - `hash_inner_and_outer`: builds HashPath via
     `initial_cost_hashjoin` and (for survivors)
     `final_cost_hashjoin`. Considers `c` as inner (smaller after
     filter).
4. **`add_path` Pareto pruning**: only the dominating paths
   survive.
5. **`set_cheapest`**: selects the lowest `total_cost` over
   unparameterized paths.

`EXPLAIN ANALYZE` reports the same numbers (with actual runtime
counters too) by walking the chosen plan and reading
`Plan.startup_cost`/`total_cost`/`plan_rows`, which were copied
from the chosen `Path` during `create_plan`.

---

## 9. Parameterized rowcount and `ParamPathInfo`

For an inner path parameterized by an outer rel, the rowcount must
account for the *added* selectivity of the join clauses being
applied as scan-time conditions. `get_baserel_parampathinfo`
(relnode.c) caches a `ParamPathInfo` keyed by `(relid,
ppi_req_outer)`:

```c
typedef struct ParamPathInfo {
    Relids       ppi_req_outer;
    Cardinality  ppi_rows;
    List        *ppi_clauses;
    Bitmapset   *ppi_serials;
} ParamPathInfo;
```

`ppi_rows` is computed once per parameterization via
`get_parameterized_baserel_size` (selectivity of the per-paramset
clauses applied to `baserel->rows`). All paths sharing this
parameterization use the same `ppi_rows` so their rowcounts are
consistent — important for `add_path` Pareto comparisons.

---

## 10. `disable_cost` and forced-method exceptions

```c
Cost disable_cost = 1.0e10;
```

Set in `cost_*` when:

- `enable_seqscan = false` and we're costing a seqscan.
- `enable_indexscan = false` and we're costing an indexscan.
- `enable_nestloop = false` and we're costing a nestloop.
- … etc.

Two important counter-rules in `add_paths_to_joinrel`:

- `enable_mergejoin` is ignored when `jointype == JOIN_FULL` —
  full outer join must have at least merge or hash.
- `enable_hashjoin` similarly.
- The `disable_cost` add-on is a **per-path** penalty, not a hard
  rejection. If every path is disabled, the smallest-cost still
  wins.

---

## 11. Performance characteristics

- `cost_seqscan` / `cost_*scan`: O(1) per call.
- `cost_index`: dominated by `amcostestimate`. For btree, it's
  O(log(tuples)) effective.
- `clauselist_selectivity`: O(clauses) plus extended-stat lookups.
- `final_cost_hashjoin`: O(num_hashclauses) plus
  `estimate_hash_bucket_stats` MCV lookup.
- `final_cost_mergejoin`: O(num_mergeclauses) per RestrictInfo plus
  one `mergejoinscansel` call per uncached sort ordering.

---

## 12. Cross-references

- Path generation for indexes: [07_index_paths.md](./07_index_paths.md)
- Joins (where cost functions are invoked):
  [08_join_paths_and_search.md](./08_join_paths_and_search.md)
- Equivalence classes (used by mergeclause selection):
  [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md)
- Selectivity for OR clauses (`extract_or_clause`):
  [11_restrictinfo_and_clause_utils.md](./11_restrictinfo_and_clause_utils.md)
- Parallel cost details:
  [14_parallel_planning.md](./14_parallel_planning.md)
- GUC reference: [appendix_guc_parameters.md](./appendix_guc_parameters.md)

---

Next: [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md)
