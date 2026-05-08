# Appendix: Planner-Relevant GUC Parameters

This appendix lists every GUC (Grand Unified Configuration variable)
that the PostgreSQL planner consults when costing paths or selecting
algorithms. Defaults are taken from
`src/include/optimizer/cost.h`,
`src/backend/optimizer/path/costsize.c`, and
`src/backend/utils/misc/guc_tables.c` (line numbers are noted in each
section). Each entry follows the same layout: **name · default · units
· what it controls · when to tune**.

> Every cost figure produced by the planner is in **abstract units**:
> 1.0 = the cost of one sequential page read. The cost knobs below let
> you re-tune that scale relative to your hardware. They never directly
> change a plan; they only change which path wins `add_path` or
> `set_cheapest`. See [`./09_cost_model_and_selectivity.md`](./09_cost_model_and_selectivity.md)
> for the cost framework.

---

## Cost parameters

Source: `src/include/optimizer/cost.h` (defaults), `src/backend/optimizer/path/costsize.c` lines 119–132 (variable definitions), `src/backend/utils/misc/guc_tables.c` (catalog entries).

### `seq_page_cost`
- **Default**: `1.0` (`DEFAULT_SEQ_PAGE_COST`, `cost.h:24`)
- **Units**: cost units per page (1 page = `BLCKSZ` = 8 kB by default)
- **Controls**: cost of a sequentially fetched 8 kB block. This is the
  reference unit for *all* other cost parameters; **leave at 1.0** and
  scale the other knobs around it.
- **When to tune**: never directly. If your storage is faster or slower
  than HDD, adjust `random_page_cost` and `effective_cache_size` instead.

### `random_page_cost`
- **Default**: `4.0` (`DEFAULT_RANDOM_PAGE_COST`, `cost.h:25`)
- **Units**: cost units per page
- **Controls**: cost of a randomly fetched page; weighed by `cost_index`
  and `cost_bitmap_heap_scan` against `seq_page_cost` to decide whether
  to use an index. Lower values favor index scans.
- **When to tune**: set to `1.1`–`2.0` on SSD/NVMe; the 4.0 default is
  conservative for spinning rust. See [`./07_index_paths.md`](./07_index_paths.md).

### `cpu_tuple_cost`
- **Default**: `0.01` (`DEFAULT_CPU_TUPLE_COST`, `cost.h:26`)
- **Units**: cost units per tuple processed
- **Controls**: per-tuple CPU overhead in `cost_seqscan`,
  `cost_index`, joins, aggregation, etc. Multiplies by row count.
- **When to tune**: rarely; raise slightly to penalize wide-row scans.

### `cpu_index_tuple_cost`
- **Default**: `0.005` (`DEFAULT_CPU_INDEX_TUPLE_COST`, `cost.h:27`)
- **Units**: cost units per tuple
- **Controls**: per-tuple CPU charge specifically for index entries
  examined; consumed by `cost_index` and `cost_bitmap_heap_scan`.
- **When to tune**: raise if many partial-match index probes show up
  cheaper than the seqscans they replace at runtime.

### `cpu_operator_cost`
- **Default**: `0.0025` (`DEFAULT_CPU_OPERATOR_COST`, `cost.h:28`)
- **Units**: cost units per operator evaluation
- **Controls**: per-call cost of a function/operator in a qual or
  expression. Used heavily by `cost_qual_eval` (line 4643).
- **When to tune**: raise for query workloads dominated by expensive
  user-defined functions.

### `parallel_tuple_cost`
- **Default**: `0.1` (`DEFAULT_PARALLEL_TUPLE_COST`, `cost.h:29`)
- **Units**: cost units per tuple shipped from a worker to the leader
- **Controls**: penalty per tuple in `cost_gather`/`cost_gather_merge`,
  representing IPC overhead.
- **When to tune**: raise to discourage parallel plans that ship many
  rows back; lower for fast shared memory and small leader-side cost.

### `parallel_setup_cost`
- **Default**: `1000.0` (`DEFAULT_PARALLEL_SETUP_COST`, `cost.h:30`)
- **Units**: cost units (one-shot)
- **Controls**: fixed cost of starting parallel workers. Discourages
  parallelism for cheap queries.
- **When to tune**: lower to `100`–`500` if you have very fast worker
  start-up and want more aggressive parallelism on small relations.

### `effective_cache_size`
- **Default**: `524288` pages = 4 GB (`DEFAULT_EFFECTIVE_CACHE_SIZE`, `cost.h:34`)
- **Units**: 8 kB pages
- **Controls**: how much of the table the planner assumes to be cached
  by the OS / shared buffers. Used by `cost_index` to discount repeated
  probes (`index_pages_fetched` formula).
- **When to tune**: set to roughly **75 % of physical RAM**. This is
  one of the most impactful knobs for OLTP.

---

## Enable flags

Source: `src/backend/optimizer/path/costsize.c:134–154`. These booleans
do **not** literally disable a code path; instead, when `false`, the
relevant `cost_*` function adds `disable_cost = 1.0e10` to the path's
total cost (see `costsize.c:130`), making it effectively unselectable
unless no alternative exists.

| GUC | Default | What it disables |
|---|---|---|
| `enable_seqscan`        | `true` | adds disable_cost in `cost_seqscan` (forces index scans even for tiny tables) |
| `enable_indexscan`      | `true` | suppresses `IndexPath` consideration in `cost_index` |
| `enable_indexonlyscan`  | `true` | suppresses index-only scans (still allows ordinary IndexScan) |
| `enable_bitmapscan`     | `true` | suppresses `BitmapHeapPath` |
| `enable_tidscan`        | `true` | suppresses `TidPath` and `TidRangePath` |
| `enable_sort`           | `true` | suppresses explicit `SortPath` |
| `enable_incremental_sort` | `true` | suppresses `IncrementalSortPath` |
| `enable_material`       | `true` | suppresses `MaterialPath` insertion |
| `enable_nestloop`       | `true` | discourages `NestPath` |
| `enable_mergejoin`      | `true` | discourages `MergePath` |
| `enable_hashjoin`       | `true` | discourages `HashPath` |
| `enable_hashagg`        | `true` | discourages `Agg(HASHED)` strategy |
| `enable_partitionwise_join`      | **`false`** | enables partitionwise join paths in `try_partitionwise_join` (default-off because of memory cost) |
| `enable_partitionwise_aggregate` | **`false`** | enables partitionwise aggregation in `create_grouping_paths` |
| `enable_parallel_hash`  | `true` | enables shared-hashtable HashJoin (`HashPath.path.parallel_aware`) |
| `enable_parallel_append`| `true` | enables `Append.parallel_aware` (round-robin workers) |
| `enable_memoize`        | `true` | suppresses `MemoizePath` (used in nestloops with parameterized inner) |
| `enable_gathermerge`    | `true` | suppresses `GatherMergePath` |
| `enable_partition_pruning` | `true` | enables run-time partition pruning (`make_partition_pruneinfo`) |
| `enable_presorted_aggregate` | `true` | allows GROUP-BY column reordering to match input pathkeys |
| `enable_async_append`   | `true` | enables async-execution Append for ForeignScan (postgres_fdw) |

Use these flags to **diagnose** a plan choice (e.g. `SET
enable_hashjoin = off; EXPLAIN ...`); never leave them off in
production because they can force unboundedly expensive plans.

---

## GEQO (Genetic Query Optimization)

Source: `src/backend/utils/misc/guc_tables.c:1001–2135` and
`src/backend/optimizer/geqo/geqo_main.c`.

GEQO is a heuristic join-order search used when DP would be too
expensive. See [`./15_geqo.md`](./15_geqo.md).

### `geqo`
- **Default**: `true` (`guc_tables.c:1007`)
- **Units**: boolean
- **Controls**: master switch for GEQO. When `false`, the DP search
  (`standard_join_search`) is used for *any* join size.

### `geqo_threshold`
- **Default**: `12` (`guc_tables.c:2103`)
- **Units**: number of relations in the join list
- **Controls**: DP search is used when fewer than this many relations
  are in `initial_rels`; GEQO kicks in at or above. The `12` default
  keeps DP search cost reasonable: `2^12 = 4096` subsets.
- **When to tune**: raise to `15`–`20` if you can afford longer
  planning time and want the global optimum more often.

### `geqo_effort`
- **Default**: `5` (`guc_tables.c:2113`; `MIN_GEQO_EFFORT=1`,
  `MAX_GEQO_EFFORT=10`)
- **Units**: integer 1–10
- **Controls**: scales `geqo_pool_size` and `geqo_generations` when
  those are 0. Higher values trade CPU for plan quality.

### `geqo_pool_size`
- **Default**: `0` (use a value derived from `geqo_effort`)
- **Units**: number of chromosomes per generation
- **Controls**: GA population size. Larger pools explore more variants.

### `geqo_generations`
- **Default**: `0` (use a value derived from `geqo_effort`)
- **Units**: number of GA generations
- **Controls**: how long to evolve before reporting the best plan.

### `geqo_selection_bias`
- **Default**: `2.0` (`guc_tables.c:3807`,
  `DEFAULT_GEQO_SELECTION_BIAS`)
- **Range**: 1.5–2.0
- **Controls**: selective pressure within a population (Goldberg's
  `s` parameter).

### `geqo_seed`
- **Default**: `0.0` (`guc_tables.c:3818`)
- **Units**: float in [0,1)
- **Controls**: deterministic seed for the GEQO PRNG. Use a non-zero
  value to make GEQO output reproducible across runs.

---

## Collapse limits

Source: `src/backend/utils/misc/guc_tables.c:2071–2095`,
`src/backend/optimizer/prep/prepjointree.c`. See
[`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md).

### `from_collapse_limit`
- **Default**: `8` (`guc_tables.c:2080`)
- **Units**: number of items in resulting FROM list
- **Controls**: the planner pulls subqueries up into the parent FROM
  list as long as the resulting FROM list is no larger than this.
- **When to tune**: raise (e.g. to `20`) along with `join_collapse_limit`
  if the optimizer is missing join-order opportunities that span
  subqueries; lower (`1`) to preserve hand-written join orders.

### `join_collapse_limit`
- **Default**: `8` (`guc_tables.c:2093`)
- **Units**: number of items in resulting FROM list
- **Controls**: explicit `JOIN ... JOIN` constructs are flattened into
  a flat FROM list as long as the result has at most this many items.
- **When to tune**: set to `1` to **lock in** the SQL-textual join
  order (debug technique). Raise to expose more reordering freedom.

> If `join_collapse_limit < geqo_threshold`, GEQO is unreachable for
> queries that came in as `JOIN` syntax — the flattening stops before
> the planner sees the join list as one big problem.

---

## Memory

Source: `src/backend/utils/misc/guc_tables.c:2438–2447, 3823–3829`.

### `work_mem`
- **Default**: `4096` kB = 4 MB (`guc_tables.c:2446`)
- **Units**: kB
- **Controls**: per-operation memory budget for `Sort`, `HashJoin`,
  `HashAgg`, `Memoize`, etc. Hash operations may use up to
  `work_mem * hash_mem_multiplier`; sorts spill to disk above
  `work_mem`.
- **When to tune**: this is per-operation per-backend per-worker, so
  budget conservatively. `64MB`–`256MB` is typical for OLAP.

### `hash_mem_multiplier`
- **Default**: `2.0` (`guc_tables.c:3829`)
- **Units**: dimensionless multiplier
- **Controls**: hash-tabled operations may allocate up to
  `work_mem * hash_mem_multiplier` before spilling. Decouples sort
  budgets from hash budgets.
- **When to tune**: raise to `4.0`–`8.0` to keep large `HashJoin` and
  `HashAgg` in memory; lower (`1.0`) to make hash ops behave like
  sorts.

---

## JIT

Source: `src/backend/utils/misc/guc_tables.c:1915–3774`. JIT is
*planner-driven*: the costing thresholds below decide whether the
LLVM backend compiles tuple deformation and expressions for each
plan.

### `jit`
- **Default**: `true` (`guc_tables.c:1920`)
- **Units**: boolean
- **Controls**: master switch.

### `jit_above_cost`
- **Default**: `100000` (`guc_tables.c:3750`)
- **Units**: cost units; `-1` disables
- **Controls**: minimum total plan cost before JIT compilation runs.

### `jit_inline_above_cost`
- **Default**: `500000` (`guc_tables.c:3771`)
- **Units**: cost units; `-1` disables
- **Controls**: threshold beyond which inlining is performed.

### `jit_optimize_above_cost`
- **Default**: `500000` (`guc_tables.c:3760`)
- **Units**: cost units; `-1` disables
- **Controls**: threshold beyond which the LLVM optimizer runs at
  full strength.

---

## Misc query-planner

Source: `src/backend/utils/misc/guc_tables.c:2061-2068, 3777, 3410-3415`.

### `cursor_tuple_fraction`
- **Default**: `0.1` (`DEFAULT_CURSOR_TUPLE_FRACTION`,
  `guc_tables.c:3784`)
- **Units**: fraction in [0, 1]
- **Controls**: how much of the result the planner assumes a cursor
  client will fetch. Used by `grouping_planner` (line 1335) to bias
  toward cheap-startup paths.

### `default_statistics_target`
- **Default**: `100` (`guc_tables.c:2067`)
- **Units**: histogram buckets / MCV slots
- **Controls**: ANALYZE statistics resolution. Higher values produce
  better selectivity estimates at the cost of `ANALYZE` time and
  catalog size.
- **When to tune**: raise to `500`–`1000` for skewed columns; combine
  with column-level `ALTER TABLE ... ALTER COLUMN ... SET STATISTICS`.

### `max_parallel_workers_per_gather`
- **Default**: `2` (`costsize.c:132`, `guc_tables.c:3415`)
- **Units**: number of workers
- **Controls**: cap on workers requested per `Gather`. Combined with
  `compute_parallel_worker` (allpaths.c:4203), determines the
  `parallel_workers` field of partial paths.
- **When to tune**: raise to `4`–`8` on multi-core OLAP boxes; ensure
  `max_parallel_workers` and `max_worker_processes` are large enough.

### `recursive_worktable_factor`
- **Default**: `10.0` (`guc_tables.c:3796`,
  `DEFAULT_RECURSIVE_WORKTABLE_FACTOR`, `cost.h:33`)
- **Units**: ratio
- **Controls**: planner's estimate of recursive-CTE worktable size as a
  multiple of the non-recursive term. Used by
  `cost_recursive_union`.

---

## Cross-references

- The cost model that consumes these GUCs:
  [`./09_cost_model_and_selectivity.md`](./09_cost_model_and_selectivity.md)
- Parallel planning (uses `parallel_*_cost`, `enable_parallel_*`):
  [`./14_parallel_planning.md`](./14_parallel_planning.md)
- GEQO subsystem:
  [`./15_geqo.md`](./15_geqo.md)
- Subquery pull-up (uses `from_collapse_limit`/`join_collapse_limit`):
  [`./04_preprocessing.md`](./04_preprocessing.md),
  [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md)
