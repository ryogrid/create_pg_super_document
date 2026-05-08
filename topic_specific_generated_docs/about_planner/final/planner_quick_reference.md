# PostgreSQL Planner — Quick Reference (2-page)

A condensed, intermediate-reader summary of PostgreSQL's
cost-based, search-driven query planner. Use this page to orient
yourself before diving into the full documentation under
[`./index.md`](./index.md).

---

## The big picture

PostgreSQL plans a query in **three stages** that operate on two
distinct tree representations:

```
            ┌──────────────────────┐
   parse →  │   1. PREPROCESSING   │  prepjointree, prepqual, preptlist
            └──────────┬───────────┘
                       ↓
            ┌──────────────────────┐  query_planner / make_one_rel
   Path  →  │  2. PATH GENERATION  │  set_rel_pathlist (per baserel)
            │  (algebraic tree)    │  make_rel_from_joinlist (joins)
            └──────────┬───────────┘  create_grouping_paths (upper)
                       ↓ set_cheapest
            ┌──────────────────────┐
   Plan  →  │  3. PLAN CREATION    │  create_plan + setrefs.c
            │  (executable tree)   │
            └──────────────────────┘
```

The defining design choice is **Path/Plan duality**:

- A **`Path`** (`pathnodes.h:1621`) is an algebraic plan candidate. The
  planner builds many alternatives per relation and lets `add_path`
  retain only the Pareto-frontier ones (cost vs sort order vs
  parameterization vs parallel-safety). Paths are cheap to compare and
  alternative-rich.
- A **`Plan`** (`plannodes.h:119`) is the immutable executable form
  produced by `create_plan` (`createplan.c:338`). Exactly *one* Plan is
  produced per relation — the cheapest survivor.

Each Path subtype has a single Plan counterpart — see
[Appendix: Path Quick Reference](./appendix_path_quick_reference.md) for the
full table.

---

## Join-search algorithms

The set of base rels in the FROM clause is given as `initial_rels`. The
planner searches join orderings via **`make_rel_from_joinlist`**
(`allpaths.c:3306`), which dispatches based on size:

| Size of `initial_rels`      | Algorithm | Driver function |
|---|---|---|
| `< geqo_threshold` (default **12**)              | DP search       | `standard_join_search` (`allpaths.c:3411`) |
| `>= geqo_threshold` and `enable_geqo = on`       | Genetic algorithm | `geqo` (`geqo_main.c:72`) |
| Custom (extension)                               | replaces both    | `join_search_hook` (`paths.h:49`) |

**DP (dynamic programming) search** builds joinrels level by level:
`join_search_one_level` (`joinrels.c:73`) constructs every legal
joinrel of size `k` from joinrels of size `k-1`. This is exhaustive on
the legal-orders space — `SpecialJoinInfo.min_lefthand` /
`min_righthand` rule out illegal orderings.

**GEQO** treats join order as a permutation problem: a *chromosome* is
a permutation of `initial_rels`; `geqo_eval` (`geqo_eval.c:57`) builds
the corresponding joinrel via `gimme_tree`/`merge_clump` and reports
its cheapest cost as the chromosome's *fitness*. Mutation + crossover
evolve the population.

> Tuning hint: if a query mixes JOIN syntax and subqueries, also raise
> `from_collapse_limit` and `join_collapse_limit` (both default `8`)
> so the planner sees a flat join list large enough to use GEQO.

See [`./08_join_paths_and_search.md`](./08_join_paths_and_search.md)
and [`./15_geqo.md`](./15_geqo.md).

---

## Key GUCs at a glance

The 10 most-tuned planner parameters in production:

| GUC | Default | Quick advice |
|---|---|---|
| `random_page_cost`              | `4.0`        | Drop to `1.1`–`1.5` on SSD/NVMe |
| `effective_cache_size`          | `4 GB`       | Set to ~75 % of RAM |
| `work_mem`                      | `4 MB`       | Increase per-operation budget; watch concurrency |
| `hash_mem_multiplier`           | `2.0`        | Raise to keep big hashes in memory |
| `from_collapse_limit`           | `8`          | Raise for many-table queries with subqueries |
| `join_collapse_limit`           | `8`          | Raise to give the search more reorder freedom; set to `1` to lock SQL order |
| `geqo_threshold`                | `12`         | Raise to delay GEQO's switch-over |
| `max_parallel_workers_per_gather` | `2`        | Raise on multi-core OLAP boxes |
| `default_statistics_target`     | `100`        | Raise to `500`–`1000` for skewed columns |
| `enable_partitionwise_join`     | `false`      | Turn on for partition-aligned joins |

Full details: [Appendix: GUC Parameters](./appendix_guc_parameters.md).

---

## EXPLAIN-reading tips

EXPLAIN's textual output maps directly to the Path → Plan layers:

1. The **executor node name** (e.g., `Hash Join`, `Index Scan`,
   `Sort`) is the `Plan.type` tag, identical to the path's
   `path->pathtype`. Look up the row in
   [Appendix: Path Quick Reference](./appendix_path_quick_reference.md)
   to find which Path produced it and which `cost_*` function set its
   numbers.
2. **`cost=startup..total`**: these numbers come straight from
   `Path.startup_cost` and `Path.total_cost`. Units are seq-page reads
   (1.0 = `seq_page_cost`).
3. **`rows=N`**: this is `Path.rows`, computed by the cost function
   from `clauselist_selectivity` (`clausesel.c:100`) over the
   relevant quals.
4. **`width=W`**: from `PathTarget.width` — sum of
   `attr_widths[]` over the columns in `pathtarget->exprs`.
5. With `EXPLAIN ANALYZE`, the post-execution **`(actual rows=...)`**
   is the runtime row count. Discrepancies signal:
   - **bad selectivity estimates** → raise `default_statistics_target`
     for the troublesome columns;
   - **uncorrelated multi-column estimates** → consider
     `CREATE STATISTICS` (`pg_statistic_ext`);
   - **stale stats** → run `ANALYZE`;
   - **MCV under-coverage** → column-level
     `ALTER TABLE ... SET STATISTICS`.
6. `Sort Method: external merge` or `Disk: NN MB` indicates a sort
   that spilled past `work_mem`.
7. `Memory Usage: NN kB` or `Batches: NN` on a `Hash Join` indicates
   how close the build side is to spilling.
8. `Workers Planned`/`Workers Launched` come from
   `compute_parallel_worker` (`allpaths.c:4203`); a divergence is
   typically `max_parallel_workers` cap pressure.

---

## Hook entry points

| Hook | Signature | When called | Use case |
|---|---|---|---|
| `planner_hook` (`planner.h:30`) | `Plan *(*)(Query *, ...)` | Top-level planner entry. | Wholesale replacement (e.g., `pg_hint_plan` could intercept here). |
| `join_search_hook` (`paths.h:49`) | `RelOptInfo *(*)(PlannerInfo *, int, List *)` | Replaces `standard_join_search`. | GEQO is itself implemented as a join_search_hook. |
| `set_rel_pathlist_hook` (`paths.h:34`) | `void(*)(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *)` | After per-baserel paths are generated. | Inject custom scan paths (CustomPath). |
| `set_join_pathlist_hook` (`paths.h:43`) | `void(*)(PlannerInfo *, RelOptInfo *, ...)` | Inside `add_paths_to_joinrel`. | Inject custom join paths. |
| `create_upper_paths_hook` (`planner.h:38`) | `void(*)(PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *, void *)` | After each upper-pipeline stage (group/window/distinct/order/final). | Inject upper-rel paths. |
| `get_relation_info_hook` (`plancat.h`) | `void(*)(PlannerInfo *, Oid, bool, RelOptInfo *)` | After the catalog populates a baserel. | Add synthetic indexes / statistics. |

See [`./17_hooks_and_extensibility.md`](./17_hooks_and_extensibility.md).

---

## Where to look for what

Common debugging intents and the source files to reach for:

| I want to… | Look at |
|---|---|
| Understand why an index isn't picked          | `src/backend/optimizer/path/indxpath.c`, `src/backend/optimizer/path/costsize.c` (`cost_index`, line 549) |
| Tweak join-order shape                        | `src/backend/optimizer/path/joinrels.c` (`join_search_one_level`, `join_is_legal`) |
| Inspect EquivalenceClass derivations          | `src/backend/optimizer/path/equivclass.c` (`process_equivalence`, `generate_*_implied_equalities`) |
| Trace a quals' selectivity                    | `src/backend/optimizer/path/clausesel.c` (`clauselist_selectivity`), `src/backend/utils/adt/selfuncs.c` |
| Debug a parameterized inner-side path         | `src/backend/optimizer/util/relnode.c` (`get_*_parampathinfo`) |
| Trace SubLink → SubPlan transformation        | `src/backend/optimizer/plan/subselect.c` (`make_subplan`, `convert_*_sublink_to_join`) |
| See partition pruning happen                  | `src/backend/partitioning/partprune.c` (`make_partition_pruneinfo`, `prune_append_rel_partitions`) |
| Add a Gather                                  | `src/backend/optimizer/path/allpaths.c` (`generate_gather_paths`, `generate_useful_gather_paths`) |
| Track Path → Plan translation                 | `src/backend/optimizer/plan/createplan.c` (start at `create_plan_recurse`, line 389) |
| Fix Var references in the final tree          | `src/backend/optimizer/plan/setrefs.c` (`set_plan_references`) |

Cross-references back to the main book:

- Big picture and lifecycle: [`./03_lifecycle_and_entry_points.md`](./03_lifecycle_and_entry_points.md)
- Cost model: [`./09_cost_model_and_selectivity.md`](./09_cost_model_and_selectivity.md)
- Join search: [`./08_join_paths_and_search.md`](./08_join_paths_and_search.md)
- Plan creation: [`./16_plan_creation_and_setrefs.md`](./16_plan_creation_and_setrefs.md)
- API surface: [`./planner_api_reference.md`](./planner_api_reference.md)
