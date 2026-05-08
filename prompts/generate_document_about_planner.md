# PostgreSQL Planner (Optimizer) Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's **Planner (Optimizer)** subsystem, covering the complete query optimization pipeline from the parser/rewriter handoff at `planner()` through preprocessing (sublink/subquery pull-up, qual canonicalization, target list preparation), initial join-tree deconstruction, base-relation path generation, index path enumeration, join-path generation across all three join methods (NestLoop, MergeJoin, HashJoin), the **two competing join-search strategies** (dynamic-programming `standard_join_search` and the genetic `geqo` for many-table queries), the cost model and selectivity machinery, equivalence classes and pathkeys, RestrictInfo and SpecialJoinInfo bookkeeping, subquery/SubLink handling, inheritance and partition-wise operations, parallel-query path generation, upper-level planning of GROUP BY / window / DISTINCT / ORDER BY, and the final Path → Plan conversion in `createplan.c` followed by `set_plan_references()`. The documentation must include a **systematic catalog of all Path subtypes** (with their constructors, cost functions, and the Plan node they materialize into) and a **catalog of every `create_*_plan()` creator function** in `createplan.c`.

## Output Directory
All generated artifacts — intermediate files (architecture_map.json, key_symbols.txt, etc.), component files, diagrams, and final documentation modules — **must** be written under the following directory:

```
topic_specific_generated_docs/about_planner/
```

Create this directory at the start of Stage 1 if it does not already exist. Use subdirectories as needed to organize outputs by stage:

```
topic_specific_generated_docs/about_planner/
├── stage1/                              # Architecture analysis outputs
│   ├── architecture_map.json
│   ├── key_symbols.txt
│   ├── initial_outline.md
│   └── path_type_inventory.txt          # Every Path subtype with constructor + cost fn + plan creator
├── stage2/                              # Detailed documentation components
│   ├── component_*.md
│   ├── path_catalog/                    # Per-Path-subtype documentation
│   │   ├── scan_paths.md
│   │   ├── join_paths.md
│   │   ├── upper_paths.md
│   │   ├── append_and_partition_paths.md
│   │   ├── parallel_paths.md
│   │   └── modify_paths.md
│   ├── plan_creator_catalog/            # Per-create_*_plan() documentation
│   │   ├── scan_creators.md
│   │   ├── join_creators.md
│   │   ├── upper_creators.md
│   │   └── modify_creators.md
│   └── diagrams/
│       └── *.mermaid
├── final/                               # Integrated final documentation
│   ├── index.md
│   ├── 01_executive_summary.md
│   ├── ...
│   ├── 19_deep_dives.md
│   ├── appendix_*.md
│   ├── planner_quick_reference.md
│   ├── planner_api_reference.md
│   └── quality_report.md
└── diagrams/                            # Final consolidated diagrams
    └── *.mermaid
```

**All file paths referenced between stages (e.g., Stage 2 reading Stage 1 outputs) must use paths relative to `topic_specific_generated_docs/about_planner/`.**

## Available Resources

### Local Source Code (PostgreSQL `src/` directory)
The PostgreSQL source tree is available locally at `./src/`. This is a direct copy of the upstream `src/` directory and should be actively referenced throughout all stages. Key directories for Planner documentation:

| Directory | Contents |
|---|---|
| `src/backend/optimizer/plan/` | **Planner top-level and plan creation** — `planner.c` (planner, standard_planner, subquery_planner, grouping_planner, create_grouping_paths, create_window_paths, create_distinct_paths, create_ordered_paths), `planmain.c` (query_planner), `createplan.c` (Path → Plan conversion: create_plan, create_plan_recurse, create_seqscan_plan, create_indexscan_plan, create_nestloop_plan, create_mergejoin_plan, create_hashjoin_plan, create_sort_plan, create_agg_plan, create_append_plan, create_gather_plan, create_modifytable_plan, …), `initsplan.c` (deconstruct_jointree, distribute_qual_to_rels, build_base_rel_tlists, find_lateral_references, process_implied_equality), `setrefs.c` (set_plan_references, fix_scan_expr, fix_upper_expr — final Var-reference fixup), `subselect.c` (convert_ANY_sublink_to_join, convert_EXISTS_sublink_to_join, SS_process_sublinks, SS_finalize_plan), `planagg.c` (MIN/MAX aggregate optimization), `analyzejoins.c` (remove_useless_joins, reduce_unique_semijoins) |
| `src/backend/optimizer/path/` | **Path generation and search** — `allpaths.c` (set_base_rel_pathlists, set_rel_pathlist dispatch, set_plain_rel_pathlist, set_subquery_pathlist, set_append_rel_pathlist, set_function_pathlist, set_values_pathlist, set_cte_pathlist, set_worktable_pathlist, set_foreign_pathlist, make_one_rel, make_rel_from_joinlist, standard_join_search, generate_gather_paths, compute_parallel_worker), `costsize.c` (cost_seqscan, cost_index, cost_bitmap_heap_scan, cost_subqueryscan, cost_functionscan, cost_valuesscan, cost_ctescan, cost_sort, cost_append, cost_material, cost_agg, cost_windowagg, cost_group, cost_nestloop, cost_mergejoin, cost_hashjoin, get_parallel_divisor, clamp_row_est), `clausesel.c` (clauselist_selectivity, clause_selectivity), `equivclass.c` (process_equivalence, generate_join_implied_equalities, EquivalenceClass machinery), `indxpath.c` (create_index_paths, build_index_paths, match_clause_to_indexcol, build_bitmap_paths), `joinpath.c` (add_paths_to_joinrel, try_nestloop_path, try_mergejoin_path, try_hashjoin_path, sort_inner_and_outer, match_unsorted_outer, hash_inner_and_outer), `joinrels.c` (join_search_one_level, make_join_rel, populate_joinrel_with_paths, join_is_legal, is_dummy_rel), `pathkeys.c` (build_index_pathkeys, build_join_pathkeys, pathkeys_contained_in, get_cheapest_path_for_pathkeys, pathkeys_useful_for_merging, pathkeys_useful_for_ordering), `tidpath.c` |
| `src/backend/optimizer/prep/` | **Preprocessing** — `prepjointree.c` (pull_up_sublinks, pull_up_subqueries, flatten_join_alias_vars, reduce_outer_joins), `prepqual.c` (canonicalize_qual), `prepunion.c` (UNION/INTERSECT/EXCEPT planning), `prepagg.c` (preprocess_aggrefs), `preptlist.c` (preprocess_targetlist) |
| `src/backend/optimizer/util/` | **Planner utilities** — `pathnode.c` (Path constructors: create_seqscan_path, create_index_path, create_nestloop_path, create_mergejoin_path, create_hashjoin_path, …; add_path machinery; set_cheapest), `relnode.c` (build_simple_rel, build_join_rel, find_base_rel, find_join_rel), `restrictinfo.c` (make_restrictinfo, extract_actual_clauses), `clauses.c` (eval_const_expressions, contain_volatile_functions, find_nonnullable_rels, expression_returns_set_rows), `plancat.c` (get_relation_info — catalog/index lookups, estimate_rel_size), `placeholder.c` (PlaceHolderVar machinery), `paramassign.c` (Param node assignment), `inherit.c` (expand_inherited_rtentry — partition expansion), `tlist.c`, `var.c`, `joininfo.c`, `appendinfo.c` (make_append_rel_info), `predtest.c` (predicate_implied_by, predicate_refuted_by), `orclauses.c` |
| `src/backend/optimizer/geqo/` | **Genetic Query Optimizer** — `geqo_main.c` (geqo entry point), `geqo_eval.c` (gimme_tree, geqo_eval candidate fitness), `geqo_pool.c` (alloc_pool, sort_pool — population management), `geqo_selection.c` (geqo_selection — fitness-based parent selection), `geqo_recombination.c` (recombination dispatcher), `geqo_cx.c`/`geqo_erx.c`/`geqo_ox1.c`/`geqo_ox2.c`/`geqo_pmx.c`/`geqo_px.c` (crossover operators), `geqo_mutation.c`, `geqo_copy.c`, `geqo_misc.c`, `geqo_random.c` |
| `src/backend/partitioning/` | **Partition pruning at plan time** — `partprune.c` (make_partition_pruneinfo, gen_partprune_steps, perform_pruning_base_step), `partbounds.c`, `partdesc.c` |
| `src/backend/utils/adt/` | **Selectivity functions** — `selfuncs.c` (eqsel, scalarltsel, mergejoinscansel, estimate_hash_bucket_stats, examine_variable, get_variable_numdistinct — selectivity functions registered via `pg_operator.oprrest`/`oprjoin`) |
| `src/include/nodes/` | **Key data structure headers** — `pathnodes.h` (PlannerGlobal, **PlannerInfo**, **RelOptInfo**, **Path** and all its subtypes — IndexPath, BitmapHeapPath, NestPath, MergePath, HashPath, AppendPath, MergeAppendPath, MaterialPath, MemoizePath, UniquePath, GatherPath, GatherMergePath, SortPath, IncrementalSortPath, AggPath, GroupingSetsPath, MinMaxAggPath, WindowAggPath, SetOpPath, RecursiveUnionPath, LockRowsPath, ModifyTablePath, LimitPath, ProjectionPath, ProjectSetPath, **EquivalenceClass**, **PathKey**, **PathTarget**, **RestrictInfo**, **SpecialJoinInfo**, **PlaceHolderVar**, **PlaceHolderInfo**, **ParamPathInfo**, **AppendRelInfo**, **JoinDomain**), `plannodes.h` (PlannedStmt, Plan and subtypes — SeqScan, IndexScan, BitmapIndexScan, BitmapHeapScan, TidScan, SubqueryScan, FunctionScan, ValuesScan, CteScan, WorkTableScan, ForeignScan, CustomScan, NestLoop, MergeJoin, HashJoin, Hash, Sort, IncrementalSort, Agg, WindowAgg, Group, Unique, SetOp, LockRows, Limit, Append, MergeAppend, Result, ProjectSet, Gather, GatherMerge, RecursiveUnion, Material, Memoize, ModifyTable), `parsenodes.h` (Query — input to the planner), `primnodes.h` (Var, Const, OpExpr, FuncExpr, Aggref, …) |
| `src/include/optimizer/` | **Planner public headers** — `optimizer.h` (public API), `planner.h` (planner_hook_type, create_upper_paths_hook_type), `planmain.h`, `paths.h`, `pathnode.h`, `cost.h`, `clauses.h`, `restrictinfo.h`, `relnode.h`, `joininfo.h`, `equivclass.h`, `pathkeys.h`, `plancat.h`, `placeholder.h`, `paramassign.h`, `inherit.h`, `prep.h`, `subselect.h`, `appendinfo.h`, `tlist.h`, `geqo.h` |
| `src/backend/tcop/` | **Caller integration** — `postgres.c` (exec_simple_query → pg_plan_query → planner), `pquery.c` |
| `src/backend/optimizer/README` | **The single most important reading material — 1499 lines of in-tree planner documentation. Read this end-to-end before doing anything else.** |
| `src/backend/optimizer/plan/README` | 158-line README on the plan-generation subdirectory |

**Usage guidelines for source code**:
- When documenting a function, always verify its actual signature and logic against the local source (`./src/...`) as the ground truth.
- Use `grep -rn` to discover call sites, `#define` constants, and struct definitions.
- When quoting source code in documentation, include the relative file path (e.g., `src/backend/optimizer/plan/planner.c:288`) for traceability.
- **For the Path-type catalog**: enumerate subtypes by grepping `grep -nE 'typedef struct .*Path' src/include/nodes/pathnodes.h` and the `T_*Path` entries in `src/include/nodes/nodes.h`. Cross-check against `pathnode.c` for the constructor function for each type.
- **For the Plan-creator catalog**: enumerate by grepping `grep -nE '^create_[a-z_]+_plan' src/backend/optimizer/plan/createplan.c` (or `grep -nE '^static Plan \*' src/backend/optimizer/plan/createplan.c`).

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Planner (Optimizer) subsystem architecture.

Use local source tree (`./src/`) for analysis.

**Source exploration strategy for this stage**:
- Begin by reading `src/backend/optimizer/README` end-to-end. This document
  authoritatively describes Path/Plan duality, join-order legality, EquivalenceClass
  reasoning, varnullingrels, PlaceHolderVar semantics, and the optimizer call graph.
- Scan key directories to identify relevant files:
  - `find ./src/backend/optimizer/plan/ -name '*.c'`
  - `find ./src/backend/optimizer/path/ -name '*.c'`
  - `find ./src/backend/optimizer/prep/ -name '*.c'`
  - `find ./src/backend/optimizer/util/ -name '*.c'`
  - `find ./src/backend/optimizer/geqo/ -name '*.c'`
  - `find ./src/include/optimizer/ -name '*.h'`
- Read key header files end-to-end:
  - `src/include/nodes/pathnodes.h` — PlannerInfo, RelOptInfo, Path subtypes,
    EquivalenceClass, PathKey, RestrictInfo, SpecialJoinInfo, PlaceHolderVar
  - `src/include/nodes/plannodes.h` — Plan and all its subtypes (executor input)
  - `src/include/optimizer/paths.h`, `src/include/optimizer/pathnode.h`,
    `src/include/optimizer/cost.h`, `src/include/optimizer/planner.h`
- Use `grep -rn 'FunctionName' ./src/` to trace call chains and discover symbols
- Enumerate every Path subtype:
  `grep -nE 'typedef struct .*Path' src/include/nodes/pathnodes.h`
  Cross-reference with `T_*Path` entries in `src/include/nodes/nodes.h` and
  with constructor functions in `src/backend/optimizer/util/pathnode.c`
  (`grep -nE '^create_[a-z_]+_path' src/backend/optimizer/util/pathnode.c`).
- Enumerate every plan creator function:
  `grep -nE '^create_[a-z_]+_plan' src/backend/optimizer/plan/createplan.c`

Build a comprehensive dependency map with depth 5 traversal. Focus on:

1. Top-level lifecycle and entry points
   - planner() → standard_planner() (with planner_hook detour)
   - subquery_planner() recursion for sublinks/subqueries
   - query_planner() — bridge to base/join path generation
   - grouping_planner() — upper-level (GROUP BY, aggregate, window, DISTINCT, ORDER BY, LIMIT)
   - make_one_rel() → make_rel_from_joinlist() → standard_join_search() (or geqo)
   - join_search_one_level() — DP per-level search
   - set_cheapest() — caching cheapest path per RelOptInfo
   - create_plan() — Path → Plan conversion entry
   - set_plan_references() — final Var-reference fixup
   - End-to-end call graph as documented in `src/backend/optimizer/README`

2. Preprocessing pipeline
   - subquery_planner() preprocessing order: SS_process_ctes → pull_up_sublinks
     → reduce_outer_joins → pull_up_subqueries → flatten_simple_union_all
     → preprocess_expression → canonicalize_qual → preprocess_targetlist
   - prepjointree.c: pull_up_sublinks, pull_up_subqueries (when a subquery is
     "simple" — no aggregates, no DISTINCT, no LIMIT, etc.),
     flatten_join_alias_vars, reduce_outer_joins
   - prepqual.c: canonicalize_qual (flatten ANDs/ORs, remove duplicates,
     simplify constants)
   - preptlist.c: preprocess_targetlist (junk columns for FOR UPDATE, RETURNING, etc.)
   - prepagg.c: preprocess_aggrefs (aggregate validation, transition functions)
   - prepunion.c: set-operation tree planning (UNION/INTERSECT/EXCEPT, including
     UNION ALL via Append vs UNION via SetOp)

3. Initial query setup (initsplan.c)
   - build_base_rel_tlists() — extract Vars used in the query, install into base rels
   - find_lateral_references() / extract_lateral_references() — LATERAL handling
   - deconstruct_jointree() — recursive walk to:
       a. enumerate base relations into root->parse->rtable
       b. construct SpecialJoinInfo for outer joins (compute min_lefthand,
          min_righthand) and semi/anti joins
       c. distribute join-clause and WHERE quals to base/join rels via
          distribute_qual_to_rels()
   - distribute_qual_to_rels() — places each qual at the lowest legal join level
     (using varnullingrels, varlevelsup, and SpecialJoinInfo)
   - process_implied_equality() — feeds equality clauses into EquivalenceClass machinery

4. Base-relation path generation (allpaths.c)
   - set_base_rel_pathlists() iterates root->simple_rel_array
   - set_rel_pathlist() dispatch by rtekind / RELOPT_BASEREL kind:
       a. set_plain_rel_pathlist (RELKIND_RELATION/RELKIND_MATVIEW)
       b. set_append_rel_pathlist (inheritance / partitioned tables)
       c. set_subquery_pathlist (RTE_SUBQUERY)
       d. set_function_pathlist (RTE_FUNCTION)
       e. set_values_pathlist (RTE_VALUES)
       f. set_cte_pathlist / set_namedtuplestore_pathlist
       g. set_worktable_pathlist (RTE_CTE recursive worktable)
       h. set_result_pathlist (FROM-less SELECT)
       i. set_foreign_pathlist (RTE_RELATION + foreign table → FDW callback)
   - set_plain_rel_pathlist generates: SeqScan via create_seqscan_path, then
     create_index_paths (indxpath.c), then create_tidscan_paths (tidpath.c),
     then partial paths for parallel + generate_gather_paths
   - set_rel_pathlist_hook called at the end (extension point)

5. Index path generation (indxpath.c, tidpath.c)
   - create_index_paths() — iterates rel->indexlist, builds IndexPath,
     IndexOnlyScan paths, BitmapHeapPath (with BitmapAnd/BitmapOr children)
   - build_index_paths() — for one IndexOptInfo: enumerate sort orderings,
     parameterized scans, ScalarArrayOpExpr usage, partial-index applicability
   - match_clause_to_indexcol() — operator/opclass matching (handles commutativity,
     RowCompareExpr, ScalarArrayOpExpr, IS NULL, etc.)
   - Bitmap path construction: choose_bitmap_and combines candidate index scans
     with bitmap-AND; build_paths_for_OR builds bitmap-OR paths
   - Parameterized index scans (via lateral references or join clauses): the
     ParamPathInfo machinery (paramassign.c) tracks parameterizations
   - tidpath.c: create_tidscan_paths for ctid = '...' style WHERE clauses

6. Join path generation and DP search (joinpath.c, joinrels.c)
   - join_search_one_level(level) iterates all combinations producing relations
     of the desired size:
       a. make_rels_by_clause_joins (joinrel from prior level + a baserel via
          a usable join clause)
       b. make_rels_by_clauseless_joins (cartesian product if forced)
       c. bushy plan generation (joinrel × joinrel)
   - make_join_rel() — gate via join_is_legal (consults SpecialJoinInfo),
     build_join_rel for the new RelOptInfo, populate_joinrel_with_paths
   - populate_joinrel_with_paths() dispatches by join type (inner / left /
     right / full / semi / anti / unique semijoin) into add_paths_to_joinrel
   - add_paths_to_joinrel(): for each (outer, inner) pair tries
       a. try_nestloop_path (incl. parameterized inner)
       b. try_mergejoin_path (with sort_inner_and_outer + match_unsorted_outer)
       c. try_hashjoin_path (incl. hash_inner_and_outer)
       d. try_partial_*_path variants for parallel join
   - join_is_legal() consults SpecialJoinInfo list (min_lefthand, min_righthand,
     commute_above_l, commute_above_r) and lateral_relids
   - is_dummy_rel() — short-circuits joins involving provably-empty relations
   - set_cheapest() picks cheapest_total_path, cheapest_startup_path,
     cheapest_parameterized_paths

7. Cost model and selectivity (costsize.c, clausesel.c, selfuncs.c)
   - cost_seqscan, cost_index, cost_bitmap_heap_scan, cost_tidscan, cost_subqueryscan,
     cost_functionscan, cost_valuesscan, cost_ctescan, cost_namedtuplestorescan,
     cost_resultscan, cost_recursive_union, cost_sort, cost_incremental_sort,
     cost_append, cost_merge_append, cost_material, cost_memoize_rescan,
     cost_agg, cost_windowagg, cost_group, cost_gather, cost_gather_merge,
     cost_subplan, cost_qual_eval, cost_qual_eval_node
   - Join costs: initial_cost_nestloop / final_cost_nestloop,
     initial_cost_mergejoin / final_cost_mergejoin,
     initial_cost_hashjoin / final_cost_hashjoin
   - Selectivity: clauselist_selectivity (AND-list selectivities), clauselist_selectivity_or,
     clause_selectivity, dispatch into operator-specific functions
     (eqsel, scalarltsel, scalargtsel, neqsel, eqjoinsel, scalarltjoinsel, etc.)
     in `src/backend/utils/adt/selfuncs.c`
   - Statistics access: examine_variable, get_variable_numdistinct,
     mcv-list usage, histogram bucket interpolation (statistic_proc_security_check)
   - GUC parameters: seq_page_cost, random_page_cost, cpu_tuple_cost,
     cpu_index_tuple_cost, cpu_operator_cost, parallel_tuple_cost,
     parallel_setup_cost, effective_cache_size, work_mem,
     enable_seqscan, enable_indexscan, enable_indexonlyscan,
     enable_bitmapscan, enable_tidscan, enable_sort, enable_material,
     enable_nestloop, enable_mergejoin, enable_hashjoin, enable_hashagg,
     enable_partitionwise_join, enable_partitionwise_aggregate,
     enable_parallel_hash, enable_parallel_append, enable_memoize,
     jit, jit_above_cost, jit_inline_above_cost, jit_optimize_above_cost,
     geqo, geqo_threshold, from_collapse_limit, join_collapse_limit

8. Equivalence classes and pathkeys (equivclass.c, pathkeys.c)
   - EquivalenceClass / EquivalenceMember structures (pathnodes.h)
   - process_equivalence() — derive EC from a single equality
   - get_eclass_for_sort_expr — find/create EC for an ORDER BY expression
   - generate_join_implied_equalities (and _for_ecs / _normal / _broken variants)
     — produce derived join clauses from ECs (transitivity)
   - PathKey: (pk_eclass, pk_strategy, pk_nulls_first); represents one sort-order dimension
   - build_index_pathkeys, build_join_pathkeys, build_expression_pathkey,
     build_partition_pathkeys
   - pathkeys_contained_in (prefix containment), get_cheapest_path_for_pathkeys,
     get_cheapest_fractional_path_for_pathkeys
   - pathkeys_useful_for_merging (mergejoin compatibility),
     pathkeys_useful_for_ordering (ORDER BY), pathkeys_count_contained_in
     (incremental-sort prefix)

9. Restriction / qual classification and clause utilities
   (restrictinfo.c, clauses.c, predtest.c, orclauses.c)
   - RestrictInfo: wraps each qual with metadata (clause, is_pushed_down,
     can_join, pseudoconstant, required_relids, outer_relids, nullable_relids,
     left_relids, right_relids, mergeopfamilies, hashjoinoperator, …)
   - make_restrictinfo, restriction_is_or_clause, restriction_is_securely_promotable
   - eval_const_expressions — fold constant expressions, simplify function calls,
     short-circuit boolean expressions
   - contain_volatile_functions, contain_mutable_functions, contain_subplans
   - find_nonnullable_rels, find_nonnullable_vars, find_forced_null_vars
   - expression_returns_set, expression_returns_set_rows
   - predicate_implied_by, predicate_refuted_by — used for partial index matching
     and CHECK constraint exclusion
   - extract_or_clause, OR-clause analysis for indexable extracted clauses

10. Subquery, SubLink, SubPlan, and special transformations
    (subselect.c, planagg.c, analyzejoins.c)
    - convert_ANY_sublink_to_join, convert_EXISTS_sublink_to_join,
      simplify_EXISTS_query, convert_VALUES_to_ANY
    - SS_process_ctes, SS_process_sublinks, SS_finalize_plan, SS_assign_special_param
    - InitPlan vs SubPlan: when a sublink stays as a SubPlan vs gets promoted to InitPlan
    - planagg.c: optimize_minmax_aggregates, build_minmax_path
      (uses an ascending/descending index to short-circuit MIN/MAX aggregates)
    - analyzejoins.c: remove_useless_joins (drops a join whose inner side has a
      unique index and whose output isn't used), reduce_unique_semijoins
      (rewrites semijoin to inner join when uniqueness guarantees no duplication)

11. Inheritance, partitioning, partition-wise operations
    - inherit.c: expand_inherited_rtentry, expand_partitioned_rtentry,
      apply_child_basequals
    - appendinfo.c: make_append_rel_info, find_appinfos_by_relids,
      adjust_appendrel_attrs (translate parent Vars → child Vars)
    - allpaths.c: set_append_rel_pathlist, add_paths_to_append_rel
    - partprune.c: make_partition_pruneinfo, gen_partprune_steps,
      perform_pruning_base_step (plan-time pruning when run-time prune steps
      can also be deferred to executor)
    - Partition-wise join: enable_partitionwise_join, generate_partitionwise_join_paths,
      try_partitionwise_join
    - Partition-wise aggregate: create_partitionwise_grouping_paths

12. Parallel query path generation
    - generate_gather_paths, generate_useful_gather_paths
    - compute_parallel_worker, max_parallel_workers_per_gather GUC
    - rel->partial_pathlist as the staging list before Gather/GatherMerge wrap
    - Parallel-safe and parallel-restricted classification (set_proparallel_for_*)
    - Parallel HashJoin (Parallel Hash) — parallel-aware shared hash table
    - Parallel append, parallel bitmap heap scan
    - Partial aggregation: AGGSPLIT_INITIAL_SERIAL → AGGSPLIT_FINAL_DESERIAL

13. GEQO (Genetic Query Optimizer)
    - Triggered when list_length(initial_rels) >= geqo_threshold (default 12)
      in standard_join_search → geqo()
    - geqo_main.c: geqo() — sets up Pool, runs generations
    - geqo_pool.c: alloc_pool, init_pool, sort_pool, geqo_copy
    - geqo_selection.c: geqo_selection — bias-toward-fitter parent picking
    - geqo_recombination.c + geqo_cx/erx/ox1/ox2/pmx/px.c — crossover operators
      selectable via #define (default ERX, edge recombination crossover)
    - geqo_mutation.c — gene swap mutation
    - geqo_eval.c: gimme_tree builds a left-deep join tree from a tour, evaluates
      via merge_clump + make_join_rel, returns a fitness (cost) value
    - geqo_random.c: dedicated PRNG state to keep planning reproducible-ish
    - GUCs: geqo, geqo_threshold, geqo_effort, geqo_pool_size, geqo_generations,
      geqo_selection_bias, geqo_seed

14. Plan creation and finalization (createplan.c, setrefs.c)
    - create_plan() entry; create_plan_recurse switches on Path nodeTag
    - For every Path subtype there is a corresponding create_*_plan(); enumerate them all
    - setrefs.c: set_plan_references walks the finished Plan tree to:
       a. fix outer/inner Var references to use OUTER_VAR/INNER_VAR/INDEX_VAR
       b. flatten range table entries via flatten_rtes_walker
       c. collect rt_index references and dependency refnames
       d. trim unused Vars from underlying scan tlists when allowed
    - SS_finalize_plan (subselect.c) computes extParam/allParam sets

15. Hooks and extension points
    - planner_hook (planner.c) — replace whole planner
    - create_upper_paths_hook (planner.c) — extend grouping_planner
    - join_search_hook (allpaths.c) — replace standard_join_search
    - set_rel_pathlist_hook (allpaths.c) — extend base-rel pathlist
    - set_join_pathlist_hook (joinpath.c) — extend join pathlist
    - get_relation_info_hook (plancat.c) — alter index list / stats
    - get_index_stats_hook, get_attavgwidth_hook, get_relation_stats_hook

Generate (all files under `topic_specific_generated_docs/about_planner/stage1/`):
- architecture_map.json with importance scores (0.0–1.0) for each symbol
- key_symbols.txt (top 50 symbols ranked by importance — larger set due to
  the planner's breadth)
- initial_outline.md with suggested documentation structure
- path_type_inventory.txt — complete enumeration of every Path subtype with:
  NodeTag, struct name (pathnodes.h:line), constructor function (pathnode.c),
  cost function (costsize.c), corresponding Plan node type, plan creator
  (createplan.c)
```

**Expected Output Check**: Verify architecture_map.json contains at least 100 symbols (larger than usual due to the planner's many functions) and identifies 8+ critical paths (planner-entry path, preprocessing path, path-generation path, join-search path, GEQO path, cost-model path, plan-creation path, parallel-planning path, partition-wise path). Verify path_type_inventory.txt lists at least 30 distinct Path subtypes.

---

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for
the PostgreSQL Planner subsystem.

**Source code usage for this stage**:
- For every Tier 1 symbol (importance > 0.8), read the full function implementation
  from `./src/` and annotate key logic steps.
- When documenting the lifecycle, read `src/backend/optimizer/plan/planner.c`
  (focus on planner, standard_planner, subquery_planner, grouping_planner)
  and `src/backend/optimizer/plan/planmain.c` (query_planner) end-to-end —
  these define the call graph the rest of the planner hangs off.
- When documenting preprocessing, read `src/backend/optimizer/prep/prepjointree.c`
  (pull_up_sublinks, pull_up_subqueries, reduce_outer_joins) and
  `src/backend/optimizer/prep/prepqual.c` (canonicalize_qual).
- When documenting initial setup, read `src/backend/optimizer/plan/initsplan.c`
  (deconstruct_jointree, distribute_qual_to_rels, build_base_rel_tlists)
  end-to-end.
- When documenting base-relation path generation, read
  `src/backend/optimizer/path/allpaths.c` end-to-end — this is the dispatch
  hub for every base relation kind.
- When documenting index paths, read `src/backend/optimizer/path/indxpath.c`
  focusing on create_index_paths, build_index_paths, match_clause_to_indexcol,
  choose_bitmap_and.
- When documenting join paths, read `src/backend/optimizer/path/joinpath.c`
  (add_paths_to_joinrel, try_nestloop_path, try_mergejoin_path, try_hashjoin_path,
  sort_inner_and_outer, match_unsorted_outer, hash_inner_and_outer) and
  `src/backend/optimizer/path/joinrels.c` (join_search_one_level, make_join_rel,
  populate_joinrel_with_paths, join_is_legal).
- When documenting the cost model, read `src/backend/optimizer/path/costsize.c`
  focusing on the join initial/final cost pairs and the GUC declarations at
  the top of the file.
- When documenting equivalence classes, read
  `src/backend/optimizer/path/equivclass.c` focusing on process_equivalence,
  generate_join_implied_equalities and helper variants.
- When documenting pathkeys, read `src/backend/optimizer/path/pathkeys.c`
  end-to-end — it is dense but coherent.
- When documenting RestrictInfo / clauses, read
  `src/backend/optimizer/util/restrictinfo.c` and the relevant parts of
  `src/backend/optimizer/util/clauses.c` (eval_const_expressions especially).
- When documenting subqueries / SubLinks, read
  `src/backend/optimizer/plan/subselect.c` focusing on the
  convert_*_sublink_to_join family and SS_finalize_plan.
- When documenting GEQO, read `src/backend/optimizer/geqo/geqo_main.c`,
  `geqo_eval.c`, `geqo_pool.c`. Read `src/backend/optimizer/README` for the
  conceptual GEQO description as well.
- When documenting plan creation, read `src/backend/optimizer/plan/createplan.c`
  systematically — for EACH create_*_plan function, document the Path → Plan
  mapping, tlist handling, qual handling, and any node-specific finalization.
- When documenting setrefs, read `src/backend/optimizer/plan/setrefs.c`
  focusing on set_plan_references, fix_scan_expr, fix_upper_expr.
- When documenting partitioning, read `src/backend/optimizer/util/inherit.c`,
  `src/backend/optimizer/util/appendinfo.c`, and
  `src/backend/partitioning/partprune.c`.
- For data structure documentation, directly quote struct definitions from
  header files (e.g., PlannerInfo, RelOptInfo, Path, RestrictInfo,
  EquivalenceClass, PathKey, SpecialJoinInfo from
  `src/include/nodes/pathnodes.h`).
- Include file paths and line numbers in all source references for traceability.
- Use `grep -rn` to find all callers of key functions to document integration
  patterns accurately.

Input files (from `topic_specific_generated_docs/about_planner/stage1/`):
- architecture_map.json
- key_symbols.txt
- initial_outline.md
- path_type_inventory.txt

Documentation Requirements:

1. For each symbol with importance > 0.8:
   - Complete API documentation (signature, parameters, return values)
   - Internal logic explanation with step-by-step walkthrough
   - Caller/callee relationships and integration patterns
   - Performance characteristics and complexity (especially DP search vs GEQO)
   - Key invariants and assumptions

2. For each symbol with importance 0.5–0.8:
   - API documentation (signature, brief description)
   - Role within the broader planner system
   - Key relationships to Tier 1 symbols

3. **Path-Type Catalog** (dedicated documentation for every Path subtype):
   For EACH Path subtype, produce a standardized entry containing:
   - **Identity**: NodeTag (T_*Path), struct definition with file:line in
     `src/include/nodes/pathnodes.h`
   - **Purpose**: what query/operation pattern produces this Path
   - **Constructor**: `create_*_path()` function in
     `src/backend/optimizer/util/pathnode.c` — signature, parameters,
     allocation pattern
   - **Cost function**: `cost_*()` function in costsize.c (or inline cost
     computation in the constructor) — formula summary, GUC dependencies
   - **Pathkey behavior**: what pathkeys this Path produces or preserves
   - **Parameterization**: whether the Path can be parameterized by lateral
     references / outer relids
   - **Parallel-aware**: whether the Path has parallel-aware variants
   - **Plan counterpart**: which `create_*_plan()` function in createplan.c
     materializes this Path into a Plan node, and which Plan struct
     (in `src/include/nodes/plannodes.h`) is produced
   - **When chosen**: the heuristics in add_path / set_cheapest that make this
     Path competitive — startup vs total cost, sort order, parameterization,
     parallel safety
   - **Example SQL**: representative query whose EXPLAIN exposes this Path

4. **Plan-Creator Catalog** (dedicated documentation for every `create_*_plan()`
   function in createplan.c):
   For EACH plan creator function, document:
   - Signature and dispatching context (which Path nodeTag triggers it)
   - Output Plan struct
   - Tlist handling: physical tlist vs logical tlist, junk columns,
     resjunk attribute, copy_plan_targetlist
   - Qual handling: how RestrictInfo is unwrapped (extract_actual_clauses)
     and split into scan-quals vs join-quals
   - Var-reference handling: relationship to set_plan_references
   - Any node-specific quirks (e.g., create_indexscan_plan handles index
     re-checks; create_modifytable_plan threads child plans for INSERT...SELECT;
     create_hashjoin_plan synthesizes the Hash node child)

5. Required Diagrams (minimum 12):
   - End-to-end planner pipeline (parse/rewrite handoff → preprocessing → query_planner
     → base/join path generation → grouping_planner → create_plan → set_plan_references
     → PlannedStmt to executor)
   - PlannerInfo / RelOptInfo / Path object model
   - DP join-search levels: how join_search_one_level builds level N from level N-1
   - GEQO main loop (population init → fitness eval via gimme_tree → selection
     → recombination → mutation → next generation → output best tour)
   - Path → Plan correspondence map (one-to-one mapping: SeqScanPath → SeqScan,
     IndexPath → IndexScan, NestPath → NestLoop, HashPath → HashJoin + Hash, etc.)
   - Equivalence class derivation flow: process_equivalence merging ECs and the
     transitivity chain to generate_join_implied_equalities
   - Pathkey / sort-order propagation through indexscan, sort, mergejoin
   - SpecialJoinInfo construction during deconstruct_jointree and the
     join_is_legal decision tree
   - Parallel path generation: partial_pathlist accumulation → generate_gather_paths
     → Gather / GatherMerge insertion above
   - Subquery handling decision: pull-up (flat into outer query) vs SubqueryScan
     (wrapped) vs SubPlan / InitPlan (executor-time evaluation)
   - Plan-time partition pruning (make_partition_pruneinfo → step graph)
   - Cost decomposition for one join: HashJoin build + probe phases with
     work_mem batching, contrasting cost_hashjoin vs cost_mergejoin vs
     cost_nestloop on a representative query

6. Special Focus Areas (dedicate extra depth):
   - Path/Plan duality: why both representations exist, what info Path keeps that
     Plan discards (and vice versa)
   - add_path() dominance test: comparing two Paths on (startup_cost, total_cost,
     pathkeys, parameterization) — when one is dominated and pruned vs both
     coexist as Pareto-optimal
   - Outer-join legality: SpecialJoinInfo's min_lefthand / min_righthand,
     identity 3 commutation, the Pbc / Pb*c clone-clause mechanism
     (read `src/backend/optimizer/README` "Valid OUTER JOIN Optimizations" section)
   - varnullingrels and PlaceHolderVar: how outer joins force values to null,
     why "above the join" Vars differ from "scan-level" Vars
   - EquivalenceClass transitivity reasoning: how WHERE t1.x = t2.x and
     t2.x = t3.x produces the implied t1.x = t3.x for join planning
   - DP join search: time complexity O(3^n - 2^(n+1) + 1) for the bushy enumeration,
     why join_collapse_limit / from_collapse_limit exist
   - GEQO mechanics: the chromosome representation (a tour over relids),
     gimme_tree's left-deep construction, geqo_eval delegating back to
     make_join_rel for fitness, why GEQO can produce different plans across runs
     unless geqo_seed is fixed
   - Cost model intuition: what each GUC controls, why random_page_cost > seq_page_cost
     by default, how effective_cache_size influences index vs seq-scan choice
   - Selectivity estimation: MCV-based equality, histogram-based ranges,
     ndistinct-based GROUP BY estimates, multivariate statistics
   - Parameterized paths: how lateral references and bottom-level join-quals
     create parameterized scans, ParamPathInfo bookkeeping
   - Subquery pull-up rules: which subqueries are "simple" enough to pull up,
     and how varlevelsup is rewritten
   - Aggregate strategies: the planner's choice between AGG_PLAIN, AGG_SORTED,
     AGG_HASHED, AGG_MIXED via cost_agg
   - Plan-time vs run-time partition pruning: what the planner can determine
     statically (Const-only quals) vs what is deferred to executor

7. Source code references:
   - For each major function, include the relevant source file path
   - Quote critical code sections (≤20 lines) with inline annotations
   - Note important #define constants and their values (HJ_MAX_BATCHES,
     DEFAULT_GEQO_THRESHOLD, MAX_FUZZY_PATH_DELTA, etc.)

Generate component files organized by functional area (all files under
`topic_specific_generated_docs/about_planner/stage2/`):
- component_lifecycle_and_entry_points.md   (planner, standard_planner, subquery_planner,
                                             query_planner, grouping_planner, end-to-end call graph)
- component_preprocessing.md                (pull_up_sublinks, pull_up_subqueries,
                                             flatten_join_alias_vars, reduce_outer_joins,
                                             canonicalize_qual, preprocess_targetlist,
                                             preprocess_aggrefs, set-op planning)
- component_initial_setup_and_jointree.md   (build_base_rel_tlists, deconstruct_jointree,
                                             distribute_qual_to_rels, SpecialJoinInfo,
                                             process_implied_equality, lateral handling)
- component_base_relation_paths.md          (set_base_rel_pathlists, set_rel_pathlist
                                             dispatch, plain/subquery/append/function/
                                             values/cte/worktable/foreign)
- component_index_paths.md                  (create_index_paths, build_index_paths,
                                             match_clause_to_indexcol, bitmap paths,
                                             parameterized index scans, tidpath)
- component_join_paths_and_search.md        (add_paths_to_joinrel, try_nestloop/merge/hash_path,
                                             sort_inner_and_outer, match_unsorted_outer,
                                             hash_inner_and_outer, join_search_one_level,
                                             make_join_rel, populate_joinrel_with_paths,
                                             join_is_legal, DP search, bushy plans)
- component_cost_model_and_selectivity.md   (cost_seqscan/index/sort/agg/...,
                                             initial/final cost_nestloop/mergejoin/hashjoin,
                                             clauselist_selectivity, selfuncs.c integration,
                                             pg_statistic, GUCs)
- component_equivalence_classes_and_pathkeys.md  (process_equivalence, generate_join_implied_equalities,
                                                  EC/EM/PathKey structures, build_index/join_pathkeys,
                                                  pathkeys_contained_in, sort-order reasoning)
- component_restrictinfo_and_clause_utils.md     (make_restrictinfo, eval_const_expressions,
                                                  contain_volatile_functions, find_nonnullable_rels,
                                                  predicate_implied_by/refuted_by, OR clauses)
- component_subquery_and_sublink.md         (convert_ANY/EXISTS_sublink_to_join, SS_process_sublinks,
                                             SS_finalize_plan, SubPlan vs InitPlan, planagg.c MIN/MAX,
                                             remove_useless_joins, reduce_unique_semijoins)
- component_inheritance_and_partitioning.md (expand_inherited_rtentry, expand_partitioned_rtentry,
                                             AppendRelInfo, set_append_rel_pathlist,
                                             partprune.c, partition-wise join, partition-wise aggregate)
- component_parallel_planning.md            (generate_gather_paths, compute_parallel_worker,
                                             partial_pathlist, parallel-safe classification,
                                             Parallel Hash, parallel append, partial aggregation)
- component_geqo.md                         (geqo entry, gimme_tree, pool/selection/recombination,
                                             crossover variants, mutation, GUC tuning)
- component_plan_creation_and_setrefs.md    (create_plan, create_plan_recurse dispatch,
                                             create_*_plan family overview, set_plan_references,
                                             fix_scan_expr, fix_upper_expr)
- component_hooks_and_extensibility.md      (planner_hook, create_upper_paths_hook,
                                             join_search_hook, set_rel_pathlist_hook,
                                             set_join_pathlist_hook, get_relation_info_hook,
                                             stats hooks)
- path_catalog/scan_paths.md                (Path, IndexPath, BitmapHeapPath, BitmapAndPath,
                                             BitmapOrPath, TidPath, SubqueryScanPath,
                                             FunctionScanPath, ValuesScanPath, TableFuncPath,
                                             CteScanPath, NamedTuplestoreScanPath,
                                             WorkTableScanPath, ForeignPath, CustomPath,
                                             SamplePath)
- path_catalog/join_paths.md                (NestPath, MergePath, HashPath — full details,
                                             parameterization, parallel-aware variants)
- path_catalog/upper_paths.md               (SortPath, IncrementalSortPath, AggPath,
                                             GroupingSetsPath, MinMaxAggPath, WindowAggPath,
                                             UniquePath, SetOpPath, RecursiveUnionPath,
                                             LimitPath, ProjectionPath, ProjectSetPath,
                                             MaterialPath, MemoizePath, GroupResultPath)
- path_catalog/append_and_partition_paths.md (AppendPath, MergeAppendPath, partition-wise
                                              join paths, PartialAppendPath)
- path_catalog/parallel_paths.md            (GatherPath, GatherMergePath, partial path
                                             variants, parallel-aware Hash and BitmapHeap)
- path_catalog/modify_paths.md              (ModifyTablePath, LockRowsPath)
- plan_creator_catalog/scan_creators.md     (create_seqscan_plan, create_indexscan_plan,
                                             create_indexonlyscan_plan, create_bitmap_scan_plan,
                                             create_tidscan_plan, create_subqueryscan_plan,
                                             create_functionscan_plan, create_valuesscan_plan,
                                             create_ctescan_plan, create_worktablescan_plan,
                                             create_foreignscan_plan, create_customscan_plan)
- plan_creator_catalog/join_creators.md     (create_nestloop_plan, create_mergejoin_plan,
                                             create_hashjoin_plan, create_hash_plan)
- plan_creator_catalog/upper_creators.md    (create_sort_plan, create_agg_plan,
                                             create_group_plan, create_windowagg_plan,
                                             create_unique_plan, create_setop_plan,
                                             create_recursiveunion_plan, create_append_plan,
                                             create_merge_append_plan, create_material_plan,
                                             create_memoize_plan, create_gather_plan,
                                             create_gather_merge_plan, create_limit_plan,
                                             create_projection_plan, create_project_set_plan)
- plan_creator_catalog/modify_creators.md   (create_modifytable_plan, create_lockrows_plan)
- diagrams/*.mermaid                        (under
                                             `topic_specific_generated_docs/about_planner/stage2/diagrams/`)
```

**Expected Output Check**: Ensure all Tier 1 symbols (importance > 0.8) have detailed documentation with source references. Verify minimum 12 diagrams are generated. Verify every Path subtype from path_type_inventory.txt has a catalog entry under path_catalog/. Verify every `create_*_plan()` function in createplan.c has a catalog entry under plan_creator_catalog/.

---

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical document.

**Source code verification for this stage**:
- Before finalizing, spot-check at least 20 critical function signatures and struct
  definitions against `./src/` to ensure accuracy (more than usual due to the
  large number of functions and Path subtypes).
- Verify that all quoted code snippets in the documentation match the actual source.
- Confirm file paths referenced in the documentation are valid: `ls ./src/path/to/file.c`.
- Cross-check every path_catalog entry: verify the constructor name in pathnode.c
  and the cost function name in costsize.c.
- Cross-check every plan_creator_catalog entry: verify the function exists in
  createplan.c with the documented signature.

Input files (from `topic_specific_generated_docs/about_planner/stage2/`):
- All component_*.md files from Stage 2
- All path_catalog/*.md files
- All plan_creator_catalog/*.md files
- All diagrams/*.mermaid files
- architecture_map.json for reference (from
  `topic_specific_generated_docs/about_planner/stage1/`)
- path_type_inventory.txt for reference

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): The planner's role between parser/rewriter
     and executor, the Path/Plan duality design philosophy, the dynamic-programming
     vs genetic-search choice for join ordering, and key trade-offs (planning time
     vs plan quality, exhaustive search vs heuristic search, cost-model accuracy
     vs statistics overhead)
   - Architecture Overview: System-wide perspective with a main structural
     diagram showing the planner's position between parser/rewriter and
     executor, and the major sub-pipelines (preprocessing → path generation →
     join search → upper-level planning → plan creation)
   - Core Components (organized by operational flow):
     a. Lifecycle and Entry Points — planner, standard_planner, subquery_planner,
        query_planner, grouping_planner, the call-graph backbone
     b. Preprocessing — sublink/subquery pull-up, qual canonicalization, target list,
        set-operation tree
     c. Initial Setup and Jointree — build_base_rel_tlists, deconstruct_jointree,
        distribute_qual_to_rels, SpecialJoinInfo construction, lateral references
     d. Base-Relation Path Generation — allpaths.c dispatch and per-RTE-kind generators
     e. Index Path Generation — indxpath.c, bitmap paths, parameterized index scans
     f. Join Path Generation and Search — joinpath.c (3 join methods), joinrels.c
        (DP search, join legality)
     g. Cost Model and Selectivity — costsize.c, clausesel.c, selfuncs.c,
        pg_statistic integration, GUCs
     h. Equivalence Classes and PathKeys — EC transitivity, sort-order reasoning
     i. RestrictInfo and Clause Utilities — qual classification, eval_const_expressions,
        predicate logic, PlaceHolderVar
     j. Subquery and SubLink Handling — pull-up vs SubPlan/InitPlan,
        MIN/MAX optimization, join removal
     k. Inheritance and Partitioning — AppendRelInfo, partprune,
        partition-wise join/aggregate
     l. Parallel Query Planning — partial_pathlist, Gather/GatherMerge,
        parallel-aware nodes
     m. GEQO — genetic search for many-table joins
     n. Plan Creation and setrefs — Path → Plan conversion, set_plan_references,
        fix_scan_expr/fix_upper_expr
     o. Hooks and Extensibility — every planner hook and what extensions plug in where
   - **Path Catalog** (dedicated chapter):
     A comprehensive catalog of every Path subtype, organized by category. Each entry
     follows the standardized template (identity, purpose, constructor, cost function,
     pathkey behavior, parameterization, parallel-aware variants, plan counterpart,
     when chosen, example SQL). This chapter is both reference and learning material
     for understanding PostgreSQL's planning vocabulary.
     - Scan Paths
     - Join Paths
     - Upper Paths (sort, aggregate, window, unique, limit, ...)
     - Append and Partition Paths
     - Parallel Paths
     - Modify Paths
   - **Plan Creator Catalog** (dedicated chapter):
     A comprehensive catalog of every `create_*_plan()` function in
     `src/backend/optimizer/plan/createplan.c`, mapping Path → Plan
     and documenting tlist handling, qual handling, and Var-reference fixup quirks.
     - Scan creators
     - Join creators
     - Upper creators
     - Modify creators
   - Deep Dives: Complex topics including:
     - add_path() Pareto-dominance test and the cost_diff_fuzz_factor
     - Outer-join identity 3 and the Pbc/Pb*c clone-clause mechanism
     - varnullingrels and PlaceHolderVar above outer joins
     - EquivalenceClass transitivity in the presence of outer joins
       (broken-EC handling)
     - DP join search complexity and the geqo_threshold cutoff
     - GEQO chromosome encoding and crossover operator selection
     - Cost-model GUC sensitivity and tuning workflow
     - Selectivity estimation: MCV, histograms, ndistinct, multivariate stats
     - Parameterized paths and ParamPathInfo
     - Subquery pull-up rules and the "simple subquery" predicate
     - Hash-aggregate spill-to-disk and the planner's row-count bound
     - Partition pruning at plan vs execution time
     - Parallel-safe vs parallel-restricted classification
     - Memoize node decision and cost
     - Planner hooks in the wild (pg_hint_plan, citus, timescaledb)
   - Appendices:
     - Symbol index (alphabetical, with source file locations)
     - Glossary of planner terminology (Path/Plan, RelOptInfo, EquivalenceClass,
       PathKey, RestrictInfo, SpecialJoinInfo, PlaceHolderVar, ParamPathInfo,
       lateral, parameterization, dummy rel, etc.)
     - Key data structure reference (PlannerInfo, PlannerGlobal, RelOptInfo,
       Path & subtypes, RestrictInfo, EquivalenceClass, EquivalenceMember,
       PathKey, SpecialJoinInfo, PlaceHolderVar, PlaceHolderInfo, ParamPathInfo,
       AppendRelInfo, JoinDomain — quote struct definitions from
       src/include/nodes/pathnodes.h)
     - Path subtype quick-reference table (NodeTag → struct → constructor →
       cost fn → plan creator → output Plan struct — one row per subtype)
     - Key GUC parameters (cost: seq_page_cost, random_page_cost, cpu_*_cost,
       parallel_*_cost, effective_cache_size; enable: enable_seqscan,
       enable_indexscan, enable_indexonlyscan, enable_bitmapscan, enable_tidscan,
       enable_sort, enable_material, enable_nestloop, enable_mergejoin,
       enable_hashjoin, enable_hashagg, enable_partitionwise_join,
       enable_partitionwise_aggregate, enable_parallel_hash,
       enable_parallel_append, enable_memoize; geqo: geqo, geqo_threshold,
       geqo_effort, geqo_pool_size, geqo_generations, geqo_selection_bias,
       geqo_seed; misc: from_collapse_limit, join_collapse_limit, work_mem,
       jit_above_cost, cursor_tuple_fraction, default_statistics_target)
     - Further reading: src/backend/optimizer/README, optimizer/plan/README,
       relevant PostgreSQL wiki pages, foundational papers (System R DP join
       enumeration, GEQO genetic algorithm references)

2. Enhancement Tasks:
   - Generate comprehensive cross-references between sections (e.g., path_catalog
     entries link back to the cost-model and join-search chapters; plan_creator
     entries link back to the corresponding path_catalog entries; Path-type
     quick-reference table links to both catalogs)
   - Eliminate redundancy between component chapters and the catalogs — the
     catalogs focus on per-subtype specifics while the chapters provide
     cross-cutting concepts
   - Standardize terminology (prefer PostgreSQL implementation terms:
     "RelOptInfo" over "relation in optimizer", "Path" for the planner-internal
     representation, "Plan" for the executor-facing representation, "qual" for
     qualification clause, "rel" for RelOptInfo/relation, "joinrel" for join
     RelOptInfo, "varnullingrels" not "outer-join nullification set")
   - Add navigation aids (Table of Contents, section breadcrumbs, next/prev links)
   - Ensure consistent diagram style and labeling across all Mermaid diagrams
   - For the path_catalog: ensure every entry has an "Example SQL" subsection
     showing a query whose EXPLAIN reveals the corresponding Plan node

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented somewhere in the output
   - Verify all Path subtypes from path_type_inventory.txt have catalog entries
   - Verify every create_*_plan in createplan.c has a catalog entry
   - Ensure logical flow: high-level concepts → architecture → implementation
     details → catalog reference
   - Validate all internal cross-reference links
   - Check all Mermaid diagrams render correctly (valid syntax)
   - Confirm code examples and source references match actual PostgreSQL source
   - Flag any remaining ambiguities or areas needing community review

4. Output Organization:
   Since total size will likely exceed 4000 lines (larger than usual due to the
   Path catalog and Plan-creator catalog):
   - Split into logical modules with clear boundaries
   - Create index.md as the navigation hub linking all modules
   - Maintain coherent reading experience with "Prerequisites" and "Next" notes per module
   - Each module should be self-contained enough for targeted reading
   - **All final output files must be written under
     `topic_specific_generated_docs/about_planner/final/`**
   - **Consolidated diagrams must be copied to
     `topic_specific_generated_docs/about_planner/diagrams/`**

   Module structure (all under `topic_specific_generated_docs/about_planner/final/`):
   - index.md                                   (navigation hub, reading guide)
   - 01_executive_summary.md                    (overview for newcomers)
   - 02_architecture_overview.md                (system-wide perspective, main diagram)
   - 03_lifecycle_and_entry_points.md           (planner / standard_planner /
                                                  subquery_planner / query_planner /
                                                  grouping_planner)
   - 04_preprocessing.md                        (sublink/subquery pull-up,
                                                  canonicalize_qual, set ops)
   - 05_initial_setup_and_jointree.md           (deconstruct_jointree,
                                                  distribute_qual_to_rels,
                                                  SpecialJoinInfo)
   - 06_base_relation_paths.md                  (allpaths.c dispatch and per-RTE
                                                  generators)
   - 07_index_paths.md                          (indxpath.c, bitmap paths,
                                                  parameterized scans)
   - 08_join_paths_and_search.md                (joinpath.c, joinrels.c, DP search)
   - 09_cost_model_and_selectivity.md           (costsize.c, clausesel.c,
                                                  selfuncs.c, GUCs)
   - 10_equivalence_classes_and_pathkeys.md     (EC machinery, pathkeys, sort orders)
   - 11_restrictinfo_and_clause_utils.md        (RestrictInfo, eval_const_expressions,
                                                  predicate logic, PlaceHolderVar)
   - 12_subquery_and_sublink.md                 (pull-up vs SubPlan/InitPlan,
                                                  MIN/MAX, join removal)
   - 13_inheritance_and_partitioning.md         (AppendRelInfo, partprune,
                                                  partition-wise ops)
   - 14_parallel_planning.md                    (Gather, partial_pathlist,
                                                  parallel-aware nodes)
   - 15_geqo.md                                 (genetic optimizer)
   - 16_plan_creation_and_setrefs.md            (create_plan family,
                                                  set_plan_references)
   - 17_hooks_and_extensibility.md              (every planner hook)
   - 18_path_catalog.md                         (all Path subtypes — detailed catalog)
   - 19_plan_creator_catalog.md                 (all create_*_plan — detailed catalog)
   - 20_deep_dives.md                           (add_path dominance, outer-join
                                                  identity 3, GEQO mechanics,
                                                  selectivity estimation, etc.)
   - appendix_symbol_index.md                  (alphabetical symbol reference)
   - appendix_glossary.md                      (planner terminology)
   - appendix_data_structures.md               (key struct definitions:
                                                 PlannerInfo, RelOptInfo, Path
                                                 subtypes, RestrictInfo,
                                                 EquivalenceClass, PathKey,
                                                 SpecialJoinInfo, PlaceHolderVar,
                                                 ParamPathInfo, AppendRelInfo)
   - appendix_path_quick_reference.md          (Path subtype lookup table:
                                                 NodeTag → struct → constructor →
                                                 cost fn → plan creator →
                                                 output Plan struct)
   - appendix_guc_parameters.md                (every planner-relevant GUC)

5. Additional Deliverables (also under
   `topic_specific_generated_docs/about_planner/final/`):
   - planner_quick_reference.md   (2-page summary: Path/Plan duality,
                                    join-search algorithms, key GUCs,
                                    EXPLAIN-reading tips, hook entry points)
   - planner_api_reference.md     (function signatures grouped by subsystem,
                                    with brief descriptions)
   - quality_report.md            (coverage metrics: % of key_symbols documented,
                                    % of Path subtypes cataloged, %
                                    of create_*_plan documented, diagram count,
                                    known gaps, improvement suggestions)
```

**Expected Output Check**: Verify professional documentation quality, complete symbol coverage (>80%), complete Path-subtype catalog coverage (100% of path_type_inventory.txt entries), complete plan_creator catalog coverage (100% of `create_*_plan` functions in createplan.c), and coherent navigation structure.

---

## Orchestration Rules

### Execution Flow
1. **Before Stage 1**: Activate the project venv and create the output directory tree:
   ```bash
   source venv/bin/activate
   mkdir -p topic_specific_generated_docs/about_planner/{stage1,stage2/diagrams,stage2/path_catalog,stage2/plan_creator_catalog,final,diagrams}
   ```
2. Execute each stage sequentially — do not proceed until the previous stage completes successfully
3. Capture all output files from each subagent into the appropriate subdirectory under `topic_specific_generated_docs/about_planner/`
4. Validate expected outputs before proceeding to the next stage
5. Report progress after each stage

### Source Tree Primacy
- The local `./src/` directory is the **single source of truth**.
- `src/backend/optimizer/README` is the authoritative conceptual document — read it before relying on any synthesized description.
- Subagents should use `./src/` for structural exploration (file layout, neighboring functions, header inclusions).
- All generated documentation must include verifiable source file paths relative to `./src/`.

### Error Handling
- **Subagent failure**: Retry once with modified parameters (e.g., reduce scope), then proceed with partial results and document gaps
- **Missing expected files**: Log warning, attempt recovery using available data, note in quality_report.md
- **Context limit approaching**: Save progress checkpoint, split remaining work into smaller focused chunks, resume from checkpoint. **For the catalogs**: if context limits are hit, process Path subtypes and plan creators in batches (scan paths first, then join paths, etc.)
- **Symbol not found**: Log missing symbol, attempt alternative names (e.g., with/without `create_` prefix, with/without `_path`/`_plan` suffix), continue with available data

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, coverage %, Path subtypes cataloged, plan creators documented>
Issues encountered: <any warnings or partial failures>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical-path symbols are documented:
   `planner`, `standard_planner`, `subquery_planner`, `query_planner`,
   `grouping_planner`, `make_one_rel`, `make_rel_from_joinlist`,
   `standard_join_search`, `join_search_one_level`, `make_join_rel`,
   `populate_joinrel_with_paths`, `add_paths_to_joinrel`,
   `try_nestloop_path`, `try_mergejoin_path`, `try_hashjoin_path`,
   `set_cheapest`, `add_path`, `create_plan`, `create_plan_recurse`,
   `set_plan_references`, `process_equivalence`,
   `generate_join_implied_equalities`, `build_index_pathkeys`,
   `build_join_pathkeys`, `pathkeys_contained_in`,
   `cost_seqscan`, `cost_index`, `cost_hashjoin`, `cost_mergejoin`,
   `cost_nestloop`, `clauselist_selectivity`, `eval_const_expressions`,
   `pull_up_subqueries`, `pull_up_sublinks`, `deconstruct_jointree`,
   `distribute_qual_to_rels`, `geqo`, `gimme_tree`,
   `expand_inherited_rtentry`, `make_partition_pruneinfo`,
   `generate_gather_paths`
2. Verify every Path subtype has a path_catalog entry (target = 100%)
3. Verify every `create_*_plan` function has a plan_creator_catalog entry (target = 100%)
4. Count and list all generated diagrams (must be ≥ 12)
5. Check total documentation coverage against key_symbols.txt (target > 80%)
6. Ensure no broken cross-references or unresolved TODO markers remain
7. Confirm file organization follows the specified module structure
8. Validate all Mermaid diagram syntax

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive planner documentation generated covering all 15 functional areas (lifecycle, preprocessing, initial setup, base paths, index paths, join paths and search, cost model, equivalence classes / pathkeys, RestrictInfo / clauses, subquery / SubLink, inheritance / partitioning, parallel planning, GEQO, plan creation / setrefs, hooks)
- [ ] Complete Path catalog covering 100% of Path subtypes with standardized entries
- [ ] Complete Plan-creator catalog covering 100% of `create_*_plan()` functions
- [ ] Minimum 12 technical diagrams included and rendering correctly
- [ ] quality_report.md shows > 80% symbol coverage, 100% Path-subtype catalog coverage, and 100% plan-creator catalog coverage
- [ ] Documentation is organized into navigable modules with index.md
- [ ] Both high-level overview (suitable for newcomers) and deep implementation details (suitable for PostgreSQL contributors) are present
- [ ] Quick reference and API reference supplements are generated

---

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages — proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Planner Documentation Generation - Stage 1: Architecture Analysis"
