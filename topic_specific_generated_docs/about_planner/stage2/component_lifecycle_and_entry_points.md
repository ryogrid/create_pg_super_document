# Component: Lifecycle and Entry Points

> Stage 2 documentation for the **ENTRY_AND_LIFECYCLE** functional area of the
> PostgreSQL planner. Source files referenced in this document live under
> `src/backend/optimizer/plan/` (`planner.c`, `planmain.c`) and
> `src/include/nodes/pathnodes.h`. All citations are repo-relative.

## 1. Why this exists

The optimizer is invoked once per `Query`. It has to:

1. Build per-query state (`PlannerInfo`, "root" everywhere) and global state
   shared across query levels (`PlannerGlobal`).
2. Recurse into sub-Queries (sub-SELECTs, CTEs that we plan separately, set
   operations) using the same machinery.
3. Run **preprocessing**, then **scan/join planning** (`query_planner` →
   `make_one_rel`), then **upper-rel planning** (grouping, window, distinct,
   ordered, final), and finally **plan creation** + **setrefs**.
4. Hand a `PlannedStmt` back to the executor.

The lifecycle is implemented by a small handful of entry points that are very
easy to over-look. Read them in this order:

- `src/backend/optimizer/plan/planner.c:275`  — `planner()`
- `src/backend/optimizer/plan/planner.c:288`  — `standard_planner()`
- `src/backend/optimizer/plan/planner.c:629`  — `subquery_planner()`
- `src/backend/optimizer/plan/planmain.c:53`  — `query_planner()`
- `src/backend/optimizer/plan/planner.c:1335` — `grouping_planner()`

See diagrams `01_planner_pipeline.mermaid` and `02_object_model.mermaid`.

---

## 2. Symbol table

| Symbol               | File:line                                 | Importance | Tier |
|----------------------|-------------------------------------------|------------|------|
| `planner`            | `src/backend/optimizer/plan/planner.c:275`  | 1.00 | 1 |
| `standard_planner`   | `src/backend/optimizer/plan/planner.c:288`  | 0.99 | 1 |
| `subquery_planner`   | `src/backend/optimizer/plan/planner.c:629`  | 0.98 | 1 |
| `grouping_planner`   | `src/backend/optimizer/plan/planner.c:1335` | 0.95 | 1 |
| `query_planner`      | `src/backend/optimizer/plan/planmain.c:53`  | 0.94 | 1 |
| `PlannerInfo`        | `src/include/nodes/pathnodes.h:195`         | 0.96 | 1 |
| `PlannerGlobal`      | `src/include/nodes/pathnodes.h:95`          | 0.78 | 2 |
| `make_one_rel`       | `src/backend/optimizer/path/allpaths.c:171` | 0.92 | 1 |
| `fetch_upper_rel`    | `src/backend/optimizer/util/relnode.c`      | 0.55 | 2 |
| `set_cheapest`       | `src/backend/optimizer/util/pathnode.c:242` | 0.92 | 1 |

---

## 3. End-to-end call graph

```
pg_plan_query()                           [tcopprot]
  └─ planner()                            (planner.c:275)
      ├─ planner_hook (optional plugin)
      └─ standard_planner()               (planner.c:288)
          ├─ makeNode(PlannerGlobal)
          ├─ max_parallel_hazard()                     (clauses.c)
          ├─ subquery_planner()           (planner.c:629)        ★ recurses for sub-Queries
          │    ├─ SS_process_ctes
          │    ├─ transform_MERGE_to_join
          │    ├─ replace_empty_jointree
          │    ├─ pull_up_sublinks
          │    ├─ preprocess_function_rtes
          │    ├─ pull_up_subqueries
          │    ├─ flatten_simple_union_all
          │    ├─ preprocess_expression()  ──► flatten_join_alias_vars,
          │    │                                eval_const_expressions,
          │    │                                canonicalize_qual,
          │    │                                SS_process_sublinks,
          │    │                                SS_replace_correlation_vars,
          │    │                                make_ands_implicit
          │    ├─ reduce_outer_joins
          │    ├─ remove_useless_result_rtes
          │    ├─ grouping_planner()       (planner.c:1335)
          │    │    ├─ preprocess_grouping_sets / preprocess_minmax_aggregates
          │    │    ├─ preprocess_targetlist / preprocess_aggrefs
          │    │    ├─ preprocess_limit
          │    │    ├─ query_planner()     (planmain.c:53)
          │    │    │     ├─ setup_simple_rel_arrays
          │    │    │     ├─ add_base_rels_to_query
          │    │    │     ├─ build_base_rel_tlists
          │    │    │     ├─ find_placeholders_in_jointree
          │    │    │     ├─ find_lateral_references
          │    │    │     ├─ deconstruct_jointree                 (initsplan.c:740)
          │    │    │     ├─ reconsider_outer_join_clauses
          │    │    │     ├─ generate_base_implied_equalities
          │    │    │     ├─ qp_callback (computes query_pathkeys)
          │    │    │     ├─ fix_placeholder_input_needed_levels
          │    │    │     ├─ remove_useless_joins
          │    │    │     ├─ reduce_unique_semijoins
          │    │    │     ├─ add_placeholders_to_base_rels
          │    │    │     ├─ create_lateral_join_info
          │    │    │     ├─ match_foreign_keys_to_quals
          │    │    │     ├─ extract_restriction_or_clauses
          │    │    │     ├─ add_other_rels_to_query  (inheritance children)
          │    │    │     ├─ distribute_row_identity_vars
          │    │    │     └─ make_one_rel()             (allpaths.c:171)
          │    │    │           ├─ set_base_rel_consider_startup
          │    │    │           ├─ set_base_rel_sizes
          │    │    │           ├─ set_base_rel_pathlists
          │    │    │           └─ make_rel_from_joinlist
          │    │    │                 └─ standard_join_search / geqo / join_search_hook
          │    │    ├─ create_grouping_paths      (UPPERREL_GROUP_AGG)
          │    │    ├─ create_window_paths        (UPPERREL_WINDOW)
          │    │    ├─ create_distinct_paths      (UPPERREL_DISTINCT)
          │    │    ├─ create_ordered_paths       (UPPERREL_ORDERED)
          │    │    └─ build UPPERREL_FINAL with ModifyTablePath / LockRowsPath / LimitPath
          │    ├─ SS_identify_outer_params
          │    ├─ SS_charge_for_initplans
          │    └─ set_cheapest(final_rel)
          ├─ get_cheapest_fractional_path(final_rel, tuple_fraction)
          ├─ create_plan()                 (createplan.c:338)
          ├─ optionally Material/Gather wrap
          ├─ SS_finalize_plan
          ├─ set_plan_references()         (setrefs.c:287)
          └─ build PlannedStmt
```

---

## 4. API: `planner`

### 4.1 Signature
```c
PlannedStmt *
planner(Query *parse, const char *query_string, int cursorOptions,
        ParamListInfo boundParams);
```
Source: `src/backend/optimizer/plan/planner.c:275`.

### 4.2 Purpose
Trampoline that gives extensions a single hook (`planner_hook`) to replace the
entire planner. If no hook is installed it forwards to `standard_planner`.

### 4.3 Body (verbatim, annotated)
```c
PlannedStmt *
planner(Query *parse, const char *query_string, int cursorOptions,
        ParamListInfo boundParams)
{
    PlannedStmt *result;

    if (planner_hook)
        result = (*planner_hook) (parse, query_string, cursorOptions, boundParams);
    else
        result = standard_planner(parse, query_string, cursorOptions, boundParams);
    return result;
}
```
The hook is the **outermost** extension point. Plugins (`pg_hint_plan`,
`pg_plan_advsr`, `auto_explain` for hooking, etc.) typically wrap the call.

### 4.4 Caller
- `pg_plan_query()` in `src/backend/tcop/postgres.c`. Some non-trivial code
  paths (PL/pgSQL preparation, `EXPLAIN`, prepared statements, autovacuum
  ANALYZE re-planning) ultimately come through here.

---

## 5. API: `standard_planner`

### 5.1 Signature
```c
PlannedStmt *
standard_planner(Query *parse, const char *query_string, int cursorOptions,
                 ParamListInfo boundParams);
```
Source: `src/backend/optimizer/plan/planner.c:288`.

### 5.2 What it does, step by step

1. **Builds `PlannerGlobal`** (`makeNode(PlannerGlobal)`) and zeroes most
   fields. This struct is shared across all sub-`PlannerInfo`s for this top-level
   `Query`. See `src/include/nodes/pathnodes.h:95`.
2. **Parallel-mode classification** (`planner.c:349`):
   ```c
   if ((cursorOptions & CURSOR_OPT_PARALLEL_OK) != 0 &&
       IsUnderPostmaster &&
       parse->commandType == CMD_SELECT &&
       !parse->hasModifyingCTE &&
       max_parallel_workers_per_gather > 0 &&
       !IsParallelWorker())
   {
       glob->maxParallelHazard = max_parallel_hazard(parse);
       glob->parallelModeOK = (glob->maxParallelHazard != PROPARALLEL_UNSAFE);
   }
   else
   {
       glob->maxParallelHazard = PROPARALLEL_UNSAFE;
       glob->parallelModeOK = false;
   }
   ```
   This is the first place that decides whether parallel paths get created at
   all. The actual partial paths and `Gather` insertion happen later (see
   `component_parallel_planning.md`).
3. **Tuple-fraction selection** (planner.c:387):
    - `CURSOR_OPT_FAST_PLAN` → `cursor_tuple_fraction` GUC.
    - Else → 0 (full retrieval assumed).
   This is the only knob for "fast-start" optimization at the top level.
   `subquery_planner` and `grouping_planner` propagate it down.
4. **Recursion via `subquery_planner`** (planner.c:416):
   ```c
   root = subquery_planner(glob, parse, NULL, false, tuple_fraction, NULL);
   ```
   Returns the topmost `PlannerInfo` for the outermost query level.
5. **Pick the winning Path**:
   ```c
   final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
   best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);
   ```
6. **Convert to Plan**: `top_plan = create_plan(root, best_path);`
7. **Cursor scrollable wrapping**: if `CURSOR_OPT_SCROLL` is set and the chosen
   plan can't run backwards, wraps it in a Material node via
   `materialize_finished_plan`.
8. **Debug-mode Gather forcing**: when `debug_parallel_query` is `on` or
   `regress` and `top_plan` is parallel-safe, slap a `Gather` on top with
   `single_copy = true` to test parallel execution paths.
9. **Sub-plan finalization** (`SS_finalize_plan`) — see
   `component_plan_creation_and_setrefs.md`.
10. **`set_plan_references`** to flatten the rangetable.
11. **Build the `PlannedStmt`** and return.

### 5.3 Critical invariants

- The caller's `Query` is **mutated** ("scribbled on"), so any caller intending
  to plan multiple times must `copyObject(parse)` first. The comment at
  `planner.c:270–272` is explicit about this.
- `glob->parallelModeNeeded` is initially `false`. It flips `true` only when
  `create_gather_plan` / `create_gather_merge_plan` actually emit a Gather.
  The exception is `debug_parallel_query` (line 384), which forces `true`
  whenever `parallelModeOK` is also true.
- `lastPHId`, `lastRowMarkId`, `lastPlanNodeId` are global (across sub-Queries)
  monotonically increasing IDs. Sub-`PlannerInfo`s share `glob` so these IDs
  remain unique throughout the whole plan tree.

---

## 6. API: `subquery_planner`

### 6.1 Signature
```c
PlannerInfo *
subquery_planner(PlannerGlobal *glob, Query *parse, PlannerInfo *parent_root,
                 bool hasRecursion, double tuple_fraction,
                 SetOperationStmt *setops);
```
Source: `src/backend/optimizer/plan/planner.c:629`.

### 6.2 Purpose
The "real" planner. Plans a **single Query level**, recursing into sub-Queries
via further `subquery_planner` calls (e.g. when a CTE or a non-pulled-up
RTE_SUBQUERY is planned separately).

### 6.3 What it does

The function is long (~520 lines). The pipeline is:

1. **Allocate and initialize `PlannerInfo`** (planner.c:642–681). Notable:
   - `query_level = parent_root ? parent_root->query_level + 1 : 1` —
     1-based, outermost query is level 1.
   - `parent_root` lets `SS_replace_correlation_vars` find outer Vars.
   - `wt_param_id` is set via `assign_special_exec_param(root)` only when
     `hasRecursion` is true (recursive CTE).
   - `eq_classes`, `join_domains`, `placeholder_list`, etc. start empty.
2. **Top-level join domain** (planner.c:688) is created up front because
   EquivalenceClasses created early (during pull-up of sublinks/subqueries)
   reference it.
3. **Preprocessing pipeline** — covered in detail in
   `component_preprocessing.md`. Order:
   - `SS_process_ctes` (only when `parse->cteList`)
   - `transform_MERGE_to_join`
   - `replace_empty_jointree`
   - `pull_up_sublinks`
   - `preprocess_function_rtes`
   - `pull_up_subqueries`
   - `flatten_simple_union_all` (if `parse->setOperations`)
   - Survey `parse->rtable` (planner.c:750-802) to set `hasJoinRTEs`,
     `hasLateralRTEs`, `hasOuterJoins`, `hasResultRTEs` and per-RTE
     `inh` (clear if no children).
   - View permission pre-checks for selectivity-leak protection
     (planner.c:831-847).
   - `preprocess_rowmarks` for FOR UPDATE / FOR SHARE.
   - `preprocess_expression()` runs over: `targetList`, `withCheckOptions`,
     `returningList`, jointree quals (via `preprocess_qual_conditions`),
     `havingQual`, `windowClause` offsets, `limitOffset`/`Count`, ON CONFLICT
     pieces, MERGE actions, `mergeJoinCondition`, `append_rel_list`, and a
     per-RTE pass for `tablesample`, RTE_FUNCTION/TABLEFUNC/VALUES, security
     quals.
   - Drop `rte->joinaliasvars` lists (planner.c:1031-1039) — they're stale
     after `flatten_join_alias_vars`.
   - **HAVING → WHERE** transfer (planner.c:1072-1100) when the clause
     contains no aggregates, no volatile functions, no subplans, and no
     non-empty grouping sets. Either `move` or `copy` depending on
     `groupClause`/`groupingSets`.
   - `reduce_outer_joins` if `hasOuterJoins`.
   - `remove_useless_result_rtes` if `hasResultRTEs || hasOuterJoins`.
4. **Main planning** (planner.c:1122):
   ```c
   grouping_planner(root, tuple_fraction, setops);
   ```
5. **Final-rel cleanup**:
   ```c
   SS_identify_outer_params(root);
   final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
   SS_charge_for_initplans(root, final_rel);
   set_cheapest(final_rel);
   ```
6. **Return `root`**. Note: `set_cheapest` is performed here (not in
   `grouping_planner`) so initPlan costs are fully accounted for before the
   final pick.

### 6.4 Recursion contract
`subquery_planner` is called recursively in three contexts:
- `SS_process_ctes` (when planning a non-inlined CTE).
- `set_subquery_pathlist` in `allpaths.c` (when an unflattened RTE_SUBQUERY's
  pathlist is being computed).
- `make_subplan` / `build_subplan` for SubLinks left as SubPlans.

Each recursive call gets a new `PlannerInfo`, but shares `glob`. `parent_root`
is non-NULL for sub-Query levels, allowing correlation-var resolution.

---

## 7. API: `query_planner`

### 7.1 Signature
```c
RelOptInfo *
query_planner(PlannerInfo *root,
              query_pathkeys_callback qp_callback, void *qp_extra);
```
Source: `src/backend/optimizer/plan/planmain.c:53`.

### 7.2 Purpose
Bridge between preprocessing and the actual scan/join planning machinery.
Builds base RelOptInfos, runs `deconstruct_jointree`, finalizes equivalence
classes, then calls `make_one_rel` to produce all paths. Returns the top-level
join `RelOptInfo` (UPPERREL semantics are *not* in `query_planner` — those are
handled by `grouping_planner` after this returns).

### 7.3 Step-by-step body
The body (planmain.c:53-288) is:

1. **Init `PlannerInfo` lists** (planmain.c:67-80): clear `join_rel_list`,
   `join_rel_hash`, `canon_pathkeys`, `left/right/full_join_clauses`,
   `placeholder_list`, `placeholder_array`, `fkey_list`, `initial_rels`.
2. **Allocate per-baserel arrays**: `setup_simple_rel_arrays(root)` allocates
   `simple_rel_array` and `simple_rte_array`, and (lazily) `append_rel_array`.
3. **Trivial fast path** (planmain.c:93-159): if the jointree is a single
   `RTE_RESULT` (FROM-less SELECT, INSERT...VALUES), build the RelOptInfo
   directly, attach a single `GroupResultPath`, run `qp_callback`, and return.
   This bypasses everything below.
4. **Build base relations**: `add_base_rels_to_query(root, parse->jointree)`
   walks the jointree and creates one `RelOptInfo` per `RangeTblRef`.
5. **`build_base_rel_tlists`** (initsplan.c): for every Var/PlaceHolderVar in
   `processed_tlist`, mark `attr_needed` on its base rel.
6. **Placeholders / lateral**:
   - `find_placeholders_in_jointree` discovers PHVs needed below outer joins.
   - `find_lateral_references` propagates LATERAL var requirements.
7. **`deconstruct_jointree`** (initsplan.c:740) — see
   `component_initial_setup_and_jointree.md`. Builds `join_info_list`
   (SpecialJoinInfos), distributes RestrictInfos to base rels and
   `join_info_list` entries, classifies join-mergeable clauses into
   `left/right/full_join_clauses`, and returns the `joinlist` that DP/GEQO
   will work on.
8. **`reconsider_outer_join_clauses`** revisits outer-join clauses now that
   ECs are partially built (may merge ECs further).
9. **`generate_base_implied_equalities`** turns `var = const` ECs into
   restrictinfos at every base rel they touch.
10. **`(*qp_callback)(root, qp_extra)`** — typically `standard_qp_callback`,
    which computes `root->group_pathkeys`, `sort_pathkeys`, `distinct_pathkeys`,
    and `query_pathkeys`. Must run **after** EC merging so
    `make_canonical_pathkey` works correctly.
11. **PHV finalization**: `fix_placeholder_input_needed_levels`.
12. **Useless-join removal** and **semijoin reduction** (analyzejoins.c) —
    see `component_subquery_and_sublink.md`.
13. **`add_placeholders_to_base_rels`** — propagate PHV needs to actual rels.
14. **`create_lateral_join_info`** — finalize `lateral_relids` /
    `lateral_referencers` per rel.
15. **`match_foreign_keys_to_quals`** (selfuncs.c support).
16. **`extract_restriction_or_clauses`** (orclauses.c).
17. **`add_other_rels_to_query`** — after everything above, expand inheritance
    parents into child RelOptInfos (so child quals can be filtered with full
    restrictinfo info).
18. **`distribute_row_identity_vars`** — RETURNING / row identity for UPDATE.
19. **`make_one_rel`** — the actual path generation. See
    `component_base_relation_paths.md` and `component_join_paths_and_search.md`.
20. **Sanity check** that the final rel has at least one unparameterized
    `cheapest_total_path`, else `elog(ERROR, "failed to construct the join
    relation")`.

### 7.4 Why these orderings matter
- **EC merging before pathkey canonicalization**: pathkeys reference EC
  pointers. If you build pathkeys before EC merging is done, you'll lock in
  pre-merge ECs and `make_canonical_pathkey` will create different
  PathKey objects for what should be equivalent orders.
- **Inheritance expansion (`add_other_rels_to_query`) deferred**: until base
  rel restrictinfos are settled, so pruning quals are visible per-child.
- **Useless-join removal before lateral/PHV propagation in some cases**: the
  comments in `query_planner` (planmain.c:222-238) call out the dependency
  graph. If you reorder steps you'll subtly break corner cases.

---

## 8. API: `grouping_planner`

### 8.1 Signature
```c
static void
grouping_planner(PlannerInfo *root, double tuple_fraction,
                 SetOperationStmt *setops);
```
Source: `src/backend/optimizer/plan/planner.c:1335`.

### 8.2 Purpose
Top-level processing that sits **above** `query_planner`'s scan/join layer.
It's what builds the chain of upper-rels (UPPERREL_GROUP_AGG → WINDOW →
DISTINCT → ORDERED → FINAL). The output is a populated UPPERREL_FINAL whose
pathlist contains the candidates `standard_planner` will pick the cheapest
fractional path from.

### 8.3 Outline

1. **LIMIT estimation** via `preprocess_limit` (sets `count_est`,
   `offset_est`, `limit_tuples`). `tuple_fraction` is mixed with these to
   refine the fast-start vs full-retrieval tradeoff.
2. **Set-op pathway**: if `parse->setOperations`, build the set-op tree via
   `prepunion.c:plan_set_operations`. The result becomes the `current_rel`
   that further upper-rel processing operates on.
3. **GROUPING SETS preprocessing** via `preprocess_grouping_sets`.
4. **MIN/MAX optimization** via `preprocess_minmax_aggregates` —
   `planagg.c:build_minmax_path` may build a per-aggregate
   "scan + LIMIT 1 with ORDER BY" path that becomes a `MinMaxAggPath`.
5. **`preprocess_targetlist`** computes `processed_tlist`, the targetlist
   the optimizer actually plans for (after junk-column expansion,
   resjunk vars for sort/group, etc.).
6. **`preprocess_aggrefs`** (prepagg.c) consolidates equivalent Aggrefs and
   chooses split modes.
7. **`query_planner(root, qp_callback, &qp_extra)`** — the scan/join layer.
   `qp_callback` here is `standard_qp_callback` which initializes
   `root->group_pathkeys` etc.
8. **UPPERREL chain** built one stage at a time:
   - `create_grouping_paths` — `UPPERREL_GROUP_AGG` (HashAgg, GroupAgg,
     groupingsets, partial agg + finalize chain when parallel).
   - `create_window_paths` — `UPPERREL_WINDOW` (one or more `WindowAggPath`).
   - `create_distinct_paths` — `UPPERREL_DISTINCT` (HashAgg-distinct or
     UpperUnique+Sort).
   - `create_ordered_paths` — `UPPERREL_ORDERED` for top-level ORDER BY when
     not already satisfied.
   - **UPPERREL_FINAL**: tacks on `LockRowsPath`, `LimitPath`, and
     `ModifyTablePath` for non-SELECT statements. Inserted in the
     order Limit on top, ModifyTable below LimitOffset/Count etc.
9. **`fetch_upper_rel(root, UPPERREL_FINAL, NULL)`** is implicitly populated
   via `add_path` calls inside `create_*_paths` and the modify-table
   handling.
10. **Returns `void`**. Output is in
    `root->upper_rels[UPPERREL_FINAL][NULL]->pathlist`.

### 8.4 Where the outputs live
- `root->upper_targets[UPPERREL_*]` — `PathTarget` per stage, derived from
  the next stage's needs.
- `root->upper_rels[UPPERREL_*]` — the rels themselves.
- `root->processed_tlist` — set by `preprocess_targetlist`, consumed by
  every stage.

---

## 9. The `PlannerInfo` "root" struct

Source: `src/include/nodes/pathnodes.h:195`.

The conventional name everywhere in the optimizer is `root`. It is per-Query.
For a query with sub-Queries that we plan separately, each has its own
`PlannerInfo`, all linked via `parent_root` and sharing one `PlannerGlobal`.

Notable fields (full listing is in `component_data_structures.md` if produced;
here we cite only what lifecycle code touches):

```c
struct PlannerInfo {
    NodeTag         type;
    Query          *parse;                /* the Query being planned */
    PlannerGlobal  *glob;                 /* global state, shared */
    Index           query_level;          /* 1-based */
    PlannerInfo    *parent_root;          /* outer query's root, or NULL */

    RelOptInfo    **simple_rel_array;     /* indexed by RT index */
    int             simple_rel_array_size;
    RangeTblEntry **simple_rte_array;
    AppendRelInfo **append_rel_array;

    Relids          all_baserels;
    Relids          outer_join_rels;
    Relids          all_query_rels;

    List           *join_rel_list;        /* every joinrel ever built */
    HTAB           *join_rel_hash;        /* lookup speedup */
    List          **join_rel_level;       /* DP arrays, or NULL */
    int             join_cur_level;

    List           *init_plans;
    List           *cte_plan_ids;
    List           *multiexpr_params;
    List           *join_domains;
    List           *eq_classes;
    bool            ec_merging_done;
    int             last_rinfo_serial;
    Relids          all_result_relids;
    Relids          leaf_result_relids;
    List           *append_rel_list;
    List           *row_identity_vars;
    List           *rowMarks;

    RelOptInfo     *upper_rels[NUM_UPPERRELS];      /* per upper-rel stage */
    PathTarget     *upper_targets[NUM_UPPERRELS];

    List           *processed_tlist;
    List           *processed_groupClause;
    List           *processed_distinctClause;
    List           *update_colnos;
    AttrNumber     *grouping_map;
    List           *minmax_aggs;
    Index           qual_security_level;
    bool            hasJoinRTEs;
    bool            hasLateralRTEs;
    bool            hasHavingQual;
    bool            hasPseudoConstantQuals;
    bool            hasAlternativeSubPlans;
    bool            hasRecursion;
    bool            placeholdersFrozen;
    int             wt_param_id;
    Path           *non_recursive_path;

    /* … many more (canon_pathkeys, query_pathkeys, etc.) … */
};
```

Important lifecycle invariants:
- `simple_rel_array[i]` is **NULL** for non-baserel RTEs (joins, views).
  Always check before dereferencing.
- `join_rel_level` is non-NULL only inside `standard_join_search`. GEQO uses
  `join_rel_list` directly.
- `ec_merging_done` flips to `true` exactly once, in `query_planner` before
  `qp_callback`. After that, EC merging is forbidden.
- `placeholdersFrozen` is set after `fix_placeholder_input_needed_levels`.
  Once set, no new PlaceHolderInfos may be created.

---

## 10. Performance characteristics of the lifecycle

| Phase | Complexity | Notes |
|-------|------------|-------|
| Preprocessing (subquery pull-up, sublink pull-up, qual canon.) | O(query tree size) | Dominated by tree walks; `flatten_join_alias_vars` is repeated per-clause but cheap. |
| `deconstruct_jointree` | O(N²) in the number of join-tree items in the worst case | Each qual is distributed to the smallest covering rel-set. |
| `make_one_rel` base-rel pathing | O(R) per relation, R = paths considered (idx + bitmap + seq + ...) | Each base rel is independent. |
| DP join search | **O(3^n − 2^(n+1) + 1)** joinrels | See `optimizer/README`. With `geqo_threshold = 12` the DP cap is 12 rels. |
| GEQO | O(`pool_size × num_generations × gimme_tree_cost`) | Each `geqo_eval` reuses a scratch memory context. |
| Upper-rel stages | O(paths-per-stage) | Each stage does a small amount of work per surviving path. |
| `create_plan` + `set_plan_references` | O(plan tree size) | Linear. |

The dominating term for OLTP is preprocessing + base-rel pathing. For
warehouse-style multi-way joins it's DP search.

---

## 11. Cross-references

- Preprocessing: `component_preprocessing.md`
- Initial setup / jointree: `component_initial_setup_and_jointree.md`
- Base-rel paths: `component_base_relation_paths.md`
- Index paths: `component_index_paths.md`
- Joins: `component_join_paths_and_search.md`
- Costing: `component_cost_model_and_selectivity.md`
- Equivalence / pathkeys: `component_equivalence_classes_and_pathkeys.md`
- Plan creation / setrefs: `component_plan_creation_and_setrefs.md`
- GEQO: `component_geqo.md`
- Hooks: `component_hooks_and_extensibility.md`
- Diagrams: `diagrams/01_planner_pipeline.mermaid`,
  `diagrams/02_object_model.mermaid`.
