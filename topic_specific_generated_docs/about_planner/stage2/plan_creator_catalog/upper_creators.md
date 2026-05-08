# Plan Creator Catalog: Upper-Relation Plan Creators

This catalog covers the per-pathtype `create_*_plan` functions for upper-layer paths: sorting, grouping, aggregation, projection, materialization, parallel coordination, set operations, recursion, and limit. All are `static` helpers in `src/backend/optimizer/plan/createplan.c`.

A few patterns are shared across these creators:
- `subplan = create_plan_recurse(root, best_path->subpath, flags)` for the input.
- `tlist = build_path_tlist(root, &best_path->path)` for the output.
- `make_*` helper to build the plan node.
- `copy_generic_path_info(plan, &best_path->path)` to copy cost/row/parallel-safe estimates from path to plan.

Where `flags` matter: `CP_EXACT_TLIST` forces children to produce exactly the requested tlist (used by Append, MergeAppend, ModifyTable, and Gather to prevent surprises). `CP_SMALL_TLIST` requests the minimum tlist (used when storing tuples in a tuplestore or sort spool). `CP_LABEL_TLIST` requires the child to label its tlist with sortgroupref attribution (used by Group/Agg/Unique for grouping-column lookup). `CP_IGNORE_TLIST` says the parent will discard whatever the child returns.

---

## create_sort_plan

**Signature**: `static Sort *create_sort_plan(PlannerInfo *root, SortPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2181`.

**Dispatching context**: Routed for `T_Sort`.

**Output Plan struct**: `Sort` (`plannodes.h:931`).

**Tlist handling**: Subplan gets `flags | CP_SMALL_TLIST` — Sort doesn't project, but we don't want to sort more columns than necessary.

**Qual handling**: None — Sort has no qual.

**Var-reference handling**: None at this level (subplan handles its own).

**Node-specific quirks**: Calls `make_sort_from_pathkeys(subplan, pathkeys, relids)`. The relids argument is non-NULL only for "other" rels (partitioned-table children); this is passed to `find_ec_member_matching_expr` so it ignores ECs from sibling partitions.

**Source file references**: `createplan.c:2181-2207`.

---

## create_incrementalsort_plan

**Signature**: `static IncrementalSort *create_incrementalsort_plan(PlannerInfo *root, IncrementalSortPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2215`.

**Dispatching context**: Routed for `T_IncrementalSort`.

**Output Plan struct**: `IncrementalSort` (`plannodes.h:955`).

**Tlist handling**: Same as create_sort_plan — `flags | CP_SMALL_TLIST`.

**Node-specific quirks**: Calls `make_incrementalsort_from_pathkeys()` with the `nPresortedCols` count from the path. Otherwise structurally identical to create_sort_plan.

**Source file references**: `createplan.c:2215-2233`.

---

## create_agg_plan

**Signature**: `static Agg *create_agg_plan(PlannerInfo *root, AggPath *best_path)` at `src/backend/optimizer/plan/createplan.c:2309`.

**Dispatching context**: Routed for `T_Agg` when `IsA(best_path, AggPath)` (not GroupingSetsPath).

**Output Plan struct**: `Agg` (`plannodes.h:996`).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST` (Agg can project, but grouping columns must be labeled). `tlist = build_path_tlist(root, &best_path->path)`.

**Qual handling**: `quals = order_qual_clauses(root, best_path->qual)` — these are HAVING quals.

**Var-reference handling**: None at this level. setrefs.c later replaces non-Var grouping-key references via `extract_grouping_cols` and friends.

**Node-specific quirks**:
- `extract_grouping_cols(groupClause, subplan->targetlist)` returns the AttrNumber array of grouping columns in the subplan's tlist.
- `extract_grouping_ops(groupClause)` returns the equality operators.
- `extract_grouping_collations(groupClause, subplan->targetlist)` returns the collations.
- Calls `make_agg(tlist, quals, aggstrategy, aggsplit, numCols, grpColIdx, grpOperators, grpCollations, NIL_groupingSets, NIL_chain, numGroups, transitionSpace, subplan)`.

**Source file references**: `createplan.c:2309-2344`.

---

## create_groupingsets_plan

**Signature**: `static Plan *create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)` at `src/backend/optimizer/plan/createplan.c:2393`.

**Dispatching context**: Routed for `T_Agg` when `IsA(best_path, GroupingSetsPath)`.

**Output Plan struct**: `Agg` chain — the topmost Agg's `chain` field contains subsidiary Agg nodes (one per non-first rollup).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST`.

**Qual handling**: HAVING goes on the topmost Agg only.

**Var-reference handling**:
- Builds `grouping_map[tleSortGroupRef] = column_index_in_subplan_tlist`. Saves to `root->grouping_map` so setrefs.c can fix up GroupingFunc nodes' `cols` lists across the chain.

**Node-specific quirks**:
- For each non-first rollup, builds an auxiliary `Agg` with `make_agg(NIL, NIL, strat, AGGSPLIT_SIMPLE, ...)`, optionally preceded by a `Sort` (`make_sort_from_groupcols`) for AGG_SORTED rollups beyond the first.
- The auxiliary Aggs have empty tlists/quals — they're vestigial nodes that just carry rollup metadata for the executor.
- Strategies per rollup: `AGG_HASHED` if rollup->is_hashed; `AGG_PLAIN` if its first gset is empty; otherwise `AGG_SORTED`.
- The topmost Agg uses the path's `aggstrategy` (which is the overall strategy: AGG_SORTED, AGG_HASHED, AGG_MIXED, or AGG_PLAIN).

**Source file references**: `createplan.c:2393-2542`.

---

## create_minmaxagg_plan

**Signature**: `static Result *create_minmaxagg_plan(PlannerInfo *root, MinMaxAggPath *best_path)` at `src/backend/optimizer/plan/createplan.c:2551`.

**Dispatching context**: Routed for `T_Result` when `IsA(best_path, MinMaxAggPath)`.

**Output Plan struct**: `Result` (`plannodes.h:196`) — the InitPlans do all the work.

**Tlist handling**: `tlist = build_path_tlist(root, &best_path->path)`.

**Qual handling**: `best_path->quals` (HAVING) becomes the `resconstantqual` of the Result.

**Var-reference handling**: Critical: each MinMaxAggInfo's path is converted into a Plan via `create_plan(subroot, mminfo->path)` (note: `create_plan`, not `create_plan_recurse`, because the subroot is a separate planning context). A `Limit 1` is wrapped on top of each. Then `SS_make_initplan_from_plan(root, subroot, plan, mminfo->param)` registers it as an InitPlan in the outer query — this assigns the result of the InitPlan to a Param that the outer plan can reference.

**Node-specific quirks**:
- Sets `root->minmax_aggs = best_path->mmaggregates` so setrefs.c knows to replace `Aggref` references in the surrounding plan with Param references to the InitPlans (the Aggref→Param replacement is the trickiest bit because Aggref's may also appear in higher plan nodes).

**Source file references**: `createplan.c:2551-2608`.

---

## create_windowagg_plan

**Signature**: `static WindowAgg *create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)` at `src/backend/optimizer/plan/createplan.c:2617`.

**Dispatching context**: Routed for `T_WindowAgg`.

**Output Plan struct**: `WindowAgg` (`plannodes.h:1038`).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST | CP_SMALL_TLIST` — WindowAgg buffers frame contents in a tuplestore so a small tlist matters; partition/order columns must be labeled.

**Var-reference handling**: Looks up partition/order column resnos via `get_sortgroupclause_tle(sgc, subplan->targetlist)`.

**Node-specific quirks**:
- Builds `partColIdx`, `partOperators`, `partCollations` arrays from `wc->partitionClause`.
- Builds `ordColIdx`, `ordOperators`, `ordCollations` arrays from `wc->orderClause`.
- Threads frame options, start/end offsets, in-range functions, and `runCondition` (for short-circuit evaluation) through `make_windowagg`.
- `topwindow` flag tells the executor that this is the topmost WindowAgg; only the top one runs HAVING-like quals.

**Source file references**: `createplan.c:2617-2711`.

---

## create_group_plan

**Signature**: `static Group *create_group_plan(PlannerInfo *root, GroupPath *best_path)` at `src/backend/optimizer/plan/createplan.c:2242`.

**Dispatching context**: Routed for `T_Group`.

**Output Plan struct**: `Group` (`plannodes.h:967`).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST` (Group can project but grouping cols must be labeled).

**Qual handling**: `quals = order_qual_clauses(root, best_path->qual)` — HAVING.

**Node-specific quirks**: `extract_grouping_cols/ops` from groupClause; `make_group(tlist, quals, numCols, grpColIdx, grpOperators, grpCollations, subplan)`.

**Source file references**: `createplan.c:2242-2272`.

---

## create_unique_plan

**Signature**: `static Plan *create_unique_plan(PlannerInfo *root, UniquePath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:1721`.

**Dispatching context**: Routed for `T_Unique` when `IsA(best_path, UniquePath)` (not UpperUniquePath).

**Output Plan struct**: Returns `Plan *` because actual return value depends on `umethod`:
- `UNIQUE_PATH_NOOP` — returns `subplan` unchanged.
- `UNIQUE_PATH_HASH` — returns an `Agg` with `AGG_HASHED` strategy.
- `UNIQUE_PATH_SORT` — returns a `Unique` node atop a synthesized `Sort`.

**Tlist handling**: Builds a new tlist starting from `build_path_tlist(root, &best_path->path)` and adding any `uniq_exprs` not already present. For SORT mode, replaces subplan's tlist via `change_plan_targetlist`. For HASH mode, leaves subplan tlist alone unless additions were needed.

**Var-reference handling**: None at this level — subplan handled its own; uniq_exprs come from the path already-resolved.

**Node-specific quirks**:
- For HASH: looks up hash equality operators via `get_compatible_hash_operators(in_oper, NULL, &eq_oper)` — the IN clause operator might be cross-type, in which case the equality op is for the RHS datatype.
- For SORT: synthesizes an explicit ORDER BY list (`SortGroupClause` instances) via `get_ordering_op_for_equality_op` and `get_equality_op_for_ordering_op` lookups; `make_sort_from_sortclauses` builds the Sort, then `make_unique_from_sortclauses` wraps it.

**Source file references**: `createplan.c:1721-1911`.

---

## create_upper_unique_plan

**Signature**: `static Unique *create_upper_unique_plan(PlannerInfo *root, UpperUniquePath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2281`.

**Dispatching context**: Routed for `T_Unique` when `IsA(best_path, UpperUniquePath)`.

**Output Plan struct**: `Unique` (`plannodes.h:1112`).

**Tlist handling**: Subplan gets `flags | CP_LABEL_TLIST`. Unique doesn't project.

**Node-specific quirks**: `make_unique_from_pathkeys(subplan, pathkeys, numkeys)` — derives equality operators from the pathkeys directly.

**Source file references**: `createplan.c:2281-2300`.

---

## create_setop_plan

**Signature**: `static SetOp *create_setop_plan(PlannerInfo *root, SetOpPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2720`.

**Dispatching context**: Routed for `T_SetOp`.

**Output Plan struct**: `SetOp` (`plannodes.h:1217`).

**Tlist handling**: Subplan gets `flags | CP_LABEL_TLIST`. SetOp doesn't project.

**Node-specific quirks**: `make_setop(cmd, strategy, subplan, distinctList, flagColIdx, firstFlag, numGroups)`. `numGroups` is converted from `Cardinality` (double) to `long` via `clamp_cardinality_to_long`.

**Source file references**: `createplan.c:2720-2747`.

---

## create_recursiveunion_plan

**Signature**: `static RecursiveUnion *create_recursiveunion_plan(PlannerInfo *root, RecursiveUnionPath *best_path)` at `src/backend/optimizer/plan/createplan.c:2756`.

**Dispatching context**: Routed for `T_RecursiveUnion`.

**Output Plan struct**: `RecursiveUnion` (`plannodes.h:325`).

**Tlist handling**: Both `leftpath` and `rightpath` get `CP_EXACT_TLIST` — they must produce identical tlists for the union to make sense.

**Node-specific quirks**: `make_recursive_union(tlist, leftplan, rightplan, wtParam, distinctList, numGroups)`. The `wtParam` is the work-table communication Param assigned earlier in plan setup.

**Source file references**: `createplan.c:2756-2783`.

---

## create_append_plan

**Signature**: `static Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:1217`.

**Dispatching context**: Routed for `T_Append`.

**Output Plan struct**: `Append` (`plannodes.h:265`). May also return a `Result` with constant-FALSE qual if `subpaths == NIL` (dummy empty rel), or wrap the Append in a Result via `inject_projection_plan` if added sort columns must be hidden.

**Tlist handling**: All children get `CP_EXACT_TLIST` — Append demands matching tlists across all children.

**Var-reference handling**: For partition pruning, `prunequal = extract_actual_clauses(rel->baserestrictinfo, false)` plus `param_info->ppi_clauses` (with `replace_nestloop_params` applied) — these are passed to `make_partition_pruneinfo` to build the executor's pruning steps.

**Node-specific quirks**:
- For ordered Appends: calls `prepare_sort_from_pathkeys` on the Append node itself to set up sort column info (which may add resjunk sort columns to the Append's tlist), then on each subplan to verify they produce the same sort keys.
- Inserts a `Sort` node atop any subplan whose pathkeys don't match the desired order.
- **Async append**: When `enable_async_append && pathkeys == NIL && !parallel_safe && nsubpaths > 1`, calls `mark_async_capable_plan(subplan, subpath)` for each child; foreign-table children that opt in get `async_capable = true`. Final count is `nasyncplans`.
- **Sort column cleanup**: If sort columns were added to the Append's tlist but the caller asked for `CP_EXACT_TLIST` or `CP_SMALL_TLIST`, wraps the Append in a Result (via `inject_projection_plan`) that projects only the original tlist columns.

**Source file references**: `createplan.c:1217-1428`.

---

## create_merge_append_plan

**Signature**: `static Plan *create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:1438`.

**Dispatching context**: Routed for `T_MergeAppend`.

**Output Plan struct**: `MergeAppend` (`plannodes.h:287`). May be wrapped in a Result via `inject_projection_plan` if extra sort columns conflict with `CP_EXACT_TLIST`/`CP_SMALL_TLIST`.

**Tlist handling**: Children get `CP_EXACT_TLIST`. Sort column setup mirrors create_append_plan's logic.

**Var-reference handling**: Asserts `param_info == NULL` (currently no parameterized MergeAppend paths are generated).

**Node-specific quirks**:
- Always inserts a Sort atop any unordered child (vs. Append which only does this when the Append itself is ordered).
- Stores per-Append sort column metadata in the MergeAppend's `numCols`, `sortColIdx`, `sortOperators`, `collations`, `nullsFirst` fields (these arrays are read by the executor's binary heap).
- Partition pruning: same as Append.

**Source file references**: `createplan.c:1438-1578`.

---

## create_material_plan

**Signature**: `static Material *create_material_plan(PlannerInfo *root, MaterialPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:1639`.

**Dispatching context**: Routed for `T_Material`.

**Output Plan struct**: `Material` (`plannodes.h:880`).

**Tlist handling**: Subplan gets `flags | CP_SMALL_TLIST` — minimize buffer width.

**Node-specific quirks**: Trivial — `make_material(subplan)` and copy cost info.

**Source file references**: `createplan.c:1639-1657`.

---

## create_memoize_plan

**Signature**: `static Memoize *create_memoize_plan(PlannerInfo *root, MemoizePath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:1667`.

**Dispatching context**: Routed for `T_Memoize`.

**Output Plan struct**: `Memoize` (`plannodes.h:889`).

**Tlist handling**: Subplan gets `flags | CP_SMALL_TLIST`.

**Var-reference handling**: `param_exprs = (List *) replace_nestloop_params(root, (Node *) best_path->param_exprs)` — converts outer-rel Vars in the cache key expressions to nestloop Params.

**Node-specific quirks**:
- Builds the `operators` and `collations` arrays from each (param_expr, hash_op) pair.
- `keyparamids = pull_paramids((Expr *) param_exprs)` — collects all Param IDs that the cache key depends on, used by the executor to invalidate the cache when these Params change.
- Calls `make_memoize(subplan, operators, collations, param_exprs, singlerow, binary_mode, est_entries, keyparamids)`.

**Source file references**: `createplan.c:1667-1711`.

---

## create_gather_plan

**Signature**: `static Gather *create_gather_plan(PlannerInfo *root, GatherPath *best_path)` at `src/backend/optimizer/plan/createplan.c:1920`.

**Dispatching context**: Routed for `T_Gather`.

**Output Plan struct**: `Gather` (`plannodes.h:1140`).

**Tlist handling**: Subplan gets `CP_EXACT_TLIST` — the leader-worker tuple queue uses MinimalTuple representation, so the worker's tlist must exactly match what the Gather emits (no system columns through queues).

**Node-specific quirks**:
- Calls `assign_special_exec_param(root)` to allocate a unique Param ID for the Gather node — used at runtime for inter-process state coordination.
- Sets `root->glob->parallelModeNeeded = true` so the executor enters parallel mode.
- `make_gather(tlist, NIL, num_workers, rescan_param, single_copy, subplan)`.

**Source file references**: `createplan.c:1920-1949`.

---

## create_gather_merge_plan

**Signature**: `static GatherMerge *create_gather_merge_plan(PlannerInfo *root, GatherMergePath *best_path)` at `src/backend/optimizer/plan/createplan.c:1958`.

**Dispatching context**: Routed for `T_GatherMerge`.

**Output Plan struct**: `GatherMerge` (`plannodes.h:1155`).

**Tlist handling**: Subplan gets `CP_EXACT_TLIST`.

**Node-specific quirks**:
- Manually allocates and populates the GatherMerge node (no `make_gather_merge`); calls `prepare_sort_from_pathkeys` to set up `numCols`, `sortColIdx`, `sortOperators`, `collations`, `nullsFirst` arrays directly on the plan node.
- Asserts `pathkeys != NIL` at construction (else why use Merge instead of plain Gather?).
- Asserts at plan time that `pathkeys_contained_in(pathkeys, best_path->subpath->pathkeys)` — if not, `elog(ERROR, "gather merge input not sufficiently sorted")`. This is because we can't safely insert a Sort here; the partial path must already produce sufficiently sorted output.
- Sets `parallelModeNeeded = true`.

**Source file references**: `createplan.c:1958-2009`.

---

## create_limit_plan

**Signature**: `static Limit *create_limit_plan(PlannerInfo *root, LimitPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2856`.

**Dispatching context**: Routed for `T_Limit`.

**Output Plan struct**: `Limit` (`plannodes.h:1270`).

**Tlist handling**: Subplan gets `flags` unchanged — Limit doesn't project.

**Node-specific quirks**: For `LIMIT_OPTION_WITH_TIES`, walks `parse->sortClause` to extract uniqkey columns/operators/collations (used by Limit at runtime to detect ties and include all tied rows beyond the count).

**Source file references**: `createplan.c:2856-2901`.

---

## create_projection_plan

**Signature**: `static Plan *create_projection_plan(PlannerInfo *root, ProjectionPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2019`.

**Dispatching context**: Routed for `T_Result` when `IsA(best_path, ProjectionPath)`.

**Output Plan struct**: Either:
- `Result` (`plannodes.h:196`) — if a separate projection node is needed.
- The subplan unchanged (with its tlist replaced) — when the subplan is projection-capable or its existing tlist already matches.

**Tlist handling**: Three cases (lines 2039-2073):
1. `use_physical_tlist(root, &path, flags)` returns true — caller doesn't need an exact tlist; subplan gets `flags = 0`, and we use the subplan's existing tlist (with optional sortgroupref labeling).
2. Subplan is projection-capable — recurse with `CP_IGNORE_TLIST` (subplan can return whatever; we'll override its tlist), then `tlist = build_path_tlist(root, &path)`.
3. Need a Result node — recurse with `flags = 0`, build the requested tlist, set `needs_result_node = !tlist_same_exprs(tlist, subplan->targetlist)`.

**Node-specific quirks**: When no Result is needed, just replaces `subplan->targetlist` and labels with the path's costs. When a Result is needed, calls `make_result(tlist, NULL, subplan)`.

**Source file references**: `createplan.c:2019-2107`.

---

## create_project_set_plan

**Signature**: `static ProjectSet *create_project_set_plan(PlannerInfo *root, ProjectSetPath *best_path)` at `src/backend/optimizer/plan/createplan.c:1613`.

**Dispatching context**: Routed for `T_ProjectSet`.

**Output Plan struct**: `ProjectSet` (`plannodes.h:208`).

**Tlist handling**: Subplan gets `flags = 0` (no constraint on subplan tlist; ProjectSet will project).

**Node-specific quirks**: Trivial — `tlist = build_path_tlist(root, &best_path->path)`, then `make_project_set(tlist, subplan)`.

**Source file references**: `createplan.c:1613-1629`.

---

## create_group_result_plan

**Signature**: `static Result *create_group_result_plan(PlannerInfo *root, GroupResultPath *best_path)` at `src/backend/optimizer/plan/createplan.c:1588`.

**Dispatching context**: Routed for `T_Result` when `IsA(best_path, GroupResultPath)`.

**Output Plan struct**: `Result` (`plannodes.h:196`).

**Tlist handling**: `tlist = build_path_tlist(root, &best_path->path)`.

**Qual handling**: `quals = order_qual_clauses(root, best_path->quals)` — these are HAVING quals stored as bare clauses (not RestrictInfos) on the path.

**Node-specific quirks**: Trivial — `make_result(tlist, (Node *) quals, NULL)`. The `subplan` argument is NULL because GroupResultPath represents a degenerate one-row result with no input.

**Source file references**: `createplan.c:1588-1604`.
