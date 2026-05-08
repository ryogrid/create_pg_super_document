# 19. Plan Creator Catalog

Prerequisites: [16 Plan creation and setrefs](16_plan_creation_and_setrefs.md), [18 Path catalog](18_path_catalog.md).

This is the consolidated catalog of every static `create_*_plan` helper in `src/backend/optimizer/plan/createplan.c`. They are all invoked from `create_plan_recurse` based on `Path.pathtype` (and sometimes `IsA(best_path, ...)` for paths that share a `pathtype` discriminator — like `T_Result` shared by ProjectionPath, MinMaxAggPath, GroupResultPath, and plain RTE_RESULT).

The standardized template per entry:

- **Signature** with file:line.
- **Dispatching context** (which `pathtype` and any `IsA` discriminator).
- **Output Plan struct** (subtype + `plannodes.h` line).
- **Tlist handling** (how the subplan's tlist is requested — `CP_*` flags).
- **Qual handling** (how RestrictInfo lists are converted to bare quals).
- **Var-reference handling** (`replace_nestloop_params`, `fix_*` helpers).
- **Node-specific quirks** (the interesting custom code).
- **Source file references** (line ranges).

Each entry cross-links back to the corresponding Path entry in [Module 18](18_path_catalog.md).

Catalog organization:
- [Scan creators](#scan-creators)
- [Join creators](#join-creators)
- [Upper creators](#upper-creators)
- [Modify creators](#modify-creators)

---

## Scan creators

These produce leaf-level Scan plan nodes. They share a common dispatch — `create_scan_plan()` (line 560) — which pre-processes the scan_clauses (extracting them into bare expressions, resolving them through `replace_nestloop_params` for parameterized scans) and then routes by `pathtype`.

Some scan types reuse the same generic `Path` struct (no specialized struct exists); those creators take a `Path *best_path` parameter rather than a path-specific subtype.

### create_scan_plan

**Signature**: `static Plan *create_scan_plan(PlannerInfo *root, Path *best_path, int flags)` at `createplan.c:560`.

**Dispatching context**: Called from `create_plan_recurse` (line 414) for any scan-type pathtype. Itself a sub-dispatcher: builds the right tlist (physical or logical), sorts and prepares scan_clauses, then switches on `best_path->pathtype` (line 678) to pick the specific `create_*scan_plan`.

**Output Plan struct**: Polymorphic — whichever Plan subtype the per-pathtype creator returns. May wrap that plan in a gating Result via `create_gating_plan` if there are pseudoconstant quals.

**Tlist handling**: Selects between `build_physical_tlist(rel)` (for table scans, allows the executor to skip projection), `copyObject(indexinfo->indextlist)` (for IndexOnlyScan), or `build_path_tlist(root, best_path)` (logical tlist). Honors `CP_IGNORE_TLIST` flag by returning `tlist = NULL`, and `CP_LABEL_TLIST` to copy sortgroupref labeling.

**Qual handling**: Pulls scan_clauses from `rel->baserestrictinfo` (or `indexinfo->indrestrictinfo` for IndexScan/IndexOnlyScan). When the path is parameterized, appends `param_info->ppi_clauses`.

**Var-reference handling**: None at this layer; per-pathtype creator handles `replace_nestloop_params`. `setrefs.c` later converts varno=baserel-rti references to scanrelid + actual attno once attachments are finalized.

**Node-specific quirks**:
- For T_ForeignScan and T_CustomScan handling joins, fetches `fdw_restrictinfo`/`custom_restrictinfo` instead of baserel restrict info.
- Calls `get_gating_quals(root, scan_clauses)` and wraps result in `create_gating_plan` when pseudoconstant clauses are present.
- Routes T_IndexScan vs. T_IndexOnlyScan to the same `create_indexscan_plan` with an `indexonly` boolean.

**Source file references**: `createplan.c:556-799`.

### create_gating_plan

**Signature**: `static Plan *create_gating_plan(PlannerInfo *root, Path *path, Plan *plan, List *gating_quals)` at `createplan.c:1023`.

**Dispatching context**: Called from `create_scan_plan` (line ~795) and `create_join_plan` (line ~1135) when `get_gating_quals` returns non-empty pseudoconstant clauses.

**Output Plan struct**: `Result` (`plannodes.h:196`) wrapping the input plan, with `resconstantqual` set to the AND of the gating quals.

**Tlist handling**: Result inherits the wrapped plan's tlist.

**Qual handling**: The gating_quals become `resconstantqual` — evaluated exactly once at plan startup; if they evaluate to false, no rows are produced.

**Node-specific quirks**: Adjusts `cost_qual_eval` so the cost is reflected in the Result's startup cost; `parallel_safe` of the resulting Result is the AND of input plan and the qual's parallel-safety.

**Source file references**: `createplan.c:1023-1070`.

### create_seqscan_plan

**Signature**: `static SeqScan *create_seqscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `createplan.c:2917`.

**Dispatching context**: `pathtype == T_SeqScan`. Path counterpart: [Path (T_SeqScan)](18_path_catalog.md#path-t_seqscan).

**Output Plan struct**: `SeqScan` (`plannodes.h:396`).

**Tlist handling**: Whatever `create_scan_plan` chose — typically physical tlist for table scans.

**Qual handling**: `scan_clauses = order_qual_clauses(root, scan_clauses)` to put cheap quals first; then `extract_actual_clauses(scan_clauses, false)` to strip RestrictInfo wrappers.

**Var-reference handling**: When `best_path->param_info` is set, runs `replace_nestloop_params` on the scan_clauses to convert outer-rel Vars to nestloop Param references.

**Node-specific quirks**: Calls `make_seqscan(tlist, scan_clauses, scan_relid)`, then `copy_generic_path_info()` to transfer cost/row estimates.

**Source file references**: `createplan.c:2917-2947`.

### create_samplescan_plan

**Signature**: `static SampleScan *create_samplescan_plan(...)` at `createplan.c:2955`.

**Dispatching context**: `T_SampleScan`. Path counterpart: [Path (T_SampleScan)](18_path_catalog.md#path-t_samplescan).

**Output Plan struct**: `SampleScan` (`plannodes.h:405`).

**Tlist handling**: From caller.

**Qual handling**: Same pattern as seqscan: order, extract, replace nestloop params.

**Var-reference handling**: Replaces nestloop params in both scan_clauses and the `tsc` (tablesample clause) — the sampling expressions can themselves reference outer Vars.

**Node-specific quirks**: Looks up the RangeTblEntry's `tablesample` field (asserts it is non-NULL).

**Source file references**: `createplan.c:2955-2993`.

### create_indexscan_plan

**Signature**: `static Scan *create_indexscan_plan(PlannerInfo *root, IndexPath *best_path, List *tlist, List *scan_clauses, bool indexonly)` at `createplan.c:3006`.

**Dispatching context**: Both `T_IndexScan` and `T_IndexOnlyScan`. Path counterpart: [IndexPath](18_path_catalog.md#indexpath-t_indexpath). Also reused by `create_bitmap_subplan` for bitmap subtrees, which always passes `indexonly = false`.

**Output Plan struct**: Returns `Scan *` because it can yield either:
- `IndexScan` (`plannodes.h:449`) with `indexqual`, `indexqualorig`, `indexorderby`, `indexorderbyorig`, `indexorderbyops`, `indexorderdir`.
- `IndexOnlyScan` (`plannodes.h:492`) with `indexqual`, `recheckqual`, `indexorderby`, `indextlist`, `indexorderdir`.

**Tlist handling**: For IndexOnlyScan, marks index tlist entries as `resjunk` when `indexinfo->canreturn[i] = false`.

**Qual handling**: Three lists:
1. `stripped_indexquals` — index quals with RestrictInfos stripped.
2. `fixed_indexquals` — index quals with index Vars (varno = INDEX_VAR) substituted for table Vars; this is what the executor uses.
3. `qpqual` — the residual scan_clauses minus those redundantly enforced by indexclauses (via `is_redundant_with_indexclauses`) and minus those provably implied by the indexquals (via `predicate_implied_by`). This becomes the executor's recheck filter.

**Var-reference handling**: `fix_indexqual_references` does the index-Var substitution and also calls `replace_nestloop_params`. Then explicit `replace_nestloop_params` on `stripped_indexquals`, `qpqual`, `indexorderbys` for parameterized scans.

**Node-specific quirks**:
- Computes `indexorderbyops` (sort operators for amcanorderbyop ORDER BY operators) by looking up opfamily members for each pathkey/expression pair.
- Two terminal `make_indexscan` / `make_indexonlyscan` calls produce the right plan flavor.

**Source file references**: `createplan.c:3006-3199`.

### create_bitmap_scan_plan

**Signature**: `static BitmapHeapScan *create_bitmap_scan_plan(...)` at `createplan.c:3202`.

**Dispatching context**: `T_BitmapHeapScan`. Path counterpart: [BitmapHeapPath](18_path_catalog.md#bitmapheappath-t_bitmapheappath).

**Output Plan struct**: `BitmapHeapScan` (`plannodes.h:538`) with `bitmapqualorig` (original quals for recheck).

**Tlist handling**: From caller.

**Qual handling**: Calls `create_bitmap_subplan` to recursively build the bitmapqual tree (yielding a Plan tree of BitmapAnd/BitmapOr/BitmapIndexScan and the original qual list). Then computes qpqual = scan_clauses minus indexquals (using equality, EC matching, and `predicate_implied_by`). Finally drops from `bitmapqualorig` any clauses that ended up in qpqual (avoiding double-evaluation).

**Var-reference handling**: `replace_nestloop_params` on qpqual and bitmapqualorig.

**Node-specific quirks**:
- `bitmap_subplan_mark_shared(bitmapqualplan)` is called when `parallel_aware`, marking the bitmap as shared so workers can probe it together.
- Predicate proof is more aggressive than for plain IndexScan because bitmap rechecks are unavoidable when scans become lossy.

**Source file references**: `createplan.c:3202-3309`.

### create_bitmap_subplan (helper, recursive)

**Signature**: `static Plan *create_bitmap_subplan(...)` at `createplan.c:3332`.

**Dispatching context**: Called by `create_bitmap_scan_plan` (line 3221) and recursively by itself for BitmapAnd/BitmapOr children. Dispatches via `IsA(bitmapqual, BitmapAndPath/BitmapOrPath/IndexPath)`.

**Output Plan struct**: One of:
- `BitmapAnd` (`plannodes.h:356`) — for `IsA(BitmapAndPath)`. Path counterpart: [BitmapAndPath](18_path_catalog.md#bitmapandpath-t_bitmapandpath).
- `BitmapOr` (`plannodes.h:370`) — for `IsA(BitmapOrPath)`. Single-element BitmapOrPath collapses to its lone child. Path counterpart: [BitmapOrPath](18_path_catalog.md#bitmaporpath-t_bitmaporpath).
- `BitmapIndexScan` (`plannodes.h:520`) — for `IsA(IndexPath)`. Built by first calling `create_indexscan_plan` to get an IndexScan, then `make_bitmap_indexscan(scanrelid, indexid, indexqual, indexqualorig)` extracts the relevant fields.

**Tlist / qual handling**: BitmapAnd/BitmapOr nodes have NIL targetlist and qual. The output parameters `qual`, `indexqual`, `indexECs` accumulate the original and indexable qual lists across the entire subtree.

**Var-reference handling**: Inherited from `create_indexscan_plan` calls.

**Node-specific quirks**:
- For BitmapOrPath with a constant-TRUE child, sets `*qual = NIL` to short-circuit OR-with-true.
- For BitmapIndexScan, also adds index predicates (from `indexinfo->indpred`) to the `subquals`/`subindexquals` outputs.
- Costs of BitmapAnd/BitmapOr are copied directly from the corresponding Path node.

**Source file references**: `createplan.c:3332-3532`.

### create_tidscan_plan

**Signature**: `static TidScan *create_tidscan_plan(...)` at `createplan.c:3540`.

**Dispatching context**: `T_TidScan`. Path counterpart: [TidPath](18_path_catalog.md#tidpath-t_tidpath).

**Output Plan struct**: `TidScan` (`plannodes.h:552`) with `tidquals` field.

**Tlist handling**: From caller.

**Qual handling**: Removes scan_clauses redundant with tidquals. For single tidqual, uses pointer equality and `is_redundant_derived_clause`. For multiple tidquals (OR semantics), converts to an explicit OR clause and uses `equal()` matching.

**Var-reference handling**: `replace_nestloop_params` on both `tidquals` and `scan_clauses`.

**Source file references**: `createplan.c:3540-3629`.

### create_tidrangescan_plan

**Signature**: `static TidRangeScan *create_tidrangescan_plan(...)` at `createplan.c:3637`.

**Dispatching context**: `T_TidRangeScan`. Path counterpart: [TidRangePath](18_path_catalog.md#tidrangepath-t_tidrangepath).

**Output Plan struct**: `TidRangeScan` (`plannodes.h:565`).

**Tlist handling**: From caller.

**Qual handling**: Drops from scan_clauses any clauses already covered by `tidrangequals`. The remaining scan_clauses become `qpqual`.

**Var-reference handling**: `replace_nestloop_params` on qpqual and tidrangequals.

**Source file references**: `createplan.c:3637-3699`.

### create_subqueryscan_plan

**Signature**: `static SubqueryScan *create_subqueryscan_plan(...)` at `createplan.c:3702`.

**Dispatching context**: `T_SubqueryScan`. Path counterpart: [SubqueryScanPath](18_path_catalog.md#subqueryscanpath-t_subqueryscanpath).

**Output Plan struct**: `SubqueryScan` (`plannodes.h:598`) embedding the subplan.

**Tlist handling**: From caller.

**Qual handling**: Standard order + extract.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses, plus `process_subquery_nestloop_params(root, rel->subplan_params)` to set up Params for lateral references into the subquery.

**Node-specific quirks**: Calls `create_plan(rel->subroot, best_path->subpath)` (NOT `create_plan_recurse`) — the subquery has its own PlannerInfo / planning context, and recursing through `create_plan_recurse` would mix contexts. The subroot's curOuterRels/curOuterParams are managed across this call.

**Source file references**: `createplan.c:3702-3753`.

### create_functionscan_plan

**Signature**: `static FunctionScan *create_functionscan_plan(...)` at `createplan.c:3761`.

**Dispatching context**: `T_FunctionScan`. Path counterpart: [Path (T_FunctionScan)](18_path_catalog.md#path-t_functionscan).

**Output Plan struct**: `FunctionScan` (`plannodes.h:609`) with `functions` list and `funcordinality`.

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on both scan_clauses and the function expression list (`functions`) — the function arguments may reference outer rels in LATERAL references.

**Source file references**: `createplan.c:3761-3796`.

### create_tablefuncscan_plan

**Signature**: `static TableFuncScan *create_tablefuncscan_plan(...)` at `createplan.c:3804`.

**Dispatching context**: `T_TableFuncScan`. Path counterpart: [Path (T_TableFuncScan)](18_path_catalog.md#path-t_tablefuncscan).

**Output Plan struct**: `TableFuncScan` (`plannodes.h:630`).

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses and the `TableFunc` itself.

**Source file references**: `createplan.c:3804-3839`.

### create_valuesscan_plan

**Signature**: `static ValuesScan *create_valuesscan_plan(...)` at `createplan.c:3847`.

**Dispatching context**: `T_ValuesScan`. Path counterpart: [Path (T_ValuesScan)](18_path_catalog.md#path-t_valuesscan).

**Output Plan struct**: `ValuesScan` (`plannodes.h:620`).

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses and the `values_lists` (each row's expressions).

**Source file references**: `createplan.c:3847-3883`.

### create_ctescan_plan

**Signature**: `static CteScan *create_ctescan_plan(...)` at `createplan.c:3891`.

**Dispatching context**: `T_CteScan` when `rte->rtekind == RTE_CTE && !rte->self_reference`. Path counterpart: [Path (T_CteScan)](18_path_catalog.md#path-t_ctescan).

**Output Plan struct**: `CteScan` (`plannodes.h:640`).

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Node-specific quirks**: Walks up the levelsup chain (`cteroot`) to find the level that owns the CTE, then locates the previously-built `SubPlan` in `cteroot->init_plans` matching the CTE name. The CTE's `plan_id` and `setParam[0]` (the CTE's communication param) are threaded into the plan via `make_ctescan(tlist, scan_clauses, scan_relid, plan_id, cte_param_id)`.

**Source file references**: `createplan.c:3891-3977`.

### create_namedtuplestorescan_plan

**Signature**: `static NamedTuplestoreScan *create_namedtuplestorescan_plan(...)` at `createplan.c:3986`.

**Dispatching context**: `T_NamedTuplestoreScan` (`rtekind == RTE_NAMEDTUPLESTORE`). Path counterpart: [Path (T_NamedTuplestoreScan)](18_path_catalog.md#path-t_namedtuplestorescan).

**Output Plan struct**: `NamedTuplestoreScan` (`plannodes.h:651`) with `enrname` field.

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Source file references**: `createplan.c:3986-4016`.

### create_resultscan_plan

**Signature**: `static Result *create_resultscan_plan(...)` at `createplan.c:4025`.

**Dispatching context**: `T_Result` when the path is a plain `Path` with `rtekind == RTE_RESULT` (not a ProjectionPath, MinMaxAggPath, or GroupResultPath — those are routed separately in `create_plan_recurse`). Path counterpart: [Path (T_Result, plain RTE_RESULT)](18_path_catalog.md#path-t_result-plain-rte_result).

**Output Plan struct**: `Result` (`plannodes.h:196`).

**Tlist handling**: From caller.

**Qual handling**: Standard. The scan_clauses become the Result's filter (regular qual, not `resconstantqual`).

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Source file references**: `createplan.c:4025-4054`.

### create_worktablescan_plan

**Signature**: `static WorkTableScan *create_worktablescan_plan(...)` at `createplan.c:4062`.

**Dispatching context**: `T_WorkTableScan` (`rtekind == RTE_CTE && rte->self_reference`). Path counterpart: [Path (T_WorkTableScan)](18_path_catalog.md#path-t_worktablescan).

**Output Plan struct**: `WorkTableScan` (`plannodes.h:661`) with `wtParam` field.

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Node-specific quirks**: The worktable param ID lives one level *below* where the CTE was declared (the level that owns the RecursiveUnion). The function walks up `levelsup - 1` to that level then reads `cteroot->wt_param_id`.

**Source file references**: `createplan.c:4062-4114`.

### create_foreignscan_plan

**Signature**: `static ForeignScan *create_foreignscan_plan(...)` at `createplan.c:4122`.

**Dispatching context**: `T_ForeignScan`. Path counterpart: [ForeignPath](18_path_catalog.md#foreignpath-t_foreignpath).

**Output Plan struct**: `ForeignScan` (`plannodes.h:707`) — but actually built by the FDW's `GetForeignPlan` callback. This function provides the FDW with the necessary inputs and post-processes the result.

**Tlist handling**: Tlist comes from the caller; FDW's `GetForeignPlan` may rewrite it.

**Qual handling**: scan_clauses are passed to the FDW for selection between local-recheck quals and remote-pushdown quals.

**Var-reference handling**: For foreign joins, transforms `fdw_outerpath` recursively into a subplan via `create_plan_recurse`. The FDW handles `replace_nestloop_params` itself (or asks core to via `replace_nestloop_params` on the relevant fields). For foreign-base-rel scans, retrieves the table OID for the FDW.

**Node-specific quirks**: The FDW returns a fully-formed ForeignScan plan; this creator only wraps and copies cost/path info.

**Source file references**: `createplan.c:4122-4275`.

### create_customscan_plan

**Signature**: `static CustomScan *create_customscan_plan(...)` at `createplan.c:4277`.

**Dispatching context**: `T_CustomScan`. Path counterpart: [CustomPath](18_path_catalog.md#custompath-t_custompath).

**Output Plan struct**: `CustomScan` (`plannodes.h:739`) — built by the extension's `methods->PlanCustomPath` callback.

**Tlist handling**: From caller; extension may rewrite.

**Qual handling**: scan_clauses passed to extension's PlanCustomPath.

**Var-reference handling**: Recursively transforms `custom_paths` children via `create_plan_recurse` first, then calls `methods->PlanCustomPath`. After the extension returns, runs `replace_nestloop_params` on `cplan->scan.plan.qual` and `cplan->custom_exprs` (so extensions do not have to think about nestloop param substitution).

**Source file references**: `createplan.c:4277-4338`.

---

## Join creators

The three join plan creators and the `create_join_plan` dispatcher. There is no separate `create_hash_plan` — the Hash node child of HashJoin is synthesized inline inside `create_hashjoin_plan` and never appears at the top level.

All join creators share a common pattern:
1. Build the path tlist via `build_path_tlist(root, &jpath.path)`.
2. Recurse on outer and inner subpaths via `create_plan_recurse`, passing different tlist-flag hints (CP_SMALL_TLIST when an inner Sort is needed, CP_EXACT_TLIST when not).
3. Sort `joinrestrictinfo` clauses with `order_qual_clauses`.
4. Split the join clauses into `joinclauses` and `otherclauses` via `extract_actual_join_clauses` (for outer joins) or `extract_actual_clauses` (for inner joins). The split distinguishes "real" join conditions from secondary "other" conditions that apply only to outer-join semantics.
5. Run `replace_nestloop_params` on the qual lists if the join is parameterized.
6. Call the type-specific `make_*` function and `copy_generic_path_info`.

### create_join_plan

**Signature**: `static Plan *create_join_plan(PlannerInfo *root, JoinPath *best_path)` at `createplan.c:1082`.

**Dispatching context**: Called from `create_plan_recurse` (line 419) for `T_HashJoin`, `T_MergeJoin`, or `T_NestLoop`. Itself dispatches via a switch on `best_path->path.pathtype`:

```c
switch (best_path->path.pathtype) {
    case T_MergeJoin:
        plan = (Plan *) create_mergejoin_plan(root, (MergePath *) best_path);
        break;
    case T_HashJoin:
        plan = (Plan *) create_hashjoin_plan(root, (HashPath *) best_path);
        break;
    case T_NestLoop:
        plan = (Plan *) create_nestloop_plan(root, (NestPath *) best_path);
        break;
}
```

**Output Plan struct**: Whichever join Plan subtype was produced; may be wrapped in a gating Result via `create_gating_plan` if pseudoconstants exist on the join restriction list.

**Node-specific quirks**: After the per-type creator returns, runs `get_gating_quals(root, joinrestrictinfo)` and wraps the join plan in a Result if any pseudoconstant clauses were found.

**Source file references**: `createplan.c:1082-1216`.

### create_nestloop_plan

**Signature**: `static NestLoop *create_nestloop_plan(PlannerInfo *root, NestPath *best_path)` at `createplan.c:4348`.

**Dispatching context**: `pathtype == T_NestLoop`. Path counterpart: [NestPath](18_path_catalog.md#nestpath-t_nestpath).

**Output Plan struct**: `NestLoop` (`plannodes.h:807`) — embeds `Join` (`plannodes.h:786`) and adds `nestParams` list.

**Tlist handling**: NestLoop can project, so children's tlists pass through unchanged (`flags = 0` to recursive calls).

**Qual handling**: Splits joinrestrictinfo into joinclauses / otherclauses for outer joins, single list for inner joins. Strips RestrictInfos and orders.

**Var-reference handling**:
- Calls `reparameterize_path_by_child(root, inner_path, outer_parent)` if the inner path is parameterized by the outer rel's topmost parent rather than the outer rel itself (a corner case affecting partition-wise join correctness).
- Saves `root->curOuterRels`, unions outer's relids into it for the inner subplan recursion (so inner-side nestloop param replacement knows which Vars are "outer"), and restores afterwards.
- After both subplans are built, calls `identify_current_nestloop_params(root, outerrelids)` to extract the nestloop Param assignments that this join is responsible for providing — these are removed from `root->curOuterParams` and stored in `nestParams` on the plan node.

**Node-specific quirks**:
- The split of nestloop params between this join and its parent is critical: a nestloop deep in the tree must not "claim" a param that is actually used by a higher join.
- Calls `make_nestloop(tlist, joinclauses, otherclauses, nestParams, outer_plan, inner_plan, jointype, inner_unique)`.

**Source file references**: `createplan.c:4348-4437`.

### create_mergejoin_plan

**Signature**: `static MergeJoin *create_mergejoin_plan(PlannerInfo *root, MergePath *best_path)` at `createplan.c:4440`.

**Dispatching context**: `pathtype == T_MergeJoin`. Path counterpart: [MergePath](18_path_catalog.md#mergepath-t_mergepath).

**Output Plan struct**: `MergeJoin` (`plannodes.h:833`) with `mergeFamilies`, `mergeCollations`, `mergeStrategies`, `mergeNullsFirst` arrays plus `skip_mark_restore` flag.

**Tlist handling**: MergeJoin can project. Outer/inner subplans get `CP_SMALL_TLIST` if a Sort is needed (to minimize sort tuple width); otherwise pass-through.

**Qual handling**:
- Splits joinrestrictinfo via `extract_actual_join_clauses` (outer joins) or `extract_actual_clauses` (inner joins).
- Pulls mergeclauses out of `path_mergeclauses` via `get_actual_clauses`.
- Computes `joinclauses = list_difference(joinclauses, mergeclauses)` so mergeclauses do not appear twice.
- `replace_nestloop_params` on joinclauses and otherclauses (mergeclauses asserted to have none).
- `get_switched_clauses(path_mergeclauses, outer_relids)` rearranges each mergeclause so the outer-rel Var is on the left.

**Var-reference handling**: As above. Mergeclause sides are guaranteed switched to outer-on-left for the executor.

**Node-specific quirks**:
- If `outersortkeys != NIL`, builds an explicit `Sort` node atop the outer subplan via `make_sort_from_pathkeys`, calls `label_sort_with_costsize` to fill in cost. Same for `innersortkeys`.
- If `materialize_inner` is set, wraps the inner side in a `Material` node (with cpu_operator_cost added per tuple as material overhead).
- The big loop at lines 4593-4715 walks `path_mergeclauses` and fills in the four arrays `mergefamilies/mergecollations/mergestrategies/mergenullsfirst` by matching each clause to outer and inner pathkeys via EquivalenceClass identity. Handles redundant pathkeys carefully (two clauses may match the same outer pathkey).
- Calls `make_mergejoin(tlist, joinclauses, otherclauses, mergeclauses, ...)`.

**Source file references**: `createplan.c:4440-4744`.

### create_hashjoin_plan

**Signature**: `static HashJoin *create_hashjoin_plan(PlannerInfo *root, HashPath *best_path)` at `createplan.c:4747`.

**Dispatching context**: `pathtype == T_HashJoin`. Path counterpart: [HashPath](18_path_catalog.md#hashpath-t_hashpath).

**Output Plan struct**: `HashJoin` (`plannodes.h:862`) with hashclauses, hashoperators, hashcollations, outer_hashkeys, inner_hashkeys (the inner side carried via the Hash plan child).

**Tlist handling**: HashJoin can project. Outer subplan gets `CP_SMALL_TLIST` if `num_batches > 1` (multi-batch needs to write tuples to disk, smaller is better). Inner subplan always gets `CP_SMALL_TLIST` (hash table memory matters).

**Qual handling**:
- Splits joinrestrictinfo into joinclauses / otherclauses.
- Pulls hashclauses out of `path_hashclauses`.
- `joinclauses = list_difference(joinclauses, hashclauses)`.
- `replace_nestloop_params` on joinclauses and otherclauses.
- `get_switched_clauses(path_hashclauses, outer_relids)` to put outer Var on left of each hashclause.

**Var-reference handling**: As above.

**Node-specific quirks**:
- **Skew optimization**: If exactly one hashclause and the outer key is a simple Var (possibly with RelabelType), saves `skewTable`/`skewColumn`/`skewInherit` for the executor's skew-MCV optimization (built into the Hash node).
- **Synthesizes the Hash node child**: Walks each hashclause and decomposes it into `(outer_hashkey, inner_hashkey, opno, collation)`. Then calls `make_hash(inner_plan, inner_hashkeys, skewTable, skewColumn, skewInherit)` to build a `Hash` plan (`plannodes.h:1197`) wrapping the inner subplan. The Hash node's costs are copied from the inner plan (with `startup_cost = total_cost` since hashing must complete before probing starts).
- **Parallel Hash**: When `parallel_aware`, sets `hash_plan->plan.parallel_aware = true` and copies `inner_rows_total` to `hash_plan->rows_total` so the executor can size the shared hash table for all participants.
- Calls `make_hashjoin(tlist, joinclauses, otherclauses, hashclauses, hashoperators, hashcollations, outer_hashkeys, outer_plan, (Plan *) hash_plan, jointype, inner_unique)`.

**Source file references**: `createplan.c:4747-4917`.

### On the lack of `create_hash_plan`

There is no top-level `create_hash_plan` because the `Hash` plan node never stands alone. It is always synthesized as the immediate inner child of a HashJoin, inside `create_hashjoin_plan` (line 4878). The HashPath struct carries the hash-related metadata (`path_hashclauses`, `inner_rows_total`, `num_batches`), and the inner child of the HashPath is whatever subpath produces the rows to be hashed — typically a SeqScan or IndexScan.

This design contrasts with how other "wrapping" plans like `Sort`, `Material`, and `Memoize` are handled — those have their own Path types and their own top-level plan creators because the planner considers them as candidate paths in their own right.

---

## Upper creators

These cover the per-pathtype `create_*_plan` functions for upper-layer paths: sorting, grouping, aggregation, projection, materialization, parallel coordination, set operations, recursion, and limit.

A few patterns are shared across these creators:
- `subplan = create_plan_recurse(root, best_path->subpath, flags)` for the input.
- `tlist = build_path_tlist(root, &best_path->path)` for the output.
- `make_*` helper to build the plan node.
- `copy_generic_path_info(plan, &best_path->path)` to copy cost/row/parallel-safe estimates.

Where `flags` matter: `CP_EXACT_TLIST` forces children to produce exactly the requested tlist (used by Append, MergeAppend, ModifyTable, Gather to prevent surprises). `CP_SMALL_TLIST` requests the minimum tlist (used when storing tuples in a tuplestore or sort spool). `CP_LABEL_TLIST` requires the child to label its tlist with sortgroupref attribution (used by Group/Agg/Unique). `CP_IGNORE_TLIST` says the parent will discard whatever the child returns.

### create_sort_plan

**Signature**: `static Sort *create_sort_plan(PlannerInfo *root, SortPath *best_path, int flags)` at `createplan.c:2181`.

**Dispatching context**: `T_Sort`. Path counterpart: [SortPath](18_path_catalog.md#sortpath-t_sortpath).

**Output Plan struct**: `Sort` (`plannodes.h:931`).

**Tlist handling**: Subplan gets `flags | CP_SMALL_TLIST`.

**Node-specific quirks**: Calls `make_sort_from_pathkeys(subplan, pathkeys, relids)`. The relids argument is non-NULL only for "other" rels (partitioned-table children); this is passed to `find_ec_member_matching_expr` so it ignores ECs from sibling partitions.

**Source file references**: `createplan.c:2181-2207`.

### create_incrementalsort_plan

**Signature**: `static IncrementalSort *create_incrementalsort_plan(...)` at `createplan.c:2215`.

**Dispatching context**: `T_IncrementalSort`. Path counterpart: [IncrementalSortPath](18_path_catalog.md#incrementalsortpath-t_incrementalsortpath).

**Output Plan struct**: `IncrementalSort` (`plannodes.h:955`).

**Tlist handling**: Same as create_sort_plan.

**Node-specific quirks**: Calls `make_incrementalsort_from_pathkeys()` with the `nPresortedCols` count from the path. Otherwise structurally identical to create_sort_plan.

**Source file references**: `createplan.c:2215-2233`.

### create_agg_plan

**Signature**: `static Agg *create_agg_plan(PlannerInfo *root, AggPath *best_path)` at `createplan.c:2309`.

**Dispatching context**: `T_Agg` when `IsA(best_path, AggPath)` (not GroupingSetsPath). Path counterpart: [AggPath](18_path_catalog.md#aggpath-t_aggpath).

**Output Plan struct**: `Agg` (`plannodes.h:996`).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST` (Agg can project, but grouping columns must be labeled).

**Qual handling**: `quals = order_qual_clauses(root, best_path->qual)` — these are HAVING quals.

**Node-specific quirks**:
- `extract_grouping_cols(groupClause, subplan->targetlist)` returns the AttrNumber array of grouping columns.
- `extract_grouping_ops(groupClause)` returns the equality operators.
- `extract_grouping_collations(groupClause, subplan->targetlist)` returns the collations.
- Calls `make_agg(tlist, quals, aggstrategy, aggsplit, numCols, grpColIdx, grpOperators, grpCollations, NIL_groupingSets, NIL_chain, numGroups, transitionSpace, subplan)`.

**Source file references**: `createplan.c:2309-2344`.

### create_groupingsets_plan

**Signature**: `static Plan *create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)` at `createplan.c:2393`.

**Dispatching context**: `T_Agg` when `IsA(best_path, GroupingSetsPath)`. Path counterpart: [GroupingSetsPath](18_path_catalog.md#groupingsetspath-t_groupingsetspath).

**Output Plan struct**: `Agg` chain — the topmost Agg's `chain` field contains subsidiary Agg nodes (one per non-first rollup).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST`.

**Qual handling**: HAVING goes on the topmost Agg only.

**Var-reference handling**: Builds `grouping_map[tleSortGroupRef] = column_index_in_subplan_tlist`. Saves to `root->grouping_map` so setrefs.c can fix up GroupingFunc nodes' `cols` lists across the chain.

**Node-specific quirks**:
- For each non-first rollup, builds an auxiliary `Agg` with `make_agg(NIL, NIL, strat, AGGSPLIT_SIMPLE, ...)`, optionally preceded by a `Sort` (`make_sort_from_groupcols`) for AGG_SORTED rollups beyond the first.
- The auxiliary Aggs have empty tlists/quals — they are vestigial nodes that just carry rollup metadata for the executor.
- Strategies per rollup: `AGG_HASHED` if rollup->is_hashed; `AGG_PLAIN` if its first gset is empty; otherwise `AGG_SORTED`.
- The topmost Agg uses the path's `aggstrategy`.

**Source file references**: `createplan.c:2393-2542`.

### create_minmaxagg_plan

**Signature**: `static Result *create_minmaxagg_plan(PlannerInfo *root, MinMaxAggPath *best_path)` at `createplan.c:2551`.

**Dispatching context**: `T_Result` when `IsA(best_path, MinMaxAggPath)`. Path counterpart: [MinMaxAggPath](18_path_catalog.md#minmaxaggpath-t_minmaxaggpath).

**Output Plan struct**: `Result` (`plannodes.h:196`) — the InitPlans do all the work.

**Tlist handling**: `tlist = build_path_tlist(root, &best_path->path)`.

**Qual handling**: `best_path->quals` (HAVING) becomes the `resconstantqual` of the Result.

**Var-reference handling**: Critical: each MinMaxAggInfo's path is converted into a Plan via `create_plan(subroot, mminfo->path)` (note: `create_plan`, not `create_plan_recurse`, because the subroot is a separate planning context). A `Limit 1` is wrapped on top of each. Then `SS_make_initplan_from_plan(root, subroot, plan, mminfo->param)` registers it as an InitPlan in the outer query.

**Node-specific quirks**: Sets `root->minmax_aggs = best_path->mmaggregates` so setrefs.c knows to replace `Aggref` references in the surrounding plan with Param references to the InitPlans.

**Source file references**: `createplan.c:2551-2608`.

### create_windowagg_plan

**Signature**: `static WindowAgg *create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)` at `createplan.c:2617`.

**Dispatching context**: `T_WindowAgg`. Path counterpart: [WindowAggPath](18_path_catalog.md#windowaggpath-t_windowaggpath).

**Output Plan struct**: `WindowAgg` (`plannodes.h:1038`).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST | CP_SMALL_TLIST` — WindowAgg buffers frame contents in a tuplestore.

**Var-reference handling**: Looks up partition/order column resnos via `get_sortgroupclause_tle(sgc, subplan->targetlist)`.

**Node-specific quirks**:
- Builds `partColIdx`, `partOperators`, `partCollations` arrays from `wc->partitionClause`.
- Builds `ordColIdx`, `ordOperators`, `ordCollations` arrays from `wc->orderClause`.
- Threads frame options, start/end offsets, in-range functions, and `runCondition` (for short-circuit evaluation) through `make_windowagg`.
- `topwindow` flag tells the executor that this is the topmost WindowAgg; only the top one runs HAVING-like quals.

**Source file references**: `createplan.c:2617-2711`.

### create_group_plan

**Signature**: `static Group *create_group_plan(PlannerInfo *root, GroupPath *best_path)` at `createplan.c:2242`.

**Dispatching context**: `T_Group`. Path counterpart: [GroupPath](18_path_catalog.md#grouppath-t_grouppath).

**Output Plan struct**: `Group` (`plannodes.h:967`).

**Tlist handling**: Subplan gets `CP_LABEL_TLIST`.

**Qual handling**: `quals = order_qual_clauses(root, best_path->qual)` — HAVING.

**Node-specific quirks**: `extract_grouping_cols/ops` from groupClause; `make_group(tlist, quals, numCols, grpColIdx, grpOperators, grpCollations, subplan)`.

**Source file references**: `createplan.c:2242-2272`.

### create_unique_plan

**Signature**: `static Plan *create_unique_plan(PlannerInfo *root, UniquePath *best_path, int flags)` at `createplan.c:1721`.

**Dispatching context**: `T_Unique` when `IsA(best_path, UniquePath)` (not UpperUniquePath). Path counterpart: [UniquePath](18_path_catalog.md#uniquepath-t_uniquepath).

**Output Plan struct**: Returns `Plan *` because actual return value depends on `umethod`:
- `UNIQUE_PATH_NOOP` — returns `subplan` unchanged.
- `UNIQUE_PATH_HASH` — returns an `Agg` with `AGG_HASHED` strategy.
- `UNIQUE_PATH_SORT` — returns a `Unique` node atop a synthesized `Sort`.

**Tlist handling**: Builds a new tlist starting from `build_path_tlist(root, &best_path->path)` and adding any `uniq_exprs` not already present. For SORT mode, replaces subplan's tlist via `change_plan_targetlist`. For HASH mode, leaves subplan tlist alone unless additions were needed.

**Node-specific quirks**:
- For HASH: looks up hash equality operators via `get_compatible_hash_operators(in_oper, NULL, &eq_oper)` — the IN clause operator might be cross-type, in which case the equality op is for the RHS datatype.
- For SORT: synthesizes an explicit ORDER BY list (`SortGroupClause` instances) via `get_ordering_op_for_equality_op` and `get_equality_op_for_ordering_op` lookups; `make_sort_from_sortclauses` builds the Sort, then `make_unique_from_sortclauses` wraps it.

**Source file references**: `createplan.c:1721-1911`.

### create_upper_unique_plan

**Signature**: `static Unique *create_upper_unique_plan(PlannerInfo *root, UpperUniquePath *best_path, int flags)` at `createplan.c:2281`.

**Dispatching context**: `T_Unique` when `IsA(best_path, UpperUniquePath)`. Path counterpart: [UpperUniquePath](18_path_catalog.md#upperuniquepath-t_upperuniquepath).

**Output Plan struct**: `Unique` (`plannodes.h:1112`).

**Tlist handling**: Subplan gets `flags | CP_LABEL_TLIST`.

**Node-specific quirks**: `make_unique_from_pathkeys(subplan, pathkeys, numkeys)` — derives equality operators from the pathkeys directly.

**Source file references**: `createplan.c:2281-2300`.

### create_setop_plan

**Signature**: `static SetOp *create_setop_plan(...)` at `createplan.c:2720`.

**Dispatching context**: `T_SetOp`. Path counterpart: [SetOpPath](18_path_catalog.md#setoppath-t_setoppath).

**Output Plan struct**: `SetOp` (`plannodes.h:1217`).

**Tlist handling**: Subplan gets `flags | CP_LABEL_TLIST`.

**Node-specific quirks**: `make_setop(cmd, strategy, subplan, distinctList, flagColIdx, firstFlag, numGroups)`. `numGroups` is converted from `Cardinality` (double) to `long` via `clamp_cardinality_to_long`.

**Source file references**: `createplan.c:2720-2747`.

### create_recursiveunion_plan

**Signature**: `static RecursiveUnion *create_recursiveunion_plan(PlannerInfo *root, RecursiveUnionPath *best_path)` at `createplan.c:2756`.

**Dispatching context**: `T_RecursiveUnion`. Path counterpart: [RecursiveUnionPath](18_path_catalog.md#recursiveunionpath-t_recursiveunionpath).

**Output Plan struct**: `RecursiveUnion` (`plannodes.h:325`).

**Tlist handling**: Both `leftpath` and `rightpath` get `CP_EXACT_TLIST` — they must produce identical tlists for the union to make sense.

**Node-specific quirks**: `make_recursive_union(tlist, leftplan, rightplan, wtParam, distinctList, numGroups)`. The `wtParam` is the work-table communication Param assigned earlier in plan setup.

**Source file references**: `createplan.c:2756-2783`.

### create_append_plan

**Signature**: `static Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path, int flags)` at `createplan.c:1217`.

**Dispatching context**: `T_Append`. Path counterpart: [AppendPath](18_path_catalog.md#appendpath-t_appendpath).

**Output Plan struct**: `Append` (`plannodes.h:265`). May also return a `Result` with constant-FALSE qual if `subpaths == NIL` (dummy empty rel), or wrap the Append in a Result via `inject_projection_plan` if added sort columns must be hidden.

**Tlist handling**: All children get `CP_EXACT_TLIST` — Append demands matching tlists across all children.

**Var-reference handling**: For partition pruning, `prunequal = extract_actual_clauses(rel->baserestrictinfo, false)` plus `param_info->ppi_clauses` (with `replace_nestloop_params` applied) — these are passed to `make_partition_pruneinfo` to build the executor's pruning steps.

**Node-specific quirks**:
- For ordered Appends: calls `prepare_sort_from_pathkeys` on the Append node itself to set up sort column info (which may add resjunk sort columns to the Append's tlist), then on each subplan to verify they produce the same sort keys.
- Inserts a `Sort` node atop any subplan whose pathkeys do not match the desired order.
- **Async append**: When `enable_async_append && pathkeys == NIL && !parallel_safe && nsubpaths > 1`, calls `mark_async_capable_plan(subplan, subpath)` for each child; foreign-table children that opt in get `async_capable = true`. Final count is `nasyncplans`.
- **Sort column cleanup**: If sort columns were added to the Append's tlist but the caller asked for `CP_EXACT_TLIST` or `CP_SMALL_TLIST`, wraps the Append in a Result (via `inject_projection_plan`) that projects only the original tlist columns.

**Source file references**: `createplan.c:1217-1428`.

### create_merge_append_plan

**Signature**: `static Plan *create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path, int flags)` at `createplan.c:1438`.

**Dispatching context**: `T_MergeAppend`. Path counterpart: [MergeAppendPath](18_path_catalog.md#mergeappendpath-t_mergeappendpath).

**Output Plan struct**: `MergeAppend` (`plannodes.h:287`). May be wrapped in a Result via `inject_projection_plan` if extra sort columns conflict with `CP_EXACT_TLIST`/`CP_SMALL_TLIST`.

**Tlist handling**: Children get `CP_EXACT_TLIST`. Sort column setup mirrors create_append_plan's logic.

**Var-reference handling**: Asserts `param_info == NULL` (currently no parameterized MergeAppend paths are generated).

**Node-specific quirks**:
- Always inserts a Sort atop any unordered child (vs. Append which only does this when the Append itself is ordered).
- Stores per-Append sort column metadata in the MergeAppend's `numCols`, `sortColIdx`, `sortOperators`, `collations`, `nullsFirst` fields (these arrays are read by the executor's binary heap).
- Partition pruning: same as Append.

**Source file references**: `createplan.c:1438-1578`.

### create_material_plan

**Signature**: `static Material *create_material_plan(PlannerInfo *root, MaterialPath *best_path, int flags)` at `createplan.c:1639`.

**Dispatching context**: `T_Material`. Path counterpart: [MaterialPath](18_path_catalog.md#materialpath-t_materialpath).

**Output Plan struct**: `Material` (`plannodes.h:880`).

**Tlist handling**: Subplan gets `flags | CP_SMALL_TLIST` — minimize buffer width.

**Node-specific quirks**: Trivial — `make_material(subplan)` and copy cost info.

**Source file references**: `createplan.c:1639-1657`.

### create_memoize_plan

**Signature**: `static Memoize *create_memoize_plan(PlannerInfo *root, MemoizePath *best_path, int flags)` at `createplan.c:1667`.

**Dispatching context**: `T_Memoize`. Path counterpart: [MemoizePath](18_path_catalog.md#memoizepath-t_memoizepath).

**Output Plan struct**: `Memoize` (`plannodes.h:889`).

**Tlist handling**: Subplan gets `flags | CP_SMALL_TLIST`.

**Var-reference handling**: `param_exprs = (List *) replace_nestloop_params(root, (Node *) best_path->param_exprs)` — converts outer-rel Vars in the cache key expressions to nestloop Params.

**Node-specific quirks**:
- Builds the `operators` and `collations` arrays from each (param_expr, hash_op) pair.
- `keyparamids = pull_paramids((Expr *) param_exprs)` — collects all Param IDs that the cache key depends on, used by the executor to invalidate the cache when these Params change.
- Calls `make_memoize(subplan, operators, collations, param_exprs, singlerow, binary_mode, est_entries, keyparamids)`.

**Source file references**: `createplan.c:1667-1711`.

### create_gather_plan

**Signature**: `static Gather *create_gather_plan(PlannerInfo *root, GatherPath *best_path)` at `createplan.c:1920`.

**Dispatching context**: `T_Gather`. Path counterpart: [GatherPath](18_path_catalog.md#gatherpath-t_gatherpath).

**Output Plan struct**: `Gather` (`plannodes.h:1140`).

**Tlist handling**: Subplan gets `CP_EXACT_TLIST` — the leader-worker tuple queue uses MinimalTuple representation, so the worker's tlist must exactly match what the Gather emits (no system columns through queues).

**Node-specific quirks**:
- Calls `assign_special_exec_param(root)` to allocate a unique Param ID for the Gather node — used at runtime for inter-process state coordination.
- Sets `root->glob->parallelModeNeeded = true` so the executor enters parallel mode.
- `make_gather(tlist, NIL, num_workers, rescan_param, single_copy, subplan)`.

**Source file references**: `createplan.c:1920-1949`.

### create_gather_merge_plan

**Signature**: `static GatherMerge *create_gather_merge_plan(PlannerInfo *root, GatherMergePath *best_path)` at `createplan.c:1958`.

**Dispatching context**: `T_GatherMerge`. Path counterpart: [GatherMergePath](18_path_catalog.md#gathermergepath-t_gathermergepath).

**Output Plan struct**: `GatherMerge` (`plannodes.h:1155`).

**Tlist handling**: Subplan gets `CP_EXACT_TLIST`.

**Node-specific quirks**:
- Manually allocates and populates the GatherMerge node (no `make_gather_merge`); calls `prepare_sort_from_pathkeys` to set up `numCols`, `sortColIdx`, `sortOperators`, `collations`, `nullsFirst` arrays directly on the plan node.
- Asserts `pathkeys != NIL` at construction (else why use Merge instead of plain Gather?).
- Asserts at plan time that `pathkeys_contained_in(pathkeys, best_path->subpath->pathkeys)` — if not, `elog(ERROR, "gather merge input not sufficiently sorted")`. This is because we cannot safely insert a Sort here; the partial path must already produce sufficiently sorted output.
- Sets `parallelModeNeeded = true`.

**Source file references**: `createplan.c:1958-2009`.

### create_limit_plan

**Signature**: `static Limit *create_limit_plan(PlannerInfo *root, LimitPath *best_path, int flags)` at `createplan.c:2856`.

**Dispatching context**: `T_Limit`. Path counterpart: [LimitPath](18_path_catalog.md#limitpath-t_limitpath).

**Output Plan struct**: `Limit` (`plannodes.h:1270`).

**Tlist handling**: Subplan gets `flags` unchanged — Limit does not project.

**Node-specific quirks**: For `LIMIT_OPTION_WITH_TIES`, walks `parse->sortClause` to extract uniqkey columns/operators/collations (used by Limit at runtime to detect ties and include all tied rows beyond the count).

**Source file references**: `createplan.c:2856-2901`.

### create_projection_plan

**Signature**: `static Plan *create_projection_plan(PlannerInfo *root, ProjectionPath *best_path, int flags)` at `createplan.c:2019`.

**Dispatching context**: `T_Result` when `IsA(best_path, ProjectionPath)`. Path counterpart: [ProjectionPath](18_path_catalog.md#projectionpath-t_projectionpath).

**Output Plan struct**: Either:
- `Result` (`plannodes.h:196`) — if a separate projection node is needed.
- The subplan unchanged (with its tlist replaced) — when the subplan is projection-capable or its existing tlist already matches.

**Tlist handling**: Three cases (lines 2039-2073):
1. `use_physical_tlist(root, &path, flags)` returns true — caller does not need an exact tlist; subplan gets `flags = 0`, and we use the subplan's existing tlist (with optional sortgroupref labeling).
2. Subplan is projection-capable — recurse with `CP_IGNORE_TLIST` (subplan can return whatever; we will override its tlist), then `tlist = build_path_tlist(root, &path)`.
3. Need a Result node — recurse with `flags = 0`, build the requested tlist, set `needs_result_node = !tlist_same_exprs(tlist, subplan->targetlist)`.

**Node-specific quirks**: When no Result is needed, just replaces `subplan->targetlist` and labels with the path's costs. When a Result is needed, calls `make_result(tlist, NULL, subplan)`.

**Source file references**: `createplan.c:2019-2107`.

### create_project_set_plan

**Signature**: `static ProjectSet *create_project_set_plan(PlannerInfo *root, ProjectSetPath *best_path)` at `createplan.c:1613`.

**Dispatching context**: `T_ProjectSet`. Path counterpart: [ProjectSetPath](18_path_catalog.md#projectsetpath-t_projectsetpath).

**Output Plan struct**: `ProjectSet` (`plannodes.h:208`).

**Tlist handling**: Subplan gets `flags = 0` (no constraint on subplan tlist; ProjectSet will project).

**Node-specific quirks**: Trivial — `tlist = build_path_tlist(root, &best_path->path)`, then `make_project_set(tlist, subplan)`.

**Source file references**: `createplan.c:1613-1629`.

### create_group_result_plan

**Signature**: `static Result *create_group_result_plan(PlannerInfo *root, GroupResultPath *best_path)` at `createplan.c:1588`.

**Dispatching context**: `T_Result` when `IsA(best_path, GroupResultPath)`. Path counterpart: [GroupResultPath](18_path_catalog.md#groupresultpath-t_groupresultpath).

**Output Plan struct**: `Result` (`plannodes.h:196`).

**Tlist handling**: `tlist = build_path_tlist(root, &best_path->path)`.

**Qual handling**: `quals = order_qual_clauses(root, best_path->quals)` — these are HAVING quals stored as bare clauses (not RestrictInfos) on the path.

**Node-specific quirks**: Trivial — `make_result(tlist, (Node *) quals, NULL)`. The `subplan` argument is NULL because GroupResultPath represents a degenerate one-row result with no input.

**Source file references**: `createplan.c:1588-1604`.

---

## Modify creators

The two plan creators that wrap the top of a DML or row-locking plan tree. Both are dispatched from `create_plan_recurse` based on `T_ModifyTable` and `T_LockRows` pathtype.

These creators are deliberately thin wrappers: most of the heavy lifting (target-list construction, FDW direct-modify dispatching, ON CONFLICT processing) happens inside the `make_modifytable()` helper that they call, or inside subsequent passes (`setrefs.c` finishes Var fixup; `subselect.c` and `set_plan_references` finalize subplan integration).

### create_modifytable_plan

**Signature**: `static ModifyTable *create_modifytable_plan(PlannerInfo *root, ModifyTablePath *best_path)` at `createplan.c:2815`.

**Dispatching context**: Routed from `create_plan_recurse` (line 533) for `pathtype == T_ModifyTable`. Path counterpart: [ModifyTablePath](18_path_catalog.md#modifytablepath-t_modifytablepath).

**Output Plan struct**: `ModifyTable` (`plannodes.h:229`).

**Tlist handling**: Subplan gets `CP_EXACT_TLIST` — the source-data plan must produce exactly the columns expected by the modification machinery (typically: target-table columns plus a `ctid` or row-identifier junk column for UPDATE/DELETE/MERGE, or generated default values for INSERT).

After the subplan is built, calls `apply_tlist_labeling(subplan->targetlist, root->processed_tlist)` to copy resname/resjunk labels from the parser-tree tlist onto the subplan's tlist. This keeps the executor happy when projecting RETURNING expressions and when matching column labels for ON CONFLICT.

**Qual handling**: ModifyTable has no `qual` of its own; quals belong to the subplan (the WHERE clause of an UPDATE/DELETE/MERGE).

**Var-reference handling**: None directly. `make_modifytable` and the subsequent setrefs.c pass handle:
- Substituting INDEX_VAR references in WithCheckOption and Returning lists.
- Resolving Vars in `mergeJoinConditions` against the subplan's tlist.
- Wiring up `epqParam` for EvalPlanQual (the subplan must be re-runnable when MVCC conflicts are resolved).
- For OnConflictExpr, fixing up references to the EXCLUDED pseudo-relation (`exclRelTlist`).

**Node-specific quirks**:
- **Single subplan for all target relations**: Even when the target is a partitioned table or inheritance hierarchy with many leaf tables, ModifyTable receives a *single* subplan that produces all source rows (typically an Append over per-leaf scans). The leaf-relation routing happens at execution time using either the `tableoid` system column or partition-routing logic.
- **Foreign-table direct modify**: `make_modifytable()` invokes each result-relation's FDW `IsForeignRelUpdatable` and `PlanDirectModify` callbacks. When a foreign table opts into direct-modify, its part of the subplan is replaced with a `ForeignScan` that performs the modification entirely on the remote side. The bitmap of direct-modify relations is recorded in `fdwDirectModifyPlans`.
- **MERGE specifics**: `mergeActionLists` (per-target list of MergeAction nodes) and `mergeJoinConditions` are threaded straight through; the executor processes them at runtime.
- **ON CONFLICT specifics**: `onconflict` (a parsed OnConflictExpr) is decomposed into `onConflictAction`, `arbiterIndexes`, `onConflictSet`, `onConflictCols`, `onConflictWhere`, and the `EXCLUDED` pseudo-relation metadata (`exclRelRTI`, `exclRelTlist`). This decomposition happens inside `make_modifytable`.
- **RETURNING**: `returningLists` is per-target-relation; each list is run through `setrefs.c` to fix up Var references against the per-relation result tuple.
- **canSetTag**: Tells the executor whether to update `pg_stat_*` counters and `es_processed` for this ModifyTable. Only set on the topmost ModifyTable in a query.
- **rowMarks (non-locking)**: `PlanRowMark`s for FOR ... SKIP LOCKED-style or trigger-related row marking; distinct from LockRowsPath's `rowMarks` which are for FOR UPDATE/SHARE.

**Source file references**: `createplan.c:2815-2847`. The complexity lives inside `make_modifytable` (also in createplan.c, near line 7000+).

### create_lockrows_plan

**Signature**: `static LockRows *create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path, int flags)` at `createplan.c:2792`.

**Dispatching context**: Routed from `create_plan_recurse` (line 528) for `pathtype == T_LockRows`. Path counterpart: [LockRowsPath](18_path_catalog.md#lockrowspath-t_lockrowspath).

**Output Plan struct**: `LockRows` (`plannodes.h:1256`) with `rowMarks` and `epqParam`.

**Tlist handling**: Subplan gets `flags` unchanged — LockRows does not project, so tlist requirements pass through.

**Qual handling**: None — LockRows has no qual.

**Var-reference handling**: None at this level. The PlanRowMarks in `rowMarks` already reference RT indexes that survive the planner pass; `setrefs.c` will normalize them.

**Node-specific quirks**: Truly trivial — three lines of substantive code:

```c
subplan = create_plan_recurse(root, best_path->subpath, flags);
plan = make_lockrows(subplan, best_path->rowMarks, best_path->epqParam);
copy_generic_path_info(&plan->plan, (Path *) best_path);
```

The runtime work happens in `nodeLockRows.c`: for each tuple from the subplan, acquire the appropriate row lock(s) on the target table(s), and if a concurrent update has occurred, use `epqParam` to re-evaluate the qual against the latest tuple version (EvalPlanQual). The row marks specify *which* RT indexes need locking and *what kind* of lock (FOR UPDATE / NO KEY UPDATE / SHARE / KEY SHARE / NO KEY SHARE).

**Source file references**: `createplan.c:2792-2806`.

---

## Notes on pseudo-creators and DML plan tree shape

A typical write plan tree shape:

```
ModifyTable (Update on partitioned_table)
└── Append   (over partitions)
    ├── Seq Scan partition_1 (with ctid as junk column)
    ├── Seq Scan partition_2
    └── Seq Scan partition_3
```

For SELECT FOR UPDATE:

```
LockRows
└── Sort (or Index Scan)
    └── Seq Scan / Index Scan
```

For INSERT ... ON CONFLICT:

```
ModifyTable (Insert on t with arbiter t_pkey)
├── conflictAction = ONCONFLICT_UPDATE
├── onConflictSet = [...]
├── onConflictWhere = ...
└── subplan: Result (or Subquery scan)
```

For MERGE:

```
ModifyTable (Merge on t)
├── mergeActionLists = [WHEN MATCHED THEN UPDATE..., WHEN NOT MATCHED THEN INSERT...]
└── subplan:
    └── Hash Right Join (or other join algorithm)
        ├── Seq Scan source
        └── Hash
            └── Seq Scan target
```

ModifyTable is purely a topmost wrapper — it never has more than one child plan, never participates in plan optimization, and its existence is dictated by the parse tree (CmdType ≠ CMD_SELECT).

---

## Cross-references

- Path counterparts: [Module 18 Path catalog](18_path_catalog.md).
- The Path-to-Plan map and create_plan_recurse dispatch: [Module 16 Plan creation and setrefs](16_plan_creation_and_setrefs.md).
- `set_plan_references` and the Var-flattening pass that runs after every creator: [Module 16](16_plan_creation_and_setrefs.md#165-set_plan_references).
- The CP_*_TLIST flags meaning: [Module 16.3](16_plan_creation_and_setrefs.md#163-create_plan).

Next: [20 Deep dives](20_deep_dives.md).
