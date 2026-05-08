# Plan Creator Catalog: Scan Plan Creators

This catalog covers every static `create_*_plan` helper in `src/backend/optimizer/plan/createplan.c` that produces a leaf-level Scan plan node. They all share a common dispatch — `create_scan_plan()` (line 560) — which pre-processes the scan_clauses (extracting them into bare expressions, resolving them through `replace_nestloop_params` for parameterized scans) and then routes by `pathtype` to the specific creator below.

Some scan types reuse the same generic `Path` struct (no specialized struct exists); those creators take a `Path *best_path` parameter rather than a path-specific subtype.

The orchestration helpers are also covered here:

- `create_scan_plan` — the dispatcher for all leaf scans (T_SeqScan, T_SampleScan, T_IndexScan, T_IndexOnlyScan, T_BitmapHeapScan, T_TidScan, T_TidRangeScan, T_SubqueryScan, T_FunctionScan, T_TableFuncScan, T_ValuesScan, T_CteScan, T_WorkTableScan, T_NamedTuplestoreScan, T_ForeignScan, T_CustomScan, and T_Result for plain RTE_RESULT).
- `create_gating_plan` — wraps a plan with a Result node bearing a one-time qual when pseudoconstants are present. Not a leaf scan creator per se, but invoked by `create_scan_plan`.
- `create_bitmap_subplan` — a recursive helper that traverses the bitmap qual tree (BitmapAnd/BitmapOr/IndexPath) to produce the BitmapAnd/BitmapOr/BitmapIndexScan plans below a BitmapHeapScan.

---

## create_scan_plan

**Signature**: `static Plan *create_scan_plan(PlannerInfo *root, Path *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:560`.

**Dispatching context**: Called from `create_plan_recurse` (line 414) for any scan-type pathtype. Itself a sub-dispatcher: it builds the right tlist (physical or logical), sorts and prepares scan_clauses, then switches on `best_path->pathtype` (line 678) to pick the specific `create_*scan_plan`.

**Output Plan struct**: Polymorphic — whichever Plan subtype the per-pathtype creator returns. May wrap that plan in a gating Result via `create_gating_plan` if there are pseudoconstant quals.

**Tlist handling**: Selects between `build_physical_tlist(rel)` (for table scans, allows the executor to skip projection), `copyObject(indexinfo->indextlist)` (for IndexOnlyScan), or `build_path_tlist(root, best_path)` (logical tlist). Honors `CP_IGNORE_TLIST` flag by returning `tlist = NULL` (subplan can return whatever it wants), and `CP_LABEL_TLIST` to copy sortgroupref labeling onto the chosen tlist.

**Qual handling**: Pulls scan_clauses from `rel->baserestrictinfo` (or `indexinfo->indrestrictinfo` for IndexScan/IndexOnlyScan, since the index implies its own predicates). When the path is parameterized, appends `param_info->ppi_clauses` so all join conditions movable into this scan are enforced.

**Var-reference handling**: None at this layer; the per-pathtype creator handles `replace_nestloop_params` for parameterized scans. `setrefs.c` later converts varno=baserel-rti references to scanrelid + actual attno once attachments are finalized.

**Node-specific quirks**:
- For T_ForeignScan and T_CustomScan handling joins, fetches `fdw_restrictinfo`/`custom_restrictinfo` instead of baserel restrict info.
- Calls `get_gating_quals(root, scan_clauses)` and wraps result in `create_gating_plan` when pseudoconstant clauses are present (these go in a separate Result above the scan because they're cheap to evaluate once and may short-circuit the scan).
- Routes T_IndexScan vs. T_IndexOnlyScan to the same `create_indexscan_plan` with an `indexonly` boolean.

**Source file references**: `createplan.c:556-799`.

---

## create_gating_plan

**Signature**: `static Plan *create_gating_plan(PlannerInfo *root, Path *path, Plan *plan, List *gating_quals)` at `src/backend/optimizer/plan/createplan.c:1023`.

**Dispatching context**: Called from `create_scan_plan` (line ~795) and `create_join_plan` (line ~1135) when `get_gating_quals` returns non-empty pseudoconstant clauses.

**Output Plan struct**: `Result` (`plannodes.h:196`) wrapping the input plan, with `resconstantqual` set to the AND of the gating quals.

**Tlist handling**: Result inherits the wrapped plan's tlist (Result is projection-capable but here it just passes through).

**Qual handling**: The gating_quals become `resconstantqual` — evaluated exactly once at plan startup; if they evaluate to false, no rows are produced from the wrapped plan.

**Node-specific quirks**: Adjusts `cost_qual_eval` so the cost is reflected in the Result's startup cost; `parallel_safe` of the resulting Result is the AND of input plan and the qual's parallel-safety.

**Source file references**: `createplan.c:1023-1070`.

---

## create_seqscan_plan

**Signature**: `static SeqScan *create_seqscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:2917`.

**Dispatching context**: Called from `create_scan_plan` for `pathtype == T_SeqScan`.

**Output Plan struct**: `SeqScan` defined at `src/include/nodes/plannodes.h:396` (just embeds `Scan`).

**Tlist handling**: Whatever `create_scan_plan` chose — typically physical tlist for table scans.

**Qual handling**: `scan_clauses = order_qual_clauses(root, scan_clauses)` to put cheap quals first; then `extract_actual_clauses(scan_clauses, false)` to strip RestrictInfo wrappers (skipping pseudoconstants — those are gated above by the gating Result).

**Var-reference handling**: When `best_path->param_info` is set (parameterized), runs `replace_nestloop_params` on the scan_clauses to convert outer-rel Vars to nestloop Param references.

**Node-specific quirks**: Calls `make_seqscan(tlist, scan_clauses, scan_relid)`, then `copy_generic_path_info()` to transfer cost/row estimates.

**Source file references**: `createplan.c:2917-2947`.

---

## create_samplescan_plan

**Signature**: `static SampleScan *create_samplescan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:2955`.

**Dispatching context**: Routed for `T_SampleScan`.

**Output Plan struct**: `SampleScan` (`plannodes.h:405`) — embeds `Scan` plus a `TableSampleClause *tablesample`.

**Tlist handling**: From caller.

**Qual handling**: Same pattern as seqscan: order, extract, replace nestloop params.

**Var-reference handling**: Replaces nestloop params in both scan_clauses *and* the `tsc` (tablesample clause) — the sampling expressions can themselves reference outer Vars.

**Node-specific quirks**: Looks up the RangeTblEntry's `tablesample` field (asserts it's non-NULL).

**Source file references**: `createplan.c:2955-2993`.

---

## create_indexscan_plan

**Signature**: `static Scan *create_indexscan_plan(PlannerInfo *root, IndexPath *best_path, List *tlist, List *scan_clauses, bool indexonly)` at `src/backend/optimizer/plan/createplan.c:3006`.

**Dispatching context**: Routed for both `T_IndexScan` and `T_IndexOnlyScan`. The `indexonly` flag determines which kind of plan is produced. Also reused by `create_bitmap_subplan` for bitmap subtrees, which always passes `indexonly = false`.

**Output Plan struct**: Returns `Scan *` because it can yield either:
- `IndexScan` (`plannodes.h:449`) with `indexqual`, `indexqualorig`, `indexorderby`, `indexorderbyorig`, `indexorderbyops`, `indexorderdir`.
- `IndexOnlyScan` (`plannodes.h:492`) with `indexqual`, `recheckqual`, `indexorderby`, `indextlist`, `indexorderdir`.

**Tlist handling**: For IndexOnlyScan, marks index tlist entries as `resjunk` when `indexinfo->canreturn[i] = false`, so setrefs.c skips them.

**Qual handling**: Three lists are computed:
1. `stripped_indexquals` — index quals with RestrictInfos stripped.
2. `fixed_indexquals` — index quals with index Vars (varno = INDEX_VAR) substituted for table Vars; this is what the executor uses.
3. `qpqual` — the residual scan_clauses minus those redundantly enforced by indexclauses (via `is_redundant_with_indexclauses`) and minus those provably implied by the indexquals (via `predicate_implied_by`). This becomes the executor's recheck filter.

**Var-reference handling**: `fix_indexqual_references` does the index-Var substitution and also calls `replace_nestloop_params`. Then explicit `replace_nestloop_params` on `stripped_indexquals`, `qpqual`, `indexorderbys` for parameterized scans.

**Node-specific quirks**:
- Computes `indexorderbyops` (sort operators for amcanorderbyop ORDER BY operators) by looking up opfamily members for each pathkey/expression pair.
- Two terminal `make_indexscan` / `make_indexonlyscan` calls produce the right plan flavor.

**Source file references**: `createplan.c:3006-3199`.

---

## create_bitmap_scan_plan

**Signature**: `static BitmapHeapScan *create_bitmap_scan_plan(PlannerInfo *root, BitmapHeapPath *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3202`.

**Dispatching context**: Routed for `T_BitmapHeapScan`.

**Output Plan struct**: `BitmapHeapScan` (`plannodes.h:538`) with `bitmapqualorig` (original quals for recheck).

**Tlist handling**: From caller.

**Qual handling**: Calls `create_bitmap_subplan` to recursively build the bitmapqual tree (yielding a Plan tree of BitmapAnd/BitmapOr/BitmapIndexScan and the original qual list). Then computes qpqual = scan_clauses minus indexquals (using equality, EC matching, and `predicate_implied_by`). Finally drops from `bitmapqualorig` any clauses that ended up in qpqual (avoiding double-evaluation).

**Var-reference handling**: `replace_nestloop_params` on qpqual and bitmapqualorig (the inner subplan tree handled its own).

**Node-specific quirks**:
- `bitmap_subplan_mark_shared(bitmapqualplan)` is called when `parallel_aware`, marking the bitmap as shared so workers can probe it together.
- Predicate proof is more aggressive than for plain IndexScan because bitmap rechecks are unavoidable when scans become lossy.

**Source file references**: `createplan.c:3202-3309`.

---

## create_bitmap_subplan (helper, recursive)

**Signature**: `static Plan *create_bitmap_subplan(PlannerInfo *root, Path *bitmapqual, List **qual, List **indexqual, List **indexECs)` at `src/backend/optimizer/plan/createplan.c:3332`.

**Dispatching context**: Called by `create_bitmap_scan_plan` (line 3221) and recursively by itself for BitmapAnd/BitmapOr children. Dispatches via `IsA(bitmapqual, BitmapAndPath/BitmapOrPath/IndexPath)`.

**Output Plan struct**: One of:
- `BitmapAnd` (`plannodes.h:356`) — for `IsA(BitmapAndPath)`.
- `BitmapOr` (`plannodes.h:370`) — for `IsA(BitmapOrPath)`. Single-element BitmapOrPath collapses to its lone child.
- `BitmapIndexScan` (`plannodes.h:520`) — for `IsA(IndexPath)`. Built by first calling `create_indexscan_plan` to get an IndexScan, then `make_bitmap_indexscan(scanrelid, indexid, indexqual, indexqualorig)` extracts the relevant fields into a BitmapIndexScan.

**Tlist / qual handling**: BitmapAnd/BitmapOr nodes have NIL targetlist and qual. The output parameters `qual`, `indexqual`, `indexECs` accumulate the original and indexable qual lists across the entire subtree, returned for use in the BitmapHeap above.

**Var-reference handling**: Inherited from `create_indexscan_plan` calls (which run `replace_nestloop_params`).

**Node-specific quirks**:
- For BitmapOrPath with a constant-TRUE child, sets `*qual = NIL` to short-circuit OR-with-true.
- For BitmapIndexScan, also adds index predicates (from `indexinfo->indpred`) to the `subquals`/`subindexquals` outputs (so partial-index predicates can be reused as quals).
- Costs of BitmapAnd/BitmapOr are copied directly from the corresponding Path node.

**Source file references**: `createplan.c:3332-3532`.

---

## create_tidscan_plan

**Signature**: `static TidScan *create_tidscan_plan(PlannerInfo *root, TidPath *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3540`.

**Dispatching context**: Routed for `T_TidScan`.

**Output Plan struct**: `TidScan` (`plannodes.h:552`) with `tidquals` field.

**Tlist handling**: From caller.

**Qual handling**: Removes scan_clauses redundant with tidquals. For single tidqual, uses pointer equality and `is_redundant_derived_clause`. For multiple tidquals (OR semantics), converts to an explicit OR clause and uses `equal()` matching.

**Var-reference handling**: `replace_nestloop_params` on both `tidquals` and `scan_clauses`.

**Source file references**: `createplan.c:3540-3629`.

---

## create_tidrangescan_plan

**Signature**: `static TidRangeScan *create_tidrangescan_plan(PlannerInfo *root, TidRangePath *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3637`.

**Dispatching context**: Routed for `T_TidRangeScan`.

**Output Plan struct**: `TidRangeScan` (`plannodes.h:565`).

**Tlist handling**: From caller.

**Qual handling**: Drops from scan_clauses any clauses already covered by `tidrangequals`. The remaining scan_clauses become `qpqual`.

**Var-reference handling**: `replace_nestloop_params` on qpqual and tidrangequals.

**Source file references**: `createplan.c:3637-3699`.

---

## create_subqueryscan_plan

**Signature**: `static SubqueryScan *create_subqueryscan_plan(PlannerInfo *root, SubqueryScanPath *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3702`.

**Dispatching context**: Routed for `T_SubqueryScan`.

**Output Plan struct**: `SubqueryScan` (`plannodes.h:598`) embedding the subplan.

**Tlist handling**: From caller.

**Qual handling**: Standard order + extract.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses, plus `process_subquery_nestloop_params(root, rel->subplan_params)` to set up Params for lateral references into the subquery.

**Node-specific quirks**: Calls `create_plan(rel->subroot, best_path->subpath)` (NOT `create_plan_recurse`) — the subquery has its own PlannerInfo / planning context, and recursing through `create_plan_recurse` would mix contexts. The subroot's curOuterRels/curOuterParams are managed across this call.

**Source file references**: `createplan.c:3702-3753`.

---

## create_functionscan_plan

**Signature**: `static FunctionScan *create_functionscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3761`.

**Dispatching context**: Routed for `T_FunctionScan` (best_path is a plain Path with that pathtype).

**Output Plan struct**: `FunctionScan` (`plannodes.h:609`) with `functions` list and `funcordinality`.

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on both scan_clauses *and* the function expression list (`functions`) — the function arguments may reference outer rels in LATERAL references.

**Source file references**: `createplan.c:3761-3796`.

---

## create_tablefuncscan_plan

**Signature**: `static TableFuncScan *create_tablefuncscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3804`.

**Dispatching context**: Routed for `T_TableFuncScan`.

**Output Plan struct**: `TableFuncScan` (`plannodes.h:630`).

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses and the `TableFunc` itself.

**Source file references**: `createplan.c:3804-3839`.

---

## create_valuesscan_plan

**Signature**: `static ValuesScan *create_valuesscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3847`.

**Dispatching context**: Routed for `T_ValuesScan`.

**Output Plan struct**: `ValuesScan` (`plannodes.h:620`).

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses and the `values_lists` (each row's expressions).

**Source file references**: `createplan.c:3847-3883`.

---

## create_ctescan_plan

**Signature**: `static CteScan *create_ctescan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3891`.

**Dispatching context**: Routed for `T_CteScan` when `rte->rtekind == RTE_CTE && !rte->self_reference`.

**Output Plan struct**: `CteScan` (`plannodes.h:640`).

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Node-specific quirks**: Walks up the levelsup chain (`cteroot`) to find the level that owns the CTE, then locates the previously-built `SubPlan` in `cteroot->init_plans` matching the CTE name. The CTE's `plan_id` and `setParam[0]` (the CTE's communication param) are threaded into the plan via `make_ctescan(tlist, scan_clauses, scan_relid, plan_id, cte_param_id)`.

**Source file references**: `createplan.c:3891-3977`.

---

## create_namedtuplestorescan_plan

**Signature**: `static NamedTuplestoreScan *create_namedtuplestorescan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:3986`.

**Dispatching context**: Routed for `T_NamedTuplestoreScan` (`rtekind == RTE_NAMEDTUPLESTORE`).

**Output Plan struct**: `NamedTuplestoreScan` (`plannodes.h:651`) with `enrname` field.

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Source file references**: `createplan.c:3986-4016`.

---

## create_resultscan_plan

**Signature**: `static Result *create_resultscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:4025`.

**Dispatching context**: Routed for `T_Result` when the path is a plain `Path` with `rtekind == RTE_RESULT` (not a ProjectionPath, MinMaxAggPath, or GroupResultPath — those are routed separately in `create_plan_recurse`).

**Output Plan struct**: `Result` (`plannodes.h:196`).

**Tlist handling**: From caller.

**Qual handling**: Standard. The scan_clauses become the Result's filter (regular qual, not `resconstantqual`).

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Source file references**: `createplan.c:4025-4054`.

---

## create_worktablescan_plan

**Signature**: `static WorkTableScan *create_worktablescan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:4062`.

**Dispatching context**: Routed for `T_WorkTableScan` (`rtekind == RTE_CTE && rte->self_reference`).

**Output Plan struct**: `WorkTableScan` (`plannodes.h:661`) with `wtParam` field.

**Tlist handling**: From caller.

**Qual handling**: Standard.

**Var-reference handling**: `replace_nestloop_params` on scan_clauses.

**Node-specific quirks**: The worktable param ID lives one level *below* where the CTE was declared (the level that owns the RecursiveUnion). The function walks up `levelsup - 1` to that level then reads `cteroot->wt_param_id`.

**Source file references**: `createplan.c:4062-4114`.

---

## create_foreignscan_plan

**Signature**: `static ForeignScan *create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:4122`.

**Dispatching context**: Routed for `T_ForeignScan`.

**Output Plan struct**: `ForeignScan` (`plannodes.h:707`) — but actually built by the FDW's `GetForeignPlan` callback. This function provides the FDW with the necessary inputs and post-processes the result.

**Tlist handling**: Tlist comes from the caller; FDW's `GetForeignPlan` may rewrite it.

**Qual handling**: scan_clauses are passed to the FDW for selection between local-recheck quals and remote-pushdown quals.

**Var-reference handling**: For foreign joins, transforms `fdw_outerpath` recursively into a subplan via `create_plan_recurse`. The FDW handles `replace_nestloop_params` itself (or asks core to via `replace_nestloop_params` on the relevant fields). For foreign-base-rel scans, retrieves the table OID for the FDW.

**Node-specific quirks**: The FDW returns a fully-formed ForeignScan plan; this creator only wraps and copies cost/path info.

**Source file references**: `createplan.c:4122-4275`.

---

## create_customscan_plan

**Signature**: `static CustomScan *create_customscan_plan(PlannerInfo *root, CustomPath *best_path, List *tlist, List *scan_clauses)` at `src/backend/optimizer/plan/createplan.c:4277`.

**Dispatching context**: Routed for `T_CustomScan`.

**Output Plan struct**: `CustomScan` (`plannodes.h:739`) — built by the extension's `methods->PlanCustomPath` callback.

**Tlist handling**: From caller; extension may rewrite.

**Qual handling**: scan_clauses passed to extension's PlanCustomPath.

**Var-reference handling**: Recursively transforms `custom_paths` children via `create_plan_recurse` first, then calls `methods->PlanCustomPath`. After the extension returns, runs `replace_nestloop_params` on `cplan->scan.plan.qual` and `cplan->custom_exprs` (so extensions don't have to think about nestloop param substitution).

**Source file references**: `createplan.c:4277-4338`.
