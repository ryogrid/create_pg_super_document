# Plan Creator Catalog: Join Plan Creators

This catalog documents the three join plan creators (`create_nestloop_plan`, `create_mergejoin_plan`, `create_hashjoin_plan`) and the `create_join_plan` dispatcher. There is no separate `create_hash_plan` — the Hash node child of HashJoin is synthesized inline inside `create_hashjoin_plan` and never appears at the top level.

All join creators share a common pattern:
1. Build the path tlist via `build_path_tlist(root, &jpath.path)`.
2. Recurse on outer and inner subpaths via `create_plan_recurse`, passing different tlist-flag hints (CP_SMALL_TLIST when an inner Sort is needed, CP_EXACT_TLIST when not).
3. Sort `joinrestrictinfo` clauses with `order_qual_clauses`.
4. Split the join clauses into `joinclauses` and `otherclauses` via `extract_actual_join_clauses` (for outer joins) or `extract_actual_clauses` (for inner joins). The split distinguishes "real" join conditions from secondary "other" conditions that apply only to outer-join semantics.
5. Run `replace_nestloop_params` on the qual lists if the join is parameterized.
6. Call the type-specific `make_*` function and `copy_generic_path_info`.

---

## create_join_plan

**Signature**: `static Plan *create_join_plan(PlannerInfo *root, JoinPath *best_path)` at `src/backend/optimizer/plan/createplan.c:1082`.

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

**Tlist / qual handling**: Mostly delegated to the per-type creator. This dispatcher's primary role is to discriminate by pathtype and apply the gating-plan wrap.

**Var-reference handling**: None at this level.

**Node-specific quirks**: After the per-type creator returns, runs `get_gating_quals(root, joinrestrictinfo)` and wraps the join plan in a Result if any pseudoconstant clauses were found.

**Source file references**: `createplan.c:1082-1216`.

---

## create_nestloop_plan

**Signature**: `static NestLoop *create_nestloop_plan(PlannerInfo *root, NestPath *best_path)` at `src/backend/optimizer/plan/createplan.c:4348`.

**Dispatching context**: Called from `create_join_plan` for `pathtype == T_NestLoop`.

**Output Plan struct**: `NestLoop` (`plannodes.h:807`) — embeds `Join` (`plannodes.h:786`) and adds `nestParams` list.

**Tlist handling**: NestLoop can project, so children's tlists pass through unchanged (`flags = 0` to recursive calls).

**Qual handling**: Splits joinrestrictinfo into joinclauses / otherclauses for outer joins, single list for inner joins. Strips RestrictInfos and orders.

**Var-reference handling**:
- Calls `reparameterize_path_by_child(root, inner_path, outer_parent)` if the inner path is parameterized by the outer rel's topmost parent rather than the outer rel itself (a corner case affecting partition-wise join correctness).
- Saves `root->curOuterRels`, unions outer's relids into it for the inner subplan recursion (so inner-side nestloop param replacement knows which Vars are "outer"), and restores afterwards.
- After both subplans are built, calls `identify_current_nestloop_params(root, outerrelids)` to extract the nestloop Param assignments that this join is responsible for providing — these are removed from `root->curOuterParams` and stored in `nestParams` on the plan node.

**Node-specific quirks**:
- The split of nestloop params between this join and its parent is critical: a nestloop deep in the tree must not "claim" a param that's actually used by a higher join.
- Calls `make_nestloop(tlist, joinclauses, otherclauses, nestParams, outer_plan, inner_plan, jointype, inner_unique)`.

**Source file references**: `createplan.c:4348-4437`.

---

## create_mergejoin_plan

**Signature**: `static MergeJoin *create_mergejoin_plan(PlannerInfo *root, MergePath *best_path)` at `src/backend/optimizer/plan/createplan.c:4440`.

**Dispatching context**: Called from `create_join_plan` for `pathtype == T_MergeJoin`.

**Output Plan struct**: `MergeJoin` (`plannodes.h:833`) with `mergeFamilies`, `mergeCollations`, `mergeStrategies`, `mergeNullsFirst` arrays plus `skip_mark_restore` flag.

**Tlist handling**: MergeJoin can project. Outer/inner subplans get `CP_SMALL_TLIST` if a Sort is needed (to minimize sort tuple width); otherwise pass-through.

**Qual handling**:
- Splits joinrestrictinfo via `extract_actual_join_clauses` (outer joins) or `extract_actual_clauses` (inner joins).
- Pulls mergeclauses out of `path_mergeclauses` via `get_actual_clauses`.
- Computes `joinclauses = list_difference(joinclauses, mergeclauses)` so mergeclauses don't appear twice.
- `replace_nestloop_params` on joinclauses and otherclauses (mergeclauses asserted to have none).
- `get_switched_clauses(path_mergeclauses, outer_relids)` rearranges each mergeclause so the outer-rel Var is on the left.

**Var-reference handling**: As above. Mergeclause sides are guaranteed switched to outer-on-left for the executor.

**Node-specific quirks**:
- If `outersortkeys != NIL`, builds an explicit `Sort` node atop the outer subplan via `make_sort_from_pathkeys`, calls `label_sort_with_costsize` to fill in cost. Same for `innersortkeys`.
- If `materialize_inner` is set, wraps the inner side in a `Material` node (with cpu_operator_cost added per tuple as material overhead).
- The big loop at lines 4593-4715 walks `path_mergeclauses` and fills in the four arrays `mergefamilies/mergecollations/mergestrategies/mergenullsfirst` by matching each clause to outer and inner pathkeys via EquivalenceClass identity. Handles redundant pathkeys carefully (two clauses may match the same outer pathkey).
- Calls `make_mergejoin(tlist, joinclauses, otherclauses, mergeclauses, ...)`.

**Source file references**: `createplan.c:4440-4744`.

---

## create_hashjoin_plan

**Signature**: `static HashJoin *create_hashjoin_plan(PlannerInfo *root, HashPath *best_path)` at `src/backend/optimizer/plan/createplan.c:4747`.

**Dispatching context**: Called from `create_join_plan` for `pathtype == T_HashJoin`.

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

---

## On the Lack of `create_hash_plan`

There is no top-level `create_hash_plan` because the `Hash` plan node never stands alone. It is always synthesized as the immediate inner child of a HashJoin, inside `create_hashjoin_plan` (line 4878). The HashPath struct carries the hash-related metadata (`path_hashclauses`, `inner_rows_total`, `num_batches`), and the inner child of the HashPath is whatever subpath produces the rows to be hashed — typically a SeqScan or IndexScan.

This design contrasts with how other "wrapping" plans like `Sort`, `Material`, and `Memoize` are handled — those have their own Path types and their own top-level plan creators because the planner considers them as candidate paths in their own right (Sort can be placed atop any subpath; Material can be inserted to enable mark/restore). Hash, however, is meaningless except as part of a HashJoin, so it gets no Path subtype and no standalone creator.
