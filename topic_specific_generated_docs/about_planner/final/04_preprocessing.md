# 04. Preprocessing

Prerequisites: [03_lifecycle_and_entry_points.md](./03_lifecycle_and_entry_points.md).

This module documents what `subquery_planner` does to a `Query` before
the cost-based machinery sees it. Source: most of
`src/backend/optimizer/prep/`, plus a few tightly-coupled helpers in
`subselect.c` and `prepqual.c`.

---

## 1. Why preprocessing exists

Many "logical" rewrites simplify what the cost-based machinery has to
consider:

- **Pull SubLinks into joins** so they can be cost-optimized as inner
  joins or semi-joins instead of correlated `SubPlan`s.
- **Pull subqueries up** into the parent's jointree when it is
  semantically legal, so the parent and subquery share one search
  space.
- **Reduce outer joins to inner joins** when an upper qual is strict
  on the nullable side.
- **Canonicalize quals** into a normal form (CNF-ish) so that
  selectivity estimation, equivalence-class extraction, and
  predicate-implication tests all see consistent shapes.
- **Constant-fold expressions** so the cost model sees realistic
  shapes.
- **Flatten join aliases** so that downstream code never has to
  traverse `joinaliasvars`.

Preprocessing must run before scan/join planning because the
path-generation machinery assumes the jointree it sees is the
canonical one.

---

## 2. Symbol table

| Symbol                    | File:line                                           | Importance |
|---------------------------|-----------------------------------------------------|------------|
| `pull_up_subqueries`      | `src/backend/optimizer/prep/prepjointree.c:934`     | 0.82 |
| `pull_up_sublinks`        | `src/backend/optimizer/prep/prepjointree.c:453`     | 0.78 |
| `flatten_join_alias_vars` | `src/backend/optimizer/util/var.c`                  | 0.55 |
| `reduce_outer_joins`      | `src/backend/optimizer/prep/prepjointree.c`         | 0.65 |
| `canonicalize_qual`       | `src/backend/optimizer/prep/prepqual.c`             | 0.65 |
| `eval_const_expressions`  | `src/backend/optimizer/util/clauses.c`              | 0.60 |
| `preprocess_targetlist`   | `src/backend/optimizer/prep/preptlist.c`            | 0.55 |
| `preprocess_aggrefs`      | `src/backend/optimizer/prep/prepagg.c`              | 0.50 |
| `flatten_simple_union_all`| `src/backend/optimizer/prep/prepjointree.c`         | 0.45 |
| `transform_MERGE_to_join` | `src/backend/optimizer/prep/prepjointree.c`         | 0.40 |
| `replace_empty_jointree`  | `src/backend/optimizer/prep/prepjointree.c`         | 0.40 |
| `preprocess_function_rtes`| `src/backend/optimizer/prep/prepjointree.c`         | 0.40 |
| `remove_useless_result_rtes` | `src/backend/optimizer/prep/prepjointree.c`      | 0.40 |
| `plan_set_operations`     | `src/backend/optimizer/prep/prepunion.c`            | 0.55 |
| `expand_inherited_rtentry`| `src/backend/optimizer/util/inherit.c`              | 0.55 |

---

## 3. Preprocessing pipeline (call order)

In `subquery_planner` (planner.c:629), the order is fixed and
matters. From top to bottom (planner.c:694-1117):

1. **`SS_process_ctes`** (subselect.c) — for every WITH item: either
   inline (via `inline_cte`) or plan separately and stash an
   InitPlan-ish `SubPlan` in `glob->subplans`.
2. **`transform_MERGE_to_join`** — MERGE's join is restructured into
   a left-join of source onto target, plus action-dispatch quals
   that the executor will follow.
3. **`replace_empty_jointree`** — empty FROM ⇒ insert a single
   `RTE_RESULT` so the rest of the planner doesn't need a special
   case.
4. **`pull_up_sublinks`** — top-level WHERE-clause `[NOT] EXISTS`
   and `IN (...)` SubLinks become semi/anti joins.
5. **`preprocess_function_rtes`** — constant-fold function-RTE
   arguments; inline `SQL` functions if inlinable.
6. **`pull_up_subqueries`** — every RTE_SUBQUERY that is "simple"
   gets inlined into the parent's jointree.
7. **`flatten_simple_union_all`** — top-level UNION ALL becomes an
   AppendRel. Done after pull-up because UNION-ALL leaves weren't
   visible to pull-up (they're pointed to by `setOperations`, not
   `jointree`).
8. **Range-table survey** — sets `hasJoinRTEs`, `hasLateralRTEs`,
   `hasOuterJoins`, `hasResultRTEs`; clears `inh` flags on tables
   that actually have no children.
9. **View-permission spot checks** — `ExecCheckOneRelPerms` per
   view.
10. **`preprocess_rowmarks`** — record FOR UPDATE / FOR SHARE
    intentions.
11. **`preprocess_expression`** runs over **every** expression:
    targetList, withCheckOptions, returningList, jointree quals
    (via `preprocess_qual_conditions`), havingQual, window offsets,
    LIMIT, ON CONFLICT, MERGE actions, append_rel_list, and per-RTE
    specials.
12. **Drop `joinaliasvars`** lists from RTEs (no longer valid).
13. **HAVING → WHERE** transfer where possible (planner.c:1072).
14. **`reduce_outer_joins`** if `hasOuterJoins`.
15. **`remove_useless_result_rtes`** if `hasResultRTEs ||
    hasOuterJoins`.

After this, `grouping_planner` is called.

The PRE subgraph of the [planner pipeline diagram](./02_architecture_overview.md#1-top-level-pipeline)
visualizes this order.

---

## 4. `pull_up_sublinks` — turn SubLinks into joins

### 4.1 Signature

```c
void pull_up_sublinks(PlannerInfo *root);
```

Source: `src/backend/optimizer/prep/prepjointree.c:453`.

### 4.2 What it does

For `WHERE` and inner `JOIN/ON` clauses, find SubLinks of kind:

- `ANY_SUBLINK` (e.g. `x = ANY (subquery)`, including `IN
  (subquery)`)
- `EXISTS_SUBLINK`

and convert them into JoinExpr nodes added to the jointree:

- `ANY_SUBLINK` → `JOIN_SEMI`
- `EXISTS_SUBLINK` → `JOIN_SEMI` (with simplified subquery)
- `NOT (EXISTS / ANY)` → `JOIN_ANTI`

Internals:

- `pull_up_sublinks_jointree_recurse` walks the jointree.
- For each SubLink found in a qual position:
  - `convert_ANY_sublink_to_join` (subselect.c) builds the
    JoinExpr.
  - `convert_EXISTS_sublink_to_join` first calls
    `simplify_EXISTS_query` (drop the subquery's targetList, GROUP
    BY, ORDER BY because EXISTS only cares about row existence).
- Successful conversion replaces the SubLink with a `RangeTblRef`
  to the pulled-up subquery and inserts a `JoinExpr` at the
  appropriate level.

### 4.3 Constraints / limits

- Only top-level (i.e. not inside `OR`) SubLinks in WHERE / inner
  JOIN ON positions are eligible. Pulling something out of an OR
  would silently change semantics.
- Lateral correlation: the converted subquery's lateral references
  are preserved by marking the resulting RTE as `lateral`.

### 4.4 Why it runs before `pull_up_subqueries`

Pulling a SubLink up may **introduce a new RTE_SUBQUERY**. The next
pass (`pull_up_subqueries`) inlines it if it's simple enough.

See [12_subquery_and_sublink.md](./12_subquery_and_sublink.md) for
the conversion details and SubLink-kind dispatch table.

---

## 5. `pull_up_subqueries` — inline simple subqueries

### 5.1 Signature

```c
void pull_up_subqueries(PlannerInfo *root);
```

Source: `src/backend/optimizer/prep/prepjointree.c:934`.

### 5.2 Outline

The recursion walks the jointree:

- `pull_up_subqueries_recurse` per node.
- For an `RTE_SUBQUERY` reachable through `RangeTblRef`:
  - Test eligibility with `is_simple_subquery`. Conditions include
    (non-exhaustive): no LIMIT/OFFSET, no DISTINCT/HAVING, no
    aggregates, no window functions, no SRF in targetlist, no
    GROUP BY, no security barrier conflicts, jointree non-empty
    (else use UNION-ALL pull-up below), and pull-up doesn't break
    LATERAL.
  - If eligible, recursively pull up the subquery first (so its own
    sublinks/subqueries are pulled up before we splice it in).
  - Splice the subquery's RTEs and jointree fragment into the
    parent; rewrite Vars in the parent that reference the
    subquery's columns using `pullup_replace_vars`.
- For UNION ALL leaf subqueries (under a parent `setop` tree), call
  `pull_up_simple_union_all`, which builds the AppendRel.

### 5.3 Var rewriting

After pull-up, references to the subquery's `varattno = k` columns
must be replaced by the actual expressions in the subquery's
targetlist. This is done by `pullup_replace_vars`. PlaceHolderVars
are introduced wherever the substituted expression could be NULL'd
by an enclosing outer join. See
[10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md)
and [11_restrictinfo_and_clause_utils.md](./11_restrictinfo_and_clause_utils.md) for
the role of PHVs.

### 5.4 Why pull-up matters

The DP / GEQO join search can only choose orderings within a single
jointree. A non-pulled-up subquery is a black box — its inner
planning is done independently and the cost-based optimizer cannot
interleave its joins with the parent's. Pulling up unlocks
reordering and EC sharing.

---

## 6. `flatten_join_alias_vars`

Source: `src/backend/optimizer/util/var.c`.

After pull-up, RTE_JOIN entries still have `joinaliasvars` lists
describing how each output column maps to underlying base-rel Vars.
`preprocess_expression` calls `flatten_join_alias_vars` to substitute
these expansions in-line so later code never has to consult the
joinaliasvars lists.

After preprocessing, all `rte->joinaliasvars` are set to `NIL`
(`planner.c:1031-1039`) — leaving them around would be a hazard
because they no longer match how Vars elsewhere look.

---

## 7. `reduce_outer_joins`

Source: `src/backend/optimizer/prep/prepjointree.c`.

Recursively walks the jointree. For each LEFT/RIGHT/FULL JOIN it
asks: does some upper-level qual "force" the nullable side to be
non-NULL? If a filter `WHERE rhs.col IS NOT NULL` (or a strict op
like `rhs.col = 5`) exists above a LEFT JOIN, then null-extended
rows would have been discarded anyway, so the LEFT JOIN can be
**demoted** to INNER JOIN.

It uses `find_nonnullable_rels` (clauses.c) to determine which rels
must be non-null at a given position. The reduction is deferred
until **after** preprocessing has finished `eval_const_expressions`
and `canonicalize_qual`, because constant-folding can expose
strictness that wasn't apparent in the parse tree (e.g. `WHERE
rhs.col = 1::int` becomes strict on `rhs.col`).

When a LEFT JOIN is reduced to INNER, its `JoinExpr->jointype` flips
and its quals become normal join quals — no longer outer-join-only.

This optimization unlocks reordering by `deconstruct_jointree`,
because INNER joins can commute with each other while OUTER joins
generally cannot.

---

## 8. `canonicalize_qual` and friends

### 8.1 `canonicalize_qual`

Source: `src/backend/optimizer/prep/prepqual.c`.

```c
Expr *canonicalize_qual(Expr *qual, bool is_check);
```

Pulls nested `AND`s and `OR`s into N-argument flat form (no `AND`
directly under `AND`, no `OR` under `OR`). Eliminates trivially
true/false branches. For non-CHECK quals it also runs
`find_duplicate_ors` to collapse redundant OR arms.

### 8.2 Call sites

`preprocess_expression` calls `canonicalize_qual` on every
`EXPRKIND_QUAL` (planner.c:1207-1215).

### 8.3 Why flat AND/OR matters

Selectivity estimation, predicate implication, and equivalence-class
extraction all rely on flat AND/OR. A nested `(AND (AND a b) c)`
would make `clauselist_selectivity` see two clauses instead of
three.

### 8.4 `eval_const_expressions`

Source: `src/backend/optimizer/util/clauses.c`.

- Constant-folds expressions.
- Inlines simple SQL functions.
- Reduces `OpExpr` over `Const` args.
- Converts named-argument calls to positional and inlines default
  args (planner.c:1196-1199 explains why this MUST happen for every
  expression).
- Flattens nested `AndExpr`/`OrExpr`.

After this, `make_ands_implicit` (planner.c:1246) converts the
top-level `AndExpr` of a qual into a `List` (implicit-AND format)
for downstream processing.

---

## 9. `preprocess_targetlist` and `preprocess_aggrefs`

### 9.1 `preprocess_targetlist`

Source: `src/backend/optimizer/prep/preptlist.c`.

Called from `grouping_planner`. Computes `root->processed_tlist`:

- Adds resjunk columns for sort/group expressions not already in the
  targetlist (so they're available at the top of the scan/join
  layer).
- Expands `*` already if not done by parser.
- For UPDATE/DELETE, adds row-identity columns (junk vars
  referencing CTID/OID/system attrs of the target relation).
- Records `update_colnos` for UPDATE.

### 9.2 `preprocess_aggrefs`

Source: `src/backend/optimizer/prep/prepagg.c`.

Walks `processed_tlist` + `havingQual` to find all `Aggref` and
`GroupingFunc` nodes. Splits aggregates into "AggInfo" /
"AggTransInfo" entries that are de-duplicated, since multiple
Aggrefs can share a transition. Decides `AGGSPLIT` mode (SIMPLE,
INITIAL_SERIAL, FINAL_DESERIAL, ...) for the Agg nodes that will be
created later.

The split modes matter for parallel aggregation and partitionwise
aggregation; see [14_parallel_planning.md](./14_parallel_planning.md)
and [13_inheritance_and_partitioning.md](./13_inheritance_and_partitioning.md).

---

## 10. Set-operations preprocessing

Source: `src/backend/optimizer/prep/prepunion.c`.

When `parse->setOperations` is non-NULL the query is a
UNION/INTERSECT/EXCEPT set-op tree. `grouping_planner` short-circuits
its normal flow and calls `plan_set_operations`, which:

1. Recursively plans each leaf via `subquery_planner`.
2. Builds an `Append` (UNION ALL), `SetOp` (INTERSECT / EXCEPT), or
   `Sort + SetOp` chain.
3. Sets up the resulting `RelOptInfo` with appropriate paths.

UNION ALL with simple leaves is normally handled earlier by
`flatten_simple_union_all` so it becomes an AppendRel and is planned
together with the parent.

---

## 11. `transform_MERGE_to_join` and `replace_empty_jointree`

### 11.1 `transform_MERGE_to_join`

For `MERGE INTO target USING source ON cond`, the parser leaves a
JoinExpr with the source on the right and `mergeActionList` on the
side. This pass massages it into a structure the planner can cost as
a left-join with action-dispatch quals.

### 11.2 `replace_empty_jointree`

A `SELECT 1` (no FROM) produces an empty `jointree.fromlist`. Rather
than have every downstream module special-case "no FROM", we insert
a single RTE_RESULT so the jointree always has at least one element.
This is also the rel handled by the trivial fast path in
`query_planner` (planmain.c:93-159, see
[03_lifecycle_and_entry_points.md](./03_lifecycle_and_entry_points.md#api-query_planner)).

---

## 12. `preprocess_function_rtes`

Source: `src/backend/optimizer/prep/prepjointree.c`.

For each RTE_FUNCTION:

- Const-simplify the function expression.
- If the function is a SQL function and inlinable (immutable
  single-target SQL function), inline it as a SubQuery RTE so
  `pull_up_subqueries` can fold it into the parent. This is how
  `LANGUAGE sql STABLE` functions get treated as views.

---

## 13. Performance characteristics

| Function                | Cost | Notes |
|-------------------------|------|-------|
| `pull_up_sublinks`      | O(jointree-size + sublinks) | One linear walk. |
| `pull_up_subqueries`    | O(sum of subquery sizes) | Recursive; pulled-up subqueries are pulled up themselves first. |
| `flatten_join_alias_vars`| O(expression size × join-RTE depth) | Cached per call via `flatten_join_alias_vars_walker`. |
| `reduce_outer_joins`    | O(jointree-size × #quals) | Calls `find_nonnullable_rels` per qual. |
| `canonicalize_qual`     | O(qual size) | Linear in expression nodes. |
| `eval_const_expressions`| O(expression size); recursive | May inline SQL functions, increasing depth. |
| `preprocess_targetlist` | O(targetlist + groupClause + ...) | |
| `plan_set_operations`   | O(set-op tree size × per-leaf planning) | Each leaf is its own `subquery_planner` recursion. |

Most queries spend < 1 ms here. Pathological cases: huge IN(...)
lists, deeply nested subqueries (each level forces another
`subquery_planner` recursion), or many INSERT/UPDATE columns
(preprocess_targetlist work).

---

## 14. Cross-references

- Main pipeline: [03_lifecycle_and_entry_points.md](./03_lifecycle_and_entry_points.md)
- SubLink internals + `SS_process_sublinks` /
  `convert_ANY_sublink_to_join`: [12_subquery_and_sublink.md](./12_subquery_and_sublink.md)
- Outer-join legality and identity-3 clones:
  [05_initial_setup_and_jointree.md](./05_initial_setup_and_jointree.md)
- Predicate implication, `find_nonnullable_rels`:
  [11_restrictinfo_and_clause_utils.md](./11_restrictinfo_and_clause_utils.md)
- Inheritance pre-expansion (`expand_inherited_rtentry`):
  [13_inheritance_and_partitioning.md](./13_inheritance_and_partitioning.md)
- GUC reference (`from_collapse_limit`, `join_collapse_limit`):
  [appendix_guc_parameters.md](./appendix_guc_parameters.md)

---

Next: [05_initial_setup_and_jointree.md](./05_initial_setup_and_jointree.md)
