# Component: Subqueries, SubLinks, and Join Removal

> Stage 2 documentation for **SUBQUERY_AND_TRANSFORMATIONS**.
> Sources:
> - `src/backend/optimizer/plan/subselect.c` (~3000 lines): SubLink
>   conversion, SubPlan / InitPlan construction, SS_finalize_plan.
> - `src/backend/optimizer/plan/analyzejoins.c`: useless-join removal,
>   semijoin reduction.
> - `src/backend/optimizer/prep/prepjointree.c`: subquery and sublink
>   pull-up (also covered in `component_preprocessing.md`).
> - `src/backend/optimizer/plan/planagg.c`: MIN/MAX aggregate
>   optimization.
>
> Diagram: `diagrams/10_subquery_handling_decision.mermaid`.

## 1. Why this exists

A "subquery" can take many forms in SQL:
- A FROM-clause subquery (RTE_SUBQUERY).
- A WITH list (CTEs).
- An ANY/ALL/EXISTS predicate (SubLink in WHERE).
- A scalar subquery in an expression context (e.g.
  `SELECT (SELECT max(x) FROM t)`).
- A correlation reference inside any of the above.

The planner's job is to:
1. **Pull up** what can be pulled up so the cost-based optimizer sees
   one big query.
2. For what can't be pulled up, decide between **SubqueryScan** (FROM
   subquery, planned independently), **InitPlan** (uncorrelated
   scalar subquery, runs once), **SubPlan** (correlated, runs per
   row), and **AlternativeSubPlan** (planner emits both forms;
   executor picks at runtime).
3. Remove **useless joins** (LEFT JOIN where the inner is unique on
   the join key and unreferenced from above) and reduce
   **semi-joins** to inner joins when the RHS is unique.
4. Recognize **MIN/MAX** patterns that can be served by an
   index-ordered scan + LIMIT 1.

---

## 2. Symbol table

| Symbol                                | File:line                                          | Importance | Tier |
|---------------------------------------|----------------------------------------------------|------------|------|
| `convert_ANY_sublink_to_join`         | `src/backend/optimizer/plan/subselect.c`           | 0.65 | 2 |
| `convert_EXISTS_sublink_to_join`      | `src/backend/optimizer/plan/subselect.c`           | 0.65 | 2 |
| `simplify_EXISTS_query`               | `src/backend/optimizer/plan/subselect.c`           | 0.50 | 2 |
| `convert_EXISTS_to_ANY`               | `src/backend/optimizer/plan/subselect.c`           | 0.45 | 3 |
| `convert_VALUES_to_ANY`               | `src/backend/optimizer/plan/subselect.c`           | 0.40 | 3 |
| `SS_process_ctes`                     | `src/backend/optimizer/plan/subselect.c`           | 0.55 | 2 |
| `SS_process_sublinks`                 | `src/backend/optimizer/plan/subselect.c`           | 0.65 | 2 |
| `SS_replace_correlation_vars`         | `src/backend/optimizer/plan/subselect.c`           | 0.55 | 2 |
| `SS_assign_special_param`             | `src/backend/optimizer/plan/subselect.c`           | 0.45 | 3 |
| `SS_charge_for_initplans`             | `src/backend/optimizer/plan/subselect.c`           | 0.40 | 3 |
| `SS_identify_outer_params`            | `src/backend/optimizer/plan/subselect.c`           | 0.40 | 3 |
| `SS_finalize_plan`                    | `src/backend/optimizer/plan/subselect.c`           | 0.65 | 2 |
| `make_subplan` / `build_subplan`      | `src/backend/optimizer/plan/subselect.c`           | 0.55 | 2 |
| `inline_cte`                          | `src/backend/optimizer/plan/subselect.c`           | 0.45 | 3 |
| `preprocess_minmax_aggregates`        | `src/backend/optimizer/plan/planagg.c`             | 0.50 | 2 |
| `build_minmax_path`                   | `src/backend/optimizer/plan/planagg.c`             | 0.45 | 3 |
| `remove_useless_joins`                | `src/backend/optimizer/plan/analyzejoins.c`        | 0.55 | 2 |
| `join_is_removable`                   | `src/backend/optimizer/plan/analyzejoins.c`        | 0.50 | 2 |
| `reduce_unique_semijoins`             | `src/backend/optimizer/plan/analyzejoins.c`        | 0.50 | 2 |
| `rel_supports_distinctness`           | `src/backend/optimizer/plan/analyzejoins.c`        | 0.40 | 3 |
| `innerrel_is_unique`                  | `src/backend/optimizer/plan/analyzejoins.c`        | 0.50 | 2 |

---

## 3. SubLink kinds and their handling

`SubLink.subLinkType` enumerates:

| Kind                | Meaning                          | Conversion goal |
|---------------------|----------------------------------|-----------------|
| `EXISTS_SUBLINK`    | `EXISTS (subquery)`              | JOIN_SEMI |
| `ALL_SUBLINK`       | `expr op ALL (subquery)`         | mostly stays as SubPlan |
| `ANY_SUBLINK`       | `expr op ANY (subquery)` / `IN`  | JOIN_SEMI |
| `ROWCOMPARE_SUBLINK`| `(a, b) op (subquery)`           | SubPlan |
| `EXPR_SUBLINK`      | `(SELECT ...)` scalar context    | InitPlan / SubPlan |
| `MULTIEXPR_SUBLINK` | `(a, b) := (subquery)` (UPDATE)  | SubPlan with multi-output |
| `ARRAY_SUBLINK`     | `ARRAY(subquery)`                | SubPlan |
| `CTE_SUBLINK`       | (synthetic) reference to CTE     | resolved by SS_process_ctes |

`pull_up_sublinks` handles `ANY` and `EXISTS` (and their `NOT`
versions) at top-level WHERE / inner JOIN ON positions, producing
JoinExpr nodes. Anything else is left for `SS_process_sublinks` (in
`preprocess_expression`) which produces `SubPlan` / `InitPlan` /
`AlternativeSubPlan` nodes.

See `diagrams/10_subquery_handling_decision.mermaid`.

---

## 4. `convert_ANY_sublink_to_join`

Source: `src/backend/optimizer/plan/subselect.c`.

Given `lhs op ANY (SELECT y FROM ...)`, the conversion:
1. Inserts the subquery's range table into the parent's rangetable
   as RTE_SUBQUERY.
2. Constructs a JoinExpr with `jointype = JOIN_SEMI`,
   `larg = original jointree position`, `rarg = RangeTblRef to the
   pulled-up subquery`, and `quals` set to `lhs op subquery_output`.
3. Returns the new JoinExpr (caller splices it in).

Eligibility checks (in the caller `pull_up_sublinks_qual_recurse`):
- The SubLink isn't inside an OR (would change semantics).
- The SubLink isn't volatile / set-returning in a forbidden context.
- The qual is in a position where converting to SEMI-JOIN preserves
  semantics (top of WHERE, or in INNER JOIN ON).

After conversion, the inner subquery is itself a candidate for
`pull_up_subqueries`. The pulled-up subquery's `tlist` becomes
referenceable Vars in the parent.

---

## 5. `convert_EXISTS_sublink_to_join`

Outline:
1. **Simplify the EXISTS subquery** via `simplify_EXISTS_query`:
   - Replace the targetlist with a single `SELECT 1` (EXISTS only
     cares about row existence).
   - Drop ORDER BY / DISTINCT / GROUP BY (no effect on existence).
   - Drop LIMIT/OFFSET except in cases that change row presence
     (LIMIT 0 etc.).
   - Drop window functions etc.
2. **Build the JoinExpr** with `jointype = JOIN_SEMI` (or
   `JOIN_ANTI` for `NOT EXISTS`), with empty `quals` (the qualifier
   is implicit in the EXISTS structure).
3. The subquery's WHERE clause becomes the JOIN ON condition (the
   pulled-up subquery is now an inner relation to the parent).

### 5.1 `convert_EXISTS_to_ANY`
For `EXISTS (SELECT ... WHERE outer_var = inner_var)`, the planner
may rewrite to `outer_var = ANY (SELECT inner_var ...)` to produce a
JOIN_SEMI on a single equality clause, which downstream code costs
better. Eligibility tests: simple correlation pattern, no
GROUP/aggregates, etc.

### 5.2 `convert_VALUES_to_ANY`
`x = ANY (VALUES (1), (2), (3))` rewrites the VALUES into an array
and uses ScalarArrayOpExpr semantics, avoiding subquery overhead
entirely. Eligibility: small VALUES list.

---

## 6. CTE handling

### 6.1 `SS_process_ctes`
Source: `src/backend/optimizer/plan/subselect.c`.

For each `CommonTableExpr` in `parse->cteList`:
- If it's recursive: plan it via `subquery_planner` and stash a
  `Plan` referencing a `RecursiveUnion`. The non-recursive arm
  becomes the seed; the recursive arm scans `wt_param_id` work-table.
- If non-recursive and inlinable (single use, not MATERIALIZED, no
  side-effect operations): mark for inlining via `inline_cte_walker`.
- Else plan as a standalone subquery and store the resulting
  `SubPlan` in `glob->subplans`. Make this CTE referenceable via
  `cte_plan_ids`.

### 6.2 `inline_cte`
The actual walker that replaces `RangeTblRef → CteScan` references
with the CTE's parsed body. Done before `pull_up_subqueries` so the
inlined body has a chance to be pulled up further.

---

## 7. SubPlans, InitPlans, and AlternativeSubPlan

### 7.1 `SS_process_sublinks`
Walks an expression. For each remaining `SubLink`:
- Recursively process child expressions (so nested SubLinks are
  handled bottom-up).
- Call `make_subplan` to construct a `SubPlan` or `InitPlan` from the
  SubLink.
- Replace the SubLink with the resulting node.

### 7.2 `make_subplan` and `build_subplan`
- `make_subplan` decides which subplan-style is appropriate based on
  the SubLink kind and correlation:
  - **Uncorrelated EXPR_SUBLINK**: build an InitPlan (runs at
    most once; result cached in a PARAM_EXEC).
  - **Correlated EXPR_SUBLINK**: build a SubPlan (per-row).
  - **ANY/ALL with hashable RHS**: build a SubPlan with
    `useHashTable = true`; executor builds a hash of the RHS and
    probes per outer tuple (fast for non-pull-uppable IN cases).
  - **MULTIEXPR_SUBLINK**: SubPlan returning multiple outputs; the
    parent UPDATE references them via `multiexpr_params`.
- `build_subplan` does the actual `subquery_planner` recursion, then
  builds the SubPlan/Plan node.

### 7.3 InitPlan vs SubPlan
- **InitPlan**: uncorrelated. Stored in `Plan.initPlan` of the node
  that uses its result; executed once at plan startup.
- **SubPlan**: correlated. Stored inline in expressions. Executed
  per-row when its parameter values change.

### 7.4 AlternativeSubPlan
For ANY/ALL where both hashed and non-hashed implementations are
plausible, the planner builds **both** as `AlternativeSubPlan` and
the executor picks at run-time using `numCalls` * cost heuristics.

---

## 8. `SS_finalize_plan`

Source: `src/backend/optimizer/plan/subselect.c`.

Called from `standard_planner` after `set_plan_references`. It walks
the final `Plan` tree to:

1. **Compute `extParam` and `allParam`** for each plan node.
   `allParam` is the set of PARAM_EXEC IDs that any descendant node
   evaluates; `extParam` is the subset that this node depends on
   (i.e. supplied from outside).
2. **Determine `parallel_safe`** propagation up the plan tree.
3. **Number sub-plans** in `glob->subplans` and assign final IDs.
4. **`rewindPlanIDs`**: set membership for SubPlans that need their
   tuplestore rewound (e.g. when used inside a node that calls them
   multiple times with the same parameter).

`SS_finalize_plan` also moves InitPlans to the right nodes — InitPlans
are attached to the lowest plan node whose `extParam` set includes
the initplan's outputs.

### 8.1 `SS_assign_special_param`
Allocates a fresh PARAM_EXEC ID in `glob->paramExecTypes`. Used by
correlation vars, recursive worktables, alternative subplans, etc.

### 8.2 `SS_replace_correlation_vars`
Replaces `Var(varlevelsup > 0)` references with `Param(PARAM_EXEC,
paramid)` nodes during `preprocess_expression`. The mapping is
recorded in `root->plan_params` so the parent query's planner knows
which expressions to evaluate for each Param.

### 8.3 `SS_charge_for_initplans`
Adjust path costs in the topmost rel to account for InitPlan
execution (one-time cost added to startup of the plan that owns the
InitPlan).

### 8.4 `SS_identify_outer_params`
Records `root->outer_params` — the PARAM_EXEC IDs visible from outer
query levels. Used by lower-level subquery planning to know which
Params it can reference.

---

## 9. MIN/MAX optimization

Source: `src/backend/optimizer/plan/planagg.c`.

`preprocess_minmax_aggregates` recognizes:
```sql
SELECT min(x), max(y) FROM t WHERE ...;
```
where each min/max can be served by an index-ordered scan + LIMIT 1
on `t`. Conditions:
- The aggregate is `min` or `max` (datatype must have an opclass).
- Index `t(x)` exists with sort order matching the aggregate.
- The qual list is "safe" — the scan with extra WHERE filter would
  return matching rows in index order.
- No GROUP BY (the optimization is for ungrouped scalar aggregates).

`build_minmax_path` constructs one inner SortPath / IndexPath +
LimitPath per aggregate. The aggregates collectively become a
`MinMaxAggPath`; createplan turns this into a `Result` whose
expression refers to per-aggregate subplans.

If multiple aggregates can be optimized this way, all benefit. If
even one can't, the optimization is abandoned for the whole query.

---

## 10. `remove_useless_joins`

Source: `src/backend/optimizer/plan/analyzejoins.c`.

Walks `joinlist` produced by `deconstruct_jointree`. For each LEFT
JOIN, `join_is_removable` checks:
- The inner side is referenced ONLY by the join condition (no
  references in tlist, no references in joins above this).
- The inner is provably **unique** on its join columns (via
  `rel_supports_distinctness` — checks unique constraints, primary
  keys, EC-derived uniqueness).
- The join clauses are pure equalities on the join columns.

If yes, drop the LEFT JOIN entirely: the result is the same as just
the LHS. Update `joinlist` and remove the inner rel from
`simple_rel_array` markers.

This optimization is a big win for views / ORM-generated joins
where the joined table is never accessed.

---

## 11. `reduce_unique_semijoins`

Source: `src/backend/optimizer/plan/analyzejoins.c`.

For each `SpecialJoinInfo` with `jointype == JOIN_SEMI`, test via
`innerrel_is_unique` whether the inner side is provably unique on
the semi-join's RHS expressions. If yes, the SJ can be downgraded to
a plain inner join — at most one inner row matches each outer row,
so semi-join's "first match wins" is a no-op.

Effect: the SpecialJoinInfo's `jointype` becomes `JOIN_INNER` (or the
SJ is dropped if no constraints remain). This unlocks more join
orderings that were illegal under SEMI.

### 11.1 `innerrel_is_unique`
Checks (with caching in `rel->unique_for_rels` /
`rel->non_unique_for_rels`):
- Unique constraint / primary key over the join columns.
- Unique partial index whose predicate is implied.
- For sub-rels: the underlying subquery's `DISTINCT`/`GROUP BY` makes
  it unique.

---

## 12. Performance characteristics

- `pull_up_sublinks` / `pull_up_subqueries`: O(jointree size + sublink
  count). Each pulled-up subquery may trigger further pull-up.
- `SS_process_sublinks`: O(expression size). Each remaining SubLink
  triggers a `subquery_planner` recursion.
- `SS_finalize_plan`: O(plan tree size). Linear walk.
- `remove_useless_joins`: O(joinlist size × per-join uniqueness check).
  Uniqueness checks are cached per rel.

---

## 13. Cross-references

- Pull-up details: `component_preprocessing.md`
- SpecialJoinInfo / OJ legality (where SEMI's RHS-uniqueness lives):
  `component_initial_setup_and_jointree.md`
- `JoinDomain` and EC interactions:
  `component_equivalence_classes_and_pathkeys.md`
- The PlannerInfo fields touched here (`init_plans`, `cte_plan_ids`,
  `multiexpr_params`, `plan_params`, `outer_params`, `wt_param_id`):
  `component_lifecycle_and_entry_points.md`
- Plan finalization (where `SS_finalize_plan` runs):
  `component_plan_creation_and_setrefs.md`
- Diagram: `diagrams/10_subquery_handling_decision.mermaid`.
