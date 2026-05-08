# Path Catalog: DML and Row-locking Paths

This file documents the two Path subtypes that introduce data-modifying or row-locking semantics: `ModifyTablePath` (the wrapper for INSERT/UPDATE/DELETE/MERGE) and `LockRowsPath` (the wrapper for `SELECT ... FOR UPDATE/SHARE`). Both sit at the top of their respective plan trees.

---

## ModifyTablePath (T_ModifyTablePath)

**Identity**: struct `ModifyTablePath` defined at `src/include/nodes/pathnodes.h:2375`.

```c
typedef struct ModifyTablePath
{
    Path        path;
    Path       *subpath;            /* Path producing source data */
    CmdType     operation;          /* INSERT, UPDATE, DELETE, or MERGE */
    bool        canSetTag;          /* do we set the command tag/es_processed? */
    Index       nominalRelation;    /* parent RT index for use of EXPLAIN */
    Index       rootRelation;       /* root RT index, if partitioned/inherited */
    bool        partColsUpdated;    /* some part key in hierarchy updated? */
    List       *resultRelations;    /* integer list of RT indexes */
    List       *updateColnosLists;  /* per-target-table update_colnos lists */
    List       *withCheckOptionLists;
    List       *returningLists;     /* per-target-table RETURNING tlists */
    List       *rowMarks;           /* PlanRowMarks (non-locking only) */
    OnConflictExpr *onconflict;     /* ON CONFLICT clause, or NULL */
    int         epqParam;           /* EvalPlanQual re-eval param */
    List       *mergeActionLists;   /* per-target-table MERGE action lists */
    List       *mergeJoinConditions;/* per-target-table MERGE join conditions */
} ModifyTablePath;
```

**Purpose**: The wrapper path for any data-modifying statement. The `subpath` produces the rows that will drive the modification (for INSERT, this is typically a Result with VALUES or a SELECT subquery; for UPDATE/DELETE/MERGE, this is the qualified source tuples with implicit ctid-or-equivalent identifiers).

**Constructor**: `create_modifytable_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, CmdType operation, bool canSetTag, Index nominalRelation, Index rootRelation, bool partColsUpdated, List *resultRelations, List *updateColnosLists, List *withCheckOptionLists, List *returningLists, List *rowMarks, OnConflictExpr *onconflict, List *mergeActionLists, List *mergeJoinConditions, int epqParam)` at `src/backend/optimizer/util/pathnode.c:3725`.
   - Allocation: `makeNode(ModifyTablePath)`.
   - Validation asserts:
     - Update lists match result-relations count.
     - WithCheckOption / Returning lists either NIL or match result-relations count.
   - Cost computation: inline. Cost = `subpath->startup_cost / subpath->total_cost` (no per-row write overhead is charged — comment says "would only be window dressing since ModifyTable is always top-level").
   - Row count: when `returningLists != NIL`, `path.rows = subpath->rows` and width is copied; otherwise `path.rows = 0` (no output stream from a no-RETURNING DML).

**Cost function**: None — inline.

**Pathkey behavior**: Always `NIL`.

**Parameterization**: No (`param_info = NULL` always).

**Parallel-aware**: `parallel_safe = false` always — DML can't currently be parallelized.

**Plan counterpart**: `create_modifytable_plan()` at `src/backend/optimizer/plan/createplan.c:2815` produces `ModifyTable` (`plannodes.h:229`). Notable features:
- Calls `create_plan_recurse(root, subpath, CP_EXACT_TLIST)` so the subplan produces exactly the columns expected by the modification.
- Calls `apply_tlist_labeling(subplan->targetlist, root->processed_tlist)` so resname/resjunk labels match the parser's expectation.
- Delegates to `make_modifytable()` which itself calls `expand_inherited_targets()` and may construct one or more `ForeignScan` direct-modify plans for foreign target tables.
- Threads ON CONFLICT info, MERGE action lists, returning lists, and PlanRowMarks into the plan node.

**Source file references**: `createplan.c:2815-2847` for the path→plan body. `make_modifytable()` is the heart of the construction (also in createplan.c at lower line numbers).

**When chosen**: Always for any DML statement. Always sole path on the topmost RelOptInfo for INSERT/UPDATE/DELETE/MERGE.

**Example SQL**:
```sql
UPDATE big SET v = v + 1 WHERE k > 100;
-- ModifyTable (Update on big)
--   -> Seq Scan on big
--      Filter: (k > 100)
```

---

## LockRowsPath (T_LockRowsPath)

**Identity**: struct `LockRowsPath` defined at `src/include/nodes/pathnodes.h:2360`.

```c
typedef struct LockRowsPath
{
    Path        path;
    Path       *subpath;
    List       *rowMarks;           /* a list of PlanRowMark's */
    int         epqParam;           /* ID of Param for EvalPlanQual re-eval */
} LockRowsPath;
```

**Purpose**: Represents `SELECT ... FOR UPDATE` / `FOR NO KEY UPDATE` / `FOR SHARE` / `FOR KEY SHARE`. Acquires row locks on the target tuples and (if a concurrent update happens) re-evaluates the query against the new tuple via EvalPlanQual.

**Constructor**: `create_lockrows_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *rowMarks, int epqParam)` at `src/backend/optimizer/util/pathnode.c:3662`.
   - Allocation: `makeNode(LockRowsPath)`.
   - Cost computation: inline. `total_cost = subpath->total_cost + cpu_tuple_cost * subpath->rows`. The comment notes this is a stab in the dark — actual lock cost is hard to estimate.

**Cost function**: None — inline.

**Pathkey behavior**: Always `NIL`. The comment explains: "result cannot be assumed sorted, since locking might cause the sort key columns to be replaced with new values" (when EvalPlanQual re-evaluates against an updated row).

**Parameterization**: No.

**Parallel-aware**: `parallel_safe = false` (row locking, like DML, can't be parallelized).

**Plan counterpart**: `create_lockrows_plan()` at `src/backend/optimizer/plan/createplan.c:2792` produces `LockRows` (`plannodes.h:1256`). Trivial: just calls `create_plan_recurse` on the subpath, then `make_lockrows()` to wrap.

**When chosen**: Whenever the query has a `FOR UPDATE/SHARE` clause (set in `root->parse->rowMarks` and propagated to LockRowsPath in `grouping_planner`).

**Example SQL**:
```sql
SELECT * FROM accounts WHERE id = 42 FOR UPDATE;
-- LockRows
--   -> Index Scan using accounts_pkey on accounts
--        Index Cond: (id = 42)
```

---

## Notes on DML Plan Generation

A few observations about how ModifyTable interacts with the rest of plan generation:

1. **Subpath construction**: For INSERT...SELECT, the subpath is the SELECT's plan tree. For UPDATE/DELETE on inherited or partitioned tables, the subpath is typically an Append (or a join above an Append) over scans of all target partitions; each scan must produce a `ctid` (or row identifier for foreign tables) so the executor can locate the row to modify. This is set up via `expand_inherited_rtentry` and the ResultRelations list.

2. **No-op for empty appends**: If the inheritance hierarchy has no live children (all pruned), the resulting AppendPath inside the ModifyTablePath is dummy (`IS_DUMMY_APPEND`), and `create_append_plan` generates a Result with a `false` gating qual instead.

3. **EvalPlanQual params**: Both ModifyTablePath and LockRowsPath carry an `epqParam` set up by `assign_special_exec_param()`. At runtime, when an MVCC conflict is detected, the executor uses this param to re-evaluate the qual against the latest tuple version.

4. **MERGE specifics**: For MERGE, `mergeActionLists` is one list of MergeAction nodes per target relation, and `mergeJoinConditions` carries the ON-clause join conditions per target. The actual subpath for MERGE is a join between the source and the target table; `create_modifytable_plan` doesn't transform this — it just attaches the MERGE-specific metadata to the ModifyTable plan.

5. **Foreign-table direct modify**: When the target table is foreign and the FDW supports direct-modify, `make_modifytable()` may replace the per-relation subplan with a `ForeignScan` that performs the modification entirely on the remote side; the resulting `fdwDirectModifyPlans` Bitmapset records which result-relations are direct-modified.
