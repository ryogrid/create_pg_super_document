# Plan Creator Catalog: DML and Row-locking Plan Creators

This catalog covers the two plan creators that wrap the top of a DML or row-locking plan tree: `create_modifytable_plan` and `create_lockrows_plan`. Both are dispatched from `create_plan_recurse` based on `T_ModifyTable` and `T_LockRows` pathtype, respectively.

These creators are deliberately thin wrappers: most of the heavy lifting (target-list construction, FDW direct-modify dispatching, ON CONFLICT processing) happens inside the `make_modifytable()` helper that they call, or inside subsequent passes (`setrefs.c` finishes Var fixup; `subselect.c` and `set_plan_references` finalize subplan integration).

---

## create_modifytable_plan

**Signature**: `static ModifyTable *create_modifytable_plan(PlannerInfo *root, ModifyTablePath *best_path)` at `src/backend/optimizer/plan/createplan.c:2815`.

**Dispatching context**: Routed from `create_plan_recurse` (line 533) for `pathtype == T_ModifyTable`.

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

**Source file references**: `createplan.c:2815-2847`. The heart is one `create_plan_recurse` call, one `apply_tlist_labeling`, one `make_modifytable`, and `copy_generic_path_info`. The complexity lives inside `make_modifytable` (also in createplan.c, near line 7000+) which builds the actual ModifyTable node.

---

## create_lockrows_plan

**Signature**: `static LockRows *create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path, int flags)` at `src/backend/optimizer/plan/createplan.c:2792`.

**Dispatching context**: Routed from `create_plan_recurse` (line 528) for `pathtype == T_LockRows`.

**Output Plan struct**: `LockRows` (`plannodes.h:1256`) with `rowMarks` and `epqParam`.

**Tlist handling**: Subplan gets `flags` unchanged — LockRows doesn't project, so tlist requirements pass through.

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

## Notes on Pseudo-creators and DML Plan Tree Shape

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

The point is that ModifyTable is purely a topmost wrapper — it never has more than one child plan, never participates in plan optimization, and its existence is dictated by the parse tree (CmdType ≠ CMD_SELECT). All the interesting planning happens *underneath* it.
