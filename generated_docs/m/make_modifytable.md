# make_modifytable

## Location
[src/backend/optimizer/plan/createplan.c:7029-7233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L7029-L7233)

## Overview
Creates and initializes a ModifyTable plan node that handles data modification operations (INSERT, UPDATE, DELETE, MERGE) in PostgreSQL's execution engine.

## Definition
```c
static ModifyTable *
make_modifytable(PlannerInfo *root, Plan *subplan,
                 CmdType operation, bool canSetTag,
                 Index nominalRelation, Index rootRelation,
                 bool partColsUpdated,
                 List *resultRelations,
                 List *updateColnosLists,
                 List *withCheckOptionLists, List *returningLists,
                 List *rowMarks, OnConflictExpr *onconflict,
                 List *mergeActionLists, List *mergeJoinConditions,
                 int epqParam)
```

## Detailed Description
The `make_modifytable` function constructs a ModifyTable plan node, which is the execution plan node responsible for all data modification operations in PostgreSQL. This function handles complex initialization including ON CONFLICT clauses for INSERT operations, foreign data wrapper (FDW) integration for foreign tables, partition column updates, row-level security checks, RETURNING clauses, and MERGE operations. It also determines whether foreign tables can use direct modification optimization or require standard FDW modification protocols.

## Parameters / Member Variables
- `root`: PlannerInfo * - The global planner state containing relation information and planning context
- `subplan`: Plan * - The child plan node that provides rows to be modified
- `operation`: CmdType - The type of modification operation (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE)
- `canSetTag`: bool - Whether this operation should set a command tag visible to the client
- `nominalRelation`: Index - The RTE index of the relation named in the query
- `rootRelation`: Index - The RTE index of the target relation's root (for partitioned tables)
- `partColsUpdated`: bool - Whether any partition key columns are being updated
- `resultRelations`: List * - List of RTE indexes for all relations to be modified
- `updateColnosLists`: List * - List of column numbers being updated for each result relation
- `withCheckOptionLists`: List * - WITH CHECK OPTION constraints for each result relation
- `returningLists`: List * - RETURNING expressions for each result relation
- `rowMarks`: List * - Row locking information
- `onconflict`: OnConflictExpr * - ON CONFLICT clause information for INSERT operations
- `mergeActionLists`: List * - Action lists for MERGE operations
- `mergeJoinConditions`: List * - Join conditions for MERGE operations
- `epqParam`: int - EvalPlanQual parameter for handling concurrent updates

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates the ModifyTable node)
  - [extract_update_targetlist_colnos](../e/extract_update_targetlist_colnos.md) (processes ON CONFLICT SET clauses)
  - [infer_arbiter_indexes](../i/infer_arbiter_indexes.md) (determines unique indexes for ON CONFLICT)
  - [GetFdwRoutineByRelId](../G/GetFdwRoutineByRelId.md) (gets FDW routines for foreign tables)
  - [has_row_triggers](../h/has_row_triggers.md) (checks for row-level triggers)
  - [has_stored_generated_columns](../h/has_stored_generated_columns.md) (checks for generated columns)
  - [has_transition_tables](../h/has_transition_tables.md) (checks for transition table usage)
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md) (generates error messages)
- Called from (representative examples):
  - [create_modifytable_plan](../c/create_modifytable_plan.md) (primary caller in the planner)

## Notes and Other Information
- Handles complex FDW integration including direct modification optimization for foreign tables
- MERGE operations are not supported on foreign tables and will generate errors
- Performs extensive validation of parameter relationships via Assert statements
- Sets up ON CONFLICT handling including arbiter index inference and exclusion constraints
- Manages partition-aware updates and row-level security considerations
- The function is quite large (200+ lines) due to the complexity of data modification planning
- Direct modification optimization is used when FDW supports it and no local triggers/constraints exist