# ExecModifyTable

## Location
[src/backend/executor/nodeModifyTable.c:3953-4372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L3953-L4372)

## Overview
The main execution function for ModifyTable plan nodes, processing INSERT, UPDATE, DELETE, and MERGE operations while handling triggers, tuple routing, and RETURNING clauses.

## Definition
```c
static TupleTableSlot *ExecModifyTable(PlanState *pstate)
```

## Detailed Description
ExecModifyTable is the core execution engine for data modification operations in PostgreSQL. It orchestrates the entire process of modifying table data, including firing BEFORE and AFTER triggers, handling partition tuple routing, managing foreign data wrapper direct modifications, processing RETURNING clauses, and executing batch inserts. The function operates in a loop, fetching tuples from its subplan and performing the appropriate modification operation based on the command type. It handles complex scenarios like MERGE operations with WHEN NOT MATCHED clauses, multi-table modifications with OID-based relation selection, and various tuple identification mechanisms for different relation types. The function ensures proper transaction semantics and trigger execution order while maintaining performance through optimizations like batch processing and direct FDW modifications.

## Parameters / Member Variables
- `pstate`: Pointer to PlanState (cast to ModifyTableState) containing the execution state, plan information, and modification context

## Dependencies
- Functions called/Symbols referenced:
  - [fireBSTriggers](../f/fireBSTriggers.md), fireASTriggers
  - [ExecProcNode](ExecProcNode.md), TupIsNull
  - [ExecMergeNotMatched](ExecMergeNotMatched.md), ExecMerge
  - [ExecInsert](ExecInsert.md), ExecUpdate, ExecDelete
  - [ExecGetInsertNewTuple](ExecGetInsertNewTuple.md), ExecGetUpdateNewTuple
  - [ExecInitInsertProjection](ExecInitInsertProjection.md), ExecInitUpdateProjection
  - [ExecProcessReturning](ExecProcessReturning.md), ExecPendingInserts
  - [ExecLookupResultRelByOid](ExecLookupResultRelByOid.md), ExecGetJunkAttribute
  - EvalPlanQualSetSlot, ResetPerTupleExprContext
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md), LockTuple, UnlockTuple
  - Various relation kind constants (RELKIND_RELATION, RELKIND_VIEW, etc.)
  - [Command](../C/Command.md) type constants (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE)
- Called from (representative examples):
  - [ExecInitModifyTable](ExecInitModifyTable.md) (at src/backend/executor/nodeModifyTable.c:4443)

## Notes and Other Information
- Returns TupleTableSlot containing RETURNING results, or NULL when processing is complete
- Prevents execution during EvalPlanQual operations to avoid corruption
- Handles multi-relation modifications using junk columns for relation OID identification
- Supports foreign data wrapper direct modifications for improved performance
- Manages tuple identity information differently based on relation type (TID for heap tables, wholerow for others)
- Implements special handling for MERGE operations including deferred WHEN NOT MATCHED processing
- Processes batch inserts at the end of execution for optimal performance
- Maintains proper trigger firing order: BEFORE triggers before modifications, AFTER triggers after all processing
- Located in src/backend/executor/nodeModifyTable.c:3953-4372