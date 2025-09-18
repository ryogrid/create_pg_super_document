# ExecDelete

## Location
[src/backend/executor/nodeModifyTable.c:1449-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1449-L1762)

## Overview
Executes the deletion of a tuple from a table, handling various scenarios including foreign tables, triggers, concurrent modifications, and RETURNING clauses.

## Definition


## Detailed Description
ExecDelete is the core function for executing DELETE operations in PostgreSQL. It handles the complete deletion workflow including:

1. **Preparation Phase**: Calls ExecDeletePrologue to handle BEFORE triggers and preliminary checks
2. **Trigger Handling**: Processes INSTEAD OF triggers for views and foreign table triggers
3. **Physical Deletion**: For regular tables, performs the actual tuple deletion via ExecDeleteAct
4. **Concurrency Control**: Handles concurrent modifications using EPQ (EvalPlanQual) mechanism
5. **Cleanup Phase**: Calls ExecDeleteEpilogue for AFTER triggers and cleanup
6. **RETURNING Processing**: Generates RETURNING clause results if requested

The function supports multiple deletion scenarios:
- Regular table deletion using tuple ID
- View deletion through INSTEAD OF triggers using old tuple data
- Foreign table deletion delegated to FDW routines
- Cross-partition updates where deletion is part of tuple movement

## Parameters / Member Variables
- : ModifyTableContext containing execution state and metadata
- : Information about the target relation for deletion
- : ItemPointer identifying the tuple to delete (invalid for foreign tables and views)
- : HeapTuple containing tuple data (used for triggers, NULL for regular table deletion)
- : Boolean indicating whether to process RETURNING clause
- : Boolean indicating if deletion is part of cross-partition update
- : Boolean controlling whether to increment processed tuple count
- : Output parameter receiving the tuple modification result
- : Output parameter indicating whether deletion actually occurred
- : Output parameter for returning updated tuple from EPQ evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecDeletePrologue](ExecDeletePrologue.md) (preparation and BEFORE triggers)
  - [ExecDeleteAct](ExecDeleteAct.md) (physical deletion operation)
  - [ExecDeleteEpilogue](ExecDeleteEpilogue.md) (cleanup and AFTER triggers)
  - [ExecIRDeleteTriggers](ExecIRDeleteTriggers.md) (INSTEAD OF row delete triggers)
  - [ExecGetReturningSlot](ExecGetReturningSlot.md) (RETURNING slot management)
  - [EvalPlanQual](EvalPlanQual.md)/EvalPlanQualBegin (concurrency control)
  - [ExecProcessReturning](ExecProcessReturning.md) (RETURNING clause processing)
- Called from:
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md) (cross-partition tuple movement)
  - [ExecModifyTable](ExecModifyTable.md) (main modify table execution)

## Notes and Other Information
- Returns TupleTableSlot containing RETURNING results, or NULL if no RETURNING clause
- Handles complex concurrency scenarios including TM_Updated, TM_SelfModified, and TM_Deleted cases
- Uses EPQ mechanism to handle concurrent updates during deletion
- For foreign tables, delegates actual deletion to FDW's ExecForeignDelete routine
- Index tuple cleanup is deferred to VACUUM rather than being done immediately
- Supports serializable transaction isolation through snapshot checks
- Part of PostgreSQL's executor framework for DML operations