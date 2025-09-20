# ExecDeleteAct

## Location
[src/backend/executor/nodeModifyTable.c:1369-1390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1369-L1390)

## Overview
Performs the actual physical deletion of a tuple from a regular (non-foreign) table by calling the storage layer's table_tuple_delete function.

## Definition

```c
static TM_Result
ExecDeleteAct(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
			  ItemPointer tupleid, bool changingPart)
```
## Detailed Description
ExecDeleteAct is a focused function that handles the core deletion operation for regular tables. It serves as a thin wrapper around the storage layer's table_tuple_delete function, providing the necessary execution context and parameters:

1. **Storage layer delegation**: Calls table_tuple_delete with appropriate snapshots and transaction information
2. **Concurrency handling**: Uses wait-for-commit semantics to handle concurrent modifications
3. **Cross-check support**: Utilizes the cross-check snapshot for serializable isolation level support
4. **Partition awareness**: The changingPart parameter indicates whether this deletion is part of a partition key update

The function is intentionally minimal, focusing solely on the physical deletion operation. Higher-level concerns like trigger execution, constraint checking, and EvalPlanQual processing are handled by the calling functions (ExecDelete and ExecMergeMatched).

## Parameters / Member Variables
- : ModifyTableContext containing execution state including snapshots and tuple metadata
- : Information about the target relation being modified
- : ItemPointer identifying the specific tuple to delete
- : Boolean indicating if this deletion is part of a cross-partition update operation

## Dependencies
- Functions called/Symbols referenced:
  - table_tuple_delete (storage layer deletion function)
- Called from (representative examples):
  - [ExecDelete](ExecDelete.md) (standard DELETE operation processing)
  - [ExecMergeMatched](ExecMergeMatched.md) (MERGE statement DELETE actions)

## Notes and Other Information
- Returns TM_Result indicating the outcome of the deletion attempt (success, updated by concurrent transaction, etc.)
- The function assumes all preparatory work (triggers, constraints) has been completed by the caller
- Uses estate->es_output_cid for command ID to maintain proper MVCC semantics
- The wait-for-commit parameter is set to true, meaning the operation will wait if the tuple is being modified by another transaction
- Cross-check snapshot support enables proper serializable isolation level behavior
- The changingPart parameter helps the storage layer optimize partition-related deletion scenarios
- This function only handles regular tables - foreign table deletions use different code paths