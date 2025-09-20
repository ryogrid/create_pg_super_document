# ExecMerge

## Location
[src/backend/executor/nodeModifyTable.c:2764-2889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2764-L2889)

## Overview
Performs MERGE operations by handling both MATCHED and NOT MATCHED cases, including concurrent update scenarios during MERGE execution.

## Definition

```c
static TupleTableSlot *
ExecMerge(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
		  ItemPointer tupleid, HeapTuple oldtuple, bool canSetTag)
```
## Detailed Description
ExecMerge is the core function that orchestrates MERGE statement execution in PostgreSQL. It handles three main scenarios:

1. **WHEN MATCHED**: When a source tuple matches a target tuple, executes the first qualifying WHEN MATCHED action
2. **WHEN NOT MATCHED BY SOURCE**: When a target tuple has no corresponding source tuple, processes WHEN NOT MATCHED BY SOURCE actions
3. **WHEN NOT MATCHED [BY TARGET]**: When a source tuple has no corresponding target tuple, processes WHEN NOT MATCHED actions

The function is designed to handle concurrent modifications during MERGE execution. It can adapt when concurrent updates change the match status of tuples, supporting scenarios where MATCHED rows become NOT MATCHED due to concurrent updates or deletes, but not vice versa to avoid livelocks.

The execution flow involves calling ExecMergeMatched() for matched cases, which may change the matched status if concurrent modifications occur, followed by ExecMergeNotMatched() for not matched cases when necessary.

## Parameters / Member Variables
- : ModifyTableContext containing the execution state and context for the MERGE operation
- : ResultRelInfo structure containing information about the target relation being modified
- : ItemPointer to the target tuple for MATCHED cases (NULL for NOT MATCHED cases)
- : HeapTuple representing the target tuple for view-based operations (NULL for table-based operations)
- : Boolean indicating whether command tags can be set during execution

## Dependencies
- Functions called/Symbols referenced:
  - [ExecMergeMatched](ExecMergeMatched.md)
  - [ExecMergeNotMatched](ExecMergeNotMatched.md)
  - [ModifyTableContext](../M/ModifyTableContext.md)
- Called from (representative examples):
  - [ExecModifyTable](ExecModifyTable.md) (multiple locations in nodeModifyTable.c)

## Notes and Other Information
- Handles concurrent update scenarios gracefully by supporting transitions from MATCHED to NOT MATCHED cases
- Does not support transitions from NOT MATCHED to MATCHED to prevent livelocks
- May execute two actions in cases where concurrent updates change match status (one NOT MATCHED BY SOURCE, one NOT MATCHED BY TARGET)
- Uses a pending mechanism to defer NOT MATCHED BY TARGET actions when RETURNING clauses are involved and multiple actions might be executed
- The function's design ensures forward progress by following update chains and never switching back from ExecMergeNotMatched() to ExecMergeMatched()