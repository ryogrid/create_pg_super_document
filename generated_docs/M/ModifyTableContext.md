# ModifyTableContext

## Location
[src/backend/executor/nodeModifyTable.c:86-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L86-L110)

## Overview
ModifyTableContext is a context structure that encapsulates the basic execution state and output variables for ModifyTable operations, providing a centralized way to pass execution context to various helper functions during INSERT, UPDATE, DELETE, and MERGE operations.

## Definition

```c
typedef struct ModifyTableContext
{
	/* Operation state */
	ModifyTableState *mtstate;
	EPQState   *epqstate;
	EState	   *estate;

	/*
	 * Slot containing tuple obtained from ModifyTable's subplan.  Used to
	 * access "junk" columns that are not going to be stored.
	 */
	TupleTableSlot *planSlot;

	/*
	 * Information about the changes that were made concurrently to a tuple
	 * being updated or deleted
	 */
	TM_FailureData tmfd;

	/*
	 * The tuple projected by the INSERT's RETURNING clause, when doing a
	 * cross-partition UPDATE
	 */
	TupleTableSlot *cpUpdateReturningSlot;
} ModifyTableContext;
```
## Detailed Description
This structure serves as a context container that bundles together all the essential state and execution information needed during ModifyTable operations. It is primarily used to simplify function signatures by avoiding the need to pass numerous individual parameters to helper functions like ExecUpdateAct(), ExecDeleteAct(), and related routines.

The context structure encapsulates the main execution states, tuple slots for data access, and specialized fields for handling concurrent modifications and cross-partition operations. It acts as a communication medium between the main ModifyTable execution logic and its various helper functions, allowing them to share state information and return results effectively.

## Parameters / Member Variables
- `*mtstate`: Pointer to the ModifyTableState containing the primary execution state for the ModifyTable operation
- `*epqstate`: Pointer to EPQState used for EvalPlanQual rechecks when handling concurrent tuple modifications
- `*estate`: Pointer to the executor state containing query execution context and metadata
- `*planSlot`: TupleTableSlot containing the tuple from ModifyTable's subplan, used to access junk columns that won't be stored
- `tmfd`: TM_FailureData structure containing information about concurrent changes made to a tuple being updated or deleted
- `*cpUpdateReturningSlot`: TupleTableSlot for holding the tuple projected by INSERT's RETURNING clause during cross-partition UPDATE operations
## Dependencies
- Functions called/Symbols referenced:
  - [ModifyTableState](ModifyTableState.md)
  - [EPQState](../E/EPQState.md)  
  - TM_FailureData
- Called from (representative examples):
  - [ExecInsert](../E/ExecInsert.md)
  - [ExecUpdate](../E/ExecUpdate.md)
  - [ExecDelete](../E/ExecDelete.md)
  - [ExecMerge](../E/ExecMerge.md)
  - [ExecUpdateAct](../E/ExecUpdateAct.md)
  - [ExecDeleteAct](../E/ExecDeleteAct.md)
  - [ExecCrossPartitionUpdate](../E/ExecCrossPartitionUpdate.md)
  - [ExecOnConflictUpdate](../E/ExecOnConflictUpdate.md)

## Notes and Other Information
- This context structure was introduced to consolidate the numerous parameters that were previously passed individually to ModifyTable helper functions
- The tmfd field is crucial for handling concurrent modifications and provides detailed information about what transaction modified a tuple and when
- The cpUpdateReturningSlot is specifically used in cross-partition UPDATE scenarios where a tuple needs to be moved between partitions
- The structure helps maintain clean interfaces between the main ModifyTable execution logic and its specialized helper functions
- It's widely used throughout the ModifyTable execution path, from the main ExecModifyTable function down to specific action handlers