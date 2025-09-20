# UpdateContext

## Location
[src/backend/executor/nodeModifyTable.c:115-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L115-L125)

## Overview
UpdateContext is a specialized context structure that contains output data specific to UPDATE operations, providing information about the results and requirements of update actions to calling functions.

## Definition

```c
typedef struct UpdateContext
{
	bool		crossPartUpdate;	/* was it a cross-partition update? */
	TU_UpdateIndexes updateIndexes; /* Which index updates are required? */

	/*
	 * Lock mode to acquire on the latest tuple version before performing
	 * EvalPlanQual on it
	 */
	LockTupleMode lockmode;
} UpdateContext;
```
## Detailed Description
This structure serves as an output parameter container for UPDATE-specific operations, allowing functions like ExecUpdateAct() to communicate important results back to their callers. It encapsulates information about whether a cross-partition update occurred, what index updates are needed, and what lock mode should be used for EvalPlanQual processing.

The structure is typically initialized by the calling function (like ExecUpdate()) and then populated by the update execution functions to provide feedback about the operation's results. This design allows the update execution logic to be decomposed into smaller, focused functions while maintaining the ability to communicate complex state information back to the coordinator.

## Parameters / Member Variables
- : Boolean flag indicating whether the update operation resulted in a cross-partition update (tuple moved from one partition to another)
- : Enum value specifying which indexes require updating as a result of the UPDATE operation
- : The lock mode that should be acquired on the latest tuple version before performing EvalPlanQual processing during concurrent update handling

## Dependencies
- Functions called/Symbols referenced:
  - TU_UpdateIndexes (enum for index update requirements)
  - [LockTupleMode](../L/LockTupleMode.md) (enum for tuple locking modes)
- Called from (representative examples):
  - [ExecUpdate](../E/ExecUpdate.md)
  - [ExecUpdateAct](../E/ExecUpdateAct.md)
  - [ExecUpdateEpilogue](../E/ExecUpdateEpilogue.md)
  - [ExecCrossPartitionUpdate](../E/ExecCrossPartitionUpdate.md)
  - [ExecMergeMatched](../E/ExecMergeMatched.md)

## Notes and Other Information
- This context structure is specifically designed for UPDATE operations and complements the more general ModifyTableContext
- The crossPartUpdate flag is crucial for determining the proper handling of RETURNING clauses in partitioned table updates
- The updateIndexes field helps optimize index maintenance by indicating exactly which indexes need to be updated
- The lockmode field is important for proper concurrency control during EvalPlanQual processing when handling concurrent modifications
- This structure is typically stack-allocated and initialized to zero before being passed to update functions
- Used extensively in the UPDATE execution path to coordinate between different phases of the update operation