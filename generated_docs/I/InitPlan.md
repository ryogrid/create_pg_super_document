# InitPlan

## Location
[src/backend/executor/execMain.c:826-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L826-L1018)

## Overview
Initializes the query execution plan by opening files, allocating storage, setting up execution state, and preparing all necessary data structures for query execution.

## Definition

```c
static void
InitPlan(QueryDesc *queryDesc, int eflags)
```
## Detailed Description
This comprehensive initialization function prepares the PostgreSQL executor for query execution. It performs several critical setup operations in sequence:

1. **Permission Checking**: Validates that the user has required permissions for all relations involved in the query
2. **Range Table Initialization**: Sets up the executor's range table with relation information
3. **Row Marking Setup**: Configures row locking mechanisms for SELECT FOR UPDATE/SHARE queries
4. **Subplan Initialization**: Prepares all subplans and initplans with appropriate execution flags
5. **Main Plan Tree Initialization**: Recursively initializes the entire plan node tree
6. **Junk Filter Setup**: For SELECT queries, creates filters to remove internal columns from result tuples

The function handles different query types and execution modes, setting appropriate flags for subplans based on whether they need rewinding capability, and properly configures row marking for various locking strengths.

## Parameters / Member Variables
- : Pointer to QueryDesc structure containing:
  - : Type of SQL command being executed
  - : The planned statement with execution plan
  - : Executor state for managing execution context
  - : Tuple descriptor for result tuples (set by this function)
  - : Root plan state node (set by this function)
- : Execution flags controlling execution behavior (EXEC_FLAG_REWIND, EXEC_FLAG_BACKWARD, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [ExecInitRangeTable](../E/ExecInitRangeTable.md)
  - [ExecGetRangeTableRelation](../E/ExecGetRangeTableRelation.md)
  - [ExecInitNode](../E/ExecInitNode.md)
  - [ExecGetResultType](../E/ExecGetResultType.md)
  - [ExecInitExtraTupleSlot](../E/ExecInitExtraTupleSlot.md)
  - [ExecInitJunkFilter](../E/ExecInitJunkFilter.md)
  - [CheckValidRowMarkRel](../C/CheckValidRowMarkRel.md)
  - [exec_rt_fetch](../e/exec_rt_fetch.md)
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md)

## Notes and Other Information
- This is a static function only called from within execMain.c during executor startup
- The function does not return a value; it modifies the queryDesc structure in place
- Row marking is only set up for relations that actually need physical table access
- Subplans are initialized before the main plan tree to ensure proper dependency resolution
- Execution flags are carefully managed to avoid unnecessary overhead in subplans
- The junk filter is only created for SELECT queries when the target list contains junk attributes
- Parent row marks are ignored at runtime as they are only needed during planning
- The function ensures all necessary memory allocations are performed in appropriate memory contexts