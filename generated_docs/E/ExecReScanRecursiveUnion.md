# ExecReScanRecursiveUnion

## Location
src/backend/executor/nodeRecursiveunion.c: 298 - 331

## Overview
Resets and rescans a RecursiveUnion plan node, clearing all accumulated state and preparing the node for a fresh execution of the recursive query.

## Definition


## Detailed Description
The `ExecReScanRecursiveUnion` function performs a complete reset of a RecursiveUnion node's execution state, allowing the recursive query to be re-executed from the beginning. This function is typically called when parameters affecting the query have changed or when the query needs to be executed again within a larger plan context.

The rescan process involves several critical steps:
1. **Parameter Invalidation**: Sets the chgParam bitmap for the inner (recursive) plan to indicate that the working table will be modified, forcing a rescan
2. **Child Plan Rescan**: Conditionally rescans the outer (non-recursive) plan if it doesn't already have pending parameter changes
3. **Memory Management**: Resets the table context memory to free hash table storage while preserving the context structure
4. **Hash Table Reset**: Clears the duplicate elimination hash table if it exists (when numCols > 0)
5. **State Reset**: Restores initial execution state by setting recursing to false, marking intermediate table as empty, and clearing both working and intermediate tuple stores

This comprehensive reset ensures that the recursive union can be executed again with clean state, as if it were being initialized for the first time.

## Parameters / Member Variables
- `node`: Pointer to the RecursiveUnionState structure containing the execution state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - innerPlanState
  - bms_add_member
  - ExecReScan
  - MemoryContextReset
  - ResetTupleHashTable
  - tuplestore_clear
- Called from (representative examples):
  - ExecReScan

## Notes and Other Information
- Part of PostgreSQL's plan node rescan framework for parameter changes and re-execution
- Efficiently handles memory management by resetting contexts rather than recreating them
- Uses parameter change notification system (chgParam) to coordinate rescans across the plan tree
- Clears tuple stores without deallocating them, maintaining efficiency for subsequent executions
- Essential for nested loop joins and other scenarios where recursive queries may need multiple executions
- Maintains proper coordination between recursive and non-recursive terms during rescan operations