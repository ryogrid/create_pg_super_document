# GetResultRTEPermissionInfo

## Location
src/backend/executor/execUtils.c: 1344 - 1394

## Overview
GetResultRTEPermissionInfo is a static utility function that looks up RTEPermissionInfo for ExecGet*Cols() routines, handling both regular result relations and inheritance child relations.

## Definition


## Detailed Description
This function retrieves the appropriate RTEPermissionInfo structure for a given result relation. It handles three distinct cases:

1. **Inheritance child result relations**: For partition routing targets (INSERT) or child UPDATE targets, it returns the root parent's RTE to fetch the RTEPermissionInfo, as only the root parent has one assigned.

2. **Non-child result relations**: These should have their own RTEPermissionInfo directly accessible via their RangeTableIndex.

3. **Trigger-only relations**: Relations not in the range table and not partition routing targets. These are typically created only for firing triggers where the relation is not being inserted into.

The function determines which case applies based on the ResultRelInfo structure's fields and retrieves the corresponding permission information from the executor state.

## Parameters / Member Variables
- : Pointer to ResultRelInfo structure containing information about the result relation
- : Pointer to EState (executor state) containing runtime execution context and range table information

## Dependencies
- Functions called/Symbols referenced:
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md) (return type)
  - exec_rt_fetch (to get RangeTblEntry from range table)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md) (to get permission info from RTE)
- Called from (representative examples):
  - [ExecGetInsertedCols](../E/ExecGetInsertedCols.md) (src/backend/executor/execUtils.c:1269)
  - [ExecGetUpdatedCols](../E/ExecGetUpdatedCols.md) (src/backend/executor/execUtils.c:1290)
  - [ExecGetResultRelCheckAsUser](../E/ExecGetResultRelCheckAsUser.md) (src/backend/executor/execUtils.c:1397)

## Notes and Other Information
- This is a static function, meaning it's only accessible within execUtils.c
- Handles the complexity of PostgreSQL's inheritance and partitioning system where child relations may not have their own permission information
- Returns NULL for trigger-only relations that don't require permission checks
- Critical for proper permission checking in DML operations involving inheritance hierarchies