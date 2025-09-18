# getRTEPermissionInfo

## Location
src/backend/parser/parse_relation.c: 3903 - 3918

## Overview
This function retrieves the RTEPermissionInfo structure for a given RangeTblEntry from the provided permission info list, with comprehensive validation checks.

## Definition
```c
RTEPermissionInfo *getRTEPermissionInfo(List *rteperminfos, RangeTblEntry *rte)
```

## Detailed Description
`getRTEPermissionInfo` is a critical function in PostgreSQL's permission checking infrastructure that safely retrieves permission information for a range table entry. While conceptually a simple list lookup operation using `list_nth()`, the function provides essential validation to ensure data integrity. It verifies that the RTE's permission info index is valid (non-zero and within bounds), retrieves the corresponding RTEPermissionInfo from the list using the 1-based index, and performs a cross-validation check to ensure the relation IDs match between the RTE and the retrieved permission info. This robust validation helps catch programming errors and data corruption issues early in the execution process.

## Parameters / Member Variables
- `rteperminfos`: List containing RTEPermissionInfo structures
- `rte`: Pointer to the RangeTblEntry whose permission information is being retrieved

## Dependencies
- Functions called/Symbols referenced:
  - list_nth_node
  - list_length
  - elog
- Data structures used:
  - RTEPermissionInfo
  - RangeTblEntry
  - List
- Called from (representative examples):
  - ExecCheckPermissions (src/backend/executor/execMain.c:598)
  - GetResultRTEPermissionInfo (src/backend/executor/execUtils.c:1381)
  - subquery_planner (src/backend/optimizer/plan/planner.c:841)
  - markRTEForSelectPriv (src/backend/parser/parse_relation.c:1075)

## Notes and Other Information
- The function performs extensive validation including bounds checking and relation ID verification
- Uses 1-based indexing as stored in RTE's perminfoindex field, but converts to 0-based for list_nth_node access
- Throws ERROR-level exceptions for invalid permission info indices or mismatched relation IDs
- This function is part of PostgreSQL's access control system and is declared in src/include/parser/parse_relation.h
- The validation checks help ensure the integrity of the permission info system and catch potential bugs early
- Widely used throughout the query execution pipeline, from planning to execution phases
- The function serves as the primary interface for accessing permission information during query processing