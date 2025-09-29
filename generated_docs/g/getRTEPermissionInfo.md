# getRTEPermissionInfo

## Location
[src/backend/parser/parse_relation.c:3903-3918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3903-L3918)

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
  - [list_length](../l/list_length.md)
  - elog
- Data structures used:
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
  - [List](../L/List.md)
- Called from (representative examples):
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md) (src/backend/executor/execMain.c:598)
  - [GetResultRTEPermissionInfo](../G/GetResultRTEPermissionInfo.md) (src/backend/executor/execUtils.c:1381)
  - [subquery_planner](../s/subquery_planner.md) (src/backend/optimizer/plan/planner.c:841)
  - [markRTEForSelectPriv](../m/markRTEForSelectPriv.md) (src/backend/parser/parse_relation.c:1075)

## Notes and Other Information
- The function performs extensive validation including bounds checking and relation ID verification
- Uses 1-based indexing as stored in RTE's perminfoindex field, but converts to 0-based for list_nth_node access
- Throws ERROR-level exceptions for invalid permission info indices or mismatched relation IDs
- This function is part of PostgreSQL's access control system and is declared in src/include/parser/parse_relation.h
- The validation checks help ensure the integrity of the permission info system and catch potential bugs early
- Widely used throughout the query execution pipeline, from planning to execution phases
- The function serves as the primary interface for accessing permission information during query processing

## Simplified Source

```c
RTEPermissionInfo *getRTEPermissionInfo(List *rteperminfos, RangeTblEntry *rte) {
    // Validate permission info index is within valid range
    if (rte->perminfoindex == 0 || rte->perminfoindex > list_length(rteperminfos)) {
        elog(ERROR, "invalid perminfoindex %u in RTE with relid %u",
             rte->perminfoindex, rte->relid);
    }

    // Get permission info from list (convert 1-based index to 0-based)
    RTEPermissionInfo *perminfo = list_nth_node(RTEPermissionInfo, rteperminfos,
                                                rte->perminfoindex - 1);

    // Verify relation IDs match between RTE and permission info
    if (perminfo->relid != rte->relid) {
        elog(ERROR, "permission info relid mismatch: index %u has relid %u, RTE has relid %u",
             rte->perminfoindex, perminfo->relid, rte->relid);
    }

    return perminfo;
}
```