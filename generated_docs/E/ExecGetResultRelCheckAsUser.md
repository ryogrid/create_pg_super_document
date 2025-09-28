# ExecGetResultRelCheckAsUser

## Location
[src/backend/executor/execUtils.c:1395-1405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1395-L1405)

## Overview
ExecGetResultRelCheckAsUser returns the user ID to use when performing permission checks for modifications to a passed-in result relation, handling both regular relations and inheritance child relations.

## Definition
```c
Oid ExecGetResultRelCheckAsUser(ResultRelInfo *relInfo, EState *estate)
```

## Detailed Description
This function determines the appropriate user ID for permission checking when modifying a result relation. It works by:

1. Calling GetResultRTEPermissionInfo() to retrieve the RTEPermissionInfo for the relation
2. If no RTEPermissionInfo is found, it raises an ERROR (though there's a comment suggesting returning GetUserId() might be acceptable)
3. If RTEPermissionInfo is found, it returns either the checkAsUser field if set, or the current user ID via GetUserId()

The function handles inheritance hierarchies correctly by delegating to GetResultRTEPermissionInfo(), which knows how to find the appropriate permission information for both parent and child relations.

## Parameters / Member Variables
- `relInfo`: Pointer to ResultRelInfo structure containing information about the result relation to check permissions for
- `estate`: Pointer to EState (executor state) containing runtime execution context and range table information

## Dependencies
- Functions called/Symbols referenced:
  - [GetResultRTEPermissionInfo](../G/GetResultRTEPermissionInfo.md) (to get permission info for the relation)
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md) (structure type for permission information)
- Called from (representative examples):
  - [exec_rt_fetch](../e/exec_rt_fetch.md) (referenced in src/include/executor/executor.h:620)

## Notes and Other Information
- Returns an Oid representing the user ID for permission checks
- Raises an ERROR if no RTEPermissionInfo is found for the relation
- There's a TODO comment (XXX) suggesting that returning GetUserId() might be acceptable when no permission info is found
- This function is critical for proper security in DML operations, ensuring modifications are checked against the correct user's permissions
- Works seamlessly with PostgreSQL's inheritance and partitioning system through its use of GetResultRTEPermissionInfo()

## Simplified Source

```c
// Simplified version of ExecGetResultRelCheckAsUser
Oid ExecGetResultRelCheckAsUser(ResultRelInfo *relInfo, EState *estate) {
    // Get permission info for the result relation
    RTEPermissionInfo *perminfo = GetResultRTEPermissionInfo(relInfo, estate);

    // Error if no permission info found
    if (perminfo == NULL) {
        elog(ERROR, "no RTEPermissionInfo found for result relation");
    }

    // Return specified user or current user
    return perminfo->checkAsUser ? perminfo->checkAsUser : GetUserId();
}
```

Key simplifications made:
- Simplified error message for clarity
- Added brief comments for each logical step
- Preserved the essential logic flow and security checks