# SwitchToUntrustedUser

## Location
[src/backend/utils/init/usercontext.c:33-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/usercontext.c#L33-L86)

## Overview
Temporarily switches to a different user ID with security restrictions, ensuring the current user has sufficient privileges to assume the target role while preventing privilege escalation vulnerabilities.

## Definition
```c
void SwitchToUntrustedUser(Oid userid, UserContext *context)
```

## Detailed Description
This function provides a secure mechanism to temporarily switch to a different user ID while maintaining security boundaries. It performs privilege checks to ensure the current user can SET ROLE to the target user and implements security restrictions when the target user cannot SET ROLE back to the original user.

The function implements a two-tier security model:
1. **Bidirectional trust**: If both users can SET ROLE to each other, no additional restrictions are imposed
2. **Unidirectional trust**: If only the current user can SET ROLE to the target user, SECURITY_RESTRICTED_OPERATION is enabled and a new GUC nest level is created to contain any configuration changes

This design prevents privilege escalation attacks where a less-privileged user could potentially gain access to a more privileged user's capabilities.

## Parameters / Member Variables
- `userid`: The target user ID (Oid) to switch to
- `context`: Pointer to UserContext structure that stores the original user context for later restoration

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - member_can_set_role
  - [GetUserNameFromId](../G/GetUserNameFromId.md)
  - [SetUserIdAndSecContext](SetUserIdAndSecContext.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)
  - SECURITY_RESTRICTED_OPERATION
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (tablecmds.c:2061, 2270)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (tablesync.c:1508)
  - [apply_handle_insert](../a/apply_handle_insert.md) (worker.c:2414)
  - [apply_handle_update](../a/apply_handle_update.md) (worker.c:2577)
  - [apply_handle_delete](../a/apply_handle_delete.md) (worker.c:2757)

## Notes and Other Information
- Always paired with RestoreUserContext() to restore the original user context
- Used primarily in logical replication and table operations where temporary privilege elevation is needed
- The SECURITY_RESTRICTED_OPERATION flag prevents certain dangerous operations while running as the target user
- GUC nest level creation ensures any configuration changes made by the target user can be rolled back
- Throws ERROR if the current user lacks permission to SET ROLE to the target user