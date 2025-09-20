# SetUserIdAndContext

## Location
[src/backend/utils/init/miscinit.c:714-733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L714-L733)

## Overview
Sets the current user ID and security context flags for role-based operations, providing controlled access to user identity changes within PostgreSQL.

## Definition

```c
void
SetUserIdAndContext(Oid userid, bool sec_def_context)
```
## Detailed Description
This function modifies the current user ID (CurrentUserId) and updates the security restriction context based on whether the operation is a security definer context change. It includes a security check to prevent user ID changes within security-restricted operations, throwing the same error that SET ROLE would produce. The function manipulates the SecurityRestrictionContext flags to track whether a local user ID change has occurred.

## Parameters / Member Variables
- : The object identifier (Oid) of the user to set as the current user
- : Boolean flag indicating whether this is a security definer context change that should set the SECURITY_LOCAL_USERID_CHANGE flag

## Dependencies
- Functions called/Symbols referenced:
  - [InSecurityRestrictedOperation](../I/InSecurityRestrictedOperation.md) (security check function)
  - SECURITY_LOCAL_USERID_CHANGE (security context flag)
- Called from (representative examples):
  - AmSpecialWorkerProcess (from src/include/miscadmin.h:413)

## Notes and Other Information
- Throws ERRCODE_INSUFFICIENT_PRIVILEGE error if called within a security-restricted operation
- Modifies the global CurrentUserId variable
- Updates SecurityRestrictionContext flags to track local user ID changes
- Part of PostgreSQL's role-based access control system
- Used primarily for SECURITY DEFINER functions and similar contexts where temporary user identity changes are needed