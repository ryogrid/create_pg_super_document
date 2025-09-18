# DropRole

## Location
src/backend/commands/user.c: 1090 - 1333

## Overview
The main function that implements the DROP ROLE SQL statement, removing database roles and cleaning up all associated dependencies and metadata.

## Definition
```c
void DropRole(DropRoleStmt *stmt)
```

## Detailed Description
DropRole implements the DROP ROLE, DROP USER, and DROP GROUP SQL statements by removing role entries from the pg_authid system catalog and cleaning up all associated dependencies. The function performs extensive validation to ensure only authorized users can drop roles, prevents dropping currently active roles, and handles dependency cleanup in a two-phase process. First, it removes pg_auth_members entries that can be silently removed, then checks for remaining dependencies that would prevent the drop operation. The function also cleans up role-related comments, security labels, and configuration settings.

## Parameters / Member Variables
- `stmt`: DropRoleStmt structure containing the parsed DROP ROLE statement with list of roles to drop and missing_ok flag

## Dependencies
- Functions called/Symbols referenced:
  - have_createrole_privilege
  - table_open
  - SearchSysCache1
  - GetUserId
  - GetOuterUserId
  - GetSessionUserId
  - superuser
  - is_admin_of_role
  - InvokeObjectDropHook
  - LockSharedObject
  - systable_beginscan
  - systable_getnext
  - deleteSharedDependencyRecordsFor
  - CatalogTupleDelete
  - checkSharedDependencies
  - DeleteSharedComments
  - DeleteSharedSecurityLabel
  - DropSetting
- Called from (representative examples):
  - standard_ProcessUtility

## Notes and Other Information
- Returns void (no return value)
- Supports IF EXISTS syntax through missing_ok flag for graceful handling of non-existent roles
- Prevents dropping the current user, outer user, or session user to avoid security issues
- Requires CREATEROLE privilege and ADMIN option on target roles
- Only superusers can drop other superuser roles
- Uses a two-phase dependency cleanup process to handle complex role membership scenarios
- Maintains exclusive locks on roles during the drop process to prevent concurrent modifications
- Automatically removes role memberships, comments, security labels, and configuration settings
- Uses AccessExclusiveLock to prevent other transactions from accessing the role during deletion