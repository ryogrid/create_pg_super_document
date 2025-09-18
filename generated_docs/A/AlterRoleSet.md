# AlterRoleSet

## Location
src/backend/commands/user.c: 1000 - 1089

## Overview
Implements the ALTER ROLE ... SET and ALTER ROLE ... IN DATABASE ... SET SQL statements for managing role-specific and database-specific configuration parameter settings.

## Definition
```c
Oid AlterRoleSet(AlterRoleSetStmt *stmt)
```

## Detailed Description
AlterRoleSet handles the ALTER ROLE ... SET syntax that allows setting configuration parameters for specific roles, databases, or globally. It validates permissions based on the scope of the setting change: superuser privileges are required for global settings, database ownership is required for database-specific settings when no role is specified, and CREATEROLE privilege plus ADMIN option is required when modifying settings for other roles. The function delegates the actual parameter modification to AlterSetting after performing authorization checks and acquiring appropriate locks.

## Parameters / Member Variables
- `stmt`: AlterRoleSetStmt structure containing the parsed ALTER ROLE ... SET statement with role specification, database name, and setting information

## Dependencies
- Functions called/Symbols referenced:
  - check_rolespec_name
  - get_rolespec_tuple
  - [shdepLockAndCheckObject](../s/shdepLockAndCheckObject.md)
  - superuser
  - [have_createrole_privilege](../h/have_createrole_privilege.md)
  - is_admin_of_role
  - [GetUserId](../G/GetUserId.md)
  - [get_database_oid](../g/get_database_oid.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [AlterSetting](AlterSetting.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Returns the OID of the role being modified, or InvalidOid if no role specified
- Supports three permission models: global settings (superuser only), database settings (database owner), and role settings (CREATEROLE + ADMIN option)
- Users can always modify their own settings regardless of other privileges
- Prevents modification of reserved roles (those starting with "pg_")
- Uses shared dependency locking to ensure role and database objects don't disappear during the operation
- The actual parameter setting logic is handled by the AlterSetting function
- Supports both SET and RESET operations through the embedded VariableSetStmt