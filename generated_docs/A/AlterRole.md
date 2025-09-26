# AlterRole

## Location
[src/backend/commands/user.c:619-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/user.c#L619-L999)

## Overview
The main function that implements the ALTER ROLE SQL statement, modifying existing database role attributes and membership.

## Definition
```c
Oid AlterRole(ParseState *pstate, AlterRoleStmt *stmt)
```

## Detailed Description
AlterRole implements the ALTER ROLE, ALTER USER, and ALTER GROUP SQL statements by modifying existing role entries in the pg_authid system catalog. The function validates permissions extensively, ensuring that only authorized users can modify role attributes. It supports changing all role attributes including superuser status, login privileges, password settings, connection limits, and role memberships. The function enforces complex privilege rules where superusers can modify other superusers, but non-superusers need both CREATEROLE privilege and ADMIN option on the target role to make most changes.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parsing context and state information
- `stmt`: AlterRoleStmt structure containing the parsed ALTER ROLE statement with role specification and options

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [check_rolespec_name](../c/check_rolespec_name.md)
  - [have_createrole_privilege](../h/have_createrole_privilege.md)
  - [is_admin_of_role](../i/is_admin_of_role.md)
  - [superuser](../s/superuser.md)
  - [have_createdb_privilege](../h/have_createdb_privilege.md)
  - [has_rolreplication](../h/has_rolreplication.md)
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [get_rolespec_tuple](../g/get_rolespec_tuple.md)
  - [table_open](../t/table_open.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [AddRoleMems](AddRoleMems.md)
  - [DelRoleMems](../D/DelRoleMems.md)
  - InvokeObjectPostAlterHook
  - [encrypt_password](../e/encrypt_password.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Returns the OID of the altered role
- Prevents modification of reserved roles (those starting with "pg_")
- Implements sophisticated privilege checking with different rules for superusers vs. regular users
- Supports backwards-compatible ALTER GROUP syntax through rolemembers option
- Users can always change their own password, but need additional privileges to change other users' passwords
- Bootstrap superuser cannot have superuser privilege revoked
- Validates connection limits and password formats
- Uses heap_modify_tuple for atomic updates to the catalog