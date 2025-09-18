# RenameRole

## Location
src/backend/commands/user.c: 1334 - 1479

## Overview
Changes the name of an existing PostgreSQL role while maintaining all its attributes and permissions, with appropriate security checks and validation.

## Definition


## Detailed Description
RenameRole is responsible for safely renaming a PostgreSQL role from one name to another. The function performs comprehensive validation to ensure the rename operation is secure and valid, including checking permissions, verifying that reserved names are not used, and handling special cases like MD5 password clearing. The function updates the pg_authid system catalog and ensures proper locking during the operation.

Key behaviors include:
- Prevents renaming of system roles and current session/effective users
- Enforces reserved name restrictions (pg_ prefix)
- Requires appropriate privileges (superuser for superuser roles, CREATEROLE + ADMIN for others)
- Automatically clears MD5 passwords since they use username as salt
- Maintains referential integrity through proper catalog updates

## Parameters / Member Variables
- : The current name of the role to be renamed
- : The desired new name for the role

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close: Access pg_authid catalog
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache: Cache-based role lookup
  - [GetSessionUserId](../G/GetSessionUserId.md)/GetOuterUserId: Current user validation
  - [IsReservedName](../I/IsReservedName.md): Reserved name checking
  - superuser/have_createrole_privilege/is_admin_of_role: Permission validation
  - [get_password_type](../g/get_password_type.md): Password type detection for MD5 handling
  - [heap_modify_tuple](../h/heap_modify_tuple.md)/CatalogTupleUpdate: Catalog modification
  - InvokeObjectPostAlterHook: Post-operation hooks
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md): ALTER ROLE RENAME statement execution

## Notes and Other Information
- [Session](../S/Session.md) users and current effective users cannot be renamed to prevent application confusion
- MD5 passwords are automatically cleared during rename since they incorporate the username as salt
- The function maintains exclusive locks on pg_authid until transaction commit
- Regression testing enforces 'regress_' prefix convention when compiled with appropriate flags
- Superuser privileges are required to rename superuser roles; CREATEROLE + ADMIN privileges are required for regular roles