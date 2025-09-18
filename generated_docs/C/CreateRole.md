# CreateRole

## Location
src/backend/commands/user.c: 132 - 618

## Overview
The main function that implements the CREATE ROLE SQL statement, creating new database roles with specified attributes and permissions.

## Definition
```c
Oid CreateRole(ParseState *pstate, CreateRoleStmt *stmt)
```

## Detailed Description
CreateRole is the core implementation function for the CREATE ROLE, CREATE USER, and CREATE GROUP SQL statements. It parses role creation options, validates permissions, creates a new role entry in the pg_authid system catalog, and sets up role memberships. The function handles all role attributes including superuser status, login privileges, password settings, connection limits, and role memberships. It enforces security constraints ensuring only authorized users can create roles with specific privileges, and implements automatic role administration grants for non-superuser creators.

## Parameters / Member Variables
- `pstate`: ParseState structure containing parsing context and state information
- `stmt`: CreateRoleStmt structure containing the parsed CREATE ROLE statement with role name and options

## Dependencies
- Functions called/Symbols referenced:
  - GetUserId
  - has_createrole_privilege
  - superuser_arg
  - have_createdb_privilege
  - has_rolreplication
  - has_bypassrls_privilege
  - IsReservedName
  - get_role_oid
  - table_open
  - RelationGetDescr
  - CatalogTupleInsert
  - AddRoleMems
  - InvokeObjectPostCreateHook
  - encrypt_password
  - heap_form_tuple
- Called from (representative examples):
  - standard_ProcessUtility

## Notes and Other Information
- Returns the OID of the newly created role
- Supports three statement types: ROLE, USER (with default LOGIN), and GROUP
- Validates that role names don't start with "pg_" (reserved namespace)
- Handles password encryption using the configured password encryption method
- Automatically grants admin privileges to non-superuser creators of roles
- Implements createrole_self_grant feature for automatic role inheritance
- Performs extensive privilege validation before allowing role creation
- Uses binary upgrade mode support for pg_upgrade operations