# CreateUserMapping

## Location
src/backend/commands/foreigncmds.c: 1111 - 1236

## Overview
Creates a new user mapping that defines authentication and connection information for a specific user to access a foreign server in PostgreSQL's foreign data wrapper system.

## Definition
```c
ObjectAddress CreateUserMapping(CreateUserMappingStmt *stmt)
```

## Detailed Description
This function implements the CREATE USER MAPPING SQL command by creating a new entry in the pg_user_mapping system catalog. It handles user authentication mapping for foreign data wrappers, allowing users to define connection credentials and options for accessing external data sources. The function performs comprehensive validation including uniqueness checks, permission verification, and proper dependency tracking. It supports both regular users and the special PUBLIC role, handles the IF NOT EXISTS clause, and validates user-provided options through the foreign data wrapper's validator function.

## Parameters / Member Variables
- `stmt`: Pointer to CreateUserMappingStmt structure containing the parsed CREATE USER MAPPING command details including user specification, server name, options, and conditional flags

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - get_rolespec_oid
  - GetForeignServerByName
  - user_mapping_ddl_aclcheck
  - GetSysCacheOid2
  - GetForeignDataWrapper
  - GetNewOidWithIndex
  - transformGenericOptions
  - heap_form_tuple
  - CatalogTupleInsert
  - heap_freetuple
  - recordDependencyOn
  - recordDependencyOnOwner
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
The function creates dependencies on both the foreign server and the mapped user to ensure proper cleanup when either is dropped. User mappings are intentionally not made members of extensions since roles themselves cannot be extension members. The function includes special handling for the PUBLIC role and supports conditional creation with IF NOT EXISTS to avoid duplicate mapping errors.