# AlterForeignServer

## Location
src/backend/commands/foreigncmds.c: 985 - 1085

## Overview
Modifies an existing foreign server definition by updating its version string and/or options while maintaining proper access control and validation.

## Definition
```c
ObjectAddress AlterForeignServer(AlterForeignServerStmt *stmt)
```

## Detailed Description
This function implements the ALTER SERVER SQL command by modifying an existing foreign server entry in the pg_foreign_server system catalog. It performs access control validation ensuring that only the server owner or a superuser can make modifications. The function supports selective updates to the server version string and server options, with options being validated through the associated foreign-data wrapper's validator function. It uses heap_modify_tuple for efficient partial updates, updating only the specified attributes rather than replacing the entire tuple.

## Parameters / Member Variables
- `stmt`: AlterForeignServerStmt structure containing the parsed ALTER SERVER statement details including server name, version changes, and option modifications

## Dependencies
- Functions called/Symbols referenced:
  - AlterForeignServerStmt
  - Form_pg_foreign_server
  - SearchSysCacheCopy1
  - CStringGetDatum
  - object_ownercheck
  - aclcheck_error
  - GetForeignDataWrapper
  - ForeignDataWrapper
  - SysCacheGetAttr
  - transformGenericOptions
  - PointerIsValid
  - heap_modify_tuple
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - heap_freetuple
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- Enforces ownership-based access control - only server owner or superuser can alter servers
- Validates server existence before attempting modifications
- Supports selective updates through has_version flag for version changes
- Server options are validated using the associated FDW's validator function
- Version string can be set to a new value or cleared (set to NULL)
- Uses heap_modify_tuple for efficient selective column updates
- Triggers object alteration hooks for extensibility
- Returns ObjectAddress of the modified server for further reference
- Does not modify server name or associated FDW - these are immutable after creation
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling dynamic reconfiguration of server properties