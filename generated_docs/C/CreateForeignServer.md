# CreateForeignServer

## Location
src/backend/commands/foreigncmds.c: 849 - 984

## Overview
Creates a new foreign server definition in PostgreSQL, establishing a logical connection endpoint for accessing external data through a specified foreign-data wrapper.

## Definition
```c
ObjectAddress CreateForeignServer(CreateForeignServerStmt *stmt)
```

## Detailed Description
This function implements the CREATE SERVER SQL command by creating a new foreign server entry in the pg_foreign_server system catalog. It performs comprehensive validation including server name uniqueness checks, FDW existence verification, and access control validation (USAGE privilege on the FDW). The function handles IF NOT EXISTS logic with proper extension membership validation for security, processes optional server type and version specifications, and validates server options using the associated FDW's validator function. It establishes proper dependencies between the server and its underlying FDW while ensuring appropriate ownership and extension membership.

## Parameters / Member Variables
- `stmt`: CreateForeignServerStmt structure containing the parsed CREATE SERVER statement details including server name, FDW name, server type, version, options, and IF NOT EXISTS flag

## Dependencies
- Functions called/Symbols referenced:
  - CreateForeignServerStmt
  - AclResult
  - [ForeignDataWrapper](../F/ForeignDataWrapper.md)
  - [get_foreign_server_oid](../g/get_foreign_server_oid.md)
  - ObjectAddressSet
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)
  - [GetForeignDataWrapperByName](../G/GetForeignDataWrapperByName.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - namein
  - DirectFunctionCall1
  - [CStringGetDatum](CStringGetDatum.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - PointerIsValid
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Automatically assigns the effective user ID as the server owner (cannot be overridden during creation)
- Supports IF NOT EXISTS semantics with proper extension membership validation for security
- Requires USAGE privilege on the underlying foreign-data wrapper
- Optional server type and version parameters can be specified for documentation purposes
- Server options are validated using the FDW's validator function if available
- Records dependency on the associated FDW to prevent orphaned servers
- Supports extension membership through recordDependencyOnCurrentExtension
- Triggers object creation hooks for extensibility
- Returns InvalidObjectAddress when IF NOT EXISTS is used and server already exists
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling logical organization of external data sources