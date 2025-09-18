# CreateAccessMethod

## Location
src/backend/commands/amcmds.c: 43 - 128

## Overview
Registers a new access method in the PostgreSQL system catalog, creating the necessary catalog entries and dependency records.

## Definition


## Detailed Description
CreateAccessMethod processes a CREATE ACCESS METHOD statement by inserting a new tuple into the pg_am system catalog. The function performs several validation checks including superuser privilege verification and name uniqueness, then creates the catalog entry with proper dependency tracking. It establishes a dependency relationship between the access method and its handler function, and records the access method as part of the current extension if applicable.

## Parameters / Member Variables
- : Pointer to CreateAmStmt structure containing the access method name, handler function name, and access method type

## Dependencies
- Functions called/Symbols referenced:
  - superuser: Checks if current user has superuser privileges
  - GetSysCacheOid1: Looks up existing access method by name
  - [lookup_am_handler_func](../l/lookup_am_handler_func.md): Validates and retrieves handler function OID
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generates new OID for the access method
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple for catalog insertion
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts tuple into pg_am catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees tuple memory
  - [recordDependencyOn](../r/recordDependencyOn.md): Records dependency on handler function
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md): Records extension membership
  - InvokeObjectPostCreateHook: Triggers post-creation hooks
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processor

## Notes and Other Information
- Requires superuser privileges to execute
- Validates that the access method name is unique in the system
- Automatically establishes DEPENDENCY_NORMAL relationship with the handler function
- Supports extension membership tracking for proper cleanup during extension drops
- Uses row-exclusive locking on the pg_am catalog during the operation
- Location: src/backend/commands/amcmds.c:43-128