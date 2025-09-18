# AlterForeignDataWrapper

## Location
src/backend/commands/foreigncmds.c: 685 - 848

## Overview
Modifies an existing foreign-data wrapper (FDW) by updating its handler function, validator function, and/or options in the PostgreSQL system catalog.

## Definition
```c
ObjectAddress AlterForeignDataWrapper(ParseState *pstate, AlterFdwStmt *stmt)
```

## Detailed Description
This function implements the ALTER FOREIGN DATA WRAPPER SQL command by modifying an existing FDW entry in the pg_foreign_data_wrapper system catalog. It performs comprehensive validation including superuser privilege checks, existence verification, and function option processing. The function handles selective updates to handler functions, validator functions, and generic options while maintaining dependency consistency. It provides warnings when changes might affect existing foreign tables or dependent objects, and properly manages function dependencies by removing old ones and creating new ones as needed.

## Parameters / Member Variables
- `pstate`: ParseState context for parsing operations and error reporting
- `stmt`: AlterFdwStmt structure containing the parsed ALTER FOREIGN DATA WRAPPER statement details including name, options, and function modifications

## Dependencies
- Functions called/Symbols referenced:
  - AlterFdwStmt
  - Form_pg_foreign_data_wrapper
  - superuser
  - SearchSysCacheCopy1
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [parse_func_options](../p/parse_func_options.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - PointerIsValid
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - InvokeObjectPostAlterHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires superuser privileges to execute; regular users cannot alter FDWs
- Validates FDW existence before attempting modifications
- Provides warnings when changing handler functions about potential behavior changes in existing foreign tables
- Provides warnings when changing validator functions about potential invalidation of dependent object options
- Supports partial updates - only specified attributes are modified
- Properly manages function dependencies by deleting old dependencies and creating new ones
- Uses heap_modify_tuple for selective column updates rather than full tuple replacement
- Triggers object alteration hooks for extensibility
- Returns ObjectAddress of the modified FDW for further reference
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling dynamic reconfiguration of external data source integrations