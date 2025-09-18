# AlterTableSpaceOptions

## Location
src/backend/commands/tablespace.c: 1015 - 1090

## Overview
Modifies the configuration options of an existing tablespace by updating the spcoptions field in the pg_tablespace system catalog.

## Definition
Oid AlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt)

## Detailed Description
This function implements the ALTER TABLESPACE ... SET/RESET option functionality in PostgreSQL. It validates the existence of the specified tablespace, checks ownership permissions, and then processes the option changes. The function retrieves the current tablespace options, transforms them according to the ALTER statement (either setting new values or resetting to defaults), validates the new options using tablespace_reloptions(), and updates the system catalog. The operation is performed with row-exclusive locking to ensure consistency. After successful modification, it triggers post-alter hooks and performs proper resource cleanup.

## Parameters / Member Variables
- stmt: Pointer to AlterTableSpaceOptionsStmt containing the tablespace name, options to modify, and operation type (SET/RESET)

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens pg_tablespace relation with RowExclusiveLock
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for catalog lookup
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Begins catalog table scan
  - [heap_getnext](../h/heap_getnext.md): Retrieves next heap tuple from scan
  - [object_ownercheck](../o/object_ownercheck.md): Verifies ownership permissions
  - [aclcheck_error](../a/aclcheck_error.md): Reports access control violations
  - [heap_getattr](../h/heap_getattr.md): Extracts attribute value from heap tuple
  - [transformRelOptions](../t/transformRelOptions.md): Processes relation option changes
  - [tablespace_reloptions](../t/tablespace_reloptions.md): Validates tablespace-specific options
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified version of heap tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates tuple in system catalog
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
  - [heap_freetuple](../h/heap_freetuple.md): Frees allocated heap tuple memory
  - [table_endscan](../t/table_endscan.md): Ends table scan
  - table_close: Closes relation

- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility command processing handler

## Notes and Other Information
- Requires ownership of the tablespace to perform option modifications
- Validates new options through tablespace_reloptions() before applying changes
- Supports both SET and RESET operations through the isReset flag in the statement
- Uses heap_modify_tuple() to construct the updated catalog tuple efficiently
- Properly handles NULL values when options are reset to defaults
- Integrates with PostgreSQL's object dependency system through post-alter hooks
- Returns the OID of the modified tablespace for further processing
- Part of PostgreSQL's DDL infrastructure for tablespace configuration management
- Maintains catalog consistency through appropriate locking and transaction handling