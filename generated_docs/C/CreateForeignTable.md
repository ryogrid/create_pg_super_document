# CreateForeignTable

## Location
src/backend/commands/foreigncmds.c: 1415 - 1494

## Overview
Creates a new foreign table entry in the system catalog, establishing the metadata and dependencies necessary for accessing external data through PostgreSQL's foreign data wrapper system.

## Definition
```c
void CreateForeignTable(CreateForeignTableStmt *stmt, Oid relid)
```

## Detailed Description
This function is called after DefineRelation() to complete the creation of a foreign table by adding foreign-table-specific metadata to the pg_foreign_table system catalog. It handles the association between the newly created relation and its corresponding foreign server, validates user permissions for server usage, processes foreign table options through the appropriate foreign data wrapper validator, and establishes proper dependency relationships. The function ensures that the effective user has USAGE privileges on the specified foreign server and creates a normal dependency between the foreign table and its server to ensure proper cleanup semantics.

## Parameters / Member Variables
- `stmt`: Pointer to CreateForeignTableStmt structure containing the parsed CREATE FOREIGN TABLE command details including server name and table-specific options
- `relid`: The OID of the relation that was already created by DefineRelation(), representing the foreign table's entry in pg_class

## Dependencies
- Functions called/Symbols referenced:
  - CommandCounterIncrement
  - table_open
  - [GetUserId](../G/GetUserId.md)
  - [GetForeignServerByName](../G/GetForeignServerByName.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - table_close
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
The function is designed to be called as a second phase after the basic relation has been created, requiring a command counter increment to ensure visibility of any previous tuple updates. Currently, the table owner is always set to the effective user ID and cannot be specified during creation. The function establishes a normal dependency on the foreign server, ensuring that foreign tables are properly dropped when their associated server is removed.