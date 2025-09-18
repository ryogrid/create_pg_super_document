# RemoveExtensionById

## Location
src/backend/commands/extension.c: 1954 - 2007

## Overview
RemoveExtensionById removes a PostgreSQL extension by deleting its pg_extension catalog tuple, relying on the dependency infrastructure to handle cleanup of associated objects.

## Definition


## Detailed Description
This function implements the core deletion logic for PostgreSQL extensions. It performs a critical safety check to prevent deletion of extensions that are currently being modified (tracked via CurrentExtensionObject global variable), which could create dangling dependency records. The function then locates and deletes the corresponding tuple in the pg_extension system catalog. All cleanup of extension-owned objects, schema dependencies, and other related resources is handled automatically by PostgreSQL's dependency infrastructure, making this function's role focused solely on catalog tuple removal.

## Parameters / Member Variables
- : OID of the extension to be removed from the system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_name](../g/get_extension_name.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_open
  - table_close
- Global variables referenced:
  - CurrentExtensionObject
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md)

## Notes and Other Information
- This function is called by the dependency management system rather than directly by DROP EXTENSION commands
- Implements crucial safety measures to prevent deletion of extensions currently being modified, avoiding dangling dependency references
- Uses RowExclusiveLock on the pg_extension catalog during deletion operations
- Relies heavily on PostgreSQL's dependency infrastructure for complete cleanup of extension objects
- The safety check against CurrentExtensionObject prevents recursive deletion scenarios that could occur through dependency cascades
- Assumes at most one matching tuple exists for the given extension OID
- Part of the broader dependency management framework, designed to be called from doDeletion() in dependency.c