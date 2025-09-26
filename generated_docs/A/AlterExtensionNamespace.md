# AlterExtensionNamespace

## Location
src/backend/commands/extension.c: 2772 - 2986

## Overview
Executes ALTER EXTENSION SET SCHEMA command to move an extension and all its member objects from one schema to another schema.

## Definition


## Detailed Description
This function implements the ALTER EXTENSION SET SCHEMA command, which relocates an extension and all its dependent objects to a new schema. The operation requires the extension to be marked as relocatable in its control file. The function performs extensive validation including ownership checks, permission checks, dependency loop detection, and no-relocate constraint enforcement.

The function iterates through all objects that depend on the extension (via pg_depend) and calls AlterObjectNamespace_oid for each one to move them to the new schema. It ensures all objects are consistently moved and maintains dependency relationships. The function also handles special cases like preventing moves that would create dependency loops and respecting no-relocate requests from dependent extensions.

## Parameters / Member Variables
-  (const char *): Name of the extension to relocate
-  (const char *): Name of the target schema
-  (Oid *): Optional output parameter to receive the OID of the old schema

## Dependencies
- Functions called/Symbols referenced:
  - get_extension_oid: Resolves extension name to OID
  - LookupCreationNamespace: Resolves target schema name to OID
  - object_ownercheck: Verifies ownership of extension
  - object_aclcheck: Checks creation permissions in target schema
  - getExtensionOfObject: Checks for dependency loops
  - table_open/systable_beginscan: Accesses pg_extension and pg_depend catalogs
  - read_extension_control_file: Reads extension control file for no_relocate list
  - AlterObjectNamespace_oid: Moves individual objects to new schema
  - changeDependencyFor: Updates schema dependency for extension
  - InvokeObjectPostAlterHook: Triggers post-alter hooks
- Called from (representative examples):
  - ExecAlterObjectSchemaStmt: Main entry point for ALTER ... SET SCHEMA commands

## Notes and Other Information
- Requires extension to be marked as relocatable in its control file
- Performs comprehensive permission checks (ownership and CREATE rights in target schema)
- Prevents dependency loops by checking if target schema is owned by the extension
- Respects no-relocate constraints from dependent extensions
- Ensures all extension objects are moved consistently to the same schema
- Returns InvalidObjectAddress if extension is already in target schema
- Updates both pg_extension.extnamespace and dependency records
- Uses RowExclusiveLock on pg_extension to prevent concurrent modifications
- Located in src/backend/commands/extension.c:2772-2986