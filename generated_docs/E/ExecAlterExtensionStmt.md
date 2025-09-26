# ExecAlterExtensionStmt

## Location
[src/backend/commands/extension.c:2987-3133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2987-L3133)

## Overview
Executes ALTER EXTENSION UPDATE command to update an extension from its current version to a specified target version by running the appropriate sequence of update scripts.

## Definition

```c
ObjectAddress
ExecAlterExtensionStmt(ParseState *pstate, AlterExtensionStmt *stmt)
```
## Detailed Description
This function implements the ALTER EXTENSION UPDATE command, which upgrades or downgrades an extension to a different version. The function validates the extension exists, determines the current version, identifies the target version (from statement options or extension default), and calculates the sequence of update scripts needed to reach the target version.

The function prevents nested ALTER EXTENSION operations using the global creating_extension flag. It performs ownership checks, reads the extension control file to understand available versions and update paths, and delegates the actual update execution to ApplyExtensionUpdates. If the extension is already at the target version, it reports a notice and returns without action.

## Parameters / Member Variables
-  (ParseState *): Parse state for the SQL statement (used for error reporting)
-  (AlterExtensionStmt *): Parsed ALTER EXTENSION statement containing extension name and options

## Dependencies
- Functions called/Symbols referenced:
  - table_open/systable_beginscan: Accesses pg_extension catalog
  - heap_getattr: Retrieves current extension version
  - text_to_cstring: Converts version datum to string
  - object_ownercheck: Verifies ownership of extension
  - read_extension_control_file: Reads extension control file
  - errorConflictingDefElem: Reports conflicting statement options
  - check_valid_version_name: Validates version name format
  - identify_update_path: Determines sequence of update scripts
  - ApplyExtensionUpdates: Executes the actual update process
  - ObjectAddressSet: Creates return address object
- Called from (representative examples):
  - ProcessUtilitySlow: Main utility command dispatcher for ALTER EXTENSION

## Notes and Other Information
- Uses global creating_extension flag to prevent nested extension operations
- Supports specifying target version via new_version option or uses control file default
- Reports notice and exits early if extension is already at target version
- Performs comprehensive ownership and version validation
- The actual update execution is delegated to ApplyExtensionUpdates function
- Handles both upgrades and downgrades by finding appropriate update path
- Returns InvalidObjectAddress if no update is needed (already at target version)
- Uses AccessShareLock for reading pg_extension catalog
- Located in src/backend/commands/extension.c:2987-3133