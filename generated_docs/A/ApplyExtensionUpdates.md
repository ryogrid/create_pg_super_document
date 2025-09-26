# ApplyExtensionUpdates

## Location
[src/backend/commands/extension.c:3134-3291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L3134-L3291)

## Overview
Applies a series of update scripts sequentially to upgrade an extension through multiple versions, treating each step as an individual ALTER EXTENSION UPDATE command.

## Definition

```c
static void
ApplyExtensionUpdates(Oid extensionOid,
					  ExtensionControlFile *pcontrol,
					  const char *initialVersion,
					  List *updateVersions,
					  char *origSchemaName,
					  bool cascade,
					  bool is_create)
```
## Detailed Description
This function manages the complex process of updating PostgreSQL extensions through multiple version increments. It iterates through a list of target versions, applying each update script in sequence while maintaining proper metadata and dependency tracking. For each version update, it:

1. Loads the version-specific control file parameters
2. Updates the pg_extension catalog entry with new version and relocatability information
3. Resolves and installs prerequisite extensions if needed
4. Updates dependency records to reflect current requirements
5. Executes the actual update script for the version transition
6. Triggers post-alter hooks for proper event handling

The function ensures that older update scripts remain functional even when newer versions have different control parameters by treating each step as a discrete update operation.

## Parameters / Member Variables
- : OID of the extension being updated
- : Primary extension control file containing base configuration
- : Starting version name for the update sequence
- : List of target version names to update through sequentially
- : Original schema name where extension was installed
- : Whether to automatically install missing prerequisite extensions
- : Flag indicating if this is part of extension creation process

## Dependencies
- Functions called/Symbols referenced:
  - [read_extension_aux_control_file](../r/read_extension_aux_control_file.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [get_namespace_name](../g/get_namespace_name.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [get_required_extension](../g/get_required_extension.md)
  - [get_extension_schema](../g/get_extension_schema.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - InvokeObjectPostAlterHook
  - [execute_extension_script](../e/execute_extension_script.md)
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md) (during extension creation)
  - [ExecAlterExtensionStmt](../E/ExecAlterExtensionStmt.md) (during explicit extension updates)

## Notes and Other Information
- This function is static and only used internally within extension.c
- Each update step is treated as a separate transaction-level operation with proper catalog updates
- The function handles prerequisite extension resolution and dependency graph updates at each step
- Proper locking (RowExclusiveLock) is maintained on the extension catalog during updates
- The design ensures backward compatibility with older update scripts even when control file parameters change in newer versions