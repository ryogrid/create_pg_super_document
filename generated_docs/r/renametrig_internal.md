# renametrig_internal

## Location
src/backend/commands/trigger.c: 1577 - 1647

## Overview
A subroutine that performs the actual work of renaming a single trigger on one table, including name conflict checking and catalog updates.

## Definition
```c
static void renametrig_internal(Relation tgrel, Relation targetrel, HeapTuple trigtup, const char *newname, const char *expected_name)
```

## Detailed Description
This function implements the low-level mechanics of trigger renaming for a single trigger on a single relation. It performs several critical operations: checks if the trigger already has the target name (early return optimization), scans for name conflicts with existing triggers on the same relation, creates a modifiable copy of the trigger tuple, updates the trigger name in the tuple, commits the change to the catalog, and invalidates the relation cache to ensure other backends see the changes.

The function also includes a notification mechanism that issues a NOTICE when the actual trigger name differs from the expected name, which can occur when following parent-child relationships in partitioned tables.

## Parameters / Member Variables
- `tgrel`: Open relation handle for the pg_trigger system catalog
- `targetrel`: Open relation handle for the table containing the trigger
- `trigtup`: HeapTuple representing the trigger to be renamed
- `newname`: The desired new name for the trigger
- `expected_name`: The expected current name of the trigger (used for validation notices)

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT
  - strcmp
  - NameStr
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - RelationGetRelid
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - HeapTupleIsValid
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - namestrcpy
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
- Called from (representative examples):
  - [renametrig](renametrig.md)
  - [renametrig_partition](renametrig_partition.md)

## Notes and Other Information
- Performs early optimization by checking if the trigger already has the target name
- Enforces uniqueness by scanning for existing triggers with the new name before attempting the rename
- Uses heap_copytuple to create a modifiable copy of the original tuple for updates
- Emits a NOTICE when the actual trigger name differs from expected (typically when following partition relationships)
- Triggers post-alter hooks to maintain consistency with the dependency system
- Invalidates relation cache to ensure distributed consistency across all backends
- Handles name conflicts gracefully with informative error messages including relation and trigger names