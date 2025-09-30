# renametrig_internal

## Location
[src/backend/commands/trigger.c:1577-1647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1577-L1647)

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
  - [namestrcpy](../n/namestrcpy.md)
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

## Simplified Source

```c
static void renametrig_internal(Relation tgrel, Relation targetrel, HeapTuple trigtup,
                              const char *newname, const char *expected_name)
{
    Form_pg_trigger tgform = (Form_pg_trigger) GETSTRUCT(trigtup);

    // Early return if trigger already has the new name
    if (strcmp(NameStr(tgform->tgname), newname) == 0)
        return;

    // Check for name conflicts with existing triggers
    ScanKeyData key[2];
    ScanKeyInit(&key[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(targetrel)));
    ScanKeyInit(&key[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ,
                PointerGetDatum(newname));

    SysScanDesc tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 2, key);
    HeapTuple tuple;
    if (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("trigger \"%s\" for relation \"%s\" already exists",
                        newname, RelationGetRelationName(targetrel))));
    systable_endscan(tgscan);

    // Create modifiable copy and update name
    tuple = heap_copytuple(trigtup);
    tgform = (Form_pg_trigger) GETSTRUCT(tuple);

    // Notify if actual name differs from expected
    if (strcmp(NameStr(tgform->tgname), expected_name) != 0)
        ereport(NOTICE,
                errmsg("renamed trigger \"%s\" on relation \"%s\"",
                       NameStr(tgform->tgname), RelationGetRelationName(targetrel)));

    // Update catalog with new name
    namestrcpy(&tgform->tgname, newname);
    CatalogTupleUpdate(tgrel, &tuple->t_self, tuple);

    // Post-alter hooks and cache invalidation
    InvokeObjectPostAlterHook(TriggerRelationId, tgform->oid, 0);
    CacheInvalidateRelcache(targetrel);
}
```