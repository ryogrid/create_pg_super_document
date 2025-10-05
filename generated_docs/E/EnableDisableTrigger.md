# EnableDisableTrigger

## Location
[src/backend/commands/trigger.c:1721-1855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1721-L1855)

## Overview
Enables or disables triggers on a relation based on specified criteria, with support for recursive processing on partitioned tables and various filtering options.

## Definition
```c
void EnableDisableTrigger(Relation rel, const char *tgname, Oid tgparent, char fires_when, bool skip_system, bool recurse, LOCKMODE lockmode)
```

## Detailed Description
This function implements the core logic for PostgreSQL's ALTER TABLE ENABLE/DISABLE TRIGGER commands. It modifies the tgenabled field in pg_trigger to control when triggers fire, supporting various modes including session replication role considerations. The function can target specific triggers by name, process triggers with specific parent relationships, and optionally recurse through partition hierarchies.

The function enforces security by requiring superuser privileges for system trigger modifications and provides flexible filtering options to control which triggers are affected. For partitioned tables, it automatically handles the complexity of maintaining consistency across all partitions when processing row-level triggers.

## Parameters / Member Variables
- `rel`: Relation to process (caller must hold suitable lock)
- `tgname`: Name of specific trigger to process, or NULL to scan all triggers
- `tgparent`: If not zero, process only triggers with this tgparentid
- `fires_when`: New value for tgenabled field (defines when trigger should fire)
- `skip_system`: If true, skip system/constraint triggers
- `recurse`: If true, recurse to partitions
- `lockmode`: Lock mode to use when opening partition relations

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - RelationGetRelid
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - HeapTupleIsValid
  - [systable_getnext](../s/systable_getnext.md)
  - GETSTRUCT
  - OidIsValid
  - [superuser](../s/superuser.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - TRIGGER_FOR_ROW
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [relation_open](../r/relation_open.md)
  - [EnableDisableTrigger](EnableDisableTrigger.md) (recursive call)
  - [table_close](../t/table_close.md)
  - InvokeObjectPostAlterHook
  - [systable_endscan](../s/systable_endscan.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
- Called from (representative examples):
  - [ATExecEnableDisableTrigger](../A/ATExecEnableDisableTrigger.md)
  - [EnableDisableTrigger](EnableDisableTrigger.md) (recursive)

## Notes and Other Information
- Called by ALTER TABLE ENABLE/DISABLE [REPLICA | ALWAYS] TRIGGER commands
- Enforces superuser requirement for modifying system triggers (tgisinternal)
- Uses efficient catalog scanning with appropriate indexes for trigger lookup
- Supports filtering by trigger name, parent relationship, and system trigger status
- For partitioned tables, recursively processes all partitions to maintain consistency
- Only processes row-level triggers when recursing to partitions
- Invalidates relation cache when changes are made to ensure distributed consistency
- Uses proper tuple copying and cleanup to avoid memory leaks during catalog updates
- Provides detailed error messages for missing triggers when specific names are requested

## Simplified Source

```c
void EnableDisableTrigger(Relation rel, const char *tgname, Oid tgparent,
                         char fires_when, bool skip_system, bool recurse,
                         LOCKMODE lockmode) {
    // Open pg_trigger catalog for modification
    Relation tgrel = table_open(TriggerRelationId, RowExclusiveLock);

    // Setup scan keys based on parameters
    ScanKeyData keys[2];
    int nkeys;
    ScanKeyInit(&keys[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));

    if (tgname) {
        ScanKeyInit(&keys[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber,
                    F_NAMEEQ, CStringGetDatum(tgname));
        nkeys = 2;
    } else {
        nkeys = 1;
    }

    // Scan triggers matching criteria
    SysScanDesc tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId,
                                           true, NULL, nkeys, keys);
    bool found = false, changed = false;
    HeapTuple tuple;

    while (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
        Form_pg_trigger oldtrig = (Form_pg_trigger) GETSTRUCT(tuple);

        // Apply filters
        if (OidIsValid(tgparent) && tgparent != oldtrig->tgparentid)
            continue;
        if (oldtrig->tgisinternal && skip_system)
            continue;
        if (oldtrig->tgisinternal && !superuser())
            ereport(ERROR, "permission denied: system trigger");

        found = true;

        // Update trigger state if different
        if (oldtrig->tgenabled != fires_when) {
            HeapTuple newtup = heap_copytuple(tuple);
            Form_pg_trigger newtrig = (Form_pg_trigger) GETSTRUCT(newtup);
            newtrig->tgenabled = fires_when;
            CatalogTupleUpdate(tgrel, &newtup->t_self, newtup);
            heap_freetuple(newtup);
            changed = true;
        }

        // Recurse to partitions if needed
        if (recurse && rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
            TRIGGER_FOR_ROW(oldtrig->tgtype)) {
            PartitionDesc partdesc = RelationGetPartitionDesc(rel, true);
            for (int i = 0; i < partdesc->nparts; i++) {
                Relation part = relation_open(partdesc->oids[i], lockmode);
                EnableDisableTrigger(part, NULL, oldtrig->oid, fires_when,
                                   skip_system, recurse, lockmode);
                table_close(part, NoLock);
            }
        }

        InvokeObjectPostAlterHook(TriggerRelationId, oldtrig->oid, 0);
    }

    systable_endscan(tgscan);
    table_close(tgrel, RowExclusiveLock);

    // Error if specific trigger not found
    if (tgname && !found)
        ereport(ERROR, "trigger does not exist");

    // Invalidate cache if changes were made
    if (changed)
        CacheInvalidateRelcache(rel);
}
```