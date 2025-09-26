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