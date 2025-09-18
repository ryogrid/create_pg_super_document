# renametrig

## Location
src/backend/commands/trigger.c: 1463 - 1576

## Overview
Renames a trigger on a relation by modifying the trigger name in the system catalog and handling partitioned table inheritance.

## Definition
```c
ObjectAddress renametrig(RenameStmt *stmt)
```

## Detailed Description
This function implements the core logic for renaming triggers in PostgreSQL. It performs several key operations: validates permissions and relation compatibility using a callback, searches for the target trigger in the system catalog, enforces restrictions on partition triggers (which cannot be renamed independently), executes the actual rename operation, and recursively renames corresponding triggers on all partitions if the target is a partitioned table.

The function ensures data consistency by acquiring exclusive locks and maintaining the hierarchical relationship between triggers on partitioned tables and their partitions. It prevents inconsistencies that could break pg_dump functionality by disallowing independent renaming of partition triggers.

## Parameters / Member Variables
- `stmt`: Pointer to RenameStmt structure containing the relation reference, old trigger name (subname), and new trigger name (newname)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelidExtended
  - RangeVarCallbackForRenameTrigger
  - relation_open
  - find_all_inheritors
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - get_partition_parent
  - get_rel_name
  - renametrig_internal
  - RelationGetPartitionDesc
  - renametrig_partition
  - ObjectAddressSet
  - systable_endscan
  - table_close
  - relation_close
- Called from (representative examples):
  - ExecRenameStmt

## Notes and Other Information
- Acquires AccessExclusiveLock on the target relation and holds it until transaction end
- For partitioned tables, locks all inheritor relations upfront to prevent deadlocks
- Rejects attempts to rename triggers that have a parent trigger (tgparentid is valid)
- Automatically propagates renames to all partition triggers when renaming on a partitioned table
- Uses system catalog scans with appropriate indexes for efficient trigger lookup
- Returns an ObjectAddress pointing to the renamed trigger for dependency tracking
- Maintains trigger name consistency across partition hierarchies to ensure pg_dump compatibility