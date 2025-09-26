# DropClonedTriggersFromPartition

## Location
[src/backend/commands/tablecmds.c:19722-19786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19722-L19786)

## Overview
DropClonedTriggersFromPartition removes triggers that were cloned to a partition when it was created or attached, undoing the work performed by CloneRowTriggersToPartition.

## Definition
```c
static void DropClonedTriggersFromPartition(Oid partitionId)
```

## Detailed Description
This function is a subroutine of ATExecDetachPartition that handles the cleanup of triggers when a partition is being detached from its parent table. When partitions are created or attached, certain triggers from the parent table are cloned to the partition to maintain trigger behavior across the partitioned table hierarchy.

The function performs a systematic cleanup process:

1. **Trigger identification**: Scans pg_trigger to find all triggers on the specified partition
2. **Filtering**: Identifies only cloned triggers (those with valid tgparentid) while excluding:
   - Non-cloned triggers (no parent)
   - Internal foreign key constraint triggers (handled separately)
3. **Dependency cleanup**: Removes both partition-primary and partition-secondary dependency records
4. **Batch deletion**: Collects all applicable triggers and performs bulk deletion

This process ensures that when a partition is detached, it doesn't retain triggers that were only meant to be part of the partitioned table's trigger inheritance hierarchy.

## Parameters / Member Variables
- `partitionId`: OID of the partition relation from which cloned triggers should be removed

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [DetachPartitionFinalize](DetachPartitionFinalize.md)

## Notes and Other Information
- Only removes triggers that have a valid tgparentid (indicating they were cloned)
- Excludes foreign key constraint triggers which are handled separately in the detachment process
- Uses DEPENDENCY_PARTITION_PRI and DEPENDENCY_PARTITION_SEC to identify partition-specific dependencies
- Employs batch deletion for efficiency when multiple triggers need to be removed
- The dependency removal is made visible via CommandCounterIncrement before performing deletions
- Essential for preventing orphaned triggers after partition detachment
- Part of the comprehensive cleanup required to properly separate a partition from its parent table