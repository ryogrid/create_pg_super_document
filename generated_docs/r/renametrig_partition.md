# renametrig_partition

## Location
src/backend/commands/trigger.c: 1648 - 1720

## Overview
A recursive helper function for renaming triggers on partitioned tables that finds and renames corresponding child triggers in partition relations.

## Definition
```c
static void renametrig_partition(Relation tgrel, Oid partitionId, Oid parentTriggerOid, const char *newname, const char *expected_name)
```

## Detailed Description
This function implements the recursive logic needed to maintain trigger name consistency across partition hierarchies. When a trigger is renamed on a partitioned table, this function ensures that all corresponding triggers on child partitions are also renamed to maintain the inheritance relationship. The function searches for triggers on the specified partition that have the given parent trigger OID, renames the matching trigger using renametrig_internal, and recursively processes any sub-partitions if the current partition is itself partitioned.

The function maintains the parent-child relationship between triggers by using the tgparentid field to identify which triggers correspond to the parent being renamed. This ensures that the entire partition hierarchy remains consistent after the rename operation.

## Parameters / Member Variables
- `tgrel`: Open relation handle for the pg_trigger system catalog
- `partitionId`: Object identifier of the partition relation to process
- `parentTriggerOid`: Object identifier of the parent trigger being renamed
- `newname`: The new name to assign to the child trigger
- `expected_name`: The expected current name of the child trigger

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - HeapTupleIsValid
  - [systable_getnext](../s/systable_getnext.md)
  - GETSTRUCT
  - table_open
  - [renametrig_internal](renametrig_internal.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [renametrig_partition](renametrig_partition.md) (recursive call)
  - NameStr
  - table_close
  - [systable_endscan](../s/systable_endscan.md)
- Called from (representative examples):
  - [renametrig](renametrig.md)
  - [renametrig_partition](renametrig_partition.md) (recursive)

## Notes and Other Information
- Uses tgparentid to identify child triggers that correspond to the parent trigger being renamed
- Implements recursive descent through partition hierarchies to handle nested partitioning
- Breaks after finding the first matching trigger (there should be at most one per partition)
- Opens and closes partition relations with NoLock since locks are already held by the caller
- Passes the current trigger name as expected_name for subsequent recursive calls
- Maintains consistency in deeply nested partition hierarchies through recursive processing
- Assumes exclusive locks on all partitions are already held by the calling function