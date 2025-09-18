# CloneRowTriggersToPartition

## Location
src/backend/commands/tablecmds.c: 18984 - 19140

## Overview
CloneRowTriggersToPartition clones row-level triggers from a parent partitioned table to a newly attached partition, excluding internal triggers and statement-level triggers.

## Definition
```c
static void CloneRowTriggersToPartition(Relation parent, Relation partition)
```

## Detailed Description
This function is responsible for replicating appropriate triggers from a partitioned table to its partitions during partition attachment or relation definition. The function performs selective trigger cloning with several important considerations:

**Trigger Selection:**
- Only processes row-level triggers (excludes statement-level triggers)
- Skips internal triggers since they are handled by constraint cloning mechanisms
- Only handles BEFORE and AFTER triggers (validates trigger types)

**Trigger Reconstruction:**
The function reads trigger metadata from pg_trigger and reconstructs each trigger by:
1. **WHEN clause processing**: Transforms trigger WHEN conditions using map_partition_varattnos to adjust attribute references for both OLD and NEW row variables
2. **Column list mapping**: Converts column attribute numbers to column names for column-specific triggers  
3. **Argument reconstruction**: Rebuilds trigger function arguments from the stored bytea format
4. **Constraint relationship**: Preserves constraint trigger relationships when applicable

**Memory Management:**
Uses a per-tuple memory context that is reset after each trigger to prevent memory leaks during bulk operations.

The function creates new triggers on the partition using CreateTriggerFiringOn, maintaining the same behavior characteristics (timing, events, deferability) as the parent triggers.

## Parameters / Member Variables
- `parent`: The parent partitioned table relation from which to clone triggers
- `partition`: The partition relation where triggers should be created

## Dependencies
- Functions called/Symbols referenced:
  - systable_beginscan, systable_getnext, AllocSetContextCreate
  - heap_getattr, stringToNode, TextDatumGetCString
  - map_partition_varattnos (for OLD and NEW variable mapping)
  - makeString, DatumGetByteaPP, makeNode
  - CreateTriggerFiringOn, MemoryContextReset, MemoryContextDelete
- Called from (representative examples):
  - ATExecAttachPartition
  - DefineRelation
  - child_dependency_type

## Notes and Other Information
- Static function used as a subroutine during partition setup operations
- Uses RowExclusiveLock on pg_trigger to ensure consistent trigger metadata access
- Handles both constraint triggers (tgconstraint) and regular triggers appropriately
- Maps variable attribute numbers in WHEN clauses to account for different column orders between parent and partition
- Does not clone transition table triggers (transitionRels set to NIL) as they are not currently supported on partitions
- Preserves trigger enablement state, deferability, and timing characteristics from the parent
- Uses ALLOCSET_SMALL_SIZES for memory context since trigger definitions are typically small
- Critical for maintaining trigger behavior consistency across the partition hierarchy