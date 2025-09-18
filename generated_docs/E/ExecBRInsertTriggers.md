# ExecBRInsertTriggers

## Location
src/backend/commands/trigger.c: 2460 - 2535

## Overview
Executes BEFORE ROW INSERT triggers for each tuple being inserted, allowing triggers to modify tuple data or skip the insert operation entirely while ensuring partition constraints are maintained.

## Definition
```c
bool ExecBRInsertTriggers(EState *estate, ResultRelInfo *relinfo,
                          TupleTableSlot *slot)
```

## Detailed Description
This function manages the execution of BEFORE ROW INSERT triggers, which fire once for each tuple being inserted and can modify the tuple data or prevent the insertion. It implements several critical features:

1. **Tuple Modification**: Triggers can return a modified tuple, which replaces the original in the slot
2. **Insert Prevention**: Triggers can return NULL to skip inserting the current tuple
3. **Partition Validation**: For partitioned tables, ensures modified tuples still belong to the correct partition
4. **Memory Management**: Properly handles HeapTuple lifecycle and memory cleanup
5. **Performance Optimization**: Lazy tuple materialization - only converts slot to HeapTuple when needed

The function iterates through all applicable triggers, calling each one with the current tuple data. If a trigger modifies the tuple, the changes are stored back in the slot. If a trigger is defined on a partition (tgisclone), additional validation ensures the modified tuple still fits the partition constraints.

## Parameters / Member Variables
- `estate`: Executor state containing execution context and memory management information
- `relinfo`: Result relation info containing trigger descriptors, function cache, and relation metadata
- `slot`: TupleTableSlot containing the tuple being inserted, which may be modified by triggers

## Return Value
- `true`: Insert operation should proceed (possibly with modified tuple data)
- `false`: Insert operation should be skipped (trigger returned NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md) (converts slot to HeapTuple for trigger processing)
  - TRIGGER_TYPE_MATCHES (trigger type filtering macro)
  - [TriggerEnabled](../T/TriggerEnabled.md) (trigger enable state and condition checking)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md) (actual trigger execution)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md) (stores modified tuple back to slot)
  - [ExecPartitionCheck](ExecPartitionCheck.md) (validates partition constraints)
  - [heap_freetuple](../h/heap_freetuple.md) (memory cleanup)
  - GetPerTupleMemoryContext (memory context management)
- Data structures used:
  - TriggerData (trigger execution context)
  - TriggerDesc (trigger descriptor from relinfo)
  - Trigger (individual trigger structure)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (during COPY FROM operations)
  - [ExecInsert](ExecInsert.md) (from nodeModifyTable executor)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md) (logical replication)

## Notes and Other Information
- BEFORE ROW triggers fire once per tuple and can inspect/modify individual row data
- Triggers returning NULL effectively act as row-level filters, preventing specific inserts
- The partition check prevents data inconsistency when triggers modify partition keys
- Memory management is carefully handled to avoid leaks during tuple modifications
- Trigger execution order follows the creation order of triggers on the table
- The function supports both regular tables and partitioned table hierarchies
- Critical for maintaining data integrity while allowing flexible business logic implementation
- Used in high-throughput operations like COPY FROM where performance optimization is essential