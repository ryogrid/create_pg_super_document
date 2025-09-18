# ReorderBufferChangeSize

## Location
src/backend/replication/logical/reorderbuffer.c: 4302 - 4386

## Overview
ReorderBufferChangeSize calculates the memory size of a ReorderBufferChange structure, accounting for the variable-sized data associated with different types of logical replication changes.

## Definition
```c
static Size ReorderBufferChangeSize(ReorderBufferChange *change)
```

## Detailed Description
This function computes the total memory footprint of a ReorderBufferChange structure by examining the change type and calculating the size of associated data. It handles various types of logical replication changes including tuple operations (INSERT, UPDATE, DELETE), messages, cache invalidations, snapshots, and truncation operations. The function is critical for memory management in the logical replication subsystem, particularly when serializing changes to disk or estimating memory usage.

The function performs a switch statement on the change action type and adds the appropriate size calculations for each type of change data structure.

## Parameters / Member Variables
- `change`: Pointer to a ReorderBufferChange structure whose memory size needs to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferChange](ReorderBufferChange.md) (structure type)
  - [HeapTupleData](../H/HeapTupleData.md) (structure type)
  - [SnapshotData](../S/SnapshotData.md) (structure type) 
  - SharedInvalidationMessage (structure type)
  - REORDER_BUFFER_CHANGE_* constants (various change type enums)
- Called from (representative examples):
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
  - [ReorderBufferTruncateTXN](ReorderBufferTruncateTXN.md)
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md)
  - [ReorderBufferToastReplace](ReorderBufferToastReplace.md)

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function handles different change types with varying data structures and sizes
- For tuple changes (INSERT/UPDATE/DELETE), it accounts for both old and new tuple data
- For message changes, it includes the prefix string and message content sizes
- For snapshot changes, it includes the transaction ID arrays (xcnt and subxcnt)
- The function is essential for proper memory accounting in logical replication operations