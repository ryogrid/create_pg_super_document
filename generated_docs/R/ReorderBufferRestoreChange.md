# ReorderBufferRestoreChange

## Location
src/backend/replication/logical/reorderbuffer.c: 4530 - 4697

## Overview
ReorderBufferRestoreChange converts a change from its serialized on-disk format back to in-memory format and adds it to the transactions changes list, handling the deserialization of various change types and their associated data.

## Definition
```c
static void ReorderBufferRestoreChange(ReorderBuffer *rb, ReorderBufferTXN *txn, char *data)
```

## Detailed Description
This function is responsible for deserializing a single logical replication change from its disk-based format back into the proper in-memory representation. It handles the complex task of reconstructing various types of changes including tuple operations (INSERT/UPDATE/DELETE), messages, cache invalidations, snapshots, and truncation operations. The function allocates appropriate memory for variable-sized data, restores heap tuple structures with proper pointer alignment, and updates memory accounting.

The function performs type-specific deserialization based on the change action, carefully managing memory allocation and pointer reconstruction for complex data structures like heap tuples and snapshots. After restoration, it adds the change to the transactions change list and updates memory accounting.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer containing memory management context and allocation functions
- `txn`: Pointer to the ReorderBufferTXN transaction that will contain the restored change
- `data`: Pointer to the serialized change data (maxalignd buffer containing ReorderBufferDiskChange)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferGetChange](ReorderBufferGetChange.md) (memory allocation for changes)
  - [ReorderBufferGetTupleBuf](ReorderBufferGetTupleBuf.md) (tuple buffer allocation)
  - [ReorderBufferGetRelids](ReorderBufferGetRelids.md) (relation ID array allocation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (general memory allocation)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (zeroed memory allocation)
  - [dlist_push_tail](../d/dlist_push_tail.md) (list management)
  - ReorderBufferChangeMemoryUpdate (memory accounting)
  - [ReorderBufferChangeSize](ReorderBufferChangeSize.md) (size calculation)
  - Various REORDER_BUFFER_CHANGE_* constants
- Called from (representative examples):
  - [ReorderBufferRestoreChanges](ReorderBufferRestoreChanges.md)

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function handles memory alignment carefully, especially for heap tuple data restoration
- Heap tuple pointers (t_data) are reconstructed to point into newly allocated tuple buffers
- The function includes specific handling for potentially unaligned data when processing new tuples
- Memory accounting is updated to track the restored change size for proper resource management
- The deserialization process must match exactly with the serialization format used when spilling to disk
- Critical for the logical replication memory management system when dealing with large transactions