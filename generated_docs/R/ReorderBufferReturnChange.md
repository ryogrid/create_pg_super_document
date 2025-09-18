# ReorderBufferReturnChange

## Location
src/backend/replication/logical/reorderbuffer.c: 518 - 587

## Overview
Frees a ReorderBufferChange object and updates memory accounting, releasing all contained data structures and associated memory allocations.

## Definition
```c
void ReorderBufferReturnChange(ReorderBuffer *rb, ReorderBufferChange *change, bool upd_mem)
```

## Detailed Description
ReorderBufferReturnChange is responsible for properly deallocating a ReorderBufferChange object and all its associated data. The function performs memory accounting updates when requested, then frees different types of data based on the change type (action field). It handles various types of logical replication changes including INSERT/UPDATE/DELETE operations, messages, invalidations, snapshots, and truncate operations. Each change type has specific cleanup requirements for its contained data structures.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer that owns the change
- `change`: The ReorderBufferChange object to be freed
- `upd_mem`: Boolean flag indicating whether to update memory accounting statistics

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferChangeMemoryUpdate
  - [ReorderBufferChangeSize](ReorderBufferChangeSize.md)
  - [ReorderBufferReturnTupleBuf](ReorderBufferReturnTupleBuf.md)
  - [ReorderBufferFreeSnap](ReorderBufferFreeSnap.md)
  - [ReorderBufferReturnRelids](ReorderBufferReturnRelids.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md)
  - [ReorderBufferIterTXNNext](ReorderBufferIterTXNNext.md)
  - [ReorderBufferIterTXNFinish](ReorderBufferIterTXNFinish.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)

## Notes and Other Information
The function uses a switch statement to handle different change types (REORDER_BUFFER_CHANGE_*), ensuring proper cleanup of type-specific data. Memory accounting is updated before freeing to maintain accurate statistics. The function sets pointers to NULL after freeing to prevent double-free errors. It's a critical component in the logical replication system's memory management.