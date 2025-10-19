# ReorderBufferReturnChange

## Location
[src/backend/replication/logical/reorderbuffer.c:518-587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L518-L587)

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
  - [ReorderBufferChangeMemoryUpdate](ReorderBufferChangeMemoryUpdate.md)
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

## Simplified Source
```c
void ReorderBufferReturnChange(ReorderBuffer *rb, ReorderBufferChange *change, bool upd_mem)
{
    // Update memory accounting if requested
    if (upd_mem)
        ReorderBufferChangeMemoryUpdate(rb, change, NULL, false, ReorderBufferChangeSize(change));

    // Free contained data based on change type
    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT:
            // Free tuple buffers for DML operations
            if (change->data.tp.newtuple)
                ReorderBufferReturnTupleBuf(change->data.tp.newtuple);
            if (change->data.tp.oldtuple)
                ReorderBufferReturnTupleBuf(change->data.tp.oldtuple);
            break;

        case REORDER_BUFFER_CHANGE_MESSAGE:
            // Free message data
            if (change->data.msg.prefix)
                pfree(change->data.msg.prefix);
            if (change->data.msg.message)
                pfree(change->data.msg.message);
            break;

        case REORDER_BUFFER_CHANGE_INVALIDATION:
            // Free invalidation data
            if (change->data.inval.invalidations)
                pfree(change->data.inval.invalidations);
            break;

        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
            // Free snapshot data
            if (change->data.snapshot)
                ReorderBufferFreeSnap(rb, change->data.snapshot);
            break;

        case REORDER_BUFFER_CHANGE_TRUNCATE:
            // Free relation OIDs for truncate
            if (change->data.truncate.relids)
                ReorderBufferReturnRelids(rb, change->data.truncate.relids);
            break;

        // Other change types have no additional data to free
        default:
            break;
    }

    // Free the change object itself
    pfree(change);
}
```