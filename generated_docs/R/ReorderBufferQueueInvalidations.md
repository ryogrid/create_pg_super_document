# ReorderBufferQueueInvalidations

## Location
[src/backend/replication/logical/reorderbuffer.c:3358-3380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3358-L3380)

## Overview
A static helper function that adds invalidation messages to the reorder buffer queue as a change entry.

## Definition
static void ReorderBufferQueueInvalidations(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, Size nmsgs, SharedInvalidationMessage *msgs)

## Detailed Description
This internal function creates a REORDER_BUFFER_CHANGE_INVALIDATION change entry containing the provided invalidation messages and queues it to the reorder buffer. The function allocates memory for the invalidation messages, copies them into the change structure, and then uses ReorderBufferQueueChange to add the change to the specified transaction. This is a low-level function used by higher-level invalidation handling functions.

## Parameters / Member Variables
- `rb`: The reorder buffer instance to queue the invalidations to
- `xid`: Transaction ID that should receive these invalidation messages
- `lsn`: Log Sequence Number where the invalidations were recorded
- `nmsgs`: Number of invalidation messages in the msgs array
- `msgs`: Array of SharedInvalidationMessage structures to be queued

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferGetChange](ReorderBufferGetChange.md)
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - REORDER_BUFFER_CHANGE_INVALIDATION
- Called from (representative examples):
  - [ReorderBufferAddInvalidations](ReorderBufferAddInvalidations.md)
  - [ReorderBufferAddDistributedInvalidations](ReorderBufferAddDistributedInvalidations.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside the reorderbuffer.c file
- Memory for invalidation messages is allocated using palloc and copied using memcpy
- The function creates a complete copy of the invalidation messages for storage
- Used as a building block for both regular and distributed invalidation handling

## Simplified Source

```c
static void ReorderBufferQueueInvalidations(ReorderBuffer *rb, TransactionId xid,
                                           XLogRecPtr lsn, Size nmsgs,
                                           SharedInvalidationMessage *msgs)
{
    // Allocate a new change structure
    ReorderBufferChange *change = ReorderBufferGetChange(rb);

    // Set up the change as an invalidation change
    change->action = REORDER_BUFFER_CHANGE_INVALIDATION;
    change->data.inval.ninvalidations = nmsgs;

    // Allocate memory and copy invalidation messages
    change->data.inval.invalidations = (SharedInvalidationMessage *)
        palloc(sizeof(SharedInvalidationMessage) * nmsgs);
    memcpy(change->data.inval.invalidations, msgs,
           sizeof(SharedInvalidationMessage) * nmsgs);

    // Queue the change to be processed at the appropriate LSN
    ReorderBufferQueueChange(rb, xid, lsn, change, false);
}
```