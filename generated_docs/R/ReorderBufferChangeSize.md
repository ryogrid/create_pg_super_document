# ReorderBufferChangeSize

## Location
[src/backend/replication/logical/reorderbuffer.c:4302-4386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4302-L4386)

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
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md) (structure type)
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

## Simplified Source

```c
static Size
ReorderBufferChangeSize(ReorderBufferChange *change)
{
    Size sz = sizeof(ReorderBufferChange);

    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT:
            {
                // Account for tuple data sizes
                HeapTuple oldtup = change->data.tp.oldtuple;
                HeapTuple newtup = change->data.tp.newtuple;

                if (oldtup)
                    sz += sizeof(HeapTupleData) + oldtup->t_len;
                if (newtup)
                    sz += sizeof(HeapTupleData) + newtup->t_len;
                break;
            }
        case REORDER_BUFFER_CHANGE_MESSAGE:
            {
                // Account for message prefix and content
                Size prefix_size = strlen(change->data.msg.prefix) + 1;
                sz += prefix_size + change->data.msg.message_size + sizeof(Size) + sizeof(Size);
                break;
            }
        case REORDER_BUFFER_CHANGE_INVALIDATION:
            {
                // Account for invalidation messages
                sz += sizeof(SharedInvalidationMessage) * change->data.inval.ninvalidations;
                break;
            }
        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
            {
                // Account for snapshot transaction arrays
                Snapshot snap = change->data.snapshot;
                sz += sizeof(SnapshotData) +
                      sizeof(TransactionId) * snap->xcnt +
                      sizeof(TransactionId) * snap->subxcnt;
                break;
            }
        case REORDER_BUFFER_CHANGE_TRUNCATE:
            {
                // Account for relation OID array
                sz += sizeof(Oid) * change->data.truncate.nrelids;
                break;
            }
        default:
            // Other change types have no additional data
            break;
    }

    return sz;
}
```