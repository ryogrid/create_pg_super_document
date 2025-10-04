# ReorderBufferRestoreChange

## Location
[src/backend/replication/logical/reorderbuffer.c:4530-4697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4530-L4697)

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
  - [ReorderBufferChangeMemoryUpdate](ReorderBufferChangeMemoryUpdate.md) (memory accounting)
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

## Simplified Source

```c
static void
ReorderBufferRestoreChange(ReorderBuffer *rb, ReorderBufferTXN *txn, char *data)
{
    ReorderBufferDiskChange *ondisk;
    ReorderBufferChange *change;

    ondisk = (ReorderBufferDiskChange *) data;
    change = ReorderBufferGetChange(rb);

    // Copy the basic change structure
    memcpy(change, &ondisk->change, sizeof(ReorderBufferChange));
    data += sizeof(ReorderBufferDiskChange);

    // Restore type-specific data
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT:
            // Restore heap tuple data
            if (change->data.tp.oldtuple)
            {
                uint32 tuplelen = ((HeapTuple) data)->t_len;
                change->data.tp.oldtuple = ReorderBufferGetTupleBuf(rb, tuplelen - SizeofHeapTupleHeader);

                // Copy tuple header and reset data pointer
                memcpy(change->data.tp.oldtuple, data, sizeof(HeapTupleData));
                data += sizeof(HeapTupleData);
                change->data.tp.oldtuple->t_data = (HeapTupleHeader) ((char *) change->data.tp.oldtuple + HEAPTUPLESIZE);

                // Copy tuple data
                memcpy(change->data.tp.oldtuple->t_data, data, tuplelen);
                data += tuplelen;
            }

            if (change->data.tp.newtuple)
            {
                uint32 tuplelen;
                memcpy(&tuplelen, data + offsetof(HeapTupleData, t_len), sizeof(uint32));
                change->data.tp.newtuple = ReorderBufferGetTupleBuf(rb, tuplelen - SizeofHeapTupleHeader);

                // Copy tuple header and reset data pointer
                memcpy(change->data.tp.newtuple, data, sizeof(HeapTupleData));
                data += sizeof(HeapTupleData);
                change->data.tp.newtuple->t_data = (HeapTupleHeader) ((char *) change->data.tp.newtuple + HEAPTUPLESIZE);

                // Copy tuple data
                memcpy(change->data.tp.newtuple->t_data, data, tuplelen);
                data += tuplelen;
            }
            break;

        case REORDER_BUFFER_CHANGE_MESSAGE:
            {
                // Restore message data
                Size prefix_size;
                memcpy(&prefix_size, data, sizeof(Size));
                data += sizeof(Size);

                change->data.msg.prefix = MemoryContextAlloc(rb->context, prefix_size);
                memcpy(change->data.msg.prefix, data, prefix_size);
                data += prefix_size;

                memcpy(&change->data.msg.message_size, data, sizeof(Size));
                data += sizeof(Size);
                change->data.msg.message = MemoryContextAlloc(rb->context, change->data.msg.message_size);
                memcpy(change->data.msg.message, data, change->data.msg.message_size);
                data += change->data.msg.message_size;
                break;
            }

        case REORDER_BUFFER_CHANGE_INVALIDATION:
            {
                // Restore invalidation data
                Size inval_size = sizeof(SharedInvalidationMessage) * change->data.inval.ninvalidations;
                change->data.inval.invalidations = MemoryContextAlloc(rb->context, inval_size);
                memcpy(change->data.inval.invalidations, data, inval_size);
                break;
            }

        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
            {
                // Restore snapshot data
                Snapshot oldsnap = (Snapshot) data;
                Size size = sizeof(SnapshotData) + sizeof(TransactionId) * oldsnap->xcnt +
                           sizeof(TransactionId) * oldsnap->subxcnt;

                change->data.snapshot = MemoryContextAllocZero(rb->context, size);
                Snapshot newsnap = change->data.snapshot;
                memcpy(newsnap, data, size);
                newsnap->xip = (TransactionId *) (((char *) newsnap) + sizeof(SnapshotData));
                newsnap->subxip = newsnap->xip + newsnap->xcnt;
                newsnap->copied = true;
                break;
            }

        case REORDER_BUFFER_CHANGE_TRUNCATE:
            {
                // Restore truncate relation list
                Oid *relids = ReorderBufferGetRelids(rb, change->data.truncate.nrelids);
                memcpy(relids, data, change->data.truncate.nrelids * sizeof(Oid));
                change->data.truncate.relids = relids;
                break;
            }

        default:
            // Other change types need no additional restoration
            break;
    }

    // Add to transaction's change list and update memory accounting
    dlist_push_tail(&txn->changes, &change->node);
    txn->nentries_mem++;
    ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, ReorderBufferChangeSize(change));
}
```