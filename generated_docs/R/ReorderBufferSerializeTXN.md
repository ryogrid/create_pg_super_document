# ReorderBufferSerializeTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:3840-3934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3840-L3934)

## Overview
Spills data of a large transaction (and its subtransactions) to disk when memory limits are exceeded, organizing changes into WAL segment-based files for later retrieval.

## Definition

```c
static void
ReorderBufferSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
```
## Detailed Description
This function handles the serialization of large transactions to disk storage when memory usage exceeds configured limits. It recursively processes all subtransactions first, then serializes the main transaction's changes. Changes are organized into separate files based on their WAL segment numbers to maintain locality and efficient access patterns during deserialization.

The function creates transient files in a directory structure that includes the replication slot name, transaction ID, and WAL segment number. Each change is written to the appropriate segment file using ReorderBufferSerializeChange, and the in-memory change list is cleared to free memory.

Key behaviors include:
- Recursive processing of subtransactions before the main transaction
- WAL segment-based file organization for efficient access
- Atomic memory management updates
- Statistics tracking for spill operations
- Proper cleanup of file descriptors and memory structures

## Parameters / Member Variables
- `*rb`: ReorderBuffer instance containing global state and configuration
- `*txn`: ReorderBufferTXN to be serialized, including all its changes and subtransactions
## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferSerializeChange](ReorderBufferSerializeChange.md) (serializes individual changes)
  - [ReorderBufferSerializedPath](ReorderBufferSerializedPath.md) (generates file paths)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md) (returns change to free pool)
  - [ReorderBufferChangeMemoryUpdate](ReorderBufferChangeMemoryUpdate.md) (updates memory accounting)
  - [UpdateDecodingStats](../U/UpdateDecodingStats.md) (updates decoding statistics)
  - [OpenTransientFile](../O/OpenTransientFile.md)/CloseTransientFile (file I/O operations)
  - XLByteToSeg/XLByteInSeg (WAL segment utilities)
- Called from (representative examples):
  - [ReorderBufferCheckMemoryLimit](ReorderBufferCheckMemoryLimit.md) (when memory limits exceeded)
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md) (during transaction iteration setup)
  - [ReorderBufferSerializeTXN](ReorderBufferSerializeTXN.md) (recursive calls for subtransactions)

## Notes and Other Information
- The function is recursive, processing subtransactions before the main transaction
- Files are organized by WAL segment number for efficient access patterns
- Memory statistics are updated atomically to maintain consistency
- The RBTXN_IS_SERIALIZED flag is set to mark the transaction as spilled
- Spill statistics (count, bytes, transactions) are maintained for monitoring
- File descriptors are properly managed to avoid resource leaks

## Simplified Source

```c
static void
ReorderBufferSerializeTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    dlist_iter subtxn_i;
    dlist_mutable_iter change_i;
    int fd = -1;
    XLogSegNo curOpenSegNo = 0;
    Size spilled = 0;
    Size size = txn->size;

    // Recursively serialize all child transactions first
    dlist_foreach(subtxn_i, &txn->subtxns)
    {
        ReorderBufferTXN *subtxn = dlist_container(ReorderBufferTXN, node, subtxn_i.cur);
        ReorderBufferSerializeTXN(rb, subtxn);
    }

    // Serialize each change in the transaction
    dlist_foreach_modify(change_i, &txn->changes)
    {
        ReorderBufferChange *change = dlist_container(ReorderBufferChange, node, change_i.cur);

        // Open new file if needed (per WAL segment)
        if (fd == -1 || !XLByteInSeg(change->lsn, curOpenSegNo, wal_segment_size))
        {
            char path[MAXPGPATH];

            if (fd != -1)
                CloseTransientFile(fd);

            XLByteToSeg(change->lsn, curOpenSegNo, wal_segment_size);
            ReorderBufferSerializedPath(path, MyReplicationSlot, txn->xid, curOpenSegNo);

            fd = OpenTransientFile(path, O_CREAT | O_WRONLY | O_APPEND | PG_BINARY);
        }

        // Serialize change to disk and remove from memory
        ReorderBufferSerializeChange(rb, txn, fd, change);
        dlist_delete(&change->node);
        ReorderBufferReturnChange(rb, change, false);
        spilled++;
    }

    // Update memory accounting and statistics
    ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false, size);
    if (spilled)
    {
        rb->spillCount += 1;
        rb->spillBytes += size;
        rb->spillTxns += (rbtxn_is_serialized(txn) || rbtxn_is_serialized_clear(txn)) ? 0 : 1;
        UpdateDecodingStats((LogicalDecodingContext *) rb->private_data);
    }

    // Mark transaction as serialized
    txn->nentries_mem = 0;
    txn->txn_flags |= RBTXN_IS_SERIALIZED;

    if (fd != -1)
        CloseTransientFile(fd);
}
```