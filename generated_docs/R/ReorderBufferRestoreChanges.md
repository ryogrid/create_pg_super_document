# ReorderBufferRestoreChanges

## Location
[src/backend/replication/logical/reorderbuffer.c:4387-4529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4387-L4529)

## Overview
ReorderBufferRestoreChanges restores a number of changes that were previously spilled to disk back into memory, managing the file I/O operations and memory allocation during the deserialization process.

## Definition
```c
static Size ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn, TXNEntryFile *file, XLogSegNo *segno)
```

## Detailed Description
This function is responsible for reading serialized logical replication changes from disk and restoring them into memory structures. It operates as part of the memory management strategy for large transactions in logical replication, where changes are spilled to disk when memory usage exceeds configured limits. The function reads changes from multiple WAL segment-based files, handles file opening/closing, performs error checking, and deserializes the changes back into proper in-memory format.

The function works within memory limits (max_changes_in_memory) and processes changes across multiple WAL segments, opening and reading from spill files as needed. It first frees existing in-memory changes to make room, then reads serialized changes from disk files and restores them using ReorderBufferRestoreChange.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer containing memory management context and buffers
- `txn`: Pointer to the ReorderBufferTXN transaction containing changes to be restored
- `file`: Pointer to TXNEntryFile structure managing the current spill file state
- `segno`: Pointer to XLogSegNo indicating the current WAL segment number being processed

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md) (to free existing changes)
  - XLByteToSeg (WAL segment calculation)
  - [ReorderBufferSerializedPath](ReorderBufferSerializedPath.md) (path generation for spill files)
  - [PathNameOpenFile](../P/PathNameOpenFile.md) (file opening)
  - [ReorderBufferSerializeReserve](ReorderBufferSerializeReserve.md) (buffer management)
  - [FileRead](../F/FileRead.md) (disk I/O)
  - [FileClose](../F/FileClose.md) (file cleanup)
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md) (change deserialization)
  - dlist_* functions (doubly-linked list operations)
- Called from (representative examples):
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md)
  - [ReorderBufferIterTXNNext](ReorderBufferIterTXNNext.md)

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function manages memory efficiently by freeing existing changes before loading new ones
- File I/O is performed using PostgreSQLs virtual file descriptor system
- The function handles multiple WAL segments and can process changes across segment boundaries
- Error handling includes comprehensive file I/O error reporting
- The function respects the max_changes_in_memory limit to prevent excessive memory usage
- Changes are read in their serialized ReorderBufferDiskChange format and then converted back to in-memory format

## Simplified Source

```c
static Size
ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn,
                            TXNEntryFile *file, XLogSegNo *segno)
{
    Size restored = 0;
    XLogSegNo last_segno;
    dlist_mutable_iter cleanup_iter;
    File *fd = &file->vfd;

    Assert(txn->first_lsn != InvalidXLogRecPtr);
    Assert(txn->final_lsn != InvalidXLogRecPtr);

    // Free current entries to make room for restored ones
    dlist_foreach_modify(cleanup_iter, &txn->changes)
    {
        ReorderBufferChange *cleanup = dlist_container(ReorderBufferChange, node, cleanup_iter.cur);
        dlist_delete(&cleanup->node);
        ReorderBufferReturnChange(rb, cleanup, true);
    }
    txn->nentries_mem = 0;

    XLByteToSeg(txn->final_lsn, last_segno, wal_segment_size);

    // Read changes from disk until memory limit or end of segments
    while (restored < max_changes_in_memory && *segno <= last_segno)
    {
        ReorderBufferDiskChange *ondisk;
        int readBytes;

        CHECK_FOR_INTERRUPTS();

        // Open new segment file if needed
        if (*fd == -1)
        {
            char path[MAXPGPATH];

            if (*segno == 0)
                XLByteToSeg(txn->first_lsn, *segno, wal_segment_size);

            ReorderBufferSerializedPath(path, MyReplicationSlot, txn->xid, *segno);
            *fd = PathNameOpenFile(path, O_RDONLY | PG_BINARY);
            file->curOffset = 0;

            if (*fd < 0 && errno == ENOENT)
            {
                *fd = -1;
                (*segno)++;
                continue;
            }
        }

        // Read change header
        ReorderBufferSerializeReserve(rb, sizeof(ReorderBufferDiskChange));
        readBytes = FileRead(file->vfd, rb->outbuf, sizeof(ReorderBufferDiskChange),
                             file->curOffset, WAIT_EVENT_REORDER_BUFFER_READ);

        if (readBytes == 0)
        {
            // End of file - move to next segment
            FileClose(*fd);
            *fd = -1;
            (*segno)++;
            continue;
        }

        file->curOffset += readBytes;
        ondisk = (ReorderBufferDiskChange *) rb->outbuf;

        // Read the full change data
        ReorderBufferSerializeReserve(rb, sizeof(ReorderBufferDiskChange) + ondisk->size);
        ondisk = (ReorderBufferDiskChange *) rb->outbuf;

        readBytes = FileRead(file->vfd, rb->outbuf + sizeof(ReorderBufferDiskChange),
                             ondisk->size - sizeof(ReorderBufferDiskChange),
                             file->curOffset, WAIT_EVENT_REORDER_BUFFER_READ);

        file->curOffset += readBytes;

        // Restore the change to in-memory format
        ReorderBufferRestoreChange(rb, txn, rb->outbuf);
        restored++;
    }

    return restored;
}
```