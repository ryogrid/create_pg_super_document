# ReorderBufferSerializeChange

## Location
[src/backend/replication/logical/reorderbuffer.c:3935-4149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3935-L4149)

## Overview
Serializes individual replication changes to disk, handling different change types with their specific data structures and ensuring proper storage format for later deserialization.

## Definition
```c
static void ReorderBufferSerializeChange(ReorderBuffer *rb, ReorderBufferTXN *txn, int fd, ReorderBufferChange *change)
```

## Detailed Description
This function handles the serialization of different types of replication changes to disk files. It creates a standardized on-disk format that includes the change metadata and type-specific data. The function uses a switch statement to handle various change types including DML operations (INSERT/UPDATE/DELETE), logical messages, invalidation messages, snapshots, and truncate operations.

Key responsibilities include:
- Converting in-memory change structures to disk-serializable format
- Managing variable-length data for different change types
- Ensuring buffer space is available for serialization
- Writing data atomically to the specified file descriptor
- Updating transaction LSN tracking for cleanup purposes
- Proper error handling for disk I/O operations

The function handles complex data structures like HeapTuples by serializing both the tuple header and tuple data separately, and manages variable-length arrays for snapshots and truncate operations.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance containing serialization buffers and global state
- `txn`: Transaction context for LSN tracking and error reporting
- `fd`: File descriptor of the open segment file to write to
- `change`: The ReorderBufferChange to be serialized to disk

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferSerializeReserve](ReorderBufferSerializeReserve.md) (ensures buffer space availability)
  - write (system call for disk I/O)
  - [CloseTransientFile](../C/CloseTransientFile.md) (closes file on error)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end (wait event reporting)
  - memcpy (memory copying operations)
- Called from (representative examples):
  - [ReorderBufferSerializeTXN](ReorderBufferSerializeTXN.md) (during transaction spilling process)

## Notes and Other Information
- Handles 8 different change types with type-specific serialization logic
- Uses a flexible buffer management system that can reallocate as needed
- Maintains transaction final_lsn for proper cleanup behavior
- Includes comprehensive error handling for disk space issues
- The on-disk format includes a size header for each serialized change
- HeapTuple data is serialized as both header and data portions
- [Variable](../V/Variable.md)-length data (messages, snapshots, truncate relations) is properly handled
- Wait events are reported for performance monitoring during disk writes

## Simplified Source

```c
static void
ReorderBufferSerializeChange(ReorderBuffer *rb, ReorderBufferTXN *txn,
                             int fd, ReorderBufferChange *change)
{
    ReorderBufferDiskChange *ondisk;
    Size sz = sizeof(ReorderBufferDiskChange);

    ReorderBufferSerializeReserve(rb, sz);
    ondisk = (ReorderBufferDiskChange *) rb->outbuf;
    memcpy(&ondisk->change, change, sizeof(ReorderBufferChange));

    // Handle different change types with their specific data
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT:
            {
                // Serialize heap tuple data (old and new tuples)
                HeapTuple oldtup = change->data.tp.oldtuple;
                HeapTuple newtup = change->data.tp.newtuple;
                char *data;

                if (oldtup)
                {
                    sz += sizeof(HeapTupleData) + oldtup->t_len;
                }
                if (newtup)
                {
                    sz += sizeof(HeapTupleData) + newtup->t_len;
                }

                ReorderBufferSerializeReserve(rb, sz);
                ondisk = (ReorderBufferDiskChange *) rb->outbuf;
                data = ((char *) rb->outbuf) + sizeof(ReorderBufferDiskChange);

                // Copy tuple headers and data
                if (oldtup)
                {
                    memcpy(data, oldtup, sizeof(HeapTupleData));
                    data += sizeof(HeapTupleData);
                    memcpy(data, oldtup->t_data, oldtup->t_len);
                    data += oldtup->t_len;
                }
                if (newtup)
                {
                    memcpy(data, newtup, sizeof(HeapTupleData));
                    data += sizeof(HeapTupleData);
                    memcpy(data, newtup->t_data, newtup->t_len);
                    data += newtup->t_len;
                }
                break;
            }
        case REORDER_BUFFER_CHANGE_MESSAGE:
            {
                // Serialize logical decoding message
                Size prefix_size = strlen(change->data.msg.prefix) + 1;
                sz += prefix_size + change->data.msg.message_size + sizeof(Size) + sizeof(Size);
                ReorderBufferSerializeReserve(rb, sz);

                char *data = ((char *) rb->outbuf) + sizeof(ReorderBufferDiskChange);
                ondisk = (ReorderBufferDiskChange *) rb->outbuf;

                memcpy(data, &prefix_size, sizeof(Size));
                data += sizeof(Size);
                memcpy(data, change->data.msg.prefix, prefix_size);
                data += prefix_size;
                memcpy(data, &change->data.msg.message_size, sizeof(Size));
                data += sizeof(Size);
                memcpy(data, change->data.msg.message, change->data.msg.message_size);
                break;
            }
        case REORDER_BUFFER_CHANGE_INVALIDATION:
        case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
        case REORDER_BUFFER_CHANGE_TRUNCATE:
            // Handle other change types with appropriate data copying
            // (simplified for brevity)
            break;
        default:
            // Other change types need no additional data
            break;
    }

    ondisk->size = sz;

    // Write to disk with error handling
    pgstat_report_wait_start(WAIT_EVENT_REORDER_BUFFER_WRITE);
    if (write(fd, rb->outbuf, ondisk->size) != ondisk->size)
    {
        CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not write to data file for XID %u: %m", txn->xid)));
    }
    pgstat_report_wait_end();

    // Update transaction's final LSN for cleanup
    if (txn->final_lsn < change->lsn)
        txn->final_lsn = change->lsn;
}
```