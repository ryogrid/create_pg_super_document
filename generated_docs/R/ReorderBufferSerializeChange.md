# ReorderBufferSerializeChange

## Location
src/backend/replication/logical/reorderbuffer.c: 3935 - 4149

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
  - CloseTransientFile (closes file on error)
  - pgstat_report_wait_start/end (wait event reporting)
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