# heap_redo

## Location
[src/backend/access/heap/heapam.c:10338-10383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10338-L10383)

## Overview
WAL (Write-Ahead Logging) redo function for heap access method operations that processes heap-related log records during crash recovery and replication.

## Definition

```c
void
heap_redo(XLogReaderState *record)
```
## Detailed Description
The  function is the primary entry point for replaying heap table operations from WAL records during PostgreSQL recovery. It serves as a dispatcher that examines the operation type encoded in the WAL record and calls the appropriate specific redo function. This function handles basic heap operations that don't involve MVCC conflicts, distinguishing it from heap2_redo which handles more complex operations requiring conflict processing.

The function extracts the operation code from the WAL record and uses a switch statement to route to the correct handler function. It supports various heap operations including INSERT, DELETE, UPDATE, HOT_UPDATE, CONFIRM, LOCK, INPLACE updates, and TRUNCATE operations.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record to be replayed, including operation type and associated data
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extracts info from WAL record)
  - [heap_xlog_insert](heap_xlog_insert.md) (handles INSERT operations)
  - [heap_xlog_delete](heap_xlog_delete.md) (handles DELETE operations) 
  - [heap_xlog_update](heap_xlog_update.md) (handles UPDATE and HOT_UPDATE operations)
  - [heap_xlog_confirm](heap_xlog_confirm.md) (handles CONFIRM operations)
  - [heap_xlog_lock](heap_xlog_lock.md) (handles LOCK operations)
  - [heap_xlog_inplace](heap_xlog_inplace.md) (handles in-place UPDATE operations)
- Called from:
  - WAL replay infrastructure (not directly referenced by other functions)

## Notes and Other Information
- This function processes only basic heap operations that don't require MVCC conflict processing
- TRUNCATE operations are handled as no-ops since the actual work is done by SMGR WAL records
- The function will panic with an error if it encounters an unknown operation code
- Part of PostgreSQL's crash recovery and replication system
- Distinguished from heap2_redo which handles operations requiring conflict processing

## Simplified Source

```c
void heap_redo(XLogReaderState *record) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Dispatch to appropriate redo handler based on operation type
    // These operations don't require MVCC conflict processing
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP_INSERT:
            heap_xlog_insert(record);
            break;
        case XLOG_HEAP_DELETE:
            heap_xlog_delete(record);
            break;
        case XLOG_HEAP_UPDATE:
            heap_xlog_update(record, false);  // Regular update
            break;
        case XLOG_HEAP_HOT_UPDATE:
            heap_xlog_update(record, true);   // HOT update
            break;
        case XLOG_HEAP_CONFIRM:
            heap_xlog_confirm(record);
            break;
        case XLOG_HEAP_LOCK:
            heap_xlog_lock(record);
            break;
        case XLOG_HEAP_INPLACE:
            heap_xlog_inplace(record);
            break;
        case XLOG_HEAP_TRUNCATE:
            // No-op: actual work done by SMGR WAL records
            // This record exists only for logical decoding
            break;
        default:
            elog(PANIC, "heap_redo: unknown op code %u", info);
    }
}
```