# heap_desc

## Location
[src/backend/access/rmgrdesc/heapdesc.c:183-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/heapdesc.c#L183-L259)

## Overview
The primary WAL record description function for heap access method operations, translating binary WAL records into human-readable text for debugging and analysis tools.

## Definition
```c
void heap_desc(StringInfo buf, XLogReaderState *record)
```

## Detailed Description
The `heap_desc` function serves as the main entry point for describing heap-related WAL (Write-Ahead Log) records in PostgreSQL. It acts as a dispatcher that examines the operation type encoded in the WAL record and delegates to appropriate formatting logic for each specific heap operation type. The function handles seven different heap operation types: INSERT, DELETE, UPDATE, HOT_UPDATE, TRUNCATE, CONFIRM, LOCK, and INPLACE.

For each operation type, the function extracts the relevant data structures from the WAL record and formats them into human-readable descriptions. It utilizes specialized helper functions like `infobits_desc`, `truncate_flags_desc`, and `array_desc` to format complex data structures consistently. This function is essential for WAL debugging tools like pg_waldump, allowing database administrators and developers to understand what operations were logged during database activity.

The function follows PostgreSQL"s WAL record format conventions, using the operation mask to identify record types and casting the raw record data to appropriate structure types for each operation.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted WAL record description will be appended
- `record`: XLogReaderState containing the WAL record data and metadata to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [infobits_desc](../i/infobits_desc.md)
  - [truncate_flags_desc](../t/truncate_flags_desc.md)
  - [array_desc](../a/array_desc.md)
  - [oid_elem_desc](../o/oid_elem_desc.md)
  - Various XLOG_HEAP_* operation constants
  - Various xl_heap_* structure types
  - XLR_INFO_MASK
  - XLOG_HEAP_OPMASK
- Called from:
  - WAL description infrastructure (likely via function pointer)

## Notes and Other Information
- Handles all major heap tuple lifecycle operations: creation, modification, deletion, and maintenance
- Uses operation-specific data structures (xl_heap_insert, xl_heap_delete, etc.) to parse WAL record contents
- HOT (Heap-Only Tuple) updates are handled separately from regular updates for optimization
- TRUNCATE operations include array descriptions of affected relation OIDs
- CONFIRM operations are used for speculative insertion confirmation
- [LOCK](../L/LOCK.md) operations describe tuple locking without modification
- INPLACE operations describe in-place tuple modifications
- Part of PostgreSQL"s resource manager description system for WAL analysis and debugging

## Simplified Source

```c
void heap_desc(StringInfo buf, XLogReaderState *record) {
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Extract the heap operation type
    info &= XLOG_HEAP_OPMASK;

    // Format description based on operation type
    if (info == XLOG_HEAP_INSERT) {
        xl_heap_insert *xlrec = (xl_heap_insert *) rec;
        appendStringInfo(buf, "off: %u, flags: 0x%02X", xlrec->offnum, xlrec->flags);
    }
    else if (info == XLOG_HEAP_DELETE) {
        xl_heap_delete *xlrec = (xl_heap_delete *) rec;
        appendStringInfo(buf, "xmax: %u, off: %u, ", xlrec->xmax, xlrec->offnum);
        infobits_desc(buf, xlrec->infobits_set, "infobits");
        appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
    }
    else if (info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE) {
        xl_heap_update *xlrec = (xl_heap_update *) rec;
        appendStringInfo(buf, "old_xmax: %u, old_off: %u, ", xlrec->old_xmax, xlrec->old_offnum);
        infobits_desc(buf, xlrec->old_infobits_set, "old_infobits");
        appendStringInfo(buf, ", flags: 0x%02X, new_xmax: %u, new_off: %u",
                         xlrec->flags, xlrec->new_xmax, xlrec->new_offnum);
    }
    else if (info == XLOG_HEAP_TRUNCATE) {
        xl_heap_truncate *xlrec = (xl_heap_truncate *) rec;
        truncate_flags_desc(buf, xlrec->flags);
        appendStringInfo(buf, ", nrelids: %u", xlrec->nrelids);
        appendStringInfoString(buf, ", relids:");
        array_desc(buf, xlrec->relids, sizeof(Oid), xlrec->nrelids, &oid_elem_desc, NULL);
    }
    else if (info == XLOG_HEAP_CONFIRM) {
        xl_heap_confirm *xlrec = (xl_heap_confirm *) rec;
        appendStringInfo(buf, "off: %u", xlrec->offnum);
    }
    else if (info == XLOG_HEAP_LOCK) {
        xl_heap_lock *xlrec = (xl_heap_lock *) rec;
        appendStringInfo(buf, "xmax: %u, off: %u, ", xlrec->xmax, xlrec->offnum);
        infobits_desc(buf, xlrec->infobits_set, "infobits");
        appendStringInfo(buf, ", flags: 0x%02X", xlrec->flags);
    }
    else if (info == XLOG_HEAP_INPLACE) {
        xl_heap_inplace *xlrec = (xl_heap_inplace *) rec;
        appendStringInfo(buf, "off: %u", xlrec->offnum);
    }
}
```