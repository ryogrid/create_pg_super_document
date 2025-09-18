# heap_desc

## Location
src/backend/access/rmgrdesc/heapdesc.c: 183 - 259

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
  - appendStringInfo
  - appendStringInfoString
  - infobits_desc
  - truncate_flags_desc
  - array_desc
  - oid_elem_desc
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
- LOCK operations describe tuple locking without modification
- INPLACE operations describe in-place tuple modifications
- Part of PostgreSQL"s resource manager description system for WAL analysis and debugging