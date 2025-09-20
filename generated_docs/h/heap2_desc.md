# heap2_desc

## Location
[src/backend/access/rmgrdesc/heapdesc.c:260-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/heapdesc.c#L260-L384)

## Overview
This function provides human-readable descriptions of heap2 WAL (Write-Ahead Logging) record types for PostgreSQL debugging and analysis purposes.

## Definition

```c
void
heap2_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
 is a WAL record description function that parses and formats heap2-related WAL records into readable text. It handles various heap2 operation types including:

- **PRUNE operations** (ON_ACCESS, VACUUM_SCAN, VACUUM_CLEANUP): Describes pruning operations with conflict horizons, catalog relation flags, and detailed information about redirected, dead, and unused tuples
- **VISIBLE operations**: Describes visibility map updates with snapshot conflict horizons and flags
- **MULTI_INSERT operations**: Describes bulk tuple insertions with tuple counts, flags, and offset information
- **LOCK_UPDATED operations**: Describes tuple lock updates with transaction IDs, offsets, and info bits
- **NEW_CID operations**: Describes new command ID assignments with relation and tuple identifiers

The function extracts structured data from the WAL record and formats it into a string buffer for display in PostgreSQL logs and debugging tools.

## Parameters / Member Variables
- : StringInfo buffer to append the formatted description text
- : XLogReaderState pointer containing the WAL record data to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetInfo
  - XLogRecHasBlockData
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [heap_xlog_deserialize_prune_and_freeze](heap_xlog_deserialize_prune_and_freeze.md)
  - appendStringInfo
  - appendStringInfoString
  - [array_desc](../a/array_desc.md)
  - [infobits_desc](../i/infobits_desc.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - WAL record description infrastructure (indirectly through resource manager tables)

## Notes and Other Information
- This function is part of PostgreSQL's WAL record description system, used for debugging and log analysis
- It handles complex heap2 operations that involve multiple tuple modifications in a single WAL record
- The function uses helper functions like  to format arrays of data structures
- Different heap2 operation types require different parsing and formatting approaches
- The function is located in src/backend/access/rmgrdesc/heapdesc.c:260-384