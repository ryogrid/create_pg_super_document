# readOneRecord

## Location
[src/bin/pg_rewind/parsexlog.c:124-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L124-L167)

## Overview
Reads a single WAL record from the specified position and returns the end position of that record without processing its contents.

## Definition

```c
XLogRecPtr
readOneRecord(const char *datadir, XLogRecPtr ptr, int tliIndex,
			  const char *restoreCommand)
```
## Detailed Description
This utility function is designed to read exactly one WAL record from a given position and determine where that record ends. Unlike extractPageMap which processes multiple records and extracts page information, readOneRecord focuses solely on reading a single record and returning its end position. This is useful for WAL position calculations and determining record boundaries during pg_rewind operations.

The function sets up an XLogReader, reads one record at the specified position, captures the end position, and then cleans up the reader resources. It provides detailed error reporting if the WAL record cannot be read from the specified position.

## Parameters / Member Variables
- `*datadir`: Path to the PostgreSQL data directory containing pg_wal subdirectory
- `ptr`: XLogRecPtr indicating the exact WAL position to read the record from
- `tliIndex`: Index into the target timeline history array indicating which timeline to read from
- `*restoreCommand`: Command string used to restore archived WAL files if needed (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md)
  - [SimpleXLogPageRead](../S/SimpleXLogPageRead.md)
  - [XLogBeginRead](../X/XLogBeginRead.md)
  - [XLogReadRecord](../X/XLogReadRecord.md)
  - [XLogReaderFree](../X/XLogReaderFree.md)
  - [XLogRecord](../X/XLogRecord.md)
  - [XLogPageReadPrivate](../X/XLogPageReadPrivate.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_rewind/pg_rewind.c:417)

## Notes and Other Information
- Returns XLogRecPtr representing the end position of the read record
- Used by pg_rewind for WAL position validation and boundary calculations
- The function reads exactly one record and immediately cleans up resources
- Includes comprehensive error handling with LSN formatting for debugging
- Similar setup to extractPageMap but designed for single-record operations
- Uses the same global xlogreadfd management pattern