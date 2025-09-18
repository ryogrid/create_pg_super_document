# readOneRecord

## Location
src/bin/pg_rewind/parsexlog.c: 124 - 167

## Overview
Reads a single WAL record from the specified position and returns the end position of that record without processing its contents.

## Definition


## Detailed Description
This utility function is designed to read exactly one WAL record from a given position and determine where that record ends. Unlike extractPageMap which processes multiple records and extracts page information, readOneRecord focuses solely on reading a single record and returning its end position. This is useful for WAL position calculations and determining record boundaries during pg_rewind operations.

The function sets up an XLogReader, reads one record at the specified position, captures the end position, and then cleans up the reader resources. It provides detailed error reporting if the WAL record cannot be read from the specified position.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory containing pg_wal subdirectory
- : XLogRecPtr indicating the exact WAL position to read the record from
- : Index into the target timeline history array indicating which timeline to read from
- : Command string used to restore archived WAL files if needed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - XLogReaderAllocate
  - SimpleXLogPageRead
  - XLogBeginRead
  - XLogReadRecord
  - XLogReaderFree
  - XLogRecord
  - XLogPageReadPrivate
- Called from (representative examples):
  - main (in src/bin/pg_rewind/pg_rewind.c:417)

## Notes and Other Information
- Returns XLogRecPtr representing the end position of the read record
- Used by pg_rewind for WAL position validation and boundary calculations
- The function reads exactly one record and immediately cleans up resources
- Includes comprehensive error handling with LSN formatting for debugging
- Similar setup to extractPageMap but designed for single-record operations
- Uses the same global xlogreadfd management pattern