# extractPageMap

## Location
[src/bin/pg_rewind/parsexlog.c:66-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/parsexlog.c#L66-L123)

## Overview
Reads WAL records from a PostgreSQL datadir starting from a specified point until an endpoint, extracting page information from the WAL records to build a page map for pg_rewind operations.

## Definition

```c
void
extractPageMap(const char *datadir, XLogRecPtr startpoint, int tliIndex,
			   XLogRecPtr endpoint, const char *restoreCommand)
```
## Detailed Description
This function is a core component of pg_rewind that processes Write-Ahead Log (WAL) records to determine which data blocks were modified. It reads WAL records sequentially from the specified starting point until the endpoint, calling extractPageInfo() for each record to build a map of modified pages. This page map is then used by pg_rewind to determine which pages need to be copied or synchronized between the source and target PostgreSQL instances.

The function initializes an XLogReader with the SimpleXLogPageRead page reading function and processes records in a loop until reaching the specified endpoint. It includes proper error handling for WAL reading failures and validates that the endpoint aligns exactly with a record boundary.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory containing pg_wal subdirectory
- : XLogRecPtr indicating the WAL position to start reading from  
- : Index into the target timeline history array indicating which timeline to read from
- : XLogRecPtr indicating the end position - the first record NOT to be read
- : Command string used to restore archived WAL files if needed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md)
  - [SimpleXLogPageRead](../S/SimpleXLogPageRead.md)
  - [XLogBeginRead](../X/XLogBeginRead.md)
  - [XLogReadRecord](../X/XLogReadRecord.md)
  - [extractPageInfo](extractPageInfo.md)
  - [XLogReaderFree](../X/XLogReaderFree.md)
  - [XLogRecord](../X/XLogRecord.md)
  - [XLogPageReadPrivate](../X/XLogPageReadPrivate.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_rewind/pg_rewind.c:487)

## Notes and Other Information
- This function is specific to pg_rewind and is used during the WAL analysis phase
- The endpoint validation ensures that WAL parsing stops at exact record boundaries
- Uses a global xlogreadfd file descriptor that gets closed when processing completes
- Error handling includes detailed LSN information for debugging WAL read failures
- The function assumes WalSegSz is properly initialized before being called