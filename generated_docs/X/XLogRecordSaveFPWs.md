# XLogRecordSaveFPWs

## Location
[src/bin/pg_waldump/pg_waldump.c:490-545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L490-L545)

## Overview
XLogRecordSaveFPWs extracts and saves all full page writes (FPWs) from a WAL record to disk as individual files.

## Definition

```c
static void
XLogRecordSaveFPWs(XLogReaderState *record, const char *savepath)
```
## Detailed Description
This function iterates through all block references in a WAL record, identifies those containing full page writes, and saves them as individual files to a specified directory. Each saved page is automatically decompressed if necessary using RestoreBlockImage. The function generates descriptive filenames that include timeline, LSR, relation identifiers, block numbers, and fork information, making it easy to identify and analyze specific page images. This functionality is particularly useful for debugging, forensic analysis, and understanding the content of full page writes in PostgreSQL's WAL stream.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record to process for full page writes
- `*savepath`: Directory path where the extracted page files should be saved
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - XLogRecHasBlockRef
  - XLogRecHasBlockImage
  - [RestoreBlockImage](../R/RestoreBlockImage.md)
  - [XLogRecGetBlockTagExtended](XLogRecGetBlockTagExtended.md)
  - fopen
  - fwrite
  - fclose
  - PGAlignedBlock (type)
  - FORKNAMECHARS (constant)
  - MAX_FORKNUM (constant)
  - PG_BINARY_W (constant)
- Called from (representative examples):
  - [main](../m/main.md) (used when --save-fpw option is specified in pg_waldump)

## Notes and Other Information
- Creates files with naming pattern: timeline-lsn-space-db-relation-block-fork.extension
- Automatically handles decompression of compressed full page writes
- Validates fork numbers and uses standard fork names in filenames
- Provides comprehensive error handling for file operations
- Useful for forensic analysis and understanding WAL content at the page level
- Each saved file represents a complete 8KB database page as stored in the WAL