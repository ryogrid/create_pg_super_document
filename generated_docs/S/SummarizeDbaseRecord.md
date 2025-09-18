# SummarizeDbaseRecord

## Location
[src/backend/postmaster/walsummarizer.c:1246-1314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L1246-L1314)

## Overview
Handles special processing of database-related WAL records (RM_DBASE_ID) during WAL summarization to ensure proper tracking of database creation and deletion operations.

## Definition
```c
static void SummarizeDbaseRecord(XLogReaderState *xlogreader, BlockRefTable *brtab)
```

## Detailed Description
SummarizeDbaseRecord provides specialized handling for database-related WAL records that require special treatment during the summarization process. This function addresses critical scenarios where database operations like creation and deletion need to be properly tracked to ensure incremental backup correctness.

The function uses a special convention where relfilenode zero is used as a marker for a given database OID and tablespace OID combination to indicate that all relations with that pair of IDs have been recreated. This effectively sets a limit block of 0 for all such relfilenodes, ensuring that incremental backups will include the complete content of affected relations rather than attempting to apply incremental changes.

The most critical case handled is XLOG_DBASE_CREATE_FILE_COPY, which can create numerous relation files in a directory without logging anything specific to each individual file. Without this special marking, a tablespace that was dropped after the reference backup and recreated using the FILE_COPY method before the incremental backup would appear unchanged, leading to catastrophic backup corruption.

## Parameters / Member Variables
- `xlogreader`: XLogReaderState containing the current WAL record being processed
- `brtab`: BlockRefTable where database-level modifications are recorded

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Extract record type information from WAL record
  - XLogRecGetData: Get the payload data from the WAL record
  - BlockRefTableSetLimitBlock: Set limit block to 0 for database/tablespace combinations
  - MAIN_FORKNUM: Main fork identifier for relation files
- Called from (representative examples):
  - [SummarizeWAL](SummarizeWAL.md): Main WAL summarization loop when processing RM_DBASE_ID records

## Notes and Other Information
- Handles three specific database operation types: XLOG_DBASE_CREATE_FILE_COPY, XLOG_DBASE_CREATE_WAL_LOG, and XLOG_DBASE_DROP
- Uses relfilenode 0 as a sentinel value to mark entire database/tablespace combinations as requiring full backup
- The conservative approach of marking more content for full backup is intentional - it's safer to backup more than necessary than to miss critical changes
- Critical for preventing incremental backup corruption in scenarios involving database recreation
- For XLOG_DBASE_DROP, iterates through all affected tablespaces to mark them appropriately
- This special handling ensures that incremental backups remain consistent even when databases are dropped and recreated between backup operations