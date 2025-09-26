# GetWalSummaries

## Location
[src/backend/backup/walsummary.c:43-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummary.c#L43-L99)

## Overview
Retrieves a list of WAL summary files from the filesystem that match specified filtering criteria based on timeline ID and LSN range.

## Definition

```c
struct dirent *dent;
```
## Detailed Description
GetWalSummaries scans the WAL summaries directory (`XLOGDIR/summaries`) and returns a list of WalSummaryFile structures that match the specified filtering criteria. The function parses WAL summary filenames to extract timeline ID and LSN range information, then applies filters to return only relevant summaries. This is primarily used to find WAL summaries that overlap with a specified LSN range on a particular timeline, which is essential for incremental backup operations and WAL summarization processes.

The function implements filename parsing logic that extracts timeline ID, start LSN, and end LSN from standardized WAL summary filenames using a specific format pattern.

## Parameters / Member Variables
- `tli`: Timeline ID filter - if non-zero, only summaries matching this timeline will be included
- `start_lsn`: Starting LSN filter - if valid, only summaries ending after this LSN will be included
- `end_lsn`: Ending LSN filter - if valid, only summaries starting before this LSN will be included

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - [IsWalSummaryFilename](../I/IsWalSummaryFilename.md)
  - XLogRecPtrIsInvalid
  - [palloc](../p/palloc.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_available_wal_summaries](../p/pg_available_wal_summaries.md)
  - [GetOldestUnsummarizedLSN](GetOldestUnsummarizedLSN.md)
  - [MaybeRemoveOldWalSummaries](../M/MaybeRemoveOldWalSummaries.md)

## Notes and Other Information
- Returns a List of WalSummaryFile structures
- Uses directory scanning to discover available WAL summary files
- Filename format follows a specific pattern that encodes timeline and LSN information
- Filtering is inclusive - summaries that overlap with the specified range are included
- Part of PostgreSQL's incremental backup infrastructure
- Located in src/backend/backup/walsummary.c:43-99