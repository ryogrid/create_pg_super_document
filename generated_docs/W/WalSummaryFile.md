# WalSummaryFile

## Location
[src/include/backup/walsummary.h:27-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/walsummary.h#L27-L32)

## Overview
WalSummaryFile is a structure that represents metadata for a WAL summary file, containing the LSN range and timeline information that the summary covers.

## Definition
```c
typedef struct WalSummaryFile
{
    XLogRecPtr  start_lsn;
    XLogRecPtr  end_lsn;
    TimeLineID  tli;
} WalSummaryFile;
```

## Detailed Description
WalSummaryFile serves as a descriptor structure that encapsulates the essential metadata about a WAL summary file. It defines the scope of WAL records that are summarized within a particular summary file by specifying the starting and ending Log Sequence Numbers (LSNs) and the associated timeline ID.

This structure is fundamental to PostgreSQL's incremental backup system, where WAL summary files contain compressed representations of changes within specific WAL ranges. The structure allows the system to efficiently locate, filter, and manage WAL summary files based on LSN ranges and timelines, enabling features like incremental backups and WAL summary completeness checks.

## Parameters / Member Variables
- `start_lsn`: The starting Log Sequence Number (XLogRecPtr) of the WAL range covered by this summary file
- `end_lsn`: The ending Log Sequence Number (XLogRecPtr) of the WAL range covered by this summary file
- `tli`: Timeline ID (TimeLineID) indicating which timeline this WAL summary belongs to

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (WAL record pointer type from access/xlogdefs.h)
  - TimeLineID (Timeline identifier type from access/xlogdefs.h)
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [GetWalSummaries](../G/GetWalSummaries.md)
  - [FilterWalSummaries](../F/FilterWalSummaries.md)
  - [WalSummariesAreComplete](WalSummariesAreComplete.md)
  - [OpenWalSummaryFile](../O/OpenWalSummaryFile.md)
  - [RemoveWalSummaryIfOlderThan](../R/RemoveWalSummaryIfOlderThan.md)
  - [pg_available_wal_summaries](../p/pg_available_wal_summaries.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)
  - [GetOldestUnsummarizedLSN](../G/GetOldestUnsummarizedLSN.md)
  - [MaybeRemoveOldWalSummaries](../M/MaybeRemoveOldWalSummaries.md)

## Notes and Other Information
- Essential component of PostgreSQL's incremental backup infrastructure
- Used extensively in WAL summary management and incremental backup operations
- The LSN range (start_lsn to end_lsn) defines exactly which WAL records are summarized
- Timeline ID ensures summary files are associated with the correct timeline branch
- Used in list operations and filtering functions for managing collections of WAL summaries
- Defined in src/include/backup/walsummary.h:27-32