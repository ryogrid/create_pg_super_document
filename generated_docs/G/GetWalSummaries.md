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

## Simplified Source

```c
// Simplified version of GetWalSummaries
List *
GetWalSummaries(TimeLineID tli, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
    DIR *sdir;
    struct dirent *dent;
    List *result = NIL;

    // Open the WAL summaries directory
    sdir = AllocateDir(XLOGDIR "/summaries");

    // Scan all files in the directory
    while ((dent = ReadDir(sdir, XLOGDIR "/summaries")) != NULL)
    {
        WalSummaryFile *ws;
        uint32 tmp[5];
        TimeLineID file_tli;
        XLogRecPtr file_start_lsn;
        XLogRecPtr file_end_lsn;

        // Skip files that don't match WAL summary filename format
        if (!IsWalSummaryFilename(dent->d_name))
            continue;

        // Parse filename to extract timeline and LSN information
        sscanf(dent->d_name, "%08X%08X%08X%08X%08X",
               &tmp[0], &tmp[1], &tmp[2], &tmp[3], &tmp[4]);
        file_tli = tmp[0];
        file_start_lsn = ((uint64) tmp[1]) << 32 | tmp[2];
        file_end_lsn = ((uint64) tmp[3]) << 32 | tmp[4];

        // Apply filtering criteria
        if (tli != 0 && tli != file_tli)
            continue;  // Skip if timeline doesn't match
        if (!XLogRecPtrIsInvalid(start_lsn) && start_lsn >= file_end_lsn)
            continue;  // Skip if summary ends before our start
        if (!XLogRecPtrIsInvalid(end_lsn) && end_lsn <= file_start_lsn)
            continue;  // Skip if summary starts after our end

        // Create and add matching summary to result list
        ws = palloc(sizeof(WalSummaryFile));
        ws->tli = file_tli;
        ws->start_lsn = file_start_lsn;
        ws->end_lsn = file_end_lsn;
        result = lappend(result, ws);
    }

    FreeDir(sdir);
    return result;
}
```

Key simplifications made:
- Added descriptive comments explaining each logical step
- Clarified the filtering logic with inline comments
- Preserved all essential algorithm components
- Enhanced readability while maintaining functional correctness
- Made variable purposes clearer through better commenting