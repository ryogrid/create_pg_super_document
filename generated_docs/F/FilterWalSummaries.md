# FilterWalSummaries

## Location
src/backend/backup/walsummary.c: 100 - 137

## Overview
Filters an existing list of WAL summaries to create a new list containing only summaries that match specified timeline and LSN range criteria.

## Definition
```c
List *FilterWalSummaries(List *wslist, TimeLineID tli, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
```

## Detailed Description
FilterWalSummaries takes an existing list of WalSummaryFile structures and applies filtering logic to create a new list containing only the summaries that match the specified criteria. Unlike GetWalSummaries which scans the filesystem, this function operates on an already-existing list in memory. The filtering logic is identical to GetWalSummaries - it checks timeline ID and LSN range overlaps to determine which summaries should be included in the result.

This function is useful when you already have a list of WAL summaries and need to narrow it down based on different criteria without rescanning the filesystem.

## Parameters / Member Variables
- `wslist`: Input list of WalSummaryFile structures to filter
- `tli`: Timeline ID filter - if non-zero, only summaries matching this timeline will be included
- `start_lsn`: Starting LSN filter - if valid, only summaries ending after this LSN will be included
- `end_lsn`: Ending LSN filter - if valid, only summaries starting before this LSN will be included

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - lappend
  - lfirst
  - foreach
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)

## Notes and Other Information
- Returns a new List of WalSummaryFile pointers (shallow copy)
- Does not modify the input list
- Uses the same filtering logic as GetWalSummaries but operates on an existing list
- Filtering criteria are inclusive - summaries that overlap with the specified range are included
- Part of PostgreSQL's incremental backup infrastructure
- Located in src/backend/backup/walsummary.c:100-137