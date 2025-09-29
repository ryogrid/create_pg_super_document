# WalSummariesAreComplete

## Location
[src/backend/backup/walsummary.c:138-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummary.c#L138-L204)

## Overview
Verifies whether a list of WAL summary files provides complete coverage for a specified LSN range without gaps.

## Definition
```c
bool WalSummariesAreComplete(List *wslist, XLogRecPtr start_lsn, XLogRecPtr end_lsn, XLogRecPtr *missing_lsn)
```

## Detailed Description
WalSummariesAreComplete analyzes a list of WalSummaryFile structures to determine if they collectively provide complete, contiguous coverage of a specified LSN range from start_lsn to end_lsn. The function sorts the summaries by start LSN and then sequentially processes them to identify any gaps in coverage. This is critical for incremental backup operations where complete WAL coverage is required to ensure data consistency.

The algorithm handles overlapping summary files correctly and can detect the exact LSN where coverage becomes incomplete. The function ignores timeline considerations, so callers should pre-filter by timeline if needed.

## Parameters / Member Variables
- `wslist`: List of WalSummaryFile structures to analyze for completeness
- `start_lsn`: Beginning of the LSN range that must be covered
- `end_lsn`: End of the LSN range that must be covered
- `missing_lsn`: Output parameter set to the first uncovered LSN if incomplete, or InvalidXLogRecPtr if the list is empty

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy](../l/list_copy.md)
  - [list_sort](../l/list_sort.md)
  - [ListComparatorForWalSummaryFiles](../L/ListComparatorForWalSummaryFiles.md)
  - lfirst
  - foreach
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)

## Notes and Other Information
- Returns true if complete coverage exists, false otherwise
- Creates a private copy of the input list for sorting (does not modify original)
- Handles overlapping summary files correctly
- Sets missing_lsn to indicate where coverage fails
- Timeline-agnostic - caller should filter by timeline beforehand
- Essential for validating incremental backup prerequisites
- Located in src/backend/backup/walsummary.c:138-204

## Simplified Source

```c
bool
WalSummariesAreComplete(List *wslist, XLogRecPtr start_lsn,
                       XLogRecPtr end_lsn, XLogRecPtr *missing_lsn)
{
    XLogRecPtr current_lsn = start_lsn;
    ListCell *lc;

    // Handle empty list case
    if (wslist == NIL)
    {
        *missing_lsn = InvalidXLogRecPtr;
        return false;
    }

    // Sort summaries by start LSN for sequential processing
    wslist = list_copy(wslist);
    list_sort(wslist, ListComparatorForWalSummaryFiles);

    // Check each summary file for continuous coverage
    foreach(lc, wslist)
    {
        WalSummaryFile *ws = lfirst(lc);

        // Check for gap between summaries
        if (ws->start_lsn > current_lsn)
        {
            // Found a gap in coverage
            break;
        }

        // Extend coverage if this summary goes beyond current position
        if (ws->end_lsn > current_lsn)
        {
            current_lsn = ws->end_lsn;

            // Check if we've reached the required end LSN
            if (current_lsn >= end_lsn)
                return true;  // Complete coverage achieved
        }
    }

    // Coverage is incomplete - indicate where it stops
    *missing_lsn = current_lsn;
    return false;
}
```