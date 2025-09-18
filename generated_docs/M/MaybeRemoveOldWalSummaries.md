# MaybeRemoveOldWalSummaries

## Location
[src/backend/postmaster/walsummarizer.c:1654-1731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L1654-L1731)

## Overview
A cleanup function that removes old WAL summary files based on their modification time and the configured retention period, ensuring that disk space doesn't grow unbounded from accumulated summary files.

## Definition
```c
static void MaybeRemoveOldWalSummaries(void)
```

## Detailed Description
This function implements a time-based cleanup mechanism for WAL summary files, removing summaries that are older than the configured retention period (`wal_summary_keep_time`). The function operates with several important constraints:

1. **Checkpoint-based execution**: Only runs when the redo pointer has advanced, effectively limiting cleanup to once per checkpoint cycle
2. **Timeline-aware processing**: Processes summaries by timeline, ensuring proper ordering and dependency management
3. **WAL availability check**: Only removes summaries whose corresponding WAL segments are no longer available on disk
4. **Time-based filtering**: Uses file modification time to determine eligibility for removal

The function retrieves all existing summary files, then processes them timeline by timeline. For each timeline, it determines the oldest available WAL segment and removes summaries that either have no corresponding WAL or whose modification time exceeds the retention period.

## Parameters / Member Variables
This function takes no parameters but operates on several configuration and state variables:
- `wal_summary_keep_time`: Global configuration setting for retention period in minutes
- `redo_pointer_at_last_summary_removal`: Static variable tracking the last cleanup execution

## Dependencies
- Functions called/Symbols referenced:
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)
  - [GetWalSummaries](../G/GetWalSummaries.md)
  - [HandleWalSummarizerInterrupts](../H/HandleWalSummarizerInterrupts.md)
  - [XLogGetOldestSegno](../X/XLogGetOldestSegno.md)
  - XLogSegNoOffsetToRecPtr
  - XLogRecPtrIsInvalid
  - [RemoveWalSummaryIfOlderThan](../R/RemoveWalSummaryIfOlderThan.md)
  - foreach_delete_current
  - SECS_PER_MINUTE (constant)
- Called from (representative examples):
  - [WalSummarizerMain](../W/WalSummarizerMain.md)

## Notes and Other Information
- Cleanup is disabled when `wal_summary_keep_time` is set to 0, allowing administrators to disable automatic cleanup
- The redo pointer check prevents excessive cleanup attempts and ties cleanup frequency to checkpoint cycles
- Timeline-based processing ensures that summaries are removed in a consistent order that respects WAL timeline relationships
- The function safely handles interrupts through HandleWalSummarizerInterrupts() calls
- Memory management is handled through pfree() calls to prevent memory leaks during the cleanup process
- The cutoff time calculation uses wall clock time minus the retention period, making cleanup independent of WAL generation rate
- Summary files are only removed if their corresponding WAL segments are no longer available, ensuring data consistency