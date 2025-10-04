# bbsink_progress_archive_contents

## Location
[src/backend/backup/basebackup_progress.c:150-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L150-L185)

## Overview
A progress tracking function that updates backup progress counters when new archive content data is processed during base backup operations.

## Definition
static void bbsink_progress_archive_contents(bbsink *sink, size_t len)

## Detailed Description
This static function handles progress tracking for incoming archive content data during a base backup. It increments the running counter of bytes processed, forwards the data to the next sink in the chain, and updates the PostgreSQL progress reporting system. The function implements intelligent total size adjustment to prevent progress percentages from exceeding 100% - if the actual data processed exceeds the estimated total, it updates the total to match the current progress. This accommodation is necessary because backup size estimates can be inaccurate, especially when WAL (Write-Ahead Logging) data is included in the backup.

## Parameters / Member Variables
- : The basebackup sink containing state information for tracking progress
- : The number of bytes of new archive content data being processed

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink_state](bbsink_state.md) (state structure type)
  - PROGRESS_BASEBACKUP_BACKUP_STREAMED (progress parameter constant)
  - PROGRESS_BASEBACKUP_BACKUP_TOTAL (progress parameter constant)  
  - [bbsink_forward_archive_contents](bbsink_forward_archive_contents.md) (forwarding function)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md) (progress update function)
- Called from (representative examples):
  - This is a static function, typically called through function pointers in the bbsink operations structure

## Notes and Other Information
- This is a static function accessible only within the same source file
- Updates the shared state's bytes_done counter with the processed data length
- Implements dynamic total size adjustment to prevent progress overflow past 100%
- The total size may change during backup execution when estimates prove inaccurate
- Particularly important when WAL is included in backups, as this can significantly affect size estimates
- Uses variable parameter count (nparam) to conditionally update either one or two progress parameters
- Part of the real-time progress reporting infrastructure for PostgreSQL base backup operations

## Simplified Source

```c
// Simplified version of bbsink_progress_archive_contents
static void bbsink_progress_archive_contents(bbsink *sink, size_t len)
{
    bbsink_state *state = sink->bbs_state;
    const int index[] = {
        PROGRESS_BASEBACKUP_BACKUP_STREAMED,
        PROGRESS_BASEBACKUP_BACKUP_TOTAL
    };
    int64 val[2];
    int nparam = 0;

    // Update bytes processed counter
    state->bytes_done += len;

    // Forward to next sink
    bbsink_forward_archive_contents(sink, len);

    // Set bytes done for progress reporting
    val[nparam++] = state->bytes_done;

    // Adjust total if we've exceeded the estimate (prevents >100% progress)
    if (state->bytes_total_is_valid && state->bytes_done > state->bytes_total)
        val[nparam++] = state->bytes_done;

    // Update progress reporting
    pgstat_progress_update_multi_param(nparam, index, val);
}
```