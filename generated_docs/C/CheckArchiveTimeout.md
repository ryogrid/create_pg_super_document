# CheckArchiveTimeout

## Location
[src/backend/postmaster/checkpointer.c:626-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L626-L686)

## Overview
Checks for archive timeout conditions and forces WAL file switches to ensure timely archiving of WAL files when the archive_timeout setting is configured.

## Definition

```c
static void
CheckArchiveTimeout(void)
```
## Detailed Description
CheckArchiveTimeout monitors the time since the last WAL segment switch and forces a new WAL segment to be created when the archive_timeout period has elapsed. This ensures that WAL files are archived in a timely manner even during periods of low database activity.

The function implements an optimization to avoid creating archive files containing only "unimportant" WAL records (such as regular snapshots of running transactions marked with XLOG_MARK_UNIMPORTANT). It only forces a segment switch when meaningful activity has been recorded since the last switch.

Key behaviors:
- Only operates when XLogArchiveTimeout > 0 and not in recovery mode
- Uses a two-stage check: first a quick check with local state, then a more thorough check with updated state
- Only switches segments when important WAL records have been logged since the last switch
- Updates timing state regardless of whether a switch occurs to prevent constant retries during idle periods

## Parameters / Member Variables
None - the function operates on global state variables and configuration settings.

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetLastSegSwitchData](../G/GetLastSegSwitchData.md)
  - [GetLastImportantRecPtr](../G/GetLastImportantRecPtr.md)
  - [RequestXLogSwitch](../R/RequestXLogSwitch.md)
  - XLogSegmentOffset
- Called from (representative examples):
  - [CheckpointerMain](CheckpointerMain.md) (checkpointer.c:520)
  - [CheckpointWriteDelay](CheckpointWriteDelay.md) (checkpointer.c:742)

## Notes and Other Information
- Relies on the XLogArchiveTimeout configuration parameter to determine timing
- Uses last_xlog_switch_time global variable to track switch timing
- The "unimportant" flag in RequestXLogSwitch prevents unnecessary checkpoints
- Segment boundary detection prevents false positives when no actual switch occurred
- Part of PostgreSQL's continuous archiving and point-in-time recovery infrastructure
- Only active during normal operation (not during recovery)

## Simplified Source

```c
// Simplified version of CheckArchiveTimeout
static void CheckArchiveTimeout(void) {
    pg_time_t now;
    pg_time_t last_switch_time;
    XLogRecPtr last_switch_lsn;

    // Skip if archive timeout disabled or in recovery
    if (XLogArchiveTimeout <= 0 || RecoveryInProgress()) {
        return;
    }

    now = (pg_time_t) time(NULL);

    // Quick check: if not enough time has passed, return early
    if ((int) (now - last_xlog_switch_time) < XLogArchiveTimeout) {
        return;
    }

    // Get accurate timing information from WAL subsystem
    last_switch_time = GetLastSegSwitchData(&last_switch_lsn);
    last_xlog_switch_time = Max(last_xlog_switch_time, last_switch_time);

    // Check if archive timeout has been reached
    if ((int) (now - last_xlog_switch_time) >= XLogArchiveTimeout) {
        // Only switch if important WAL activity has occurred
        if (GetLastImportantRecPtr() > last_switch_lsn) {
            XLogRecPtr switchpoint;

            // Request WAL switch (marked as unimportant to avoid checkpoints)
            switchpoint = RequestXLogSwitch(true);

            // Log debug message if switch actually occurred
            if (XLogSegmentOffset(switchpoint, wal_segment_size) != 0) {
                elog(DEBUG1, "write-ahead log switch forced (archive_timeout=%d)",
                     XLogArchiveTimeout);
            }
        }

        // Update timing state to prevent constant retries
        last_xlog_switch_time = now;
    }
}
```

Key simplifications made:
- Removed detailed comments within the code for clarity
- Consolidated variable declarations at the top
- Simplified the conditional logic flow
- Added high-level comments explaining each major step
- Preserved all essential functionality and error handling
- Maintained the two-stage checking optimization