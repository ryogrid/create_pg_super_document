# CheckArchiveTimeout

## Location
src/backend/postmaster/checkpointer.c: 626 - 686

## Overview
Checks for archive timeout conditions and forces WAL file switches to ensure timely archiving of WAL files when the archive_timeout setting is configured.

## Definition


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
  - RecoveryInProgress
  - GetLastSegSwitchData
  - GetLastImportantRecPtr
  - RequestXLogSwitch
  - XLogSegmentOffset
- Called from (representative examples):
  - CheckpointerMain (checkpointer.c:520)
  - CheckpointWriteDelay (checkpointer.c:742)

## Notes and Other Information
- Relies on the XLogArchiveTimeout configuration parameter to determine timing
- Uses last_xlog_switch_time global variable to track switch timing
- The "unimportant" flag in RequestXLogSwitch prevents unnecessary checkpoints
- Segment boundary detection prevents false positives when no actual switch occurred
- Part of PostgreSQL's continuous archiving and point-in-time recovery infrastructure
- Only active during normal operation (not during recovery)