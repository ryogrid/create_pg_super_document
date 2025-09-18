# LogCheckpointEnd

## Location
[src/backend/access/transam/xlog.c:6660-6762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6660-L6762)

## Overview
LogCheckpointEnd logs the completion of checkpoint or restart point operations, providing comprehensive performance statistics and timing information for monitoring and performance analysis.

## Definition
```c
static void LogCheckpointEnd(bool restartpoint)
```

## Detailed Description
This static function generates detailed log messages upon checkpoint completion, calculating and reporting extensive performance metrics including buffer statistics, WAL file operations, timing breakdowns, and distance estimates. It computes timing differences in milliseconds for write and sync phases, accumulates statistics for the checkpointer process, and formats comprehensive completion messages. The function only produces log output when log_checkpoints is enabled, though it always maintains internal statistics regardless.

## Parameters / Member Variables
- `restartpoint`: Boolean indicating whether this is a restart point (true) or regular checkpoint (false)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
  - ereport (with LOG level)
  - [errmsg](../e/errmsg.md)
  - LSN_FORMAT_ARGS macro
  - CheckpointStats global structure
  - PendingCheckpointerStats global structure
  - ControlFile global structure
  - NBuffers global variable
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (in xlog.c:7348)
  - [CreateRestartPoint](../C/CreateRestartPoint.md) (in xlog.c:7833)

## Notes and Other Information
- Static function only called internally within xlog.c
- Provides extensive performance metrics including buffer counts, percentages, WAL file operations
- Reports timing breakdowns for write, sync, and total duration with millisecond precision
- Includes sync file statistics with longest and average sync times
- Shows checkpoint distance and estimates for tuning purposes
- Reports current and redo LSN positions
- Respects log_checkpoints configuration setting for output control
- Always accumulates statistics in PendingCheckpointerStats regardless of logging