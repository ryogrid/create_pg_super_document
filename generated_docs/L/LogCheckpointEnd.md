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

## Simplified Source

```c
// Simplified version of LogCheckpointEnd
static void LogCheckpointEnd(bool restartpoint) {
    // Record checkpoint end time
    CheckpointStats.ckpt_end_t = GetCurrentTimestamp();

    // Calculate timing metrics in milliseconds
    long write_time = TimestampDifferenceMilliseconds(CheckpointStats.ckpt_write_t,
                                                     CheckpointStats.ckpt_sync_t);
    long sync_time = TimestampDifferenceMilliseconds(CheckpointStats.ckpt_sync_t,
                                                    CheckpointStats.ckpt_sync_end_t);

    // Accumulate statistics for monitoring
    PendingCheckpointerStats.write_time += write_time;
    PendingCheckpointerStats.sync_time += sync_time;

    // Exit early if checkpoint logging is disabled
    if (!log_checkpoints)
        return;

    // Calculate additional metrics for logging
    long total_time = TimestampDifferenceMilliseconds(CheckpointStats.ckpt_start_t,
                                                     CheckpointStats.ckpt_end_t);
    long longest_sync = (CheckpointStats.ckpt_longest_sync + 999) / 1000;

    // Calculate average sync time
    long average_sync = 0;
    if (CheckpointStats.ckpt_sync_rels > 0) {
        average_sync = (CheckpointStats.ckpt_agg_sync_time /
                       CheckpointStats.ckpt_sync_rels + 999) / 1000;
    }

    // Log comprehensive checkpoint completion message
    if (restartpoint) {
        ereport(LOG, (errmsg("restartpoint complete: wrote %d buffers (%.1f%%); "
                            "write=%ld.%03d s, sync=%ld.%03d s, total=%ld.%03d s; "
                            "sync files=%d, longest=%ld.%03d s, average=%ld.%03d s",
                            CheckpointStats.ckpt_bufs_written,
                            (double) CheckpointStats.ckpt_bufs_written * 100 / NBuffers,
                            write_time / 1000, (int)(write_time % 1000),
                            sync_time / 1000, (int)(sync_time % 1000),
                            total_time / 1000, (int)(total_time % 1000),
                            CheckpointStats.ckpt_sync_rels,
                            longest_sync / 1000, (int)(longest_sync % 1000),
                            average_sync / 1000, (int)(average_sync % 1000))));
    } else {
        ereport(LOG, (errmsg("checkpoint complete: wrote %d buffers (%.1f%%); "
                            "write=%ld.%03d s, sync=%ld.%03d s, total=%ld.%03d s; "
                            "sync files=%d, longest=%ld.%03d s, average=%ld.%03d s",
                            CheckpointStats.ckpt_bufs_written,
                            (double) CheckpointStats.ckpt_bufs_written * 100 / NBuffers,
                            write_time / 1000, (int)(write_time % 1000),
                            sync_time / 1000, (int)(sync_time % 1000),
                            total_time / 1000, (int)(total_time % 1000),
                            CheckpointStats.ckpt_sync_rels,
                            longest_sync / 1000, (int)(longest_sync % 1000),
                            average_sync / 1000, (int)(average_sync % 1000))));
    }
}
```

Key simplifications made:
- Removed detailed WAL file statistics for clarity
- Simplified variable names (write_msecs → write_time, etc.)
- Consolidated timing calculations with clearer variable names
- Abbreviated the extensive log message formatting while preserving core metrics
- Removed distance and LSN reporting to focus on timing and buffer statistics
- Maintained the essential logic flow and conditional structure