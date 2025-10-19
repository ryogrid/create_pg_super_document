# stop_streaming

## Location
[src/bin/pg_basebackup/pg_receivewal.c:184-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_receivewal.c#L184-L234)

## Overview
A control function that determines whether WAL streaming should be stopped based on specified end positions, timeline changes, and interrupt signals.

## Definition

```c
static bool
stop_streaming(XLogRecPtr xlogpos, uint32 timeline, bool segment_finished)
```
## Detailed Description
The stop_streaming function serves as the main control mechanism for terminating WAL streaming in pg_receivewal. It evaluates multiple conditions to determine when streaming should cease: whether the specified ending LSN has been reached, if a timeline switch has occurred, or if an interrupt signal has been received. The function maintains static variables to track the previous timeline and position for proper timeline switch reporting. When verbose mode is enabled, it provides detailed logging about segment completion, timeline switches, and stopping reasons. The function is designed to be called at the end of each WAL segment to make streaming control decisions.

## Parameters / Member Variables
- `xlogpos`: Current WAL Log Sequence Number (LSN) position being processed
- `timeline`: Current timeline identifier for the WAL stream
- `segment_finished`: Boolean indicating whether the current WAL segment has been completed
## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info (PostgreSQL logging function)
  - XLogRecPtrIsInvalid (macro to check for invalid LSN)
  - LSN_FORMAT_ARGS (macro for formatting LSN values)
  - InvalidXLogRecPtr (constant for invalid LSN)
- Called from (representative examples):
  - [StreamLog](../S/StreamLog.md) (in pg_receivewal.c)

## Notes and Other Information
- This is a static function with file-local scope within pg_receivewal.c
- Uses static variables (prevtimeline, prevpos) to maintain state between calls
- Provides comprehensive verbose logging for debugging and monitoring WAL streaming
- Handles timeline switches by reporting the end of the previous timeline
- Returns true when streaming should stop, false when it should continue
- The function accounts for the fact that timeline switches may not align perfectly with WAL segment boundaries
- Part of pg_receivewal's core streaming control logic
- Responds to the global time_to_stop flag for graceful shutdown on interrupts

## Simplified Source

```c
static bool stop_streaming(XLogRecPtr xlogpos, uint32 timeline, bool segment_finished) {
    static uint32 prevtimeline = 0;
    static XLogRecPtr prevpos = InvalidXLogRecPtr;

    // Log segment completion if verbose mode enabled
    if (verbose && segment_finished) {
        pg_log_info("finished segment at %X/%X (timeline %u)",
                    LSN_FORMAT_ARGS(xlogpos), timeline);
    }

    // Check if we've reached the specified end position
    if (!XLogRecPtrIsInvalid(endpos) && endpos < xlogpos) {
        if (verbose) {
            pg_log_info("stopped log streaming at %X/%X (timeline %u)",
                        LSN_FORMAT_ARGS(xlogpos), timeline);
        }
        time_to_stop = true;
        return true;
    }

    // Log timeline switches
    if (verbose && prevtimeline != 0 && prevtimeline != timeline) {
        pg_log_info("switched to timeline %u at %X/%X",
                    timeline, LSN_FORMAT_ARGS(prevpos));
    }

    // Update state for next call
    prevtimeline = timeline;
    prevpos = xlogpos;

    // Check for interrupt signal
    if (time_to_stop) {
        if (verbose) {
            pg_log_info("received interrupt signal, exiting");
        }
        return true;
    }

    return false;  // Continue streaming
}
```