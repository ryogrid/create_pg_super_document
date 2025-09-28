# pgstat_prepare_io_time

## Location
[src/backend/utils/activity/pgstat_io.c:100-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L100-L121)

## Overview
Initializes timing instrumentation for IO operations based on configuration settings, returning a timestamp for later duration calculations.

## Definition
instr_time pgstat_prepare_io_time(bool track_io_guc)

## Detailed Description
This function serves as the initialization step for IO timing measurements in PostgreSQL's statistics system. It conditionally captures the current time based on whether IO timing tracking is enabled through configuration.

The function implements an optimization where timing instrumentation is only activated when needed:
- If tracking is enabled (track_io_guc is true), it captures the current high-resolution timestamp
- If tracking is disabled, it sets the timer to zero to avoid unnecessary overhead while preventing compiler warnings

This design allows PostgreSQL to minimize performance impact when IO timing statistics are not required, while providing accurate measurements when they are needed. The returned instr_time value is typically used later with pgstat_count_io_op_time() to calculate and record the duration of an IO operation.

## Parameters / Member Variables
- `track_io_guc`: Boolean flag indicating whether IO timing tracking is enabled through PostgreSQL configuration (GUC - Grand Unified Configuration)

## Dependencies
- Functions called/Symbols referenced:
  - [instr_time](../i/instr_time.md)
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SET_ZERO
- Called from (representative examples):
  - [WaitReadBuffers](../W/WaitReadBuffers.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - [FlushBuffer](../F/FlushBuffer.md)
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)
  - [IssuePendingWritebacks](../I/IssuePendingWritebacks.md)
  - [GetLocalVictimBuffer](../G/GetLocalVictimBuffer.md)
  - [ExtendBufferedRelLocal](../E/ExtendBufferedRelLocal.md)
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - [mdsyncfiletag](../m/mdsyncfiletag.md)

## Notes and Other Information
- Returns an instr_time structure that represents either the current time or zero
- The track_io_guc parameter typically corresponds to GUC settings like track_io_timing
- Used in conjunction with pgstat_count_io_op_time() to measure and record IO operation durations
- Part of PostgreSQL's performance monitoring infrastructure for analyzing IO bottlenecks
- The zero initialization when tracking is disabled prevents undefined behavior and compiler warnings

## Simplified Source

```c
// Simplified version of pgstat_prepare_io_time
instr_time
pgstat_prepare_io_time(bool track_io_guc) {
    instr_time io_start;

    if (track_io_guc) {
        // Capture current time for timing measurement
        INSTR_TIME_SET_CURRENT(io_start);
    } else {
        // Set to zero when timing disabled (avoids compiler warnings)
        INSTR_TIME_SET_ZERO(io_start);
    }

    return io_start;
}
```

Key simplifications made:
- This function is already very simple - just a conditional timestamp capture
- Added clear comments explaining when timing is captured vs when it's zeroed
- Emphasized the optimization aspect (avoid overhead when timing disabled)
- Preserved the essential pattern used throughout PostgreSQL's IO timing infrastructure