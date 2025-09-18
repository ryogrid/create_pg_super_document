# pgstat_count_io_op_time

## Location
[src/backend/utils/activity/pgstat_io.c:122-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L122-L156)

## Overview
Records IO operations with timing information, accumulating both operation counts and elapsed time for comprehensive PostgreSQL IO performance monitoring.

## Definition
void pgstat_count_io_op_time(IOObject io_object, IOContext io_context, IOOp io_op, instr_time start_time, uint32 cnt)

## Detailed Description
This function extends the basic IO operation counting functionality by also tracking the time duration of IO operations. It serves as the most comprehensive IO statistics recording function in PostgreSQL, capturing both count and timing data for performance analysis.

The function operates in two phases:
1. **Timing Phase** (when track_io_timing is enabled):
   - Calculates elapsed time by subtracting start_time from current time
   - Updates specialized buffer usage statistics for read/write operations
   - Accumulates timing data in the pending IO statistics structure
   - Handles different object types (relations vs temp relations) appropriately

2. **Counting Phase**:
   - Delegates to pgstat_count_io_op_n() to record operation counts
   - Ensures consistent statistics tracking regardless of timing settings

The function provides detailed tracking for specific operation types:
- Read operations: Updates shared/local block read time statistics
- Write/Extend operations: Updates shared/local block write time statistics  
- All operations: Accumulates timing in pending IO statistics for later reporting

## Parameters / Member Variables
- `io_object`: The IOObject enum value specifying the type of object involved in the IO operation
- `io_context`: The IOContext enum value indicating the context in which the IO operation occurred
- `io_op`: The IOOp enum value identifying the specific type of IO operation being recorded
- `start_time`: The instr_time timestamp when the IO operation began (typically from pgstat_prepare_io_time)
- `cnt`: The number of operations to record (uint32 value)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_io_op_n](pgstat_count_io_op_n.md)
  - pgstat_count_buffer_write_time
  - pgstat_count_buffer_read_time
  - [IOObject](../I/IOObject.md)
  - [IOContext](../I/IOContext.md)
  - [IOOp](../I/IOOp.md)
  - [instr_time](../i/instr_time.md)
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SUBTRACT
  - INSTR_TIME_ADD
  - INSTR_TIME_GET_MICROSEC
  - IOOP_READ
  - IOOP_WRITE
  - IOOP_EXTEND
  - IOOBJECT_RELATION
  - IOOBJECT_TEMP_RELATION
  - [PgStat_IO](../P/PgStat_IO.md)
- Called from (representative examples):
  - [WaitReadBuffers](../W/WaitReadBuffers.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - [FlushBuffer](../F/FlushBuffer.md)
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)
  - [IssuePendingWritebacks](../I/IssuePendingWritebacks.md)
  - GetLocalVictimBuffer
  - ExtendBufferedRelLocal
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - [mdsyncfiletag](../m/mdsyncfiletag.md)

## Notes and Other Information
- Timing collection is controlled by the track_io_timing global variable
- Updates both general IO statistics and specialized buffer usage counters (pgBufferUsage)
- Timing data is accumulated in microseconds for high precision
- Always calls pgstat_count_io_op_n() to ensure operation counts are recorded even when timing is disabled
- Part of PostgreSQL's comprehensive IO performance monitoring system
- The start_time parameter should typically be obtained from pgstat_prepare_io_time()