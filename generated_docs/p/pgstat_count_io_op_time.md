# pgstat_count_io_op_time

## Location
src/backend/utils/activity/pgstat_io.c: 122 - 156

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
  - pgstat_count_io_op_n
  - pgstat_count_buffer_write_time
  - pgstat_count_buffer_read_time
  - IOObject
  - IOContext
  - IOOp
  - instr_time
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SUBTRACT
  - INSTR_TIME_ADD
  - INSTR_TIME_GET_MICROSEC
  - IOOP_READ
  - IOOP_WRITE
  - IOOP_EXTEND
  - IOOBJECT_RELATION
  - IOOBJECT_TEMP_RELATION
  - PgStat_IO
- Called from (representative examples):
  - WaitReadBuffers
  - ExtendBufferedRelShared
  - FlushBuffer
  - FlushRelationBuffers
  - IssuePendingWritebacks
  - GetLocalVictimBuffer
  - ExtendBufferedRelLocal
  - register_dirty_segment
  - mdsyncfiletag

## Notes and Other Information
- Timing collection is controlled by the track_io_timing global variable
- Updates both general IO statistics and specialized buffer usage counters (pgBufferUsage)
- Timing data is accumulated in microseconds for high precision
- Always calls pgstat_count_io_op_n() to ensure operation counts are recorded even when timing is disabled
- Part of PostgreSQL's comprehensive IO performance monitoring system
- The start_time parameter should typically be obtained from pgstat_prepare_io_time()