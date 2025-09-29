# BufferUsageAccumDiff

## Location
[src/backend/executor/instrument.c:248-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L248-L277)

## Overview
A utility function that accumulates buffer usage statistics by computing the difference between two BufferUsage snapshots and adding that difference to a destination BufferUsage structure.

## Definition

```c
void
BufferUsageAccumDiff(BufferUsage *dst,
					 const BufferUsage *add,
					 const BufferUsage *sub)
```
## Detailed Description
BufferUsageAccumDiff is a public function that calculates the incremental buffer usage statistics between two points in time and accumulates those differences into a destination structure. It performs the operation dst += (add - sub) for all buffer usage counters and timing measurements.

This function is essential for PostgreSQL's query instrumentation system, allowing the database to track how much buffer activity occurred during specific operations or time periods. Since BufferUsage counters are monotonically increasing and never reset, this difference-based approach enables precise measurement of resource consumption for individual query operations.

The function handles all three categories of buffer usage (shared, local, and temporary) and their associated timing measurements, using specialized macros for proper time arithmetic.

## Parameters / Member Variables
- : Pointer to the destination BufferUsage structure that will accumulate the computed differences
- : Pointer to the BufferUsage structure representing the ending state (higher counter values)
- : Pointer to the BufferUsage structure representing the starting state (lower counter values)

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_ACCUM_DIFF (macro for computing and accumulating timing differences)
  - [BufferUsage](BufferUsage.md) (struct type definition)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md)
  - [serializeAnalyzeReceive](../s/serializeAnalyzeReceive.md)
  - [ExplainExecuteQuery](../E/ExplainExecuteQuery.md)
  - [InstrStopNode](../I/InstrStopNode.md)
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)

## Notes and Other Information
- This is a public function (non-static), making it accessible from other compilation units
- The function is crucial for PostgreSQL's EXPLAIN functionality to show buffer usage statistics
- Used extensively in query execution instrumentation to measure resource consumption
- The difference calculation assumes that 'add' contains values greater than or equal to 'sub'
- Essential for parallel query execution where statistics from multiple workers need to be aggregated
- Located in src/backend/executor/instrument.c:248-277

## Simplified Source

```c
void BufferUsageAccumDiff(BufferUsage *dst,
                         const BufferUsage *add,
                         const BufferUsage *sub)
{
    // Accumulate shared buffer statistics: dst += (add - sub)
    dst->shared_blks_hit += add->shared_blks_hit - sub->shared_blks_hit;
    dst->shared_blks_read += add->shared_blks_read - sub->shared_blks_read;
    dst->shared_blks_dirtied += add->shared_blks_dirtied - sub->shared_blks_dirtied;
    dst->shared_blks_written += add->shared_blks_written - sub->shared_blks_written;

    // Accumulate local buffer statistics
    dst->local_blks_hit += add->local_blks_hit - sub->local_blks_hit;
    dst->local_blks_read += add->local_blks_read - sub->local_blks_read;
    dst->local_blks_dirtied += add->local_blks_dirtied - sub->local_blks_dirtied;
    dst->local_blks_written += add->local_blks_written - sub->local_blks_written;

    // Accumulate temporary buffer statistics
    dst->temp_blks_read += add->temp_blks_read - sub->temp_blks_read;
    dst->temp_blks_written += add->temp_blks_written - sub->temp_blks_written;

    // Accumulate timing statistics using specialized macros
    INSTR_TIME_ACCUM_DIFF(dst->shared_blk_read_time,
                          add->shared_blk_read_time, sub->shared_blk_read_time);
    INSTR_TIME_ACCUM_DIFF(dst->shared_blk_write_time,
                          add->shared_blk_write_time, sub->shared_blk_write_time);
    INSTR_TIME_ACCUM_DIFF(dst->local_blk_read_time,
                          add->local_blk_read_time, sub->local_blk_read_time);
    INSTR_TIME_ACCUM_DIFF(dst->local_blk_write_time,
                          add->local_blk_write_time, sub->local_blk_write_time);
    INSTR_TIME_ACCUM_DIFF(dst->temp_blk_read_time,
                          add->temp_blk_read_time, sub->temp_blk_read_time);
    INSTR_TIME_ACCUM_DIFF(dst->temp_blk_write_time,
                          add->temp_blk_write_time, sub->temp_blk_write_time);
}
```