# BufferUsageAdd

## Location
[src/backend/executor/instrument.c:226-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L226-L247)

## Overview
A static utility function that accumulates buffer usage statistics by adding all fields from a source BufferUsage structure to a destination BufferUsage structure.

## Definition

```c
static void
BufferUsageAdd(BufferUsage *dst, const BufferUsage *add)
```
## Detailed Description
BufferUsageAdd is a helper function used to aggregate buffer usage statistics in PostgreSQL's instrumentation system. It performs element-wise addition of all buffer usage counters and timing information from the  parameter to the  parameter. This function is essential for accumulating statistics across multiple operations or parallel workers.

The function handles three categories of buffer usage:
1. **Shared buffers**: Blocks in PostgreSQL's shared buffer pool
2. **Local buffers**: Process-local temporary buffers  
3. **Temporary blocks**: Used for temporary files during operations like sorting

For each category, it tracks hit counts, read counts, blocks dirtied, blocks written, and timing information for read/write operations. The timing fields use the  macro to properly accumulate timing measurements.

## Parameters / Member Variables
- : Pointer to the destination BufferUsage structure that will receive the accumulated values
- : Pointer to the source BufferUsage structure whose values will be added to dst

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_ADD (macro for adding timing measurements)
  - [BufferUsage](BufferUsage.md) (struct type definition)
- Called from (representative examples):
  - [InstrAggNode](../I/InstrAggNode.md)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md)

## Notes and Other Information
- This is a static function, so it's only accessible within the same compilation unit (instrument.c)
- The function performs simple accumulation without any validation or overflow checking
- All BufferUsage counters are designed to be monotonically increasing and never reset to zero
- The function is crucial for PostgreSQL's query execution instrumentation and performance monitoring
- Located in src/backend/executor/instrument.c:226-247

## Simplified Source

```c
static void
BufferUsageAdd(BufferUsage *dst, const BufferUsage *add)
{
    // Accumulate shared buffer statistics
    dst->shared_blks_hit += add->shared_blks_hit;
    dst->shared_blks_read += add->shared_blks_read;
    dst->shared_blks_dirtied += add->shared_blks_dirtied;
    dst->shared_blks_written += add->shared_blks_written;

    // Accumulate local buffer statistics
    dst->local_blks_hit += add->local_blks_hit;
    dst->local_blks_read += add->local_blks_read;
    dst->local_blks_dirtied += add->local_blks_dirtied;
    dst->local_blks_written += add->local_blks_written;

    // Accumulate temporary buffer statistics
    dst->temp_blks_read += add->temp_blks_read;
    dst->temp_blks_written += add->temp_blks_written;

    // Accumulate timing information
    INSTR_TIME_ADD(dst->shared_blk_read_time, add->shared_blk_read_time);
    INSTR_TIME_ADD(dst->shared_blk_write_time, add->shared_blk_write_time);
    INSTR_TIME_ADD(dst->local_blk_read_time, add->local_blk_read_time);
    INSTR_TIME_ADD(dst->local_blk_write_time, add->local_blk_write_time);
    INSTR_TIME_ADD(dst->temp_blk_read_time, add->temp_blk_read_time);
    INSTR_TIME_ADD(dst->temp_blk_write_time, add->temp_blk_write_time);
}
```