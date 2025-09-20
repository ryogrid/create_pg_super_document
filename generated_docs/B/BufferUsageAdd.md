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
  - BufferUsage (struct type definition)
- Called from (representative examples):
  - [InstrAggNode](../I/InstrAggNode.md)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md)

## Notes and Other Information
- This is a static function, so it's only accessible within the same compilation unit (instrument.c)
- The function performs simple accumulation without any validation or overflow checking
- All BufferUsage counters are designed to be monotonically increasing and never reset to zero
- The function is crucial for PostgreSQL's query execution instrumentation and performance monitoring
- Located in src/backend/executor/instrument.c:226-247