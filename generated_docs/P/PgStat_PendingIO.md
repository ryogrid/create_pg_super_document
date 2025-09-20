# PgStat_PendingIO

## Location
[src/backend/utils/activity/pgstat_io.c:24-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L24-L28)

## Overview
Structure that holds locally accumulated IO statistics before they are flushed to shared memory for PostgreSQL's IO statistics tracking system.

## Definition

```c
typedef struct PgStat_PendingIO
{
	PgStat_Counter counts[IOOBJECT_NUM_TYPES][IOCONTEXT_NUM_TYPES][IOOP_NUM_TYPES];
	instr_time	pending_times[IOOBJECT_NUM_TYPES][IOCONTEXT_NUM_TYPES][IOOP_NUM_TYPES];
} PgStat_PendingIO;
```
## Detailed Description
PgStat_PendingIO is a local buffer structure that accumulates IO statistics before they are periodically flushed to shared memory. This structure tracks both operation counts and timing information for different combinations of IO objects, contexts, and operations.

The structure uses a three-dimensional array organization to categorize IO statistics by:
- IOObject: The type of database object being accessed (relations, temporary relations)
- IOContext: The context in which IO operations occur (normal, bulk read, bulk write, vacuum)  
- IOOp: The specific type of IO operation (read, write, extend, fsync, etc.)

This design allows PostgreSQL to maintain detailed IO statistics that can be queried through system views like pg_stat_io. The pending nature of this structure enables efficient local accumulation before expensive shared memory updates.

## Parameters / Member Variables
- : Three-dimensional array of PgStat_Counter values tracking the number of IO operations for each combination of [IOObject][IOContext][IOOp]
- : Three-dimensional array of instr_time values tracking the accumulated time spent on IO operations for each combination of [IOObject][IOContext][IOOp]

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter
  - IOOBJECT_NUM_TYPES (value: 2, covering IOOBJECT_RELATION and IOOBJECT_TEMP_RELATION)
  - IOCONTEXT_NUM_TYPES (value: 4, covering BULKREAD, BULKWRITE, NORMAL, VACUUM contexts)
  - IOOP_NUM_TYPES (value: 8, covering EVICT, EXTEND, FSYNC, HIT, READ, REUSE, WRITE, WRITEBACK operations)
  - [instr_time](../i/instr_time.md)

- Called from (representative examples):
  - Used in pgstat_count_io_op_n() at src/backend/utils/activity/pgstat_io.c:90
  - Used in pgstat_count_io_op_time() at src/backend/utils/activity/pgstat_io.c:149
  - Used in pgstat_flush_io() at src/backend/utils/activity/pgstat_io.c:198-213

## Notes and Other Information
- A static instance named PendingIOStats is declared in pgstat_io.c to serve as the process-local accumulator
- The structure is zeroed out after flushing statistics to shared memory in pgstat_flush_io()
- The have_iostats global variable tracks whether any pending IO statistics exist
- Statistics are only tracked for BackendTypes that participate in IO statistics collection
- Array dimensions are compile-time constants, making the structure efficient for frequent access during IO operations