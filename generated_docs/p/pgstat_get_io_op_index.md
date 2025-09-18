# pgstat_get_io_op_index

## Location
[src/backend/utils/adt/pgstatfuncs.c:1297-1329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1297-L1329)

## Overview
This static function maps I/O operation types (IOOp) to their corresponding statistics column indices for PostgreSQL's I/O statistics system.

## Definition
```c
static io_stat_col pgstat_get_io_op_index(IOOp io_op)
```

## Detailed Description
This function serves as a mapping utility that converts I/O operation enumeration values (IOOp) to their corresponding statistic column indices (io_stat_col). It uses a switch statement to provide the appropriate column index for each supported I/O operation type. The function is essential for PostgreSQL's I/O statistics collection system, ensuring that each type of I/O operation is tracked in the correct statistical category. If an unrecognized IOOp value is provided, the function logs an error and calls pg_unreachable() to indicate a programming error.

## Parameters / Member Variables
- `io_op`: An IOOp enumeration value representing the type of I/O operation (evict, extend, fsync, hit, read, reuse, write, writeback)

## Dependencies
- Functions called/Symbols referenced:
  - [IOOp](../I/IOOp.md) (parameter type)
  - IO_COL_EVICTIONS, IO_COL_EXTENDS, IO_COL_FSYNCS, IO_COL_HITS, IO_COL_READS, IO_COL_REUSES, IO_COL_WRITES, IO_COL_WRITEBACKS (return values)
  - IOOP_EVICT, IOOP_EXTEND, IOOP_FSYNC, IOOP_HIT, IOOP_READ, IOOP_REUSE, IOOP_WRITE, IOOP_WRITEBACK (enum values)
  - pg_unreachable (error handling)
  - io_stat_col (return type)
- Called from (representative examples):
  - [pgstat_get_io_time_index](pgstat_get_io_time_index.md)
  - [pg_stat_get_io](pg_stat_get_io.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- The function includes a comment indicating that when adding new IOOp values, corresponding io_stat_col entries and case statements should be added
- Uses elog(ERROR) for unrecognized operation types, which will terminate the current transaction
- The mapping ensures consistent indexing for I/O statistics across PostgreSQL's statistics system