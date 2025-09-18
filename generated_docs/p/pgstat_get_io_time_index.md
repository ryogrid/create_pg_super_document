# pgstat_get_io_time_index

## Location
[src/backend/utils/adt/pgstatfuncs.c:1330-1350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1330-L1350)

## Overview
This static function determines the column index for I/O timing statistics corresponding to specific I/O operations, returning IO_COL_INVALID for operations that don't have associated timing data.

## Definition
```c
static io_stat_col pgstat_get_io_time_index(IOOp io_op)
```

## Detailed Description
This function serves as a specialized mapping utility that determines the appropriate column index for I/O timing statistics based on the I/O operation type. It implements the assumption that I/O timing data is stored in the column immediately following the operation count column. The function distinguishes between I/O operations that have timing data (read, write, writeback, extend, fsync) and those that don't (evict, hit, reuse). For operations with timing data, it calls pgstat_get_io_op_index() and adds 1 to get the timing column. For operations without timing data, it returns IO_COL_INVALID.

## Parameters / Member Variables
- `io_op`: An IOOp enumeration value representing the type of I/O operation for which timing column index is requested

## Dependencies
- Functions called/Symbols referenced:
  - [IOOp](../I/IOOp.md) (parameter type)
  - IOOP_READ, IOOP_WRITE, IOOP_WRITEBACK, IOOP_EXTEND, IOOP_FSYNC (operations with timing)
  - IOOP_EVICT, IOOP_HIT, IOOP_REUSE (operations without timing)
  - [pgstat_get_io_op_index](pgstat_get_io_op_index.md) (to get base column index)
  - IO_COL_INVALID (returned for operations without timing)
  - pg_unreachable (error handling)
- Called from (representative examples):
  - [pg_stat_get_io](pg_stat_get_io.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Encodes the design assumption that timing columns immediately follow count columns in I/O statistics views
- Not all I/O operations have associated timing data - evict, hit, and reuse operations return IO_COL_INVALID
- For timing-enabled operations, it calculates the timing column by adding 1 to the operation count column index
- Uses elog(ERROR) for unrecognized operation types, which will terminate the current transaction