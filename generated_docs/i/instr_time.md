# instr_time

## Location
[src/include/portability/instr_time.h:69-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/portability/instr_time.h#L69-L72)

## Overview
The  struct is PostgreSQL's portable high-precision timing data type used for measuring intervals and storing absolute timestamps across different platforms.

## Definition

```c
typedef struct instr_time
{
	int64		ticks;			/* in platforms specific unit */
} instr_time;
```
## Detailed Description
The  struct provides a platform-independent abstraction for high-precision interval timing in PostgreSQL. It encapsulates timing functionality that works consistently across Unix-like systems (using ) and Windows (using ).

The design philosophy centers around efficiency for the most common operations (addition/subtraction of intervals) while hiding platform-specific implementation details. All timing values are stored as 64-bit integers in platform-specific units, providing both performance and sufficient precision for timing measurements.

The struct is designed as an opaque data type - users should never directly access the  member but instead use the provided macros for all operations. This abstraction allows PostgreSQL to optimize timing operations while maintaining portability.

Key capabilities include:
- Storing both absolute timestamps and time intervals
- High-precision measurements (nanosecond resolution on Unix, high-resolution counter on Windows)
- Efficient arithmetic operations for timing calculations
- Platform-independent API through standardized macros

## Parameters / Member Variables
- `ticks`: A 64-bit integer storing time values in platform-specific units. On Unix systems, this represents nanoseconds since an epoch. On Windows, this represents high-resolution performance counter ticks.
## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL's 64-bit integer type)
- Called from (representative examples):
  -  (src/include/executor/instrument.h:36-41)
  -  (src/include/executor/instrument.h:77-78)
  -  (src/include/jit/jit.h:33-45)
  -  (src/include/pgstat.h:110-111)
  -  (src/include/portability/instr_time.h:113)
  - Various timing-critical functions across vacuum, WAL, buffer management, and statistics subsystems

## Notes and Other Information
- The struct is intentionally wrapped around a simple int64 to prevent direct manipulation and ensure API consistency
- Platform-specific implementations:
  - Unix: Uses  with  (or  on macOS,  as fallback)
  - Windows: Uses  for high-resolution timing
- Associated macros provide the complete API:
  -  - capture current time
  -  /  - arithmetic operations
  -  /  /  /  - conversion to various units
- Widely used throughout PostgreSQL for performance monitoring, query execution timing, I/O operation measurement, and statistics collection
- Critical for PostgreSQL's EXPLAIN ANALYZE functionality and performance instrumentation
- Designed to handle interval arithmetic efficiently while maintaining nanosecond precision where supported by the platform