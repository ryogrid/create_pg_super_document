# pg_time_now

## Location
[src/bin/pgbench/pgbench.c:851-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L851-L860)

## Overview
A static inline function that returns the current time in microseconds as a pg_time_usec_t value, primarily used by pgbench for timing operations.

## Definition

```c
static inline pg_time_usec_t
pg_time_now(void)
```
## Detailed Description
pg_time_now is a utility function in pgbench that provides a convenient way to obtain the current time with microsecond precision. It acts as a wrapper around PostgreSQL's instrumentation time functions, converting the result to a standardized microsecond timestamp format. This function is essential for pgbench's timing measurements, allowing accurate benchmarking of database operations by providing high-precision timestamps for performance analysis.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [instr_time](../i/instr_time.md) (type for time measurement)
  - INSTR_TIME_SET_CURRENT (macro to set current time)
  - pg_time_usec_t (return type for microsecond timestamps)
  - INSTR_TIME_GET_MICROSEC (macro to extract microseconds)

- Called from (representative examples):
  - [pg_time_now_lazy](pg_time_now_lazy.md)
  - [advanceConnectionState](../a/advanceConnectionState.md)
  - [doLog](../d/doLog.md)
  - [initPopulateTable](../i/initPopulateTable.md)
  - [runInitSteps](../r/runInitSteps.md)
  - [set_random_seed](../s/set_random_seed.md)
  - [main](../m/main.md)
  - [threadRun](../t/threadRun.md)

## Notes and Other Information
- This function is marked as static inline for performance optimization, avoiding function call overhead
- Used extensively throughout pgbench for timing measurements during benchmark execution
- Provides microsecond precision timing which is crucial for accurate performance benchmarking
- The function is a key component of pgbench's timing infrastructure, enabling precise measurement of database operation latencies