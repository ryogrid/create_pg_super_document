# pg_time_now_lazy

## Location
[src/bin/pgbench/pgbench.c:861-866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L861-L866)

## Overview
A static inline function that implements lazy evaluation for time retrieval, only calling pg_time_now() if the provided timestamp is zero (uninitialized).

## Definition
static inline void pg_time_now_lazy(pg_time_usec_t *now)

## Detailed Description
pg_time_now_lazy provides a performance optimization for scenarios where time retrieval should be deferred until actually needed. The function takes a pointer to a pg_time_usec_t variable and only updates it with the current time if the value is currently zero. This lazy evaluation pattern helps avoid unnecessary system calls when timing information might not be required, particularly useful in conditional logging or error reporting scenarios where timestamps are only needed when specific conditions are met.

## Parameters / Member Variables
- now: Pointer to a pg_time_usec_t variable that will be set to the current time if it is currently zero

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_usec_t (type for microsecond timestamps)
  - [pg_time_now](pg_time_now.md) (function to get current time)

- Called from (representative examples):
  - [doRetry](../d/doRetry.md)
  - [printVerboseErrorMessages](printVerboseErrorMessages.md)
  - [advanceConnectionState](../a/advanceConnectionState.md)
  - [executeMetaCommand](../e/executeMetaCommand.md)
  - [processXactStats](processXactStats.md)
  - [threadRun](../t/threadRun.md)

## Notes and Other Information
- Implements lazy evaluation pattern to avoid unnecessary time system calls
- Commonly used in error handling and logging contexts where timestamps are conditionally needed
- The function modifies the value through the pointer, making it suitable for scenarios where multiple functions might need the same timestamp
- Helps optimize performance by preventing redundant time retrievals when the same timestamp might be used multiple times