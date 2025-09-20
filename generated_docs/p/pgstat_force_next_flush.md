# pgstat_force_next_flush

## Location
[src/backend/utils/activity/pgstat.c:693-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L693-L701)

## Overview
A utility function that forces the next call to pgstat_report_stat() to flush all pending statistics updates, primarily used for testing purposes.

## Definition

```c
void
pgstat_force_next_flush(void)
```
## Detailed Description
This function sets a global flag (pgStatForceNextFlush) that will cause the next invocation of pgstat_report_stat() to perform a forced flush of all pending statistics updates. The function is specifically designed to provide deterministic statistics flushing behavior for testing scenarios where precise control over when statistics are written to shared memory is required.

When the flag is set, pgstat_report_stat() will ignore its normal timing constraints (PGSTAT_MIN_INTERVAL) and perform an immediate, blocking flush of all pending statistics categories. The flag is automatically cleared after being consumed by pgstat_report_stat().

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pgStatForceNextFlush (global variable assignment)
- Called from (representative examples):
  - [pg_stat_force_next_flush](pg_stat_force_next_flush.md) (SQL-callable function)

## Notes and Other Information
- This function is primarily intended for testing infrastructure where deterministic statistics behavior is needed
- The function only sets a flag; the actual forced flush occurs on the next pgstat_report_stat() call
- The flag is automatically reset to false after being consumed, so each call to this function affects only the next single statistics flush
- Provides a way to bypass the normal timing-based statistics flushing logic for scenarios requiring immediate statistics visibility