# injection_points_wakeup

## Location
[src/test/modules/injection_points/injection_points.c:324-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L324-L363)

## Overview
A SQL-callable function that awakens processes waiting on a named injection point, providing the coordination mechanism for synchronization-based testing scenarios.

## Definition
Datum injection_points_wakeup(PG_FUNCTION_ARGS)

## Detailed Description
The `injection_points_wakeup` function complements `injection_wait` by providing the mechanism to wake up processes that are blocked waiting on injection points. It searches the shared memory structure for waiters on the specified injection point name, increments the wait counter for that injection point, and broadcasts a condition variable to wake up all waiting processes. This function is essential for creating controlled test scenarios where multiple processes need to coordinate their execution timing, allowing tests to create precise race conditions or ensure specific execution orders.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: Text string representing the injection point name to wake up

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - [injection_init_shmem](injection_init_shmem.md)
  - ConditionVariableBroadcast
  - PG_RETURN_VOID
  - INJ_MAX_WAIT
- Called from (representative examples):
  - SQL interface (can be called from SQL queries as a function)

## Notes and Other Information
- Requires the target injection point to have active waiters, otherwise throws an ERROR
- Uses a wait counter mechanism to signal waiting processes that they should stop waiting
- The condition variable broadcast ensures all waiters on the injection point are awakened simultaneously
- Critical for test synchronization scenarios where timing coordination between processes is needed
- Works exclusively with injection points created using the "wait" action type
- Searches through all available wait slots (up to INJ_MAX_WAIT) to find the target injection point
- Part of the injection_points extension module and commonly used in parallel processing tests
- Provides SQL-level control over process synchronization during testing