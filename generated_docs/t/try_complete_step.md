# try_complete_step

## Location
[src/test/isolation/isolationtester.c:818-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L818-L1079)

## Overview
Attempts to complete a single isolation test step by waiting for its associated database query to finish, while handling blocking conditions, timeouts, and lock waits.

## Definition

```c
struct timeval start_time;
```
## Detailed Description
This function is a critical component of PostgreSQL's isolation testing framework that manages the execution lifecycle of individual test steps. It waits for a database query (already sent by the caller) to complete while handling various blocking scenarios including lock waits, blocker conditions, and timeouts.

The function implements sophisticated timeout handling with two thresholds: after  it attempts to cancel the query, and after twice that duration it terminates the program. When the STEP_NONBLOCK flag is specified, it actively checks if the step is waiting for a lock by querying the database's lock information.

The function processes different types of query results (tuples, errors, notifications) and formats them appropriately for test output. It also handles NOTIFY messages from other sessions and manages the connection's active step state.

## Parameters / Member Variables
- : Pointer to the test specification containing session and step configuration
- : Pointer to the PermutationStep being executed, containing the step definition and blocker information
- : Control flags including STEP_RETRY (for subsequent calls) and STEP_NONBLOCK (to check for lock waits)

## Dependencies
- Functions called/Symbols referenced:
  - [step_has_blocker](../s/step_has_blocker.md)
  - [printResultSet](../p/printResultSet.md)
  - [PQsocket](../P/PQsocket.md), PQisBusy, PQconsumeInput, PQgetResult, PQnotifies
  - [PQexecPrepared](../P/PQexecPrepared.md) (for lock wait detection)
  - [PQcancelCreate](../P/PQcancelCreate.md), PQcancelBlocking (for query cancellation)
  - select, gettimeofday (for timing and I/O)
  - Various libpq result processing functions
- Called from (representative examples):
  - [try_complete_steps](try_complete_steps.md)
  - [run_permutation](../r/run_permutation.md)

## Notes and Other Information
- Returns true if the step was NOT completed (blocked or waiting), false if completed
- Implements a progressive timeout strategy: warn/cancel at max_step_wait, exit at 2x max_step_wait
- Handles special PSB_ONCE blockers that force immediate waiting on first call
- Processes and displays query results, errors, and NOTIFY messages in standardized format
- Manages connection state by clearing active_step when step completes
- Critical for preventing test hangs in buildfarm environments while maintaining test determinism
- Uses microsecond precision timing for accurate timeout handling