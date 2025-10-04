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

## Simplified Source

```c
static bool
try_complete_step(TestSpec *testspec, PermutationStep *pstep, int flags)
{
    Step *step = pstep->step;
    IsoConnInfo *iconn = &conns[1 + step->session];
    PGconn *conn = iconn->conn;
    struct timeval start_time;
    bool canceled = false;

    // Handle PSB_ONCE blockers on first call
    if (!(flags & STEP_RETRY)) {
        for (int i = 0; i < pstep->nblockers; i++) {
            if (pstep->blockers[i]->blocktype == PSB_ONCE) {
                printf("step %s: %s <waiting ...>\n", step->name, step->sql);
                return true;
            }
        }
    }

    gettimeofday(&start_time, NULL);

    // Wait for query to complete or detect lock waits
    while (PQisBusy(conn)) {
        fd_set read_set;
        struct timeval timeout = {0, 10000}; // 10ms timeout

        int ret = select(PQsocket(conn) + 1, &read_set, NULL, NULL, &timeout);

        if (ret == 0) { // Timeout - check for lock wait
            if (flags & STEP_NONBLOCK) {
                // Query database to see if step is waiting for lock
                PGresult *res = PQexecPrepared(conns[0].conn, PREP_WAITING, 1,
                                             &conns[step->session + 1].backend_pid_str,
                                             NULL, NULL, 0);
                bool waiting = (PQgetvalue(res, 0, 0)[0] == 't');
                PQclear(res);

                if (waiting) {
                    if (!(flags & STEP_RETRY))
                        printf("step %s: %s <waiting ...>\n", step->name, step->sql);
                    return true;
                }
            }

            // Handle timeouts and cancellation
            struct timeval current_time;
            gettimeofday(&current_time, NULL);
            int64 elapsed = (current_time.tv_sec - start_time.tv_sec) * USECS_PER_SEC +
                           (current_time.tv_usec - start_time.tv_usec);

            if (elapsed > max_step_wait && !canceled) {
                // Try to cancel the query
                PGcancelConn *cancel_conn = PQcancelCreate(conn);
                if (PQcancelBlocking(cancel_conn)) {
                    printf("isolationtester: canceling step %s\n", step->name);
                    canceled = true;
                }
                PQcancelFinish(cancel_conn);
            }

            if (elapsed > 2 * max_step_wait) {
                fprintf(stderr, "step %s timed out\n", step->name);
                exit(1);
            }
        } else if (ret > 0) {
            // Data available - consume input
            if (!PQconsumeInput(conn)) {
                fprintf(stderr, "PQconsumeInput failed\n");
                exit(1);
            }
        }
    }

    // Check for blocker conditions
    if (step_has_blocker(pstep)) {
        if (!(flags & STEP_RETRY))
            printf("step %s: %s <waiting ...>\n", step->name, step->sql);
        return true;
    }

    // Process query results
    if (flags & STEP_RETRY)
        printf("step %s: <... completed>\n", step->name);
    else
        printf("step %s: %s\n", step->name, step->sql);

    // Handle all result sets
    PGresult *res;
    while ((res = PQgetResult(conn))) {
        switch (PQresultStatus(res)) {
            case PGRES_TUPLES_OK:
                printResultSet(res);
                break;
            case PGRES_FATAL_ERROR:
                // Print error message
                printf("ERROR: %s\n", PQresultErrorMessage(res));
                break;
            // Handle other result types...
        }
        PQclear(res);
    }

    // Process NOTIFY messages
    PQconsumeInput(conn);
    PGnotify *notify;
    while ((notify = PQnotifies(conn)) != NULL) {
        printf("NOTIFY \"%s\" from session\n", notify->relname);
        PQfreemem(notify);
    }

    iconn->active_step = NULL;
    return false; // Step completed
}
```