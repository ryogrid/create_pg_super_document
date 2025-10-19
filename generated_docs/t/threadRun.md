# threadRun

## Location
[src/bin/pgbench/pgbench.c:7431-7730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7431-L7730)

## Overview
The main worker thread function for pgbench that executes database transactions concurrently, manages connection states, and coordinates progress reporting across multiple client connections.

## Definition

```c
static THREAD_FUNC_RETURN_TYPE THREAD_FUNC_CC
threadRun(void *arg)
```
## Detailed Description
This function implements the core execution engine for pgbench's multi-threaded benchmark operations. Each thread manages multiple client connections (represented by CState structures) through a sophisticated state machine that handles connection establishment, transaction execution, result waiting, throttling, and error handling. The function uses an event-driven approach with socket polling to efficiently manage multiple concurrent database connections. It coordinates with other threads through barriers, handles progress reporting (only from thread 0), manages connection timeouts and retries, and maintains detailed statistics and optional transaction logging. The main execution loop continues until all client connections have completed or been aborted.

## Parameters / Member Variables
- `*arg`: Void pointer cast to TState structure containing thread-specific data including client states, thread ID, timing information, and logging configuration
## Dependencies
- Functions called/Symbols referenced:
  - [alloc_socket_set](../a/alloc_socket_set.md)/free_socket_set (socket set management)
  - [pg_time_now](../p/pg_time_now.md)/pg_time_now_lazy (timing functions)
  - doConnect (database connection establishment)
  - THREAD_BARRIER_WAIT (thread synchronization)
  - [advanceConnectionState](../a/advanceConnectionState.md) (state machine progression)
  - [wait_on_socket_set](../w/wait_on_socket_set.md)/socket_has_input (socket I/O operations)
  - [printProgressReport](../p/printProgressReport.md) (progress tracking)
  - [disconnect_all](../d/disconnect_all.md) (connection cleanup)
  - [doLog](../d/doLog.md) (transaction logging)
  - Various CSTATE_* constants (connection states)
- Called from (representative examples):
  - [main](../m/main.md) (in pgbench.c at lines 7359 and 7371)

## Notes and Other Information
- Handles multiple client connection states: CSTATE_CHOOSE_SCRIPT, CSTATE_WAIT_RESULT, CSTATE_SLEEP, CSTATE_THROTTLE, etc.
- Implements sophisticated socket polling with timeouts for efficient I/O multiplexing
- Only thread 0 performs progress reporting to avoid coordination overhead
- Supports both connection-per-transaction and persistent connection modes
- Handles graceful shutdown on EINTR signals and error conditions
- Manages transaction logging with configurable aggregation intervals
- Uses barrier synchronization for coordinated thread startup phases (READY, STEADY, GO)
- Exits early if --exit-on-abort option is used and any client encounters an error

## Simplified Source

```c
static THREAD_FUNC_RETURN_TYPE THREAD_FUNC_CC
threadRun(void *arg)
{
    TState *thread = (TState *) arg;
    CState *state = thread->state;
    int nstate = thread->nstate;
    int remains = nstate;  // number of remaining clients
    socket_set *sockets = alloc_socket_set(nstate);
    pg_time_usec_t start, thread_start;
    StatsData aggs;

    // Open log file if requested
    if (use_log)
    {
        char logpath[MAXPGPATH];
        char *prefix = logfile_prefix ? logfile_prefix : "pgbench_log";

        if (thread->tid == 0)
            snprintf(logpath, sizeof(logpath), "%s.%d", prefix, main_pid);
        else
            snprintf(logpath, sizeof(logpath), "%s.%d.%d", prefix, main_pid, thread->tid);

        thread->logfile = fopen(logpath, "w");
        if (thread->logfile == NULL)
            pg_fatal("could not open logfile \"%s\": %m", logpath);
    }

    // Initialize all client state machines
    for (int i = 0; i < nstate; i++)
        state[i].state = CSTATE_CHOOSE_SCRIPT;

    // Thread synchronization barriers
    THREAD_BARRIER_WAIT(&barrier);  // READY

    // Create initial connections if not in connect mode
    if (!is_connect)
    {
        for (int i = 0; i < nstate; i++)
        {
            if ((state[i].con = doConnect()) == NULL)
                pg_fatal("could not create connection for client %d", state[i].id);
        }
    }

    THREAD_BARRIER_WAIT(&barrier);  // GO

    start = pg_time_now();
    thread_start = start;
    initStats(&aggs, start);

    // Main execution loop - continue until all clients finish
    while (remains > 0)
    {
        pg_time_usec_t min_usec = PG_INT64_MAX;
        int nsocks = 0;

        // Check each client's state and determine what to wait for
        clear_socket_set(sockets);
        for (int i = 0; i < nstate; i++)
        {
            CState *st = &state[i];

            if (st->state == CSTATE_SLEEP || st->state == CSTATE_THROTTLE)
            {
                // Client is sleeping/throttled - calculate wake time
                pg_time_usec_t wake_time = (st->state == CSTATE_SLEEP) ?
                    st->sleep_until : st->txn_scheduled;
                pg_time_usec_t delay = wake_time - pg_time_now();
                if (min_usec > delay)
                    min_usec = delay;
            }
            else if (st->state == CSTATE_WAIT_RESULT ||
                     st->state == CSTATE_WAIT_ROLLBACK_RESULT)
            {
                // Client waiting for database response - add socket to set
                int sock = PQsocket(st->con);
                if (sock >= 0)
                    add_socket_to_set(sockets, sock, nsocks++);
            }
            else if (st->state != CSTATE_ABORTED && st->state != CSTATE_FINISHED)
            {
                // Client ready to work - don't wait
                min_usec = 0;
                break;
            }
        }

        // Wait for socket activity or timeout if needed
        if (min_usec > 0)
        {
            if (nsocks > 0)
                wait_on_socket_set(sockets, min_usec);
            else
                pg_usleep(min_usec);
        }

        // Advance state machine for each active client
        for (int i = 0; i < nstate; i++)
        {
            CState *st = &state[i];

            if (st->state == CSTATE_FINISHED || st->state == CSTATE_ABORTED)
                continue;

            if ((st->state == CSTATE_WAIT_RESULT ||
                 st->state == CSTATE_WAIT_ROLLBACK_RESULT) &&
                !socket_has_input(sockets, PQsocket(st->con), nsocks))
                continue;

            advanceConnectionState(thread, st, &aggs);

            // Check if client finished
            if (st->state == CSTATE_FINISHED || st->state == CSTATE_ABORTED)
                remains--;

            // Exit early if abort requested
            if (exit_on_abort && st->state == CSTATE_ABORTED)
                goto done;
        }

        // Thread 0 handles progress reporting
        if (progress && thread->tid == 0)
        {
            pg_time_usec_t now = pg_time_now();
            if (now >= next_report)
            {
                printProgressReport(thread, thread_start, now, &last, &last_report);
                next_report += (int64) 1000000 * progress;
            }
        }
    }

done:
    // Cleanup
    disconnect_all(state, nstate);
    if (thread->logfile)
    {
        fclose(thread->logfile);
        thread->logfile = NULL;
    }
    free_socket_set(sockets);
    THREAD_FUNC_RETURN;
}
```