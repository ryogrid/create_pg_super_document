# advanceConnectionState

## Location
[src/bin/pgbench/pgbench.c:3602-4299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3602-L4299)

## Overview
The core state machine engine of pgbench that manages the complete lifecycle of client connections, transaction execution, error handling, and performance monitoring.

## Definition
static void advanceConnectionState(TState *thread, CState *st, StatsData *agg)

## Detailed Description
This function implements pgbench's sophisticated state machine that orchestrates all aspects of benchmarking execution for individual client connections. It operates as the central control mechanism that:

**Core State Management:**
- Manages 13 distinct connection states from script selection through completion
- Implements throttling mechanisms for rate-controlled benchmarking
- Handles both pipeline and non-pipeline query execution modes
- Coordinates transaction lifecycle management

**Transaction Processing:**
- Executes SQL commands and meta-commands within transaction boundaries
- Supports conditional command execution (if/elif/else/endif constructs)
- Manages prepared statement lifecycles and session state
- Handles both successful completion and error scenarios

**Error Recovery:**
- Implements sophisticated retry logic with configurable limits
- Performs automatic transaction rollbacks for failed transactions  
- Manages pipeline synchronization during error conditions
- Provides detailed error reporting and diagnostic information

**Performance Monitoring:**
- Tracks per-command and per-transaction latencies
- Implements latency limit enforcement with transaction skipping
- Monitors connection establishment and teardown times
- Accumulates statistics for final reporting

The function uses a continuous loop with state transitions, allowing non-blocking operation and efficient handling of multiple concurrent connections.

## Parameters / Member Variables
- : Pointer to TState structure containing thread-local data (connection statistics, throttling state, random number generators)
- : Pointer to CState structure representing individual client state (connection handle, current command, transaction state, error status)
- : Pointer to StatsData structure for accumulating performance statistics across all operations

## Dependencies
- Functions called/Symbols referenced:
  - [chooseScript](../c/chooseScript.md), doConnect, sendCommand, readCommandResponse
  - [executeMetaCommand](../e/executeMetaCommand.md), commandFailed, processXactStats
  - [discardUntilSync](../d/discardUntilSync.md), getTransactionStatus, doRetry
  - [printVerboseErrorMessages](../p/printVerboseErrorMessages.md), finishCon
  - [PQpipelineStatus](../P/PQpipelineStatus.md), PQsendQuery, PQisBusy, PQconsumeInput, PQgetResult
  - [pg_time_now_lazy](../p/pg_time_now_lazy.md), pg_time_now, getPoissonRand
  - conditional stack management functions
- Global variables referenced:
  - timer_exceeded, throttle_delay, latency_limit, max_tries
  - verbose_errors, report_per_command, is_connect
- Called from (representative examples):
  - [threadRun](../t/threadRun.md)

## Notes and Other Information
- Contains 697 lines of complex state machine logic handling 13 distinct states
- Critical for pgbench's performance and correctness - handles all execution scenarios
- Implements lazy timestamp evaluation to minimize gettimeofday() syscalls
- Supports both synchronous and pipelined query execution modes
- Essential for multi-client benchmarking coordination and resource management
- Uses assertions extensively to validate state transitions and maintain invariants
- Handles connection lifecycle management including establishment and cleanup
- Integrates with pgbench's comprehensive statistics collection and reporting system

## Simplified Source

```c
static void advanceConnectionState(TState *thread, CState *st, StatsData *agg)
{
    pg_time_usec_t now = 0;

    // Main state machine loop
    for (;;)
    {
        Command *command;

        switch (st->state)
        {
            case CSTATE_CHOOSE_SCRIPT:
                // Select transaction script to run
                st->use_file = chooseScript(thread);
                st->estatus = ESTATUS_NO_ERROR;
                st->tries = 1;

                st->state = timer_exceeded ? CSTATE_FINISHED :
                           throttle_delay > 0 ? CSTATE_PREPARE_THROTTLE : CSTATE_START_TX;
                break;

            case CSTATE_START_TX:
                // Start new transaction
                pg_time_now_lazy(&now);

                // Establish connection if needed
                if (st->con == NULL)
                {
                    if ((st->con = doConnect()) == NULL)
                    {
                        st->state = CSTATE_ABORTED;
                        break;
                    }
                }

                // Record transaction start time and setup
                st->random_state = st->cs_func_rs;
                st->txn_begin = now;
                if (!throttle_delay)
                    st->txn_scheduled = now;

                st->state = CSTATE_START_COMMAND;
                st->command = 0;
                break;

            case CSTATE_PREPARE_THROTTLE:
                // Setup throttling delay
                thread->throttle_trigger += getPoissonRand(&thread->ts_throttle_rs, throttle_delay);
                st->txn_scheduled = thread->throttle_trigger;

                // Skip transactions that would exceed latency limit
                if (latency_limit)
                {
                    pg_time_now_lazy(&now);
                    if (thread->throttle_trigger < now - latency_limit)
                    {
                        processXactStats(thread, st, &now, true, agg);
                        if (timer_exceeded || (nxacts > 0 && st->cnt >= nxacts))
                            st->state = CSTATE_FINISHED;
                        break;
                    }
                }

                st->state = end_time > 0 && st->txn_scheduled > end_time ?
                           CSTATE_FINISHED : CSTATE_THROTTLE;
                break;

            case CSTATE_THROTTLE:
                // Wait for throttling delay
                pg_time_now_lazy(&now);
                if (now < st->txn_scheduled)
                    return;

                st->state = timer_exceeded ? CSTATE_FINISHED : CSTATE_START_TX;
                break;

            case CSTATE_START_COMMAND:
                // Execute next command in script
                command = sql_script[st->use_file].commands[st->command];

                if (command == NULL)
                {
                    // End of script reached
                    st->state = PQpipelineStatus(st->con) == PQ_PIPELINE_OFF ?
                               CSTATE_END_TX : CSTATE_ABORTED;
                    break;
                }

                // Record command start time
                if (report_per_command)
                {
                    pg_time_now_lazy(&now);
                    st->stmt_begin = now;
                }

                // Execute SQL or meta command
                if (command->type == SQL_COMMAND)
                {
                    if (!sendCommand(st, command))
                    {
                        commandFailed(st, "SQL", "SQL command send failed");
                        st->state = CSTATE_ABORTED;
                    }
                    else
                    {
                        st->state = PQpipelineStatus(st->con) == PQ_PIPELINE_OFF ?
                                   CSTATE_WAIT_RESULT : CSTATE_END_COMMAND;
                    }
                }
                else if (command->type == META_COMMAND)
                {
                    st->state = executeMetaCommand(st, &now);
                    if (st->state == CSTATE_ABORTED)
                        st->estatus = ESTATUS_META_COMMAND_ERROR;
                }
                break;

            case CSTATE_SKIP_COMMAND:
                // Skip commands in inactive conditional branches
                while (true)
                {
                    command = sql_script[st->use_file].commands[st->command];

                    // Handle conditional meta commands
                    if (command->type == META_COMMAND &&
                        (command->meta == META_IF || command->meta == META_ELIF ||
                         command->meta == META_ELSE || command->meta == META_ENDIF))
                    {
                        // Update conditional stack state
                        // (simplified conditional logic)
                        st->command++;
                        if (conditional_active(st->cstack))
                        {
                            st->state = CSTATE_START_COMMAND;
                            break;
                        }
                    }
                    else
                    {
                        st->command++;
                    }
                }
                break;

            case CSTATE_WAIT_RESULT:
                // Wait for query results
                if (PQisBusy(st->con) && !PQconsumeInput(st->con))
                {
                    commandFailed(st, "SQL", "backend connection lost");
                    st->state = CSTATE_ABORTED;
                    break;
                }
                if (PQisBusy(st->con))
                    return;

                // Process query results
                if (readCommandResponse(st, command->meta, command->varprefix))
                {
                    st->state = PQpipelineStatus(st->con) != PQ_PIPELINE_ON ?
                               CSTATE_END_COMMAND : st->state;
                }
                else if (canRetryError(st->estatus))
                {
                    st->state = CSTATE_ERROR;
                }
                else
                {
                    st->state = CSTATE_ABORTED;
                }
                break;

            case CSTATE_SLEEP:
                // Sleep command delay
                pg_time_now_lazy(&now);
                if (now < st->sleep_until)
                    return;
                st->state = CSTATE_END_COMMAND;
                break;

            case CSTATE_END_COMMAND:
                // Command completed, record stats and advance
                if (report_per_command)
                {
                    pg_time_now_lazy(&now);
                    command = sql_script[st->use_file].commands[st->command];
                    addToSimpleStats(&command->stats, PG_TIME_GET_DOUBLE(now - st->stmt_begin));
                }

                st->command++;
                st->state = conditional_active(st->cstack) ?
                           CSTATE_START_COMMAND : CSTATE_SKIP_COMMAND;
                break;

            case CSTATE_ERROR:
                // Handle transaction errors
                conditional_stack_reset(st->cstack);

                // Handle pipeline cleanup
                if (PQpipelineStatus(st->con) != PQ_PIPELINE_OFF)
                {
                    if (!discardUntilSync(st))
                    {
                        st->state = CSTATE_ABORTED;
                        break;
                    }
                }

                // Check transaction status and rollback if needed
                TStatus tstatus = getTransactionStatus(st->con);
                if (tstatus == TSTATUS_IN_BLOCK)
                {
                    if (!PQsendQuery(st->con, "ROLLBACK"))
                        st->state = CSTATE_ABORTED;
                    else
                        st->state = CSTATE_WAIT_ROLLBACK_RESULT;
                }
                else if (tstatus == TSTATUS_IDLE)
                {
                    st->state = timer_exceeded ? CSTATE_FINISHED :
                               doRetry(st, &now) ? CSTATE_RETRY : CSTATE_FAILURE;
                }
                else
                {
                    st->state = CSTATE_ABORTED;
                }
                break;

            case CSTATE_WAIT_ROLLBACK_RESULT:
                // Wait for rollback completion
                if (!PQconsumeInput(st->con) || PQisBusy(st->con))
                {
                    if (!PQconsumeInput(st->con))
                        st->state = CSTATE_ABORTED;
                    return;
                }

                PGresult *res = PQgetResult(st->con);
                if (PQresultStatus(res) == PGRES_COMMAND_OK)
                {
                    PQclear(res);
                    PQgetResult(st->con); // consume NULL result
                    st->state = timer_exceeded ? CSTATE_FINISHED :
                               doRetry(st, &now) ? CSTATE_RETRY : CSTATE_FAILURE;
                }
                else
                {
                    PQclear(res);
                    st->state = CSTATE_ABORTED;
                }
                break;

            case CSTATE_RETRY:
                // Retry transaction after error
                if (verbose_errors)
                    printVerboseErrorMessages(st, &now, true);

                st->tries++;
                command->retries++;
                st->cs_func_rs = st->random_state;
                st->command = 0;
                st->estatus = ESTATUS_NO_ERROR;
                st->state = CSTATE_START_COMMAND;
                break;

            case CSTATE_FAILURE:
                // Record failed transaction
                command = sql_script[st->use_file].commands[st->command];
                command->failures++;

                if (verbose_errors)
                    printVerboseErrorMessages(st, &now, false);

                st->state = CSTATE_END_TX;
                break;

            case CSTATE_END_TX:
                // End transaction and process stats
                processXactStats(thread, st, &now, false, agg);

                // Verify transaction is properly closed
                tstatus = getTransactionStatus(st->con);
                if (tstatus != TSTATUS_IDLE)
                {
                    st->state = CSTATE_ABORTED;
                    break;
                }

                // Handle connection cleanup if needed
                if (is_connect)
                {
                    pg_time_usec_t start = now;
                    finishCon(st);
                    now = pg_time_now();
                    thread->conn_duration += now - start;
                }

                // Check for completion
                if ((st->cnt >= nxacts && duration <= 0) || timer_exceeded)
                {
                    st->state = CSTATE_FINISHED;
                    break;
                }

                st->state = CSTATE_CHOOSE_SCRIPT;
                return; // Allow other clients to run

            case CSTATE_ABORTED:
            case CSTATE_FINISHED:
                // Final cleanup
                finishCon(st);
                return;
        }
    }
}
```