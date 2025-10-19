# executeMetaCommand

## Location
[src/bin/pgbench/pgbench.c:4300-4518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4300-L4518)

## Overview
Executes pgbench meta-commands (backslash commands) that provide scripting capabilities including variable assignment, conditional logic, shell execution, and pipeline control.

## Definition
static ConnectionStateEnum executeMetaCommand(CState *st, pg_time_usec_t *now)

## Detailed Description
This function serves as the execution engine for pgbench's meta-command system, which extends basic SQL execution with powerful scripting features. It processes backslash-prefixed commands that enable:

**Variable and Expression Management:**
- \set commands for variable assignment with expression evaluation
- Support for arithmetic, logical, and string operations
- Integration with pgbench's variable system for dynamic benchmarks

**Conditional Execution Logic:**
- \if/\elif/\else/\endif constructs for conditional command execution
- Boolean expression evaluation and branching logic
- Conditional stack management for nested conditionals

**System Integration:**
- \shell commands for external process execution
- \setshell commands for capturing shell output into variables
- Environment interaction capabilities

**Pipeline Mode Control:**
- \startpipeline, \syncpipeline, \endpipeline commands
- PostgreSQL pipeline protocol management
- Performance optimization for batch operations

**Timing Control:**
- \sleep commands with configurable delays
- Microsecond-precision sleep scheduling
- Integration with pgbench's timing infrastructure

The function includes comprehensive error handling, debug logging, and state management to ensure robust script execution within the broader benchmarking framework.

## Parameters / Member Variables
- : Pointer to CState structure containing client state (connection, variables, conditional stack, command position)
- : Pointer to pg_time_usec_t timestamp that is reset to 0 when commands might take significant time, enabling lazy timestamp evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [evaluateSleep](evaluateSleep.md), evaluateExpr, putVariableValue, valueTruth
  - [runShellCommand](../r/runShellCommand.md), prepareCommandsInPipeline, commandFailed
  - conditional stack functions (push, pop, peek, poke, empty)
  - [PQpipelineStatus](../P/PQpipelineStatus.md), PQenterPipelineMode, PQsendPipelineSync, PQpipelineSync
  - PQExpBuffer functions for debug logging
  - [pg_time_now_lazy](../p/pg_time_now_lazy.md)
- Types and enums referenced:
  - META_SLEEP, META_SET, META_IF, META_ELIF, META_ELSE, META_ENDIF
  - META_SETSHELL, META_SHELL, META_STARTPIPELINE, META_SYNCPIPELINE, META_ENDPIPELINE
  - IFSTATE_* conditional states, QUERY_SIMPLE, QUERY_PREPARED
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- Returns ConnectionStateEnum indicating next state: CSTATE_SLEEP, CSTATE_WAIT_RESULT, CSTATE_END_COMMAND, or CSTATE_ABORTED
- Critical for pgbench's advanced scripting capabilities beyond basic SQL execution
- Handles both immediate execution (variables, conditionals) and deferred execution (sleep, pipeline) commands
- Implements sophisticated pipeline mode validation and state management
- Essential for complex benchmark scenarios requiring programmatic control flow
- Provides detailed debug logging for troubleshooting script execution
- Resets timestamp after potentially time-consuming operations to maintain accurate performance measurements

## Simplified Source

```c
static ConnectionStateEnum executeMetaCommand(CState *st, pg_time_usec_t *now)
{
    Command *command = sql_script[st->use_file].commands[st->command];
    int argc = command->argc;
    char **argv = command->argv;

    // Debug logging for meta command execution
    if (unlikely(__pg_log_level <= PG_LOG_DEBUG))
    {
        PQExpBufferData buf;
        initPQExpBuffer(&buf);
        printfPQExpBuffer(&buf, "client %d executing \\%s", st->id, argv[0]);
        for (int i = 1; i < argc; i++)
            appendPQExpBuffer(&buf, " %s", argv[i]);
        pg_log_debug("%s", buf.data);
        termPQExpBuffer(&buf);
    }

    // Handle different meta command types
    if (command->meta == META_SLEEP)
    {
        // Sleep command: parse delay and schedule wake-up
        int usec;
        if (!evaluateSleep(&st->variables, argc, argv, &usec))
        {
            commandFailed(st, "sleep", "execution of meta-command failed");
            return CSTATE_ABORTED;
        }

        pg_time_now_lazy(now);
        st->sleep_until = (*now) + usec;
        return CSTATE_SLEEP;
    }
    else if (command->meta == META_SET)
    {
        // Variable assignment: evaluate expression and store result
        PgBenchExpr *expr = command->expr;
        PgBenchValue result;

        if (!evaluateExpr(st, expr, &result))
        {
            commandFailed(st, argv[0], "evaluation of meta-command failed");
            return CSTATE_ABORTED;
        }

        if (!putVariableValue(&st->variables, argv[0], argv[1], &result))
        {
            commandFailed(st, "set", "assignment of meta-command failed");
            return CSTATE_ABORTED;
        }
    }
    else if (command->meta == META_IF)
    {
        // Conditional: evaluate expression and push state to conditional stack
        PgBenchExpr *expr = command->expr;
        PgBenchValue result;

        if (!evaluateExpr(st, expr, &result))
        {
            commandFailed(st, argv[0], "evaluation of meta-command failed");
            return CSTATE_ABORTED;
        }

        bool cond = valueTruth(&result);
        conditional_stack_push(st->cstack, cond ? IFSTATE_TRUE : IFSTATE_FALSE);
    }
    else if (command->meta == META_ELIF)
    {
        // Else-if: handle conditional logic for alternative branches
        if (conditional_stack_peek(st->cstack) == IFSTATE_TRUE)
        {
            // Skip evaluation if previous condition was true
            conditional_stack_poke(st->cstack, IFSTATE_IGNORED);
            return CSTATE_END_COMMAND;
        }

        PgBenchExpr *expr = command->expr;
        PgBenchValue result;

        if (!evaluateExpr(st, expr, &result))
        {
            commandFailed(st, argv[0], "evaluation of meta-command failed");
            return CSTATE_ABORTED;
        }

        bool cond = valueTruth(&result);
        conditional_stack_poke(st->cstack, cond ? IFSTATE_TRUE : IFSTATE_FALSE);
    }
    else if (command->meta == META_ELSE)
    {
        // Else branch: flip conditional state
        switch (conditional_stack_peek(st->cstack))
        {
            case IFSTATE_TRUE:
                conditional_stack_poke(st->cstack, IFSTATE_ELSE_FALSE);
                break;
            default:
                Assert(false); // Invalid conditional state
        }
    }
    else if (command->meta == META_ENDIF)
    {
        // End conditional: pop from conditional stack
        Assert(!conditional_stack_empty(st->cstack));
        conditional_stack_pop(st->cstack);
    }
    else if (command->meta == META_SETSHELL)
    {
        // Shell command with output capture
        if (!runShellCommand(&st->variables, argv[1], argv + 2, argc - 2))
        {
            commandFailed(st, "setshell", "execution of meta-command failed");
            return CSTATE_ABORTED;
        }
    }
    else if (command->meta == META_SHELL)
    {
        // Shell command execution
        if (!runShellCommand(&st->variables, NULL, argv + 1, argc - 1))
        {
            commandFailed(st, "shell", "execution of meta-command failed");
            return CSTATE_ABORTED;
        }
    }
    else if (command->meta == META_STARTPIPELINE)
    {
        // Start pipeline mode
        if (querymode == QUERY_SIMPLE)
        {
            commandFailed(st, "startpipeline", "cannot use pipeline mode with simple query protocol");
            return CSTATE_ABORTED;
        }

        // Prepare commands for pipeline mode if using prepared statements
        if (querymode == QUERY_PREPARED)
            prepareCommandsInPipeline(st);

        if (PQpipelineStatus(st->con) != PQ_PIPELINE_OFF)
        {
            commandFailed(st, "startpipeline", "already in pipeline mode");
            return CSTATE_ABORTED;
        }

        if (PQenterPipelineMode(st->con) == 0)
        {
            commandFailed(st, "startpipeline", "failed to enter pipeline mode");
            return CSTATE_ABORTED;
        }
    }
    else if (command->meta == META_SYNCPIPELINE)
    {
        // Send pipeline synchronization point
        if (PQpipelineStatus(st->con) != PQ_PIPELINE_ON)
        {
            commandFailed(st, "syncpipeline", "not in pipeline mode");
            return CSTATE_ABORTED;
        }

        if (PQsendPipelineSync(st->con) == 0)
        {
            commandFailed(st, "syncpipeline", "failed to send pipeline sync");
            return CSTATE_ABORTED;
        }
        st->num_syncs++;
    }
    else if (command->meta == META_ENDPIPELINE)
    {
        // End pipeline mode
        if (PQpipelineStatus(st->con) != PQ_PIPELINE_ON)
        {
            commandFailed(st, "endpipeline", "not in pipeline mode");
            return CSTATE_ABORTED;
        }

        if (!PQpipelineSync(st->con))
        {
            commandFailed(st, "endpipeline", "failed to send pipeline sync");
            return CSTATE_ABORTED;
        }

        st->num_syncs++;
        return CSTATE_WAIT_RESULT; // Wait for sync response
    }

    // Reset timestamp after potentially time-consuming operations
    *now = 0;

    return CSTATE_END_COMMAND;
}
```