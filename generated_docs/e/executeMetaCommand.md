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