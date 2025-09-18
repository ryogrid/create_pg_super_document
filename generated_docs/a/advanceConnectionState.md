# advanceConnectionState

## Location
src/bin/pgbench/pgbench.c: 3602 - 4299

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