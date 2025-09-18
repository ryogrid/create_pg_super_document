# threadRun

## Location
src/bin/pgbench/pgbench.c: 7431 - 7730

## Overview
The main worker thread function for pgbench that executes database transactions concurrently, manages connection states, and coordinates progress reporting across multiple client connections.

## Definition


## Detailed Description
This function implements the core execution engine for pgbench's multi-threaded benchmark operations. Each thread manages multiple client connections (represented by CState structures) through a sophisticated state machine that handles connection establishment, transaction execution, result waiting, throttling, and error handling. The function uses an event-driven approach with socket polling to efficiently manage multiple concurrent database connections. It coordinates with other threads through barriers, handles progress reporting (only from thread 0), manages connection timeouts and retries, and maintains detailed statistics and optional transaction logging. The main execution loop continues until all client connections have completed or been aborted.

## Parameters / Member Variables
- : Void pointer cast to TState structure containing thread-specific data including client states, thread ID, timing information, and logging configuration

## Dependencies
- Functions called/Symbols referenced:
  - alloc_socket_set/free_socket_set (socket set management)
  - pg_time_now/pg_time_now_lazy (timing functions)
  - doConnect (database connection establishment)
  - THREAD_BARRIER_WAIT (thread synchronization)
  - advanceConnectionState (state machine progression)
  - wait_on_socket_set/socket_has_input (socket I/O operations)
  - printProgressReport (progress tracking)
  - disconnect_all (connection cleanup)
  - doLog (transaction logging)
  - Various CSTATE_* constants (connection states)
- Called from (representative examples):
  - main (in pgbench.c at lines 7359 and 7371)

## Notes and Other Information
- Handles multiple client connection states: CSTATE_CHOOSE_SCRIPT, CSTATE_WAIT_RESULT, CSTATE_SLEEP, CSTATE_THROTTLE, etc.
- Implements sophisticated socket polling with timeouts for efficient I/O multiplexing
- Only thread 0 performs progress reporting to avoid coordination overhead
- Supports both connection-per-transaction and persistent connection modes
- Handles graceful shutdown on EINTR signals and error conditions
- Manages transaction logging with configurable aggregation intervals
- Uses barrier synchronization for coordinated thread startup phases (READY, STEADY, GO)
- Exits early if --exit-on-abort option is used and any client encounters an error