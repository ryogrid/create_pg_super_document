# CState

## Location
src/bin/pgbench/pgbench.c: 640 - 672

## Overview
The CState structure represents the complete state of a single client connection in pgbench, encapsulating all information needed to track a client's execution progress, variables, timing, and error handling.

## Definition


## Detailed Description
The CState structure serves as the comprehensive state container for individual pgbench clients, implementing a complete client execution context that tracks database connections, script execution progress, variable storage, timing measurements, and error handling capabilities.

This structure is central to pgbench's multi-client architecture, where each client operates independently with its own connection, variable space, and execution state. The structure supports complex benchmarking scenarios including conditional execution, transaction retries, prepared statements, and detailed performance timing.

The state machine approach allows pgbench to handle asynchronous operations efficiently, with the ConnectionStateEnum tracking the current phase of execution while maintaining all necessary context for resuming operations after database interactions.

## Parameters / Member Variables
- : PostgreSQL database connection handle for this client
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Unique identifier number for this client instance
- : Current state in the client's state machine execution flow
- : Stack for tracking nested conditional blocks in scripts
- : Random number generator state for PGBENCH_RANDOM_* functions
- : Index into the sql_script array indicating which script this client is executing
- : Current command number within the active script
- : Count of synchronous commands currently in progress
- : Client-specific variable storage container
- : Microsecond timestamp when transaction was scheduled to start
- : Microsecond timestamp when next command should begin
- : Transaction start time for measuring schedule lag
- : Statement start time for measuring individual statement latencies
- : 2D boolean array tracking which commands are prepared for each script
- : Error status of the current transaction (ESTATUS_NO_ERROR if successful)
- : Random number generator state for transaction retry logic
- : Number of retry attempts made for the current transaction
- : Total transaction count for this client (includes skipped and failed transactions)

## Dependencies
- Functions called/Symbols referenced:
  - PGconn (PostgreSQL connection type)
  - ConnectionStateEnum (state machine states)
  - [ConditionalStack](ConditionalStack.md) (conditional execution stack)
  - pg_prng_state (random number generator state)
  - [Variables](../V/Variables.md) (variable storage structure)
  - pg_time_usec_t (microsecond timestamp type)
  - EStatus (error status enumeration)
  - [StatsData](../S/StatsData.md) (statistics collection structure)
- Called from (representative examples):
  - evalLazyFunc (for lazy function evaluation)
  - evalStandardFunc (for standard function evaluation)
  - [commandFailed](../c/commandFailed.md) (for error handling)
  - [advanceConnectionState](../a/advanceConnectionState.md) (for state machine progression)
  - [executeMetaCommand](../e/executeMetaCommand.md) (for meta-command execution)
  - [threadRun](../t/threadRun.md) (for thread execution management)
  - [prepareCommand](../p/prepareCommand.md) (for statement preparation)
  - [sendCommand](../s/sendCommand.md) (for command transmission)
  - [readCommandResponse](../r/readCommandResponse.md) (for response processing)

## Notes and Other Information
- Located in src/bin/pgbench/pgbench.c at lines 596-640
- Core component of pgbench's multi-threaded client architecture
- Supports both synchronous and asynchronous database operations through state machine design
- Maintains separate random number generators for different purposes to ensure deterministic behavior
- Comprehensive timing infrastructure supports detailed latency measurements and performance analysis
- Error handling and retry mechanism allows robust transaction processing with serialization and deadlock recovery
- [Variable](../V/Variable.md) system provides per-client isolated variable spaces for complex benchmark scenarios
- Used extensively throughout pgbench for client management, script execution, and performance measurement