# CState

## Location
[src/bin/pgbench/pgbench.c:640-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L640-L672)

## Overview
The CState structure represents the complete state of a single client connection in pgbench, encapsulating all information needed to track a client's execution progress, variables, timing, and error handling.

## Definition

```c
typedef struct
{
	int			tid;			/* thread id */
	THREAD_T	thread;			/* thread handle */
	CState	   *state;			/* array of CState */
	int			nstate;			/* length of state[] */

	/*
	 * Separate randomness for each thread. Each thread option uses its own
	 * random state to make all of them independent of each other and
	 * therefore deterministic at the thread level.
	 */
	pg_prng_state ts_choose_rs; /* random state for selecting a script */
	pg_prng_state ts_throttle_rs;	/* random state for transaction throttling */
	pg_prng_state ts_sample_rs; /* random state for log sampling */

	int64		throttle_trigger;	/* previous/next throttling (us) */
	FILE	   *logfile;		/* where to log, or NULL */

	/* per thread collected stats in microseconds */
	pg_time_usec_t create_time; /* thread creation time */
	pg_time_usec_t started_time;	/* thread is running */
	pg_time_usec_t bench_start; /* thread is benchmarking */
	pg_time_usec_t conn_duration;	/* cumulated connection and disconnection
									 * delays */

	StatsData	stats;
	int64		latency_late;	/* count executed but late transactions */
} TState;
```
## Detailed Description
The CState structure serves as the comprehensive state container for individual pgbench clients, implementing a complete client execution context that tracks database connections, script execution progress, variable storage, timing measurements, and error handling capabilities.

This structure is central to pgbench's multi-client architecture, where each client operates independently with its own connection, variable space, and execution state. The structure supports complex benchmarking scenarios including conditional execution, transaction retries, prepared statements, and detailed performance timing.

The state machine approach allows pgbench to handle asynchronous operations efficiently, with the ConnectionStateEnum tracking the current phase of execution while maintaining all necessary context for resuming operations after database interactions.

## Parameters / Member Variables
- `con`: PostgreSQL database connection handle for this client
- `id`: Unique identifier number for this client instance
- `state`: Current state in the client's state machine execution flow
- `cstack`: Stack for tracking nested conditional blocks in scripts
- `cs_func_rs`: Random number generator state for PGBENCH_RANDOM_* functions
- `use_file`: Index into the sql_script array indicating which script this client is executing
- `command`: Current command number within the active script
- `num_syncs`: Count of synchronous commands currently in progress
- `variables`: Client-specific variable storage container
- `txn_scheduled`: Microsecond timestamp when transaction was scheduled to start
- `sleep_until`: Microsecond timestamp when next command should begin
- `txn_begin`: Transaction start time for measuring schedule lag
- `stmt_begin`: Statement start time for measuring individual statement latencies
- `prepared`: 2D boolean array tracking which commands are prepared for each script
- `estatus`: Error status of the current transaction (ESTATUS_NO_ERROR if successful)
- `random_state`: Random number generator state for transaction retry logic
- `tries`: Number of retry attempts made for the current transaction
- `cnt`: Total transaction count for this client (includes skipped and failed transactions)

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