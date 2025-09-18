# doLog

## Location
src/bin/pgbench/pgbench.c: 4561 - 4680

## Overview
Prints log entries after completing transactions in pgbench, handling both aggregated and raw transaction logging with Unix-epoch timestamps.

## Definition
```c
static void doLog(TState *thread, CState *st, StatsData *agg, bool skipped, double latency, double lag)
```

## Detailed Description
This function handles transaction logging in pgbench, supporting both aggregated interval-based logging and raw per-transaction logging. It writes log entries with Unix-epoch timestamps for correlation with other logs. The function implements sampling-based logging when enabled, and handles different logging formats based on configuration options like throttle delay, retry settings, and detailed failure reporting. For aggregated logging, it processes time intervals and prints statistical summaries, while for raw logging it outputs individual transaction details.

## Parameters / Member Variables
- `thread`: Pointer to TState structure containing thread-specific information including the log file handle
- `st`: Pointer to CState structure representing the client state with transaction details
- `agg`: Pointer to StatsData structure for aggregated statistics (used in interval logging mode)
- `skipped`: Boolean flag indicating whether the transaction was skipped
- `latency`: Transaction latency in milliseconds
- `lag`: Transaction lag time (delay between intended and actual start time)

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_now (timestamp function)
  - pg_prng_double (random number generation)
  - getResultString (status string generation)
  - initStats (statistics initialization)
  - accumStats (statistics accumulation)
  - Various types: TState, CState, StatsData, pg_time_usec_t
  - Constants: INT64_FORMAT, INT64CONST, ESTATUS_NO_ERROR
- Called from (representative examples):
  - processXactStats (at src/bin/pgbench/pgbench.c:4709)
  - threadRun (at src/bin/pgbench/pgbench.c:7721)

## Notes and Other Information
- This is a static function, only accessible within pgbench.c
- Implements sampling-based logging when `sample_rate` is configured
- Handles two logging modes: aggregated (interval-based) and raw (per-transaction)
- Uses Unix-epoch timestamps for log correlation across different systems
- Includes conditional logging of throttle delay, retry information, and detailed failure types
- Automatically handles empty intervals in aggregated logging mode for low TPS scenarios
- The function contains a performance optimization note about potentially avoiding extra pg_time_now() calls