# accumStats

## Location
src/bin/pgbench/pgbench.c: 1451 - 1499

## Overview
Accumulates one additional transaction item into the given stats object, handling successful transactions, retries, failures, and skipped transactions for pgbench performance measurements.

## Definition


## Detailed Description
The  function is a core statistics collection function in pgbench that processes and records transaction execution results. It categorizes transactions based on their execution status and updates various performance counters accordingly. The function handles successful transactions by recording latency and lag statistics, tracks retry attempts, counts different types of failures (serialization errors, deadlocks), and maintains skipped transaction counts. This data is essential for pgbench's performance reporting and analysis capabilities.

## Parameters / Member Variables
- : Pointer to StatsData structure where statistics will be accumulated
- : Boolean flag indicating if the transaction was skipped (true) or executed (false)
- : Transaction latency in milliseconds for successful transactions
- : Schedule lag in milliseconds when throttle_delay is enabled
- : Execution status enum indicating transaction outcome (success, serialization error, deadlock, etc.)
- : Number of attempts made to execute this transaction (including retries)

## Dependencies
- Functions called/Symbols referenced:
  - addToSimpleStats (for latency and lag statistics)
  - pg_fatal (for internal error reporting)
- Data types used:
  - StatsData (statistics accumulation structure)
  - EStatus (execution status enumeration)
  - ESTATUS_NO_ERROR, ESTATUS_SERIALIZATION_ERROR, ESTATUS_DEADLOCK_ERROR (status constants)
- Called from (representative examples):
  - doLog (transaction logging function)
  - processXactStats (transaction statistics processing)

## Notes and Other Information
- The function only records latency statistics for non-skipped, successful transactions
- Retry statistics are tracked regardless of final transaction outcome
- Schedule lag is only recorded when throttle_delay is enabled
- Different failure types (serialization errors, deadlocks) are counted separately for detailed analysis
- Contains defensive programming with pg_fatal for unexpected error statuses
- Part of pgbench's comprehensive performance measurement system in src/bin/pgbench/pgbench.c:1451-1499