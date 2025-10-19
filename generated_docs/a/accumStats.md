# accumStats

## Location
[src/bin/pgbench/pgbench.c:1451-1499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1451-L1499)

## Overview
Accumulates one additional transaction item into the given stats object, handling successful transactions, retries, failures, and skipped transactions for pgbench performance measurements.

## Definition

```c
static void
accumStats(StatsData *stats, bool skipped, double lat, double lag,
		   EStatus estatus, int64 tries)
```
## Detailed Description
The  function is a core statistics collection function in pgbench that processes and records transaction execution results. It categorizes transactions based on their execution status and updates various performance counters accordingly. The function handles successful transactions by recording latency and lag statistics, tracks retry attempts, counts different types of failures (serialization errors, deadlocks), and maintains skipped transaction counts. This data is essential for pgbench's performance reporting and analysis capabilities.

## Parameters / Member Variables
- `*stats`: Pointer to StatsData structure where statistics will be accumulated
- `skipped`: Boolean flag indicating if the transaction was skipped (true) or executed (false)
- `lat`: Transaction latency in milliseconds for successful transactions
- `lag`: Schedule lag in milliseconds when throttle_delay is enabled
- `estatus`: Execution status enum indicating transaction outcome (success, serialization error, deadlock, etc.)
- `tries`: Number of attempts made to execute this transaction (including retries)
## Dependencies
- Functions called/Symbols referenced:
  - [addToSimpleStats](addToSimpleStats.md) (for latency and lag statistics)
  - [pg_fatal](../p/pg_fatal.md) (for internal error reporting)
- Data types used:
  - [StatsData](../S/StatsData.md) (statistics accumulation structure)
  - EStatus (execution status enumeration)
  - ESTATUS_NO_ERROR, ESTATUS_SERIALIZATION_ERROR, ESTATUS_DEADLOCK_ERROR (status constants)
- Called from (representative examples):
  - [doLog](../d/doLog.md) (transaction logging function)
  - [processXactStats](../p/processXactStats.md) (transaction statistics processing)

## Notes and Other Information
- The function only records latency statistics for non-skipped, successful transactions
- Retry statistics are tracked regardless of final transaction outcome
- Schedule lag is only recorded when throttle_delay is enabled
- Different failure types (serialization errors, deadlocks) are counted separately for detailed analysis
- Contains defensive programming with pg_fatal for unexpected error statuses
- Part of pgbench's comprehensive performance measurement system in src/bin/pgbench/pgbench.c:1451-1499

## Simplified Source

```c
static void accumStats(StatsData *stats, bool skipped, double lat, double lag,
                       EStatus estatus, int64 tries) {
    // Handle skipped transactions
    if (skipped) {
        stats->skipped++;
        return;
    }

    // Track retry attempts (tries > 1 means retries occurred)
    if (tries > 1) {
        stats->retries += (tries - 1);
        stats->retried++;
    }

    // Process based on transaction status
    switch (estatus) {
        case ESTATUS_NO_ERROR:
            // Successful transaction: count and record timing
            stats->cnt++;
            addToSimpleStats(&stats->latency, lat);

            // Record lag if throttling is enabled
            if (throttle_delay)
                addToSimpleStats(&stats->lag, lag);
            break;

        case ESTATUS_SERIALIZATION_ERROR:
            stats->serialization_failures++;
            break;

        case ESTATUS_DEADLOCK_ERROR:
            stats->deadlock_failures++;
            break;

        default:
            pg_fatal("unexpected error status: %d", estatus);
    }
}
```