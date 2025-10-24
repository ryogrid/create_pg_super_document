# doLog

## Location
[src/bin/pgbench/pgbench.c:4561-4680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4561-L4680)

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
  - [pg_time_now](../p/pg_time_now.md) (timestamp function)
  - [pg_prng_double](../p/pg_prng_double.md) (random number generation)
  - [getResultString](../g/getResultString.md) (status string generation)
  - [initStats](../i/initStats.md) (statistics initialization)
  - [accumStats](../a/accumStats.md) (statistics accumulation)
  - Various types: TState, CState, StatsData, pg_time_usec_t
  - Constants: INT64_FORMAT, INT64CONST, ESTATUS_NO_ERROR
- Called from (representative examples):
  - [processXactStats](../p/processXactStats.md) (at src/bin/pgbench/pgbench.c:4709)
  - [threadRun](../t/threadRun.md) (at src/bin/pgbench/pgbench.c:7721)

## Notes and Other Information
- This is a static function, only accessible within pgbench.c
- Implements sampling-based logging when `sample_rate` is configured
- Handles two logging modes: aggregated (interval-based) and raw (per-transaction)
- Uses Unix-epoch timestamps for log correlation across different systems
- Includes conditional logging of throttle delay, retry information, and detailed failure types
- Automatically handles empty intervals in aggregated logging mode for low TPS scenarios
- The function contains a performance optimization note about potentially avoiding extra pg_time_now() calls

## Simplified Source

```c
static void doLog(TState *thread, CState *st, StatsData *agg, bool skipped, double latency, double lag) {
    FILE *logfile = thread->logfile;
    pg_time_usec_t now = pg_time_now() + epoch_shift;

    Assert(use_log);

    // Skip log entry if sampling is enabled and this row doesn't belong to sample
    if (sample_rate != 0.0 && pg_prng_double(&thread->ts_sample_rs) > sample_rate)
        return;

    // Handle aggregated logging (interval-based)
    if (agg_interval > 0) {
        pg_time_usec_t next;

        // Process all completed intervals
        while ((next = agg->start_time + agg_interval * INT64CONST(1000000)) <= now) {
            // Print aggregated statistics for this interval
            fprintf(logfile, INT64_FORMAT " " INT64_FORMAT " %.0f %.0f %.0f %.0f",
                   agg->start_time / 1000000,    // Unix epoch seconds
                   agg->cnt,                     // transaction count
                   agg->latency.sum,            // latency statistics
                   agg->latency.sum2,
                   agg->latency.min,
                   agg->latency.max);

            // Add lag statistics if throttling is enabled
            if (throttle_delay) {
                fprintf(logfile, " %.0f %.0f %.0f %.0f",
                       agg->lag.sum, agg->lag.sum2, agg->lag.min, agg->lag.max);
            }

            // Add skipped count if latency limit is set
            if (latency_limit)
                fprintf(logfile, " " INT64_FORMAT, agg->skipped);

            // Add retry statistics if retries are enabled
            if (max_tries != 1)
                fprintf(logfile, " " INT64_FORMAT " " INT64_FORMAT, agg->retried, agg->retries);

            // Add detailed failure statistics if enabled
            if (failures_detailed) {
                fprintf(logfile, " " INT64_FORMAT " " INT64_FORMAT,
                       agg->serialization_failures, agg->deadlock_failures);
            }

            fputc('\n', logfile);

            // Move to next interval
            initStats(agg, next);
        }

        // Accumulate current transaction into aggregated stats
        accumStats(agg, skipped, latency, lag, st->estatus, st->tries);
    } else {
        // Handle raw transaction logging
        if (!skipped && st->estatus == ESTATUS_NO_ERROR) {
            // Log successful transaction
            fprintf(logfile, "%d " INT64_FORMAT " %.0f %d " INT64_FORMAT " " INT64_FORMAT,
                   st->id, st->cnt, latency, st->use_file,
                   now / 1000000, now % 1000000);
        } else {
            // Log failed/skipped transaction
            fprintf(logfile, "%d " INT64_FORMAT " %s %d " INT64_FORMAT " " INT64_FORMAT,
                   st->id, st->cnt, getResultString(skipped, st->estatus),
                   st->use_file, now / 1000000, now % 1000000);
        }

        // Add optional fields
        if (throttle_delay)
            fprintf(logfile, " %.0f", lag);
        if (max_tries != 1)
            fprintf(logfile, " %u", st->tries - 1);

        fputc('\n', logfile);
    }
}
```