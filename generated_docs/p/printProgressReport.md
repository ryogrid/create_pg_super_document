printProgressReport

## Overview
Generates and displays real-time progress reports during pgbench execution showing transaction statistics and performance metrics.

## Definition
static void printProgressReport(TState *threads, int64 test_start, pg_time_usec_t now, StatsData *last, int64 *last_report)

## Detailed Description
The printProgressReport function collects statistics from all active worker threads and displays a comprehensive progress report to stderr during benchmark execution. It aggregates transaction counts, latencies, failures, and other metrics from all threads to provide real-time feedback on benchmark performance. The function calculates rates like transactions per second (TPS), average latency, standard deviation, and failure counts since the last report. It handles both timestamp-based and elapsed-time-based progress reporting formats.

## Parameters / Member Variables
- threads: Array of TState structures containing per-thread statistics and state information
- test_start: Start time of the benchmark test in microseconds
- now: Current time in microseconds when the report is being generated
- last: Pointer to StatsData structure containing statistics from the previous report (updated on exit)
- last_report: Pointer to timestamp of the last report (updated on exit)

## Dependencies
- Functions called/Symbols referenced:
  - [initStats](../i/initStats.md) - Initializes statistics structure
  - [mergeSimpleStats](../m/mergeSimpleStats.md) - Merges statistics from multiple threads
  - [getFailures](../g/getFailures.md) - Calculates total failure count from statistics
  - PG_TIME_GET_DOUBLE - Converts time to double precision
  - snprintf - [String](../S/String.md) formatting
  - fprintf - Output formatting to stderr
- Called from (representative examples):
  - [threadRun](../t/threadRun.md) - Main thread execution function that calls this for periodic progress reporting

## Notes and Other Information
- Statistics aggregation is performed without locking, so values may not be perfectly atomic but are sufficient for progress reporting purposes
- Displays different metrics based on configuration options like throttle_delay and max_tries
- Calculates transactions per second, latency statistics with standard deviation, and failure counts
- Updates the last statistics and timestamp parameters for the next report cycle
- Progress can be displayed as either elapsed time or absolute timestamps based on progress_timestamp setting
- Part of pgbench monitoring and reporting system for long-running benchmark tests

## Simplified Source

```c
static void
printProgressReport(TState *threads, int64 test_start, pg_time_usec_t now,
                    StatsData *last, int64 *last_report)
{
    pg_time_usec_t run = now - *last_report;
    StatsData cur;
    char tbuf[315];

    // Aggregate statistics from all threads
    initStats(&cur, 0);
    for (int i = 0; i < nthreads; i++)
    {
        mergeSimpleStats(&cur.latency, &threads[i].stats.latency);
        mergeSimpleStats(&cur.lag, &threads[i].stats.lag);
        cur.cnt += threads[i].stats.cnt;
        cur.skipped += threads[i].stats.skipped;
        cur.retries += threads[i].stats.retries;
        cur.retried += threads[i].stats.retried;
        cur.serialization_failures += threads[i].stats.serialization_failures;
        cur.deadlock_failures += threads[i].stats.deadlock_failures;
    }

    // Calculate metrics for this reporting period
    int64 cnt = cur.cnt - last->cnt;
    double total_run = (now - test_start) / 1000000.0;
    double tps = 1000000.0 * cnt / run;

    double latency = 0, stdev = 0, lag = 0;
    if (cnt > 0)
    {
        latency = 0.001 * (cur.latency.sum - last->latency.sum) / cnt;
        double sqlat = 1.0 * (cur.latency.sum2 - last->latency.sum2) / cnt;
        stdev = 0.001 * sqrt(sqlat - 1000000.0 * latency * latency);
        lag = 0.001 * (cur.lag.sum - last->lag.sum) / cnt;
    }

    int64 failures = getFailures(&cur) - getFailures(last);
    int64 retried = cur.retried - last->retried;

    // Format timestamp
    if (progress_timestamp)
        snprintf(tbuf, sizeof(tbuf), "%.3f s", PG_TIME_GET_DOUBLE(now + epoch_shift));
    else
        snprintf(tbuf, sizeof(tbuf), "%.1f s", total_run);

    // Print progress report
    fprintf(stderr, "progress: %s, %.1f tps, lat %.3f ms stddev %.3f, " INT64_FORMAT " failed",
            tbuf, tps, latency, stdev, failures);

    // Add optional metrics
    if (throttle_delay)
    {
        fprintf(stderr, ", lag %.3f ms", lag);
        if (latency_limit)
            fprintf(stderr, ", " INT64_FORMAT " skipped", cur.skipped - last->skipped);
    }

    if (max_tries != 1)
        fprintf(stderr, ", " INT64_FORMAT " retried, " INT64_FORMAT " retries",
                retried, cur.retries - last->retries);

    fprintf(stderr, "\n");

    // Update state for next report
    *last = cur;
    *last_report = now;
}
```