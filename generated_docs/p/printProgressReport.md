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