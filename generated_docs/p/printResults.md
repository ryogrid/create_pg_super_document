printResults

## Overview
Generates and displays comprehensive benchmark results including transaction statistics, performance metrics, and detailed per-script/per-command breakdowns.

## Definition
static void printResults(StatsData *total, pg_time_usec_t total_duration, pg_time_usec_t conn_total_duration, pg_time_usec_t conn_elapsed_duration, int64 latency_late)

## Detailed Description
The printResults function produces a comprehensive final report of pgbench execution results. It displays test configuration parameters, transaction counts and rates, failure statistics, latency measurements, and optional detailed breakdowns by script and command. The function calculates transactions per second (TPS), failure percentages, and various timing statistics. It handles different reporting modes based on configuration flags like per-script stats, detailed failures, and connection modes. The output format adapts to show relevant information based on test parameters such as retry limits, throttling, and latency limits.

## Parameters / Member Variables
- total: Pointer to StatsData structure containing aggregated statistics from all threads
- total_duration: Total time spent executing transactions during the benchmark
- conn_total_duration: Total time spent on connections when using connect mode
- conn_elapsed_duration: Initial connection setup time when not using connect mode  
- latency_late: Count of transactions that exceeded the configured latency limit

## Dependencies
- Functions called/Symbols referenced:
  - [getFailures](../g/getFailures.md) - Calculates total failures from statistics
  - PG_TIME_GET_DOUBLE - Converts time to double precision
  - [printSimpleStats](printSimpleStats.md) - Displays formatted statistics for latency and other metrics
  - printf - Output formatting function
  - [StatsData](../S/StatsData.md) - Structure type for statistics data
  - [Command](../C/Command.md) - Structure type for individual SQL commands
  - SimpleStats - Structure type for basic statistical data
  - INT64_FORMAT - Platform-specific format string for 64-bit integers
- Called from (representative examples):
  - [main](../m/main.md) - Called at the end of benchmark execution to display final results

## Notes and Other Information
- Reports test parameters including scaling factor, client/thread counts, and execution mode
- Calculates and displays TPS based on actual executed transactions
- Shows failure statistics with percentages when failures occur
- Displays detailed failure breakdowns when failures_detailed is enabled
- Reports retry statistics when max_tries is greater than 1
- Shows latency statistics using either measured values or computed averages
- Provides per-script statistics when per_script_stats is enabled
- Displays per-command latencies and failure counts when report_per_command is enabled
- Handles different connection reporting modes for connect vs persistent connections
- Part of pgbench final reporting system providing comprehensive benchmark analysis