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

## Simplified Source

```c
static void
printResults(StatsData *total,
             pg_time_usec_t total_duration,
             pg_time_usec_t conn_total_duration,
             pg_time_usec_t conn_elapsed_duration,
             int64 latency_late)
{
    int64 failures = getFailures(total);
    int64 total_cnt = total->cnt + total->skipped + failures;
    double bench_duration = PG_TIME_GET_DOUBLE(total_duration);
    double tps = total->cnt / bench_duration;

    // Print test configuration
    printf("transaction type: %s\n",
           num_scripts == 1 ? sql_script[0].desc : "multiple scripts");
    printf("scaling factor: %d\n", scale);

    if (partition_method != PART_NONE)
        printf("partition method: %s\npartitions: %d\n",
               PARTITION_METHOD[partition_method], partitions);

    printf("query mode: %s\n", QUERYMODE[querymode]);
    printf("number of clients: %d\n", nclients);
    printf("number of threads: %d\n", nthreads);

    // Print transaction counts and duration
    if (duration <= 0)
    {
        printf("number of transactions per client: %d\n", nxacts);
        printf("number of transactions actually processed: " INT64_FORMAT "/%d\n",
               total->cnt, nxacts * nclients);
    }
    else
    {
        printf("duration: %d s\n", duration);
        printf("number of transactions actually processed: " INT64_FORMAT "\n",
               total->cnt);
    }

    // Print failure statistics
    printf("number of failed transactions: " INT64_FORMAT " (%.3f%%)\n",
           failures, 100.0 * failures / total_cnt);

    if (failures_detailed)
    {
        printf("number of serialization failures: " INT64_FORMAT " (%.3f%%)\n",
               total->serialization_failures,
               100.0 * total->serialization_failures / total_cnt);
        printf("number of deadlock failures: " INT64_FORMAT " (%.3f%%)\n",
               total->deadlock_failures,
               100.0 * total->deadlock_failures / total_cnt);
    }

    // Print retry statistics if applicable
    if (max_tries != 1)
    {
        printf("number of transactions retried: " INT64_FORMAT " (%.3f%%)\n",
               total->retried, 100.0 * total->retried / total_cnt);
        printf("total number of retries: " INT64_FORMAT "\n", total->retries);
    }

    // Exit early if no transactions executed
    if (total->cnt + total->skipped <= 0)
        return;

    // Print latency statistics
    if (throttle_delay || progress || latency_limit)
        printSimpleStats("latency", &total->latency);
    else
        printf("latency average = %.3f ms%s\n",
               0.001 * total_duration * nclients / total_cnt,
               failures > 0 ? " (including failures)" : "");

    // Print connection and TPS information
    if (is_connect)
    {
        printf("average connection time = %.3f ms\n",
               0.001 * conn_total_duration / (total->cnt + failures));
        printf("tps = %f (including reconnection times)\n", tps);
    }
    else
    {
        printf("initial connection time = %.3f ms\n", 0.001 * conn_elapsed_duration);
        printf("tps = %f (without initial connection time)\n", tps);
    }

    // Print per-script and per-command statistics if enabled
    if (per_script_stats || report_per_command)
    {
        for (int i = 0; i < num_scripts; i++)
        {
            if (per_script_stats)
            {
                // Print script-level statistics
                StatsData *sstats = &sql_script[i].stats;
                printf("SQL script %d: %s\n", i + 1, sql_script[i].desc);
                printf(" - %" INT64_FORMAT " transactions (tps = %f)\n",
                       sstats->cnt, sstats->cnt / bench_duration);
                printSimpleStats(" - latency", &sstats->latency);
            }

            if (report_per_command)
            {
                // Print per-command statistics
                printf("statement latencies in milliseconds:\n");
                for (Command **commands = sql_script[i].commands;
                     *commands != NULL; commands++)
                {
                    SimpleStats *cstats = &(*commands)->stats;
                    printf("   %11.3f  %s\n",
                           (cstats->count > 0) ? 1000.0 * cstats->sum / cstats->count : 0.0,
                           (*commands)->first_line);
                }
            }
        }
    }
}
```