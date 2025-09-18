# StatsData

## Location
src/bin/pgbench/pgbench.c: 376 - 443

## Overview
The StatsData structure is a comprehensive statistics container in pgbench that tracks various transaction execution metrics including successes, failures, retries, and timing information for performance analysis.

## Definition
```c
typedef struct StatsData
{
    pg_time_usec_t start_time;    /* interval start time, for aggregates */
    
    int64    cnt;                 /* number of successful transactions */
    int64    skipped;             /* number of transactions skipped under --rate and --latency-limit */
    int64    retries;             /* number of retries after serialization or deadlock errors */
    int64    retried;             /* number of all transactions that were retried */
    int64    serialization_failures; /* transactions not successfully retried after serialization error */
    int64    deadlock_failures;   /* transactions not successfully retried after deadlock error */
    SimpleStats latency;
    SimpleStats lag;
} StatsData;
```

## Detailed Description
This structure serves as the central repository for all transaction statistics in pgbench. It implements a detailed categorization system that tracks transactions through their complete lifecycle - from initiation through potential retries to final success or failure. The structure supports pgbench's advanced features like rate limiting, latency limits, and retry mechanisms, providing comprehensive metrics for performance analysis and benchmarking accuracy.

## Parameters / Member Variables
- `start_time`: Timestamp marking the beginning of the measurement interval, used for calculating aggregate statistics
- `cnt`: Count of successfully completed transactions (excluding skipped transactions)
- `skipped`: Number of transactions that were skipped due to rate limiting (--rate) or latency constraints (--latency-limit)
- `retries`: Total number of retry attempts across all transactions that experienced serialization or deadlock errors
- `retried`: Count of transactions that required at least one retry attempt (regardless of ultimate success or failure)
- `serialization_failures`: Number of transactions that failed due to serialization errors and could not be successfully retried
- `deadlock_failures`: Number of transactions that failed due to deadlock errors and could not be successfully retried
- `latency`: Statistical data for transaction latency measurements
- `lag`: Statistical data for transaction lag measurements

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_usec_t
  - SimpleStats
- Called from (representative examples):
  - [CState](../C/CState.md)
  - [ParsedScript](../P/ParsedScript.md)
  - [initStats](../i/initStats.md)
  - [accumStats](../a/accumStats.md)
  - [advanceConnectionState](../a/advanceConnectionState.md)
  - [getFailures](../g/getFailures.md)
  - [doLog](../d/doLog.md)
  - [processXactStats](../p/processXactStats.md)
  - [printProgressReport](../p/printProgressReport.md)
  - [printResults](../p/printResults.md)
  - [threadRun](../t/threadRun.md)

## Notes and Other Information
The structure implements a sophisticated transaction accounting model that distinguishes between different types of transaction outcomes. This granular tracking enables accurate performance reporting even in complex scenarios involving connection failures, serialization conflicts, and retry logic. The statistics are designed to be aggregated across multiple threads and time intervals for comprehensive benchmark reporting.