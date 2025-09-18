# initStats

## Location
src/bin/pgbench/pgbench.c: 1434 - 1450

## Overview
Initializes a StatsData structure to default values with a specified start time, preparing it for collecting comprehensive pgbench performance statistics.

## Definition
```c
static void initStats(StatsData *sd, pg_time_usec_t start)
```

## Detailed Description
This function initializes a StatsData structure, which is the main container for pgbench's comprehensive performance statistics. It sets the start time to the provided value and zeros out all transaction counters and failure tracking fields. Additionally, it initializes the embedded SimpleStats structures for latency and lag measurements.

The StatsData structure tracks:
- Transaction counts (successful, skipped, retried)
- Various failure types (serialization failures, deadlock failures)  
- Retry statistics
- Latency and lag measurements via embedded SimpleStats

This initialization ensures all counters start from zero and timing measurements begin from the specified start time.

## Parameters / Member Variables
- `sd`: Pointer to the StatsData structure to initialize
- `start`: Start time in microseconds (pg_time_usec_t) to mark the beginning of the measurement interval

## Dependencies
- Functions called/Symbols referenced:
  - StatsData (structure type)
  - pg_time_usec_t (time type)
  - initSimpleStats (called twice for latency and lag fields)
- Called from (representative examples):
  - doLog
  - printProgressReport
  - main
  - threadRun

## Notes and Other Information
- Part of pgbench's comprehensive statistics tracking system
- Initializes both simple counters and complex SimpleStats structures  
- The StatsData structure contains detailed transaction lifecycle tracking including retries and various failure modes
- Essential for multi-threaded benchmark coordination and progress reporting
- Located in src/bin/pgbench/pgbench.c:1434-1450