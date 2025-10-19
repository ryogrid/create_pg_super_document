# initSimpleStats

## Location
[src/bin/pgbench/pgbench.c:1394-1402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1394-L1402)

## Overview
Initializes a SimpleStats structure to all zero values, preparing it for statistical data collection in pgbench.

## Definition
```c
static void initSimpleStats(SimpleStats *ss)
```

## Detailed Description
This function performs a simple initialization of a SimpleStats structure by zeroing out all its fields using memset. The SimpleStats structure is used throughout pgbench to collect statistical information about various operations (execution times, latencies, etc.). By zeroing all fields, it ensures that:
- count starts at 0 (no values recorded yet)
- min, max, sum, and sum2 are all initialized to 0.0

This provides a clean starting state for subsequent statistical data accumulation operations.

## Parameters / Member Variables
- `ss`: Pointer to the SimpleStats structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - SimpleStats (structure type)
  - memset (C library function)
- Called from (representative examples):
  - [initStats](initStats.md)
  - [create_sql_command](../c/create_sql_command.md) 
  - [process_backslash_command](../p/process_backslash_command.md)

## Notes and Other Information
- Simple wrapper around memset for type safety and code clarity
- Part of pgbench's statistical tracking infrastructure
- The SimpleStats structure contains: count (int64), min/max/sum/sum2 (double)
- Located in src/bin/pgbench/pgbench.c:1394-1402

## Simplified Source

```c
static void initSimpleStats(SimpleStats *ss) {
    // Initialize all fields to zero: count, min, max, sum, sum2
    memset(ss, 0, sizeof(SimpleStats));
}
```