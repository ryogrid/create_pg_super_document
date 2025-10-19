# print_elapse

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:630-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L630-L641)

## Overview
A utility function in pg_test_fsync that calculates and displays performance statistics for filesystem synchronization operations, showing operations per second and average operation time.

## Definition

```c
static void
print_elapse(struct timeval start_t, struct timeval stop_t, int ops)
```
## Detailed Description
The print_elapse function performs timing analysis for filesystem synchronization benchmarking in the pg_test_fsync utility. It calculates the elapsed time between two timestamp measurements, computes performance metrics including operations per second and average operation time in microseconds, then formats and displays the results. The function is essential for providing meaningful performance feedback during PostgreSQL's filesystem synchronization method testing, helping users understand the relative performance characteristics of different sync methods on their system.

## Parameters / Member Variables
- `start_t`: struct timeval containing the start timestamp of the measured operation
- `stop_t`: struct timeval containing the end timestamp of the measured operation
- `ops`: Integer count of operations performed during the measured time period
## Dependencies
- Functions called/Symbols referenced:
  - USECS_SEC (constant for microseconds per second conversion)
  - OPS_FORMAT (format string macro for output display)
  - printf (standard output function)
- Called from (representative examples):
  - STOP_TIMER macro (performance measurement completion)

## Notes and Other Information
- Calculates total elapsed time by combining seconds and microseconds from timeval structures
- Uses floating-point arithmetic for precise performance calculations
- Results are displayed using internationalization support via gettext (_() macro)
- Critical component of pg_test_fsync's benchmarking capabilities
- Provides both throughput (ops/sec) and latency (avg time per op) metrics
- Handles microsecond precision timing for accurate performance measurement

## Simplified Source

```c
static void
print_elapse(struct timeval start_t, struct timeval stop_t, int ops)
{
    // Calculate total elapsed time in seconds
    double total_time = (stop_t.tv_sec - start_t.tv_sec) +
                       (stop_t.tv_usec - start_t.tv_usec) * 0.000001;

    // Calculate performance metrics
    double per_second = ops / total_time;
    double avg_op_time_us = (total_time / ops) * USECS_SEC;

    // Display results
    printf(_(OPS_FORMAT), per_second, avg_op_time_us);
}
```