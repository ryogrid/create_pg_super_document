# test_timing

## Location
[src/bin/pg_test_timing/pg_test_timing.c:122-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_timing/pg_test_timing.c#L122-L181)

## Overview
The test_timing function performs a timing measurement loop to assess the resolution and accuracy of the system clock, collecting statistics about timing granularity for performance analysis.

## Definition

```c
static uint64
test_timing(unsigned int duration)
```
## Detailed Description
This function implements the core timing measurement logic for the pg_test_timing utility. It runs a continuous loop for the specified duration, repeatedly sampling the system clock to measure timing resolution and detect timing anomalies. The function maintains a histogram of timing differences between consecutive clock readings, categorized by the highest bit position of the time difference.

The function detects and reports clock irregularities, specifically when time appears to go backwards, which can indicate system clock issues or virtualization problems. It uses PostgreSQL's portable timing instrumentation macros (INSTR_TIME_*) for cross-platform compatibility. The collected timing data helps database administrators understand the timing characteristics of their system, which is crucial for PostgreSQL performance tuning.

## Parameters / Member Variables
- `duration`: Test duration in seconds (0 means run indefinitely until interrupted)
## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_SET_CURRENT (get current timestamp)
  - INSTR_TIME_GET_MICROSEC (extract microseconds from timestamp)
  - INSTR_TIME_SUBTRACT (compute time difference)
  - INSTR_TIME_GET_DOUBLE (convert timestamp to double)
  - INT64CONST (64-bit integer constant macro)
  - fprintf (error output)
  - printf (standard output)
  - exit (program termination)
- Called from:
  - [main](../m/main.md) (pg_test_timing.c:35)

## Dependencies on Global Variables
- : Global array for collecting timing difference statistics

## Notes and Other Information
- Returns the total number of timing loop iterations performed
- Uses a logarithmic histogram based on highest bit position of timing differences
- Detects and reports clock regression (time going backwards)
- Provides overhead calculation showing nanoseconds per loop iteration
- Function is static, indicating it's only used within the same compilation unit
- Uses PostgreSQL's portable timing infrastructure for cross-platform compatibility
- Critical for diagnosing timing-related performance issues in PostgreSQL deployments

## Simplified Source

```c
static uint64 test_timing(unsigned int duration)
{
    uint64 total_time, loop_count = 0;
    int64 time_elapsed = 0;
    uint64 prev, cur;
    instr_time start_time, end_time, temp;

    // Convert duration to microseconds
    total_time = duration > 0 ? duration * INT64CONST(1000000) : 0;

    // Initialize timing
    INSTR_TIME_SET_CURRENT(start_time);
    cur = INSTR_TIME_GET_MICROSEC(start_time);

    // Main timing loop
    while (time_elapsed < total_time) {
        int32 diff, bits = 0;

        // Get time difference from previous measurement
        prev = cur;
        INSTR_TIME_SET_CURRENT(temp);
        cur = INSTR_TIME_GET_MICROSEC(temp);
        diff = cur - prev;

        // Check for clock going backwards
        if (diff < 0) {
            fprintf(stderr, _("Detected clock going backwards in time.\n"));
            fprintf(stderr, _("Time warp: %d ms\n"), diff);
            exit(1);
        }

        // Calculate highest bit position for histogram
        while (diff) {
            diff >>= 1;
            bits++;
        }

        // Update timing histogram
        histogram[bits]++;

        // Update loop tracking
        loop_count++;
        INSTR_TIME_SUBTRACT(temp, start_time);
        time_elapsed = INSTR_TIME_GET_MICROSEC(temp);
    }

    // Calculate and display overhead
    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);

    printf(_("Per loop time including overhead: %0.2f ns\n"),
           INSTR_TIME_GET_DOUBLE(end_time) * 1e9 / loop_count);

    return loop_count;
}
```