# pg_clock_gettime_ns

## Location
[src/include/portability/instr_time.h:111-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/portability/instr_time.h#L111-L121)

## Overview
A helper function that retrieves the current time using clock_gettime() and converts it to PostgreSQL's internal instr_time format for performance measurement purposes.

## Definition
```c
static inline instr_time pg_clock_gettime_ns(void)
```

## Detailed Description
This function provides a platform-specific implementation for getting high-resolution timestamps in PostgreSQL. It uses the POSIX clock_gettime() system call with the CLOCK_MONOTONIC_RAW clock source to obtain precise timing information. The function converts the timespec structure returned by clock_gettime() into PostgreSQL's internal instr_time representation, where time is stored as nanoseconds in a single 64-bit integer. The monotonic raw clock ensures that measurements are not affected by system clock adjustments or NTP corrections, making it ideal for performance instrumentation.

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**: `instr_time` structure containing the current timestamp in nanoseconds

## Dependencies
- Functions called/Symbols referenced:
  - clock_gettime (system call)
  - [instr_time](../i/instr_time.md) (PostgreSQL time structure)
  - PG_INSTR_CLOCK (defined as CLOCK_MONOTONIC_RAW)
  - NS_PER_S (nanoseconds per second constant)
- Called from (representative examples):
  - INSTR_TIME_SET_CURRENT (macro)

## Notes and Other Information
- This function is marked as `static inline` for optimal performance in timing-critical code paths
- It serves as a helper specifically for the INSTR_TIME_SET_CURRENT macro
- The use of CLOCK_MONOTONIC_RAW ensures consistent timing measurements unaffected by system clock adjustments
- The conversion formula combines seconds and nanoseconds: `tmp.tv_sec * NS_PER_S + tmp.tv_nsec`
- This is part of PostgreSQL's portable timing infrastructure, allowing consistent time measurements across different platforms