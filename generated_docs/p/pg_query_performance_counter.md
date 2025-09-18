# pg_query_performance_counter

## Location
src/include/portability/instr_time.h: 136 - 147

## Overview
A Windows-specific helper function that retrieves high-resolution performance counter values using the Windows QueryPerformanceCounter() API for precise timing measurements in PostgreSQL.

## Definition
```c
static inline instr_time pg_query_performance_counter(void)
```

## Detailed Description
This function provides a Windows-specific implementation for obtaining high-resolution timestamps in PostgreSQL. It uses the Windows QueryPerformanceCounter() API to retrieve precise timing information from the system's performance counter. The QueryPerformanceCounter() function returns a 64-bit value that represents the current value of a high-resolution performance counter. This counter is ideal for measuring elapsed time with high precision and is not affected by system clock adjustments. The function stores the counter value directly in PostgreSQL's instr_time structure for use in performance instrumentation and timing measurements.

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**: `instr_time` structure containing the performance counter value

## Dependencies
- Functions called/Symbols referenced:
  - QueryPerformanceCounter() (Windows API function)
  - instr_time (PostgreSQL time structure)
  - LARGE_INTEGER (Windows data type)
- Called from (representative examples):
  - INSTR_TIME_SET_CURRENT (macro, Windows version)

## Notes and Other Information
- This function is Windows-specific and is used as an alternative to pg_clock_gettime_ns() on non-POSIX systems
- It is marked as `static inline` for optimal performance in timing-critical code paths
- The function serves as a helper specifically for the Windows version of the INSTR_TIME_SET_CURRENT macro
- QueryPerformanceCounter() provides microsecond-level precision on most modern Windows systems
- The counter value is stored directly in the ticks field without unit conversion, as the frequency is handled separately by GetTimerFrequency()
- This is part of PostgreSQL's portable timing infrastructure, ensuring consistent timing behavior across different platforms including Windows