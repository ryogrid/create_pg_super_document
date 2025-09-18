# GetTimerFrequency

## Location
[src/include/portability/instr_time.h:148-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/portability/instr_time.h#L148-L155)

## Overview
A Windows-specific function that retrieves the frequency of the high-resolution performance counter, which is essential for converting performance counter ticks to time units in PostgreSQL timing measurements.

## Definition
```c
static inline double GetTimerFrequency(void)
```

## Detailed Description
This function provides the frequency information needed to convert Windows performance counter values to meaningful time units. It uses the QueryPerformanceFrequency() Windows API function to obtain the number of performance counter ticks per second. This frequency value is crucial for converting the raw counter values obtained by pg_query_performance_counter() into nanoseconds or other time units. The frequency is typically a large integer value (often in millions or billions) representing how many counter increments occur per second. By knowing this frequency, PostgreSQL can convert the raw performance counter ticks into standardized time measurements for consistent timing across different Windows systems.

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**: `double` representing the performance counter frequency in ticks per second

## Dependencies
- Functions called/Symbols referenced:
  - QueryPerformanceFrequency() (Windows API function)  
  - LARGE_INTEGER (Windows data type)
- Called from (representative examples):
  - INSTR_TIME_GET_NANOSEC (macro, Windows version)

## Notes and Other Information
- This function is Windows-specific and complements pg_query_performance_counter() for complete timing functionality
- It is marked as `static inline` for optimal performance since it may be called frequently in timing calculations
- The frequency value is system-dependent but remains constant for a given system boot session
- The frequency is used in the formula: `(ticks * NS_PER_S / GetTimerFrequency())` to convert counter ticks to nanoseconds
- QueryPerformanceFrequency() typically returns frequencies in the range of 1MHz to several GHz depending on the system hardware
- This function is part of PostgreSQL's portable timing infrastructure, providing the necessary conversion factor for Windows systems
- The return type is double to handle potential precision requirements in the conversion calculations