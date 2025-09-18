# dt2time

## Location
src/backend/utils/adt/timestamp.c: 1874 - 1900

## Overview
dt2time is a utility function that extracts time components (hours, minutes, seconds, and fractional seconds) from a Timestamp value.

## Definition
```c
void dt2time(Timestamp jd, int *hour, int *min, int *sec, fsec_t *fsec)
```

## Detailed Description
This function decomposes a Timestamp value into its constituent time components by performing successive division and modulo operations. It takes a Timestamp (which represents microseconds since the PostgreSQL epoch) and breaks it down into hours, minutes, seconds, and fractional seconds. The function works by successively dividing the input timestamp by the number of microseconds in each time unit, extracting each component and subtracting it from the remaining time value. This is a fundamental utility function used throughout PostgreSQL's datetime processing system.

## Parameters / Member Variables
- `jd`: The input Timestamp value to be decomposed into time components
- `hour`: Pointer to integer where the extracted hour value (0-23) will be stored
- `min`: Pointer to integer where the extracted minute value (0-59) will be stored  
- `sec`: Pointer to integer where the extracted second value (0-59) will be stored
- `fsec`: Pointer to fsec_t where the extracted fractional seconds (in microseconds) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp (PostgreSQL timestamp type)
  - TimeOffset (time offset type for calculations)
  - fsec_t (fractional seconds type)
  - USECS_PER_HOUR (microseconds per hour constant)
  - USECS_PER_MINUTE (microseconds per minute constant)
  - USECS_PER_SEC (microseconds per second constant)
- Called from (representative examples):
  - DecodeDateTime (datetime decoding functions)
  - DecodeTimeOnly (time-only decoding)
  - timestamp2tm (timestamp to broken-down time conversion)

## Notes and Other Information
- The function modifies the values pointed to by the output parameters
- Uses microsecond precision throughout the calculation
- Essential building block for timestamp formatting and conversion functions
- The algorithm works by successively extracting larger time units first (hours, then minutes, then seconds)
- Fractional seconds represent the remainder in microseconds after extracting whole seconds
- Used in both backend datetime processing and ECPG (Embedded SQL in C) interfaces
- The function name "dt2time" suggests "datetime to time" conversion
- No return value - all results are returned through pointer parameters