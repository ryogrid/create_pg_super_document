# tadd

## Location
src/timezone/zic.c: 3772 - 3800

## Overview
A time-bounded addition function that performs overflow-safe arithmetic on zic_t values while clamping results to valid time range boundaries instead of failing on overflow.

## Definition
static zic_t tadd(zic_t t1, zic_t t2)

## Detailed Description
The tadd function provides safe addition of zic_t values with intelligent overflow handling that differs from oadd by clamping results to boundary values rather than terminating on overflow. This function is designed for time calculations where reaching the boundary values is acceptable behavior.

The function implements a sophisticated overflow detection and handling strategy:

1. **Negative t1 case**: 
   - Checks if t2 < min_time - t1 (potential underflow)
   - If t1 is not already min_time, calls time_overflow() to terminate
   - If t1 is min_time, returns min_time (clamping behavior)

2. **Non-negative t1 case**:
   - Checks if max_time - t1 < t2 (potential overflow)
   - If t1 is not already max_time, calls time_overflow() to terminate
   - If t1 is max_time, returns max_time (clamping behavior)

3. **Safe case**: Returns t1 + t2 when no overflow would occur

This design allows the function to gracefully handle boundary conditions while still protecting against genuine overflow situations where the inputs are not already at the boundary values.

## Parameters / Member Variables
- : First zic_t operand for the addition operation
- : Second zic_t operand for the addition operation

## Dependencies
- Functions called/Symbols referenced:
  - time_overflow (overflow error handler)
  - min_time (minimum valid time value)
  - max_time (maximum valid time value)
  - zic_t (timezone-specific integer type)
- Called from (representative examples):
  - getleapdatetime
  - writezone
  - years_of_observations
  - adjleap

## Notes and Other Information
- Part of the timezone compiler's time arithmetic infrastructure
- Differs from oadd by providing boundary clamping instead of strict overflow detection
- Uses global min_time and max_time constants to define the valid time range
- Provides more lenient overflow handling suitable for time boundary calculations
- Essential for timezone rule processing where time values may legitimately reach system limits
- The clamping behavior allows operations to continue when one operand is already at a boundary
- Still calls time_overflow() for genuine overflow cases to maintain data integrity
- Commonly used in contexts where time values may approach or reach system time limits