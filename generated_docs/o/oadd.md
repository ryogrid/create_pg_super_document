# oadd

## Location
[src/timezone/zic.c:3764-3771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3764-L3771)

## Overview
An overflow-safe addition function that performs arithmetic on zic_t values while detecting and preventing integer overflow conditions.

## Definition
static zic_t oadd(zic_t t1, zic_t t2)

## Detailed Description
The oadd function provides safe addition of two zic_t values with built-in overflow detection. It implements a comprehensive overflow check that handles both positive and negative operands before performing the actual addition operation.

The overflow detection logic works by:
1. **Negative t1 case**: Checks if t2 is less than (ZIC_MIN - t1), which would cause underflow
2. **Non-negative t1 case**: Checks if (ZIC_MAX - t1) is less than t2, which would cause overflow

If either overflow condition is detected, the function calls time_overflow() which reports the error and terminates the program. Only if the addition is safe does the function proceed to return the sum.

This function is essential for the timezone compiler's arithmetic operations, ensuring that time calculations remain within the valid range of the zic_t type and preventing silent wraparound errors that could lead to incorrect timezone data.

## Parameters / Member Variables
- : First zic_t operand for the addition operation
- : Second zic_t operand for the addition operation

## Dependencies
- Functions called/Symbols referenced:
  - [time_overflow](../t/time_overflow.md) (overflow error handler)
  - ZIC_MIN (minimum value constant for zic_t type)
  - ZIC_MAX (maximum value constant for zic_t type)
  - zic_t (timezone-specific integer type)
- Called from (representative examples):
  - [gethms](../g/gethms.md)
  - [getleapdatetime](../g/getleapdatetime.md)
  - [years_of_observations](../y/years_of_observations.md)
  - [adjleap](../a/adjleap.md)
  - [rpytime](../r/rpytime.md)

## Notes and Other Information
- Part of the timezone compiler's safe arithmetic infrastructure
- Prevents silent integer overflow that could corrupt timezone calculations
- Uses compile-time constants ZIC_MIN and ZIC_MAX to define the valid range
- The overflow check is performed using mathematically equivalent but overflow-safe comparisons
- Essential for maintaining data integrity in timezone rule processing
- Returns the sum only when it's guaranteed to be within valid bounds
- Works with both positive and negative time values commonly used in timezone calculations