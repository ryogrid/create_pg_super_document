# AdjustDays

## Location
[src/backend/utils/adt/datetime.c:633-648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L633-L648)

## Overview
A static helper function that multiplies a value by a scale factor to produce days and adds the result to the days field of a pg_itm_in structure with comprehensive overflow checking.

## Definition
```c
static bool AdjustDays(int64 val, int scale, struct pg_itm_in *itm_in)
```

## Detailed Description
AdjustDays is a utility function in PostgreSQL's datetime processing system that safely converts and accumulates day values. It takes a 64-bit integer value, multiplies it by a scale factor to produce days, and adds the result to the tm_mday field of the input structure.

The function includes multiple layers of overflow protection: first checking that the input value fits within 32-bit integer bounds, then using safe multiplication to compute the scaled days value, and finally using safe addition to update the target field. This multi-step validation ensures robustness when processing potentially large interval values.

## Parameters / Member Variables
- `val`: An int64 value to be scaled and converted to days
- `scale`: An int scale factor to multiply the value by
- `itm_in`: A pointer to a pg_itm_in structure whose tm_mday field will be modified

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md) (for safe 32-bit multiplication with overflow checking)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (for safe 32-bit addition with overflow checking)
  - [pg_itm_in](../p/pg_itm_in.md) (structure type)
- Called from (representative examples):
  - [DecodeInterval](../D/DecodeInterval.md) (for processing day components in interval parsing)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (for ISO 8601 interval parsing day handling)

## Notes and Other Information
- Returns true on success, false if any overflow occurs during processing
- Performs range checking to ensure 64-bit input fits in 32-bit arithmetic
- Uses a local variable `days` for the multiplication result before adding to the structure
- Part of PostgreSQL's comprehensive overflow-safe datetime arithmetic system
- Located at src/backend/utils/adt/datetime.c:633-648
- The function demonstrates defensive programming with multiple overflow checks