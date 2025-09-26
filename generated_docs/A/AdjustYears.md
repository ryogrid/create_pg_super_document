# AdjustYears

## Location
src/backend/utils/adt/datetime.c: 661 - 679

## Overview
A static helper function that multiplies a value by a scale factor to produce years and adds the result to the years field of a pg_itm_in structure with comprehensive overflow checking.

## Definition
```c
static bool AdjustYears(int64 val, int scale, struct pg_itm_in *itm_in)
```

## Detailed Description
AdjustYears is a utility function in PostgreSQL's datetime processing system that safely converts and accumulates year values. It takes a 64-bit integer value, multiplies it by a scale factor to produce years, and adds the result to the tm_year field of the input structure.

Similar to AdjustDays, this function implements multiple layers of overflow protection: first checking that the input value fits within 32-bit integer bounds, then using safe multiplication to compute the scaled years value, and finally using safe addition to update the target field. This comprehensive validation ensures robustness when processing interval values that could potentially cause integer overflow.

## Parameters / Member Variables
- `val`: An int64 value to be scaled and converted to years
- `scale`: An int scale factor to multiply the value by
- `itm_in`: A pointer to a pg_itm_in structure whose tm_year field will be modified

## Dependencies
- Functions called/Symbols referenced:
  - pg_mul_s32_overflow (for safe 32-bit multiplication with overflow checking)
  - pg_add_s32_overflow (for safe 32-bit addition with overflow checking)
  - pg_itm_in (structure type)
- Called from (representative examples):
  - DecodeInterval (extensively for processing year components in various interval formats)
  - DecodeISO8601Interval (for ISO 8601 interval parsing year handling)

## Notes and Other Information
- Returns true on success, false if any overflow occurs during processing
- Performs range checking to ensure 64-bit input fits in 32-bit arithmetic
- Uses a local variable `years` for the multiplication result before adding to the structure
- Part of PostgreSQL's comprehensive overflow-safe datetime arithmetic system
- Located at src/backend/utils/adt/datetime.c:661-679
- Functionally similar to AdjustDays but operates on the year field instead of days