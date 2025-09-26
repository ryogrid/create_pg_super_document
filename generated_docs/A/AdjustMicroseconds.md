# AdjustMicroseconds

## Location
src/backend/utils/adt/datetime.c: 618 - 632

## Overview
A static helper function that adds both integer and fractional microsecond values (scaled by a factor) to the microseconds field of a pg_itm_in structure with overflow checking.

## Definition
```c
static bool AdjustMicroseconds(int64 val, double fval, int64 scale, struct pg_itm_in *itm_in)
```

## Detailed Description
AdjustMicroseconds is a composite utility function in PostgreSQL's datetime processing system that handles both integer and fractional microsecond adjustments. It first processes the integer part by multiplying val by scale and adding it to the tm_usec field using safe arithmetic. Then it delegates the fractional part processing to AdjustFractMicroseconds.

This function is critical for accurate time interval calculations, ensuring that both whole and fractional time components are properly accumulated without integer overflow. It provides a unified interface for microsecond adjustments that maintains precision while handling edge cases safely.

## Parameters / Member Variables
- `val`: An int64 representing the integer part of the value to be scaled and added
- `fval`: A double representing the fractional part of the value to be processed
- `scale`: An int64 scale factor to multiply the values by
- `itm_in`: A pointer to a pg_itm_in structure whose tm_usec field will be modified

## Dependencies
- Functions called/Symbols referenced:
  - int64_multiply_add (for safe integer arithmetic with overflow checking)
  - AdjustFractMicroseconds (for handling the fractional component)
  - pg_itm_in (structure type)
- Called from (representative examples):
  - DecodeInterval (extensively for various time unit processing)
  - DecodeISO8601Interval (for ISO 8601 interval parsing with microsecond precision)

## Notes and Other Information
- Returns true on success, false if any overflow occurs during processing
- Processes integer and fractional parts separately for maximum precision
- Part of PostgreSQL's comprehensive datetime/interval parsing infrastructure
- Used extensively in both standard and ISO 8601 interval parsing routines
- Located at src/backend/utils/adt/datetime.c:618-632
- The two-stage approach (integer then fractional) ensures optimal precision handling