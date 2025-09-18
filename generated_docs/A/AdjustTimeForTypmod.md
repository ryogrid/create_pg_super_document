# AdjustTimeForTypmod

## Location
src/backend/utils/adt/date.c: 1645 - 1679

## Overview
A utility function that forces the precision of TIME values to a specified precision level by applying rounding and truncation based on type modifiers.

## Definition
```c
void AdjustTimeForTypmod(TimeADT *time, int32 typmod)
```

## Detailed Description
AdjustTimeForTypmod adjusts the precision of TIME data type values according to the specified type modifier (typmod). The function uses pre-calculated scale and offset arrays to perform precise rounding operations that conform to PostgreSQL's TIME precision requirements. It handles both positive and negative time values differently to ensure proper rounding behavior. The implementation uses the same algorithm as AdjustTimestampForTypmod but maintains a separate copy due to the coincidental rather than fundamental relationship between the TIME and TIMESTAMP types. The function performs no operation if the typmod is outside the valid range (0 to MAX_TIME_PRECISION).

## Parameters / Member Variables
- `time`: TimeADT pointer to the time value to be adjusted (modified in place)
- `typmod`: int32 type modifier specifying the desired precision level (0-6, where 6 is microsecond precision)

## Dependencies
- Functions called/Symbols referenced:
  - TimeADT (data type for time values)
  - MAX_TIME_PRECISION (constant defining maximum time precision)
  - INT64CONST (macro for 64-bit integer constants)
  - TimeScales array (static scaling factors for each precision level)
  - TimeOffsets array (static offset values for rounding)
- Called from (representative examples):
  - GetSQLCurrentTime (src/backend/utils/adt/date.c:354)
  - GetSQLLocalTime (src/backend/utils/adt/date.c:373)
  - time_in (src/backend/utils/adt/date.c:1407)
  - time_recv (src/backend/utils/adt/date.c:1538)
  - time_scale (src/backend/utils/adt/date.c:1632)
  - timetz_in (src/backend/utils/adt/date.c:2308)
  - timetz_recv (src/backend/utils/adt/date.c:2362)
  - timetz_scale (src/backend/utils/adt/date.c:2436)

## Notes and Other Information
- Uses static lookup tables for performance optimization (TimeScales and TimeOffsets arrays)
- Handles positive and negative time values with different rounding logic to ensure mathematical correctness
- The TimeScales array contains powers of 10 for truncating precision (1000000 for 0 digits, down to 1 for 6 digits)
- The TimeOffsets array contains rounding offsets (500000 for 0 digits, down to 0 for 6 digits)
- Located in src/backend/utils/adt/date.c:1645-1679
- Modifies the input time value in place rather than returning a new value
- Critical component in PostgreSQL's temporal data type precision handling system