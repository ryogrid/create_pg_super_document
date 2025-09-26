# AdjustFractMicroseconds

## Location
[src/backend/utils/adt/datetime.c:537-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L537-L568)

## Overview
AdjustFractMicroseconds converts a fractional value to microseconds by scaling and adds it to an interval time structure's microseconds field with overflow protection.

## Definition

```c
static bool
AdjustFractMicroseconds(double frac, int64 scale,
						struct pg_itm_in *itm_in)
```
## Detailed Description
AdjustFractMicroseconds handles the conversion of fractional time components (typically less than 1.0) into microseconds and safely adds them to an interval time structure. The function performs several key operations:

1. **Fast path optimization**: Returns immediately if frac is 0
2. **Scaling**: Multiplies the fractional value by the provided scale to convert to microseconds
3. **Rounding**: Implements banker's rounding to the nearest microsecond (rounds 0.5 up, -0.5 down)
4. **Safe addition**: Uses overflow-protected addition to update the microseconds field

The function assumes the input fractional value has an absolute value less than 1, which prevents overflow during the scaling operation for reasonable scale values.

## Parameters / Member Variables
- `frac`: Fractional time component (assumed to have absolute value < 1.0)
- `scale`: Scaling factor to convert fraction to microseconds (e.g., USECS_PER_SEC)
- `itm_in`: Pointer to pg_itm_in structure whose tm_usec field will be updated

## Dependencies
- Functions called/Symbols referenced:
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (safe 64-bit addition with overflow detection)
  - [pg_itm_in](../p/pg_itm_in.md) (PostgreSQL interval time input structure)
- Called from (representative examples):
  - [AdjustFractDays](AdjustFractDays.md)
  - [AdjustMicroseconds](AdjustMicroseconds.md)  
  - [DecodeInterval](../D/DecodeInterval.md)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (multiple locations)

## Notes and Other Information
- Returns true on successful operation, false if overflow would occur
- Uses proper rounding: values >= 0.5 round up, values <= -0.5 round down
- The input structure is modified in-place only if the operation succeeds
- Part of PostgreSQL's interval parsing and processing infrastructure
- Commonly used with scale values like USECS_PER_SEC, USECS_PER_MINUTE, etc.
- The fractional input assumption (abs(frac) < 1) is critical for preventing overflow during scaling
- Used extensively in ISO8601 interval parsing where fractional seconds, minutes, hours, etc. need to be converted to microseconds