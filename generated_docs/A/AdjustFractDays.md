# AdjustFractDays

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:41-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L41-L55)

## Overview
AdjustFractDays multiplies a fractional value by a scale factor to produce days, adds the integral part to the day field of a time structure, and handles the fractional remainder appropriately.

## Definition

```c
static void
AdjustFractDays(double frac, struct /* pg_ */ tm *tm, fsec_t *fsec, int scale)
```
## Detailed Description
This function is part of PostgreSQL's datetime processing system, specifically designed to handle fractional day calculations during interval parsing. It takes a fractional value (typically with absolute value less than 1), scales it to produce days, and distributes the result between the integer day field and fractional microseconds. The function includes overflow checking to ensure safe arithmetic operations and returns a boolean indicating success or failure.

The function operates through these steps:
1. Fast-path return for zero fractional values
2. Scale the fractional value by the provided scale factor
3. Extract the integer days portion
4. Safely add the integer days to the existing tm_mday using overflow-safe addition
5. Process any remaining fractional portion by delegating to AdjustFractMicroseconds

## Parameters / Member Variables
- : Double precision fractional value to be processed (assumed to have absolute value < 1)
- : Integer scaling factor to convert the fractional value to days
- : Pointer to PostgreSQL's internal time structure that will be modified

## Dependencies
- Functions called/Symbols referenced:
  - [pg_itm_in](../p/pg_itm_in.md) (PostgreSQL internal time structure type)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (overflow-safe 32-bit integer addition)
  - [AdjustFractMicroseconds](AdjustFractMicroseconds.md) (handles fractional day remainder)
  - USECS_PER_DAY (constant defining microseconds per day)
- Called from (representative examples):
  - [DecodeInterval](../D/DecodeInterval.md) (multiple locations in backend)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (multiple locations in backend and ECPG)

## Notes and Other Information
- This is a static function within src/backend/utils/adt/datetime.c
- Returns false if overflow occurs during day addition, true on success
- The function assumes input frac has absolute value less than 1 to prevent overflow
- There is also a simpler ECPG version in src/interfaces/ecpg/pgtypeslib/interval.c that works with struct tm instead of pg_itm_in and doesn't include overflow checking
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer overflow
- Part of the broader datetime/interval parsing infrastructure in PostgreSQL

## Simplified Source

```c
static bool AdjustFractDays(double frac, int scale, struct pg_itm_in *itm_in) {
    // Fast path for zero - no work needed
    if (frac == 0)
        return true;

    // Scale the fraction to get total days (whole + fractional)
    frac *= scale;
    int extra_days = (int) frac;  // Extract whole days

    // Safely add whole days to existing day count
    if (pg_add_s32_overflow(itm_in->tm_mday, extra_days, &itm_in->tm_mday))
        return false;  // Overflow in day addition

    // Handle the remaining fractional part
    frac -= extra_days;  // Get fractional remainder
    return AdjustFractMicroseconds(frac, USECS_PER_DAY, itm_in);
}
```