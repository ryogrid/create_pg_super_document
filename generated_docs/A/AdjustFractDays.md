# AdjustFractDays

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 41 - 55

## Overview
AdjustFractDays multiplies a fractional value by a scale factor to produce days, adds the integral part to the day field of a time structure, and handles the fractional remainder appropriately.

## Definition


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
  - pg_itm_in (PostgreSQL internal time structure type)
  - pg_add_s32_overflow (overflow-safe 32-bit integer addition)
  - AdjustFractMicroseconds (handles fractional day remainder)
  - USECS_PER_DAY (constant defining microseconds per day)
- Called from (representative examples):
  - DecodeInterval (multiple locations in backend)
  - DecodeISO8601Interval (multiple locations in backend and ECPG)

## Notes and Other Information
- This is a static function within src/backend/utils/adt/datetime.c
- Returns false if overflow occurs during day addition, true on success
- The function assumes input frac has absolute value less than 1 to prevent overflow
- There is also a simpler ECPG version in src/interfaces/ecpg/pgtypeslib/interval.c that works with struct tm instead of pg_itm_in and doesn't include overflow checking
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer overflow
- Part of the broader datetime/interval parsing infrastructure in PostgreSQL