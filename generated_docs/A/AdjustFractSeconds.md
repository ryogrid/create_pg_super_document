# AdjustFractSeconds

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 23 - 40

## Overview
AdjustFractSeconds is a utility function that adjusts fractional seconds by converting them to whole seconds and microseconds, updating a time structure and fractional seconds field accordingly.

## Definition


## Detailed Description
This function handles the conversion of fractional seconds into their constituent parts when processing time intervals. It takes a fractional value, scales it appropriately, extracts the whole second portion to add to the tm_sec field of a time structure, and converts the remaining fractional part to microseconds stored in the fsec parameter. The function is copied and adapted from the backend datetime utilities for use in the ECPG (Embedded SQL in C for PostgreSQL) pgtypeslib.

The function operates by:
1. Checking if the fractional value is zero and returning early if so
2. Scaling the fractional value by the provided scale factor
3. Extracting the integer seconds portion and adding it to tm->tm_sec
4. Converting the remaining fractional portion to microseconds using rint() for proper rounding

## Parameters / Member Variables
- : The fractional seconds value to be processed (double precision)
- : Pointer to a time structure where the whole seconds will be added to tm_sec
- : Pointer to fractional seconds field (in microseconds) where the remaining fractional part will be stored
- : Integer scaling factor to apply to the fractional value

## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (type definition for fractional seconds)
  - rint (standard math function for rounding)
- Called from (representative examples):
  - AdjustFractDays
  - DecodeISO8601Interval (multiple locations)
  - DecodeInterval (multiple locations)

## Notes and Other Information
- This is a static function, so it's only accessible within the interval.c file
- The function is a copy-paste adaptation from src/backend/utils/adt/datetime.c, modified to work with standard struct tm instead of PostgreSQL's struct pg_tm
- The function uses rint() to ensure proper rounding when converting fractional seconds to microseconds
- It's primarily used in interval parsing and decoding operations within the ECPG pgtypeslib
- The scale parameter allows for different precision handling depending on the context of use