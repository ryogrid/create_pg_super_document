# adjleap

## Location
src/timezone/zic.c: 3425 - 3467

## Overview
The adjleap function processes and validates leap second data by propagating leap second corrections forward through time and handling leap second expiration settings.

## Definition


## Detailed Description
The adjleap function performs post-processing of leap second data within PostgreSQL's timezone compiler. It propagates cumulative leap second corrections forward through all leap second entries, ensuring proper temporal relationships and validating minimum spacing between leap seconds. The function also handles leap second expiration times, checking for consistency between the last leap second and any specified expiration date. Additionally, it adjusts the global time boundaries based on leap second expiration constraints to maintain temporal integrity.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - tadd (time addition with overflow handling)
  - oadd (overflow-safe addition)
  - warning (warning message output)
  - SECSPERDAY (seconds per day constant)
  - EXIT_FAILURE (error exit status)
  - zic_t (timestamp type definition)
- Called from (representative examples):
  - main (at line 811)

## Notes and Other Information
- Validates that leap seconds are spaced at least 28 days apart to comply with leap second regulations
- Propagates cumulative leap second corrections through the trans[] and corr[] arrays
- Handles backward compatibility for obsolescent "#expires" syntax with warning
- Ensures leap second expiration time follows the last leap second transition
- Adjusts the global hi_time boundary when leap second expiration constrains the valid time range
- Uses overflow-safe arithmetic functions (tadd, oadd) for robust time calculations
- Critical for maintaining leap second data integrity and temporal consistency in timezone files