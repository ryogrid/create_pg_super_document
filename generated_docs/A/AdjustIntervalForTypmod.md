# AdjustIntervalForTypmod

## Location
[src/backend/utils/adt/timestamp.c:1359-1538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1359-L1538)

## Overview
Adjusts an interval value for specified precision constraints in both YEAR to SECOND range and sub-second precision, supporting PostgreSQL's interval type modifier functionality.

## Definition


## Detailed Description
This static function modifies an interval value to conform to type modifier constraints specified in PostgreSQL's INTERVAL type declarations. It handles two main aspects of interval adjustment:

1. **Range limiting**: Enforces field range restrictions (e.g., YEAR TO MONTH, DAY TO SECOND) by zeroing out fields outside the specified range
2. **Precision adjustment**: Rounds fractional seconds to the specified precision level (0-6 digits)

The function implements PostgreSQL's interpretation that fields to the right of the last specified field are zeroed out, while fields to the left remain valid. It uses static lookup tables (IntervalScales and IntervalOffsets) for efficient precision rounding operations.

For infinite intervals, no adjustments are applied. The function returns true on success or false on failure when using error context handling.

## Parameters / Member Variables
- : Pointer to the Interval structure to be modified in-place
- : Type modifier encoding both range and precision constraints (-1 means no constraints)
- : Error context node for soft error handling (NULL for hard errors)

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_NOT_FINITE (macro for checking infinite intervals)
  - INTERVAL_RANGE (macro to extract range from typmod)
  - INTERVAL_PRECISION (macro to extract precision from typmod)
  - INTERVAL_MASK (macro for field masking)
  - INTERVAL_FULL_RANGE, INTERVAL_FULL_PRECISION (constants)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md), pg_sub_s64_overflow (overflow-safe arithmetic)
  - ereturn (error context return macro)
- Called from:
  - [interval_in](../i/interval_in.md) (interval input parsing)
  - [interval_recv](../i/interval_recv.md) (binary interval reception)
  - [interval_scale](../i/interval_scale.md) (interval scaling function)

## Notes and Other Information
- Implements PostgreSQL's post-8.4 behavior where only truncation/rounding of low-order fields occurs
- Uses microsecond-based time representation internally
- Static lookup tables optimize precision rounding for common cases
- Supports all standard SQL interval range specifications
- Error handling varies based on whether escontext is provided (soft vs hard error modes)