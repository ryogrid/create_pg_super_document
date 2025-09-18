# timestamptz_mi_interval_internal

## Location
src/backend/utils/adt/timestamp.c: 3324 - 3338

## Overview
Internal function that subtracts an interval from a timestamp with timezone (timestamptz) by negating the interval and delegating to the addition function.

## Definition


## Detailed Description
This function implements timestamptz-interval subtraction using the same efficient approach as its plain timestamp counterpart. It leverages code reuse by:

1. Negating the input interval using interval_um_internal
2. Delegating to timestamptz_pl_interval_internal with the negated interval and specified timezone

This design ensures that all the complex timezone-aware calendar arithmetic, DST handling, infinity processing, and overflow detection logic is shared between addition and subtraction operations.

## Parameters / Member Variables
- : The input TimestampTz (UTC-based timestamp with timezone)
- : Pointer to the Interval structure containing the values to subtract  
- : Timezone to use for calendar calculations (NULL uses session timezone)
- Returns: TimestampTz result after timezone-aware subtraction

## Dependencies
- Functions called/Symbols referenced:
  - [interval_um_internal](../i/interval_um_internal.md) (interval negation/unary minus)
  - [timestamptz_pl_interval_internal](timestamptz_pl_interval_internal.md) (delegated timezone-aware addition with negated interval)
- Called from:
  - [timestamptz_mi_interval](timestamptz_mi_interval.md) (src/backend/utils/adt/timestamp.c:3353)
  - [timestamptz_mi_interval_at_zone](timestamptz_mi_interval_at_zone.md) (src/backend/utils/adt/timestamp.c:3378)
  - [in_range_timestamptz_interval](../i/in_range_timestamptz_interval.md) (src/backend/utils/adt/timestamp.c:3824)

## Notes and Other Information
- Static function - not directly exposed to SQL, only used internally by other timestamptz functions
- Demonstrates PostgreSQL's efficient code reuse pattern for arithmetic operations
- Inherits all timezone-aware behavior from timestamptz_pl_interval_internal, including proper DST handling
- Creates a local copy of the interval (tspan) for negation, leaving the original unchanged
- Critical component in PostgreSQL's timezone-aware temporal arithmetic system
- Part of the internal API used by public timestamptz functions and window function implementations