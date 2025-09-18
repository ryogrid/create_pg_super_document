# interval_justify_interval

## Location
[src/backend/utils/adt/timestamp.c:2880-2959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2880-L2959)

## Overview
Normalizes an interval by adjusting month, day, and time portions to be within customary bounds and ensuring consistent sign across all fields.

## Definition
```c
Datum interval_justify_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs comprehensive normalization of PostgreSQL intervals by applying the following rules:
1. Ensures 0 ≤ |time| < 24 hours by converting excess hours to days
2. Ensures 0 ≤ |day| < 30 days by converting excess days to months  
3. Makes all three fields (month, day, time) have the same sign (all positive or all negative)

The function handles potential overflow scenarios and includes pre-justification logic to prevent overflow when days and time have the same sign. It uses TMODULO for time normalization and includes careful overflow checking with pg_add_s32_overflow.

The normalization process ensures that intervals are represented in a canonical form that makes arithmetic operations predictable and prevents unexpected behavior in date/time calculations.

## Parameters / Member Variables
- `span`: Input interval to be justified (from PG_GETARG_INTERVAL_P(0))
- `result`: Normalized interval with justified month, day, and time fields
- `wholeday`: Temporary variable holding whole days extracted from time
- `wholemonth`: Temporary variable holding whole months extracted from days

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P (PostgreSQL function call interface macro)
  - INTERVAL_NOT_FINITE (infinity checking macro for intervals)
  - DAYS_PER_MONTH (constant defining days per month normalization)
  - USECS_PER_DAY (constant for microseconds per day conversion)
  - TMODULO (time modulo operation macro)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (safe 32-bit addition with overflow detection)
  - PG_RETURN_INTERVAL_P (PostgreSQL return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL interval justification family alongside interval_justify_hours and interval_justify_days
- Handles infinite intervals by returning them unchanged
- Uses pre-justification optimization to prevent overflow in certain scenarios
- Implements complex sign normalization logic to ensure consistent interval representation
- Critical for maintaining predictable behavior in interval arithmetic operations
- Located at src/backend/utils/adt/timestamp.c:2880-2959