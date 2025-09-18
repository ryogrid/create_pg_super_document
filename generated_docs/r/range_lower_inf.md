# range_lower_inf

## Location
src/backend/utils/adt/rangetypes.c: 521 - 530

## Overview
Checks whether the lower bound of a range type is infinite (unbounded), returning a boolean result indicating if the range extends to negative infinity on the lower end.

## Definition
```c
Datum range_lower_inf(PG_FUNCTION_ARGS)
```

## Detailed Description
This function examines a PostgreSQL range type to determine if its lower bound is infinite (unbounded). It extracts the flags from the range structure using `range_get_flags()` and tests the `RANGE_LB_INF` bit (0x08) to check for infinity. An infinite lower bound means the range extends without limit toward negative infinity, representing an unbounded range on the lower side (e.g., (-∞, 10] or (-∞, 10)).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro containing:
  - `r1`: RangeType pointer - the range to examine for lower bound infinity

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - macro to extract range argument
  - [range_get_flags](range_get_flags.md) - function to extract flags byte from range
  - `RANGE_LB_INF` - constant (0x08) representing lower bound infinite flag
  - `PG_RETURN_BOOL` - macro to return boolean result

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function exposed to SQL as `lower_inf(range)`
- Returns true if the lower bound is infinite (-∞), false if bounded
- Part of the range types infrastructure in PostgreSQL's type system
- The function is located in `src/backend/utils/adt/rangetypes.c:521-530`
- Works with all range types (int4range, numrange, tsrange, etc.)
- Infinite bounds are useful for representing half-open or fully open ranges
- When lower bound is infinite, the inclusivity setting becomes irrelevant