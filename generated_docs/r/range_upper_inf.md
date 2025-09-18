# range_upper_inf

## Location
src/backend/utils/adt/rangetypes.c: 531 - 543

## Overview
Checks whether the upper bound of a range type is infinite (unbounded), returning a boolean result indicating if the range extends to positive infinity on the upper end.

## Definition
```c
Datum range_upper_inf(PG_FUNCTION_ARGS)
```

## Detailed Description
This function examines a PostgreSQL range type to determine if its upper bound is infinite (unbounded). It extracts the flags from the range structure using `range_get_flags()` and tests the `RANGE_UB_INF` bit (0x10) to check for infinity. An infinite upper bound means the range extends without limit toward positive infinity, representing an unbounded range on the upper side (e.g., [10, +∞) or (10, +∞)).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro containing:
  - `r1`: RangeType pointer - the range to examine for upper bound infinity

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - macro to extract range argument
  - `range_get_flags` - function to extract flags byte from range
  - `RANGE_UB_INF` - constant (0x10) representing upper bound infinite flag
  - `PG_RETURN_BOOL` - macro to return boolean result

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function exposed to SQL as `upper_inf(range)`
- Returns true if the upper bound is infinite (+∞), false if bounded
- Part of the range types infrastructure in PostgreSQL's type system
- The function is located in `src/backend/utils/adt/rangetypes.c:531-543`
- Works with all range types (int4range, numrange, tsrange, etc.)
- Infinite bounds are useful for representing half-open or fully open ranges
- When upper bound is infinite, the inclusivity setting becomes irrelevant
- Complementary to `range_lower_inf` function for complete range bound analysis