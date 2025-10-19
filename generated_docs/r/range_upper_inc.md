# range_upper_inc

## Location
[src/backend/utils/adt/rangetypes.c:511-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L511-L520)

## Overview
Checks whether the upper bound of a range type is inclusive, returning a boolean result indicating if the upper bound includes its boundary value.

## Definition
```c
Datum range_upper_inc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function examines a PostgreSQL range type to determine if its upper bound is inclusive. It extracts the flags from the range structure using `range_get_flags()` and tests the `RANGE_UB_INC` bit (0x04) to determine inclusivity. An inclusive upper bound means the boundary value itself is considered part of the range (e.g., [1,5] includes 5), while an exclusive upper bound does not include the boundary value (e.g., [1,5) excludes 5).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro containing:
  - `r1`: RangeType pointer - the range to examine for upper bound inclusivity

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - macro to extract range argument
  - [range_get_flags](range_get_flags.md) - function to extract flags byte from range
  - `RANGE_UB_INC` - constant (0x04) representing upper bound inclusive flag
  - `PG_RETURN_BOOL` - macro to return boolean result

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function exposed to SQL as `upper_inc(range)`
- Returns true if the upper bound is inclusive (]), false if exclusive ()
- Part of the range types infrastructure in PostgreSQL's type system
- The function is located in `src/backend/utils/adt/rangetypes.c:511-520`
- Works with all range types (int4range, numrange, tsrange, etc.)
- Complementary to `range_lower_inc` function for complete range bound analysis

## Simplified Source

```c
Datum
range_upper_inc(PG_FUNCTION_ARGS)
{
	RangeType *range = PG_GETARG_RANGE_P(0);

	// Check RANGE_UB_INC flag for upper bound inclusivity
	char flags = range_get_flags(range);

	PG_RETURN_BOOL(flags & RANGE_UB_INC);
}
```