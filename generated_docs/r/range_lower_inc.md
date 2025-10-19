# range_lower_inc

## Location
[src/backend/utils/adt/rangetypes.c:501-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L501-L510)

## Overview
Checks whether the lower bound of a range type is inclusive, returning a boolean result indicating if the lower bound includes its boundary value.

## Definition

```c
Datum
range_lower_inc(PG_FUNCTION_ARGS)
```
## Detailed Description
This function examines a PostgreSQL range type to determine if its lower bound is inclusive. It extracts the flags from the range structure using  and tests the  bit (0x02) to determine inclusivity. An inclusive lower bound means the boundary value itself is considered part of the range (e.g., [1,5) includes 1), while an exclusive lower bound does not include the boundary value (e.g., (1,5) excludes 1).

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro containing:
  - : RangeType pointer - the range to examine for lower bound inclusivity

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to extract range argument
  -  - function to extract flags byte from range
  -  - constant (0x02) representing lower bound inclusive flag
  -  - macro to return boolean result

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function exposed to SQL as 
- Returns true if the lower bound is inclusive ([), false if exclusive (()
- Part of the range types infrastructure in PostgreSQL's type system
- The function is located in 
- Works with all range types (int4range, numrange, tsrange, etc.)

## Simplified Source

```c
Datum
range_lower_inc(PG_FUNCTION_ARGS)
{
	RangeType *range = PG_GETARG_RANGE_P(0);

	// Check RANGE_LB_INC flag for lower bound inclusivity
	char flags = range_get_flags(range);

	PG_RETURN_BOOL(flags & RANGE_LB_INC);
}
```