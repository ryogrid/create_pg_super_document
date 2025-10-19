# range_lower

## Location
[src/backend/utils/adt/rangetypes.c:446-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L446-L466)

## Overview
Extracts the lower bound value from a range type, returning NULL if the range is empty or has an infinite lower bound.

## Definition
Datum range_lower(PG_FUNCTION_ARGS)

## Detailed Description
The `range_lower` function is a range accessor function that extracts and returns the lower bound value from a PostgreSQL range type. It deserializes the range to access its boundary information, then checks if the range is empty or has an infinite lower bound. If the lower bound is finite and the range is not empty, it returns the lower bound value; otherwise, it returns NULL. This function is typically used in SQL queries to access the starting point of a range.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `r1` (RangeType *): Input range from which to extract the lower bound

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - Extracts range argument from function parameters
  - `RangeBound` - Structure representing range boundary information
  - [range_get_typcache](range_get_typcache.md) - Retrieves type cache entry for the range type
  - `RangeTypeGetOid` - Gets the OID of the range type
  - [range_deserialize](range_deserialize.md) - Deserializes range into boundary components
  - `PG_RETURN_DATUM` - Returns the lower bound value as a PostgreSQL Datum
- Called from (representative examples):
  - SQL queries using the `lower()` function on range types
  - [Range](../R/Range.md) analysis and boundary checking operations

## Notes and Other Information
- Returns NULL for empty ranges or ranges with infinite lower bounds
- Only returns finite lower bound values
- The returned value maintains the original data type of the range subtype
- This is part of the standard range accessor function family
- Commonly used in range queries and boundary analysis operations

## Simplified Source

```c
Datum
range_lower(PG_FUNCTION_ARGS)
{
	RangeType *range = PG_GETARG_RANGE_P(0);
	TypeCacheEntry *typcache;
	RangeBound lower, upper;
	bool empty;

	// Get type information and deserialize range
	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(range));
	range_deserialize(typcache, range, &lower, &upper, &empty);

	// Return NULL if range is empty or lower bound is infinite
	if (empty || lower.infinite)
		PG_RETURN_NULL();

	// Return the finite lower bound value
	PG_RETURN_DATUM(lower.val);
}
```