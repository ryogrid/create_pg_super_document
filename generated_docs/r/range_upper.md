# range_upper

## Location
[src/backend/utils/adt/rangetypes.c:467-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L467-L490)

## Overview
Extracts the upper bound value from a range type, returning NULL if the range is empty or has an infinite upper bound.

## Definition
Datum range_upper(PG_FUNCTION_ARGS)

## Detailed Description
The `range_upper` function is a range accessor function that extracts and returns the upper bound value from a PostgreSQL range type. It deserializes the range to access its boundary information, then checks if the range is empty or has an infinite upper bound. If the upper bound is finite and the range is not empty, it returns the upper bound value; otherwise, it returns NULL. This function is the counterpart to `range_lower` and is typically used in SQL queries to access the ending point of a range.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `r1` (RangeType *): Input range from which to extract the upper bound

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - Extracts range argument from function parameters
  - `RangeBound` - Structure representing range boundary information
  - [range_get_typcache](range_get_typcache.md) - Retrieves type cache entry for the range type
  - `RangeTypeGetOid` - Gets the OID of the range type
  - [range_deserialize](range_deserialize.md) - Deserializes range into boundary components
  - `PG_RETURN_DATUM` - Returns the upper bound value as a PostgreSQL Datum
- Called from (representative examples):
  - SQL queries using the `upper()` function on range types
  - [Range](../R/Range.md) analysis and boundary checking operations

## Notes and Other Information
- Returns NULL for empty ranges or ranges with infinite upper bounds
- Only returns finite upper bound values
- The returned value maintains the original data type of the range subtype
- This is part of the standard range accessor function family alongside `range_lower`
- Commonly used in range queries and boundary analysis operations

## Simplified Source

```c
Datum
range_upper(PG_FUNCTION_ARGS)
{
	RangeType *range = PG_GETARG_RANGE_P(0);
	TypeCacheEntry *typcache;
	RangeBound lower, upper;
	bool empty;

	// Get type information and deserialize range
	typcache = range_get_typcache(fcinfo, RangeTypeGetOid(range));
	range_deserialize(typcache, range, &lower, &upper, &empty);

	// Return NULL if range is empty or upper bound is infinite
	if (empty || upper.infinite)
		PG_RETURN_NULL();

	// Return the finite upper bound value
	PG_RETURN_DATUM(upper.val);
}
```