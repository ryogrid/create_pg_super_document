# range_constructor2

## Location
[src/backend/utils/adt/rangetypes.c:377-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L377-L405)

## Overview
Creates a standard-form range value from two boundary arguments, constructing a range with an inclusive lower bound and exclusive upper bound.

## Definition

```c
Datum
range_constructor2(PG_FUNCTION_ARGS)
```
## Detailed Description
The `range_constructor2` function constructs a PostgreSQL range type from two input arguments representing the lower and upper bounds. This is one of the core range constructor functions that follows the standard mathematical convention of [lower, upper) - inclusive lower bound and exclusive upper bound. The function handles null values by treating them as infinite bounds, automatically determines the range type from the function call context, and validates the constructed range through the `make_range` function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1` (Datum): Lower bound value (treated as negative infinity if NULL)
  - `arg2` (Datum): Upper bound value (treated as positive infinity if NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md) - Gets the return type (range type) from function call info
  - `RangeBound` - Structure representing range boundary information
  - [range_get_typcache](range_get_typcache.md) - Retrieves type cache entry for the range type
  - [make_range](../m/make_range.md) - Constructs and validates the actual range value
  - `PG_RETURN_RANGE_P` - Returns the constructed range as a PostgreSQL Datum
- Called from (representative examples):
  - SQL range constructor functions
  - [Range](../R/Range.md) type operations in query execution

## Notes and Other Information
- The function creates ranges with inclusive lower bound (`lower.inclusive = true`) and exclusive upper bound (`upper.inclusive = false`)
- NULL arguments are interpreted as infinite bounds rather than causing errors
- The range type is automatically inferred from the function call context
- This is the most commonly used range constructor following mathematical interval notation [a, b)

## Simplified Source

```c
Datum
range_constructor2(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	Datum arg2 = PG_GETARG_DATUM(1);
	Oid range_type_oid = get_fn_expr_rettype(fcinfo->flinfo);

	// Get type cache for range operations
	TypeCacheEntry *typcache = range_get_typcache(fcinfo, range_type_oid);

	// Set up lower bound: inclusive, treat NULL as infinite
	RangeBound lower;
	lower.val = PG_ARGISNULL(0) ? (Datum) 0 : arg1;
	lower.infinite = PG_ARGISNULL(0);
	lower.inclusive = true;
	lower.lower = true;

	// Set up upper bound: exclusive, treat NULL as infinite
	RangeBound upper;
	upper.val = PG_ARGISNULL(1) ? (Datum) 0 : arg2;
	upper.infinite = PG_ARGISNULL(1);
	upper.inclusive = false;
	upper.lower = false;

	// Create and validate the range
	RangeType *range = make_range(typcache, &lower, &upper, false, NULL);

	PG_RETURN_RANGE_P(range);
}
```