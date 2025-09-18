# range_constructor2

## Location
src/backend/utils/adt/rangetypes.c: 377 - 405

## Overview
Creates a standard-form range value from two boundary arguments, constructing a range with an inclusive lower bound and exclusive upper bound.

## Definition


## Detailed Description
The `range_constructor2` function constructs a PostgreSQL range type from two input arguments representing the lower and upper bounds. This is one of the core range constructor functions that follows the standard mathematical convention of [lower, upper) - inclusive lower bound and exclusive upper bound. The function handles null values by treating them as infinite bounds, automatically determines the range type from the function call context, and validates the constructed range through the `make_range` function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1` (Datum): Lower bound value (treated as negative infinity if NULL)
  - `arg2` (Datum): Upper bound value (treated as positive infinity if NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `[get_fn_expr_rettype](../g/get_fn_expr_rettype.md)` - Gets the return type (range type) from function call info
  - `RangeBound` - Structure representing range boundary information
  - `[range_get_typcache](range_get_typcache.md)` - Retrieves type cache entry for the range type
  - `[make_range](../m/make_range.md)` - Constructs and validates the actual range value
  - `PG_RETURN_RANGE_P` - Returns the constructed range as a PostgreSQL Datum
- Called from (representative examples):
  - SQL range constructor functions
  - Range type operations in query execution

## Notes and Other Information
- The function creates ranges with inclusive lower bound (`lower.inclusive = true`) and exclusive upper bound (`upper.inclusive = false`)
- NULL arguments are interpreted as infinite bounds rather than causing errors
- The range type is automatically inferred from the function call context
- This is the most commonly used range constructor following mathematical interval notation [a, b)