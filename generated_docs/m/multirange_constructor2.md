# multirange_constructor2

## Location
[src/backend/utils/adt/multirangetypes.c:941-1022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L941-L1022)

## Overview
Constructs a multirange value from an array of ranges, providing the main entry point for creating multiranges from multiple range inputs.

## Definition


## Detailed Description
This function implements the PostgreSQL function interface for constructing multirange values from arrays of ranges. It serves as the backend implementation for SQL multirange constructor functions. The function handles various input scenarios including empty arrays, single-dimensional range arrays, and validates that all input ranges are of the correct type. It performs comprehensive error checking for null values, multidimensional arrays, and type mismatches before delegating the actual multirange construction to the  function.

## Parameters / Member Variables
- Function uses PostgreSQL's PG_FUNCTION_ARGS macro which provides access to:
  - Function call info through fcinfo
  - Arguments through PG_GETARG_* macros
  - Null checks through PG_ARGISNULL macro

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md) (determines return type)
  - [multirange_get_typcache](multirange_get_typcache.md) (gets type cache information)
  - [make_multirange](make_multirange.md) (constructs the actual multirange)
  - PG_RETURN_MULTIRANGE_P (returns multirange result)
  - ARR_NDIM, ARR_ELEMTYPE (array introspection)
  - [deconstruct_array](../d/deconstruct_array.md) (extracts array elements)
  - DatumGetRangeTypeP (converts Datum to RangeType)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This is a PostgreSQL function callable from SQL (supports VARIADIC arrays)
- Handles edge cases like zero-argument calls and empty arrays
- Validates input array dimensionality (rejects multidimensional arrays)
- Enforces non-null constraints on multirange members
- Performs type checking to ensure array elements match expected range type
- Uses PostgreSQL's memory allocation functions (palloc0)
- Located in 