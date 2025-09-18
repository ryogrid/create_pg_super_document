# multirange_constructor1

## Location
src/backend/utils/adt/multirangetypes.c: 1023 - 1058

## Overview
Constructs a multirange value from a single range input, primarily used to enable casting from a range type to its corresponding multirange type.

## Definition


## Detailed Description
This function creates a multirange containing exactly one range element. While it might seem redundant compared to the variadic multirange_constructor2, this single-argument version is specifically required to support PostgreSQL's casting mechanism from range types to their corresponding multirange types. The function validates that the input range matches the expected range type for the target multirange, then constructs the multirange using the internal make_multirange function.

## Parameters / Member Variables
- : PostgreSQL function call information structure containing the range argument and return type information

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md)
  - [multirange_get_typcache](multirange_get_typcache.md)
  - PG_GETARG_RANGE_P
  - RangeTypeGetOid
  - [make_multirange](make_multirange.md)
  - PG_RETURN_MULTIRANGE_P
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- Designed specifically to support CAST operations from range to multirange
- Performs type validation to ensure the input range matches the constructor's expected range type
- Raises an error if a NULL range is provided, as multirange values cannot contain null members
- Located in src/backend/utils/adt/multirangetypes.c:1023-1058