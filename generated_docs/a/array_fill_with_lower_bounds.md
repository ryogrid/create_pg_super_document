# array_fill_with_lower_bounds

## Location
src/backend/utils/adt/arrayfuncs.c: 5980 - 6020

## Overview
Creates and fills a new PostgreSQL array with specified dimensions, lower bounds, and a fill value, providing full control over array structure including custom lower bounds for each dimension.

## Definition


## Detailed Description
This function implements the PostgreSQL SQL function `array_fill(anyelement, int[], int[])` which creates a new array filled with a specified value. Unlike the simpler `array_fill` function, this variant allows specification of custom lower bounds for each array dimension, providing complete control over the array's index structure.

The function validates input parameters, extracts the fill value (handling NULL values appropriately), determines the element type dynamically, and delegates the actual array construction to `array_fill_internal`. It supports multi-dimensional arrays with arbitrary lower bounds for each dimension.

## Parameters / Member Variables
The function uses the PostgreSQL function call interface with three arguments:
- Argument 0: Fill value (anyelement) - the value to populate the array with, can be NULL
- Argument 1: Dimensions array (int[]) - specifies the size of each dimension, cannot be NULL
- Argument 2: Lower bounds array (int[]) - specifies the lower bound for each dimension, cannot be NULL

Internal variables:
- `dims`: ArrayType pointer to dimensions array
- `lbs`: ArrayType pointer to lower bounds array  
- `result`: The constructed output array
- `elmtype`: OID of the element type
- `value`: The fill value as a Datum
- `isnull`: Boolean flag indicating if the fill value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (macro for extracting array arguments)
  - get_fn_expr_argtype (determines argument data type)
  - array_fill_internal (performs actual array construction)
  - PG_RETURN_ARRAYTYPE_P (macro for returning array result)
- Called from (representative examples):
  - Used as PostgreSQL function implementation (no direct code references found)

## Notes and Other Information
- Provides stricter validation than `array_fill` by explicitly rejecting NULL dimension or lower bound arrays
- Element type is determined dynamically using expression context information
- Supports multi-dimensional arrays with custom indexing schemes
- NULL fill values are handled correctly and preserved in the result array
- Located in src/backend/utils/adt/arrayfuncs.c at lines 5980-6020
- Part of PostgreSQL's array manipulation infrastructure
- Error handling includes specific error codes for invalid NULL inputs (ERRCODE_NULL_VALUE_NOT_ALLOWED)