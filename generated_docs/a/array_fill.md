# array_fill

## Location
src/backend/utils/adt/arrayfuncs.c: 6021 - 6055

## Overview
Creates and fills a new PostgreSQL array with specified dimensions using default lower bounds (starting from 1), providing a simpler interface compared to the full lower-bounds variant.

## Definition


## Detailed Description
This function implements the PostgreSQL SQL function `array_fill(anyelement, int[])` which creates a new array filled with a specified value using default lower bounds. This is the simpler two-argument version of array_fill that automatically uses lower bounds of 1 for each dimension, which is the standard PostgreSQL array indexing convention.

The function validates the dimensions array, extracts the fill value (handling NULL values appropriately), determines the element type dynamically, and delegates the actual array construction to `array_fill_internal` with NULL passed as the lower bounds parameter to indicate default bounds should be used.

## Parameters / Member Variables
The function uses the PostgreSQL function call interface with two arguments:
- Argument 0: Fill value (anyelement) - the value to populate the array with, can be NULL
- Argument 1: Dimensions array (int[]) - specifies the size of each dimension, cannot be NULL

Internal variables:
- `dims`: ArrayType pointer to dimensions array
- `result`: The constructed output array
- `elmtype`: OID of the element type
- `value`: The fill value as a Datum
- `isnull`: Boolean flag indicating if the fill value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (macro for extracting array arguments)
  - get_fn_expr_argtype (determines argument data type)
  - array_fill_internal (performs actual array construction with NULL lower bounds)
  - PG_RETURN_ARRAYTYPE_P (macro for returning array result)
- Called from (representative examples):
  - Used as PostgreSQL function implementation (no direct code references found)

## Notes and Other Information
- Simpler interface than `array_fill_with_lower_bounds` by using default lower bounds of 1
- Provides validation for NULL dimension array with specific error messaging
- Element type is determined dynamically using expression context information
- Supports multi-dimensional arrays with standard 1-based indexing
- NULL fill values are handled correctly and preserved in the result array
- Located in src/backend/utils/adt/arrayfuncs.c at lines 6021-6055
- Part of PostgreSQL's array manipulation infrastructure
- Passes NULL as the lower bounds parameter to `array_fill_internal` to trigger default behavior
- More commonly used than the explicit lower-bounds variant due to simpler interface