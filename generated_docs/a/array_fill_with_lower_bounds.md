# array_fill_with_lower_bounds

## Location
[src/backend/utils/adt/arrayfuncs.c:5980-6020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5980-L6020)

## Overview
Creates and fills a new PostgreSQL array with specified dimensions, lower bounds, and a fill value, providing full control over array structure including custom lower bounds for each dimension.

## Definition

```c
Datum
array_fill_with_lower_bounds(PG_FUNCTION_ARGS)
```
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
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md) (determines argument data type)
  - [array_fill_internal](array_fill_internal.md) (performs actual array construction)
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

## Simplified Source

```c
Datum array_fill_with_lower_bounds(PG_FUNCTION_ARGS) {
    ArrayType *dims, *lbs, *result;
    Oid elmtype;
    Datum value;
    bool isnull;

    // Validate required array arguments are not NULL
    if (PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("dimension array or low bound array cannot be null")));
    }

    // Extract dimensions and lower bounds arrays
    dims = PG_GETARG_ARRAYTYPE_P(1);
    lbs = PG_GETARG_ARRAYTYPE_P(2);

    // Handle fill value (may be NULL)
    if (!PG_ARGISNULL(0)) {
        value = PG_GETARG_DATUM(0);
        isnull = false;
    } else {
        value = 0;
        isnull = true;
    }

    // Determine element type and create the array
    elmtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
    if (!OidIsValid(elmtype)) {
        elog(ERROR, "could not determine data type of input");
    }

    result = array_fill_internal(dims, lbs, value, isnull, elmtype, fcinfo);
    PG_RETURN_ARRAYTYPE_P(result);
}
```

This function creates a new array with custom dimensions and lower bounds, filled with a specified value. It validates inputs, handles NULL fill values, determines the element type dynamically, then delegates to `array_fill_internal()` for the actual array construction.