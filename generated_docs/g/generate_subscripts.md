# generate_subscripts

## Location
[src/backend/utils/adt/arrayfuncs.c:5905-5968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5905-L5968)

## Overview
generate_subscripts is a set-returning function that generates all valid subscripts for a specified dimension of an array, optionally in reverse order.

## Definition
```c
Datum
generate_subscripts(PG_FUNCTION_ARGS)
```

SQL signature: generate_subscripts(array anyarray, dim int [, reverse bool])

## Detailed Description
This function implements PostgreSQL's generate_subscripts() SQL function, which returns a set of integers representing all valid subscripts for a specific dimension of an array. It uses PostgreSQL's set-returning function (SRF) framework to iterate through subscript values from the lower bound to upper bound of the specified dimension. The function supports an optional reverse parameter to return subscripts in descending order. It maintains state across multiple calls using FuncCallContext to track iteration progress.

## Parameters / Member Variables
- Uses PostgreSQL function call convention via PG_FUNCTION_ARGS:
  - Argument 0: array (anyarray) - The input array to generate subscripts for
  - Argument 1: dim (int) - The dimension number (1-based) to generate subscripts for
  - Argument 2: reverse (bool, optional) - Whether to return subscripts in reverse order (default: false)

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP (SRF framework macros)
  - SRF_RETURN_NEXT, SRF_RETURN_DONE (SRF result return macros)
  - PG_GETARG_ANY_ARRAY_P, PG_GETARG_INT32, PG_GETARG_BOOL (argument access macros)
  - AARR_NDIM, AARR_LBOUND, AARR_DIMS (array metadata access macros)
  - AnyArrayType, FuncCallContext, generate_subscripts_fctx (supporting types)
  - MAXDIM (maximum array dimensions constant)
- Called from (representative examples):
  - [generate_subscripts_nodir](generate_subscripts_nodir.md) (wrapper function in arrayfuncs.c:5972)

## Notes and Other Information
- Implements PostgreSQL's generate_subscripts() SQL function for array subscript enumeration
- Uses the SRF (Set Returning Function) framework for efficient iteration over large ranges
- Performs bounds checking to ensure the requested dimension exists and is valid
- Supports both forward and reverse iteration through subscripts
- Memory context management ensures proper cleanup across multiple function calls
- Returns subscripts as 1-based integers matching PostgreSQL's array indexing convention
- Essential for SQL queries that need to iterate over array dimensions programmatically

## Simplified Source

```c
Datum generate_subscripts(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    generate_subscripts_fctx *fctx;

    // First call: Initialize the function context
    if (SRF_IS_FIRSTCALL()) {
        AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);
        int reqdim = PG_GETARG_INT32(1);

        funcctx = SRF_FIRSTCALL_INIT();

        // Validate array and dimension
        if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM ||
            reqdim <= 0 || reqdim > AARR_NDIM(v)) {
            SRF_RETURN_DONE(funcctx);
        }

        // Set up iteration context
        fctx = palloc(sizeof(generate_subscripts_fctx));
        fctx->lower = AARR_LBOUND(v)[reqdim - 1];
        fctx->upper = AARR_DIMS(v)[reqdim - 1] + fctx->lower - 1;
        fctx->reverse = (PG_NARGS() < 3) ? false : PG_GETARG_BOOL(2);

        funcctx->user_fctx = fctx;
    }

    // Subsequent calls: Return next subscript
    funcctx = SRF_PERCALL_SETUP();
    fctx = funcctx->user_fctx;

    if (fctx->lower <= fctx->upper) {
        if (!fctx->reverse) {
            SRF_RETURN_NEXT(funcctx, Int32GetDatum(fctx->lower++));
        } else {
            SRF_RETURN_NEXT(funcctx, Int32GetDatum(fctx->upper--));
        }
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}
```

This set-returning function generates all valid subscripts for a specified array dimension. It validates the array and dimension, then iterates through subscripts from lower to upper bound (or in reverse). The SRF framework maintains state between calls for efficient iteration.