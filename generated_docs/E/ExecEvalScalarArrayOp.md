# ExecEvalScalarArrayOp

## Location
[src/backend/executor/execExprInterp.c:3467-3619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3467-L3619)

## Overview
ExecEvalScalarArrayOp evaluates "scalar op ANY/ALL (array)" expressions by applying a comparison operator between a scalar value and each element of an array, combining results using OR (ANY) or AND (ALL) semantics.

## Definition
```c
void ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function implements the evaluation of scalar array operations, which are expressions of the form "scalar op ANY (array)" or "scalar op ALL (array)". The function iterates through each element of the input array, applies the specified comparison operator between the scalar value and each array element, and combines the boolean results using logical OR (for ANY) or AND (for ALL) semantics.

The function includes several optimizations:
- Short-circuiting when the final result is determined (true for ANY, false for ALL)
- Special handling for NULL arrays (returns NULL)
- Empty array handling (returns FALSE for ANY, TRUE for ALL)
- Caching of element type information to avoid repeated lookups
- Proper handling of strict functions with NULL arguments

The comparison operator is invoked via function call protocol, with the scalar as the first argument and each array element as the second argument.

## Parameters / Member Variables
- `state`: Expression state context (unused in this function)
- `op`: Expression evaluation step containing scalar array operation data, including function info, useOr flag, and cached type information

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP: Detoasts and extracts ArrayType from input Datum
  - [ArrayGetNItems](../A/ArrayGetNItems.md): Calculates total number of elements in the array
  - ARR_NDIM/ARR_DIMS: Macros for accessing array dimensions
  - ARR_ELEMTYPE: Macro for getting array element type OID
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md): Retrieves type information for array elements
  - ARR_DATA_PTR: Gets pointer to array data storage
  - ARR_NULLBITMAP: Gets pointer to array's NULL bitmap
  - [fetch_att](../f/fetch_att.md): Extracts individual array element values
  - att_addlength_pointer/att_align_nominal: Navigate through array storage
  - [BoolGetDatum](../B/BoolGetDatum.md)/DatumGetBool: Convert between boolean values and Datums
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation type mapping function

## Notes and Other Information
- Supports both ANY (useOr=true) and ALL (useOr=false) semantics with appropriate short-circuiting
- Handles NULL arrays by returning NULL, and empty arrays by returning the appropriate default value
- Caches element type information across calls to avoid repeated type system lookups
- Properly handles NULL bitmap navigation for arrays with NULL elements
- Respects strict function semantics when encountering NULL values
- The scalar argument is pre-evaluated and stored in fcinfo->args[0] before this function is called
- Uses bitmap masking to efficiently track NULL elements in sparse arrays

## Simplified Source

```c
void ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)
{
    FunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo_data;
    bool useOr = op->d.scalararrayop.useOr;  // true for ANY, false for ALL
    bool strictfunc = op->d.scalararrayop.finfo->fn_strict;

    // Return NULL if array is NULL
    if (*op->resnull)
        return;

    ArrayType *arr = DatumGetArrayTypeP(*op->resvalue);
    int nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

    // Handle empty array: FALSE for ANY, TRUE for ALL
    if (nitems <= 0) {
        *op->resvalue = BoolGetDatum(!useOr);
        *op->resnull = false;
        return;
    }

    // If scalar is NULL and function is strict, return NULL
    if (fcinfo->args[0].isnull && strictfunc) {
        *op->resnull = true;
        return;
    }

    // Cache element type info for efficiency
    if (op->d.scalararrayop.element_type != ARR_ELEMTYPE(arr)) {
        get_typlenbyvalalign(ARR_ELEMTYPE(arr),
                           &op->d.scalararrayop.typlen,
                           &op->d.scalararrayop.typbyval,
                           &op->d.scalararrayop.typalign);
        op->d.scalararrayop.element_type = ARR_ELEMTYPE(arr);
    }

    // Initialize result: FALSE for ANY, TRUE for ALL
    Datum result = BoolGetDatum(!useOr);
    bool resultnull = false;

    // Iterate through array elements
    char *s = (char *) ARR_DATA_PTR(arr);
    bits8 *bitmap = ARR_NULLBITMAP(arr);
    int bitmask = 1;

    for (int i = 0; i < nitems; i++) {
        // Extract element value, checking for NULL
        if (bitmap && (*bitmap & bitmask) == 0) {
            fcinfo->args[1].value = (Datum) 0;
            fcinfo->args[1].isnull = true;
        } else {
            Datum elt = fetch_att(s, op->d.scalararrayop.typbyval,
                                 op->d.scalararrayop.typlen);
            s = att_addlength_pointer(s, op->d.scalararrayop.typlen, s);
            s = (char *) att_align_nominal(s, op->d.scalararrayop.typalign);
            fcinfo->args[1].value = elt;
            fcinfo->args[1].isnull = false;
        }

        // Apply comparison operator
        Datum thisresult;
        if (fcinfo->args[1].isnull && strictfunc) {
            fcinfo->isnull = true;
            thisresult = (Datum) 0;
        } else {
            fcinfo->isnull = false;
            thisresult = op->d.scalararrayop.fn_addr(fcinfo);
        }

        // Combine results with short-circuiting
        if (fcinfo->isnull) {
            resultnull = true;
        } else if (useOr) {
            // ANY: short-circuit on first TRUE
            if (DatumGetBool(thisresult)) {
                result = BoolGetDatum(true);
                resultnull = false;
                break;
            }
        } else {
            // ALL: short-circuit on first FALSE
            if (!DatumGetBool(thisresult)) {
                result = BoolGetDatum(false);
                resultnull = false;
                break;
            }
        }

        // Advance NULL bitmap pointer
        if (bitmap) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }

    *op->resvalue = result;
    *op->resnull = resultnull;
}
```