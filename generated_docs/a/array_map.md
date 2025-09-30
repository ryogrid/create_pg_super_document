# array_map

## Location
[src/backend/utils/adt/arrayfuncs.c:3201-3360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3201-L3360)

## Overview
Transforms each element of an array through an arbitrary expression, returning a new array with the same dimensions but potentially different element types.

## Definition

```c
struct_empty_array(retType));
```
## Detailed Description
This function implements a higher-order array transformation operation, similar to the map function in functional programming languages. It applies a given expression to each element of the source array and constructs a new array with the results.

Key features include:
1. **Element-wise transformation**: Applies the expression to each individual array element
2. **Type transformation**: Can change element types between input and output arrays (if binary-compatible)
3. **Dimension preservation**: Maintains the same array structure (dimensions, bounds) as the source
4. **Null handling**: Properly processes NULL elements and maintains null bitmap when needed
5. **Performance optimization**: Uses ArrayMapState for caching type information across multiple calls
6. **Expression evaluation**: Leverages PostgreSQL's expression evaluation framework for transformations

The function is designed for efficient bulk transformations and integrates with PostgreSQL's expression evaluation system.

## Parameters / Member Variables
- : Datum representing the source array to be transformed
- : Compiled expression state representing the per-element transformation
- : Expression evaluation context providing variable bindings and memory management
- : OID of the element type for the output array (must be binary-compatible with expression result)
- : Workspace for array_map operations that caches type information for performance (must be zeroed before first use)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetAnyArrayP](../D/DatumGetAnyArrayP.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [array_iter_setup](array_iter_setup.md)
  - [array_iter_next](array_iter_next.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - PG_DETOAST_DATUM
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - SET_VARSIZE
- Called from:
  - [ExecEvalArrayCoerce](../E/ExecEvalArrayCoerce.md)

## Notes and Other Information
- The caller must ensure the input array is not NULL (NULL elements within the array are acceptable)
- The caller should run in the econtext's per-tuple memory context for proper memory management
- [ArrayMapState](../A/ArrayMapState.md) can be reused across multiple calls for better performance by caching type lookup information
- Source elements are placed in  and  for expression evaluation
- Handles both fixed-length and variable-length element types with proper alignment and detoasting
- Returns an empty array if the source array is empty
- Includes overflow protection when calculating result array size
- The function does not attempt to free results from expression evaluation to avoid corruption
- Essential for array type coercion operations in PostgreSQL's executor
- Located in src/backend/utils/adt/arrayfuncs.c:3201-3360

## Simplified Source

```c
Datum
array_map(Datum arrayd, ExprState *exprstate, ExprContext *econtext,
          Oid retType, ArrayMapState *amstate)
{
    AnyArrayType *v = DatumGetAnyArrayP(arrayd);
    ArrayType *result;
    Datum *values;
    bool *nulls;
    int ndim = AARR_NDIM(v);
    int *dim = AARR_DIMS(v);
    int nitems = ArrayGetNItems(ndim, dim);
    int i;
    bool hasnulls = false;
    Oid inpType = AARR_ELEMTYPE(v);
    array_iter iter;
    ArrayMetaState *inp_extra = &amstate->inp_extra;
    ArrayMetaState *ret_extra = &amstate->ret_extra;
    Datum *transform_source = exprstate->innermost_caseval;
    bool *transform_source_isnull = exprstate->innermost_casenull;

    // Check for empty array
    if (nitems <= 0)
        return PointerGetDatum(construct_empty_array(retType));

    // Cache type information for input and output elements
    if (inp_extra->element_type != inpType) {
        get_typlenbyvalalign(inpType, &inp_extra->typlen,
                            &inp_extra->typbyval, &inp_extra->typalign);
        inp_extra->element_type = inpType;
    }

    if (ret_extra->element_type != retType) {
        get_typlenbyvalalign(retType, &ret_extra->typlen,
                            &ret_extra->typbyval, &ret_extra->typalign);
        ret_extra->element_type = retType;
    }

    // Allocate space for transformed values
    values = (Datum *) palloc(nitems * sizeof(Datum));
    nulls = (bool *) palloc(nitems * sizeof(bool));

    // Process each array element through the expression
    array_iter_setup(&iter, v);
    int32 nbytes = 0;

    for (i = 0; i < nitems; i++) {
        // Get source element
        *transform_source = array_iter_next(&iter, transform_source_isnull, i,
                                           inp_extra->typlen, inp_extra->typbyval, inp_extra->typalign);

        // Apply expression transformation
        values[i] = ExecEvalExpr(exprstate, econtext, &nulls[i]);

        if (nulls[i]) {
            hasnulls = true;
        } else {
            // Detoast if necessary and calculate size
            if (ret_extra->typlen == -1)
                values[i] = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
            nbytes = att_addlength_datum(nbytes, ret_extra->typlen, values[i]);
            nbytes = att_align_nominal(nbytes, ret_extra->typalign);
        }
    }

    // Build result array
    int32 dataoffset = hasnulls ? ARR_OVERHEAD_WITHNULLS(ndim, nitems) : 0;
    nbytes += hasnulls ? dataoffset : ARR_OVERHEAD_NONULLS(ndim);

    result = (ArrayType *) palloc0(nbytes);
    SET_VARSIZE(result, nbytes);
    result->ndim = ndim;
    result->dataoffset = dataoffset;
    result->elemtype = retType;
    memcpy(ARR_DIMS(result), AARR_DIMS(v), ndim * sizeof(int));
    memcpy(ARR_LBOUND(result), AARR_LBOUND(v), ndim * sizeof(int));

    CopyArrayEls(result, values, nulls, nitems,
                 ret_extra->typlen, ret_extra->typbyval, ret_extra->typalign, false);

    pfree(values);
    pfree(nulls);

    return PointerGetDatum(result);
}
```