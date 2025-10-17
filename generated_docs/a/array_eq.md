# array_eq

## Location
[src/backend/utils/adt/arrayfuncs.c:3802-3930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3802-L3930)

## Overview
Compares two PostgreSQL arrays for complete equality, including dimensions, bounds, and element-by-element comparison using the appropriate equality operator for the element type.

## Definition

```c
Datum
array_eq(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements comprehensive array equality comparison for PostgreSQL arrays. It performs a multi-stage comparison process: first checking array metadata (element types, dimensions, dimension sizes, and lower bounds), then performing element-by-element comparison using the appropriate equality operator for the element type.

The function is designed to work with any array element type that has a defined equality operator, making it more general than array comparison functions that require total ordering. It uses PostgreSQL's type cache system to efficiently look up and cache the equality operator for the element type, avoiding repeated operator lookups in scenarios where the function is called multiple times with the same element type.

The function handles null elements specially: two null elements are considered equal, but a null element and a non-null element are not equal. The comparison short-circuits as soon as any inequality is found, making it efficient for arrays that differ early in the comparison process.

## Parameters / Member Variables
- Function receives two array arguments via  macro:
  - : First array to compare (argument 0)
  - : Second array to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ANY_ARRAY_P (extracts array arguments)
  - PG_GET_COLLATION (gets collation for comparison)
  - AARR_NDIM, AARR_DIMS, AARR_LBOUND (array metadata access macros)
  - AARR_ELEMTYPE (gets element type)
  - [lookup_type_cache](../l/lookup_type_cache.md) (looks up type information and operators)
  - TYPECACHE_EQ_OPR_FINFO (type cache flag for equality operator)
  - InitFunctionCallInfoData (initializes function call structure)
  - [ArrayGetNItems](../A/ArrayGetNItems.md) (calculates total number of elements)
  - [array_iter_setup](array_iter_setup.md), array_iter_next (array iteration functions)
  - FunctionCallInvoke (invokes the equality operator)
  - AARR_FREE_IF_COPY (memory cleanup for toasted arrays)
  - LOCAL_FCINFO (local function call info structure)
  - AnyArrayType (generalized array type)

- Called from (representative examples):
  - [array_ne](array_ne.md) (array inequality function uses this as basis)

## Notes and Other Information
- Does not use array_cmp for comparison, since equality can be meaningful for types without total ordering
- Implements fast-path optimization: returns false immediately if array metadata differs (dimensions, sizes, bounds)
- Uses function info extra space to cache TypeCacheEntry across multiple calls for performance
- Performs element type validation and throws error for arrays with different element types
- Handles memory management by freeing toasted input arrays to prevent memory leaks
- NULL handling follows SQL semantics: NULL = NULL is true, NULL = not-NULL is false
- Short-circuits on first inequality found, making it efficient for dissimilar arrays
- Uses collation-aware comparison when the element type requires it
- Returns a boolean Datum value using PG_RETURN_BOOL macro
- Throws specific errors for unsupported element types (no equality operator) and type mismatches
- Critical function for array indexing, joins, and WHERE clause operations involving arrays

## Simplified Source

```c
Datum
array_eq(PG_FUNCTION_ARGS)
{
    LOCAL_FCINFO(locfcinfo, 2);
    AnyArrayType *array1 = PG_GETARG_ANY_ARRAY_P(0);
    AnyArrayType *array2 = PG_GETARG_ANY_ARRAY_P(1);
    Oid collation = PG_GET_COLLATION();

    // Extract array metadata
    int ndims1 = AARR_NDIM(array1);
    int ndims2 = AARR_NDIM(array2);
    int *dims1 = AARR_DIMS(array1);
    int *dims2 = AARR_DIMS(array2);
    int *lbs1 = AARR_LBOUND(array1);
    int *lbs2 = AARR_LBOUND(array2);
    Oid element_type = AARR_ELEMTYPE(array1);

    // Validate element types match
    if (element_type != AARR_ELEMTYPE(array2))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("cannot compare arrays of different element types")));

    // Fast path: check if array structures differ
    if (ndims1 != ndims2 ||
        memcmp(dims1, dims2, ndims1 * sizeof(int)) != 0 ||
        memcmp(lbs1, lbs2, ndims1 * sizeof(int)) != 0)
    {
        AARR_FREE_IF_COPY(array1, 0);
        AARR_FREE_IF_COPY(array2, 1);
        PG_RETURN_BOOL(false);
    }

    // Get cached equality operator for element type
    TypeCacheEntry *typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
    if (typentry == NULL || typentry->type_id != element_type)
    {
        typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);
        if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("could not identify an equality operator for type %s",
                                  format_type_be(element_type))));
        fcinfo->flinfo->fn_extra = (void *) typentry;
    }

    // Setup element iteration and comparison
    InitFunctionCallInfoData(*locfcinfo, &typentry->eq_opr_finfo, 2,
                            collation, NULL, NULL);

    int nitems = ArrayGetNItems(ndims1, dims1);
    array_iter it1, it2;
    array_iter_setup(&it1, array1);
    array_iter_setup(&it2, array2);

    // Compare each element pair
    for (int i = 0; i < nitems; i++)
    {
        bool isnull1, isnull2;
        Datum elt1 = array_iter_next(&it1, &isnull1, i,
                                    typentry->typlen, typentry->typbyval, typentry->typalign);
        Datum elt2 = array_iter_next(&it2, &isnull2, i,
                                    typentry->typlen, typentry->typbyval, typentry->typalign);

        // Handle NULL comparisons: NULL == NULL is true, NULL != non-NULL is false
        if (isnull1 && isnull2)
            continue;
        if (isnull1 || isnull2)
        {
            AARR_FREE_IF_COPY(array1, 0);
            AARR_FREE_IF_COPY(array2, 1);
            PG_RETURN_BOOL(false);
        }

        // Call equality operator for elements
        locfcinfo->args[0].value = elt1;
        locfcinfo->args[0].isnull = false;
        locfcinfo->args[1].value = elt2;
        locfcinfo->args[1].isnull = false;
        locfcinfo->isnull = false;

        bool oprresult = DatumGetBool(FunctionCallInvoke(locfcinfo));
        if (locfcinfo->isnull || !oprresult)
        {
            AARR_FREE_IF_COPY(array1, 0);
            AARR_FREE_IF_COPY(array2, 1);
            PG_RETURN_BOOL(false);
        }
    }

    // All elements equal
    AARR_FREE_IF_COPY(array1, 0);
    AARR_FREE_IF_COPY(array2, 1);
    PG_RETURN_BOOL(true);
}
```