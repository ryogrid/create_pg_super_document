# array_cmp

## Location
[src/backend/utils/adt/arrayfuncs.c:3973-4145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3973-L4145)

## Overview
Internal comparison function for arrays that provides lexicographic ordering by comparing array elements pairwise and handling dimensionality differences.

## Definition

```c
static int
array_cmp(FunctionCallInfo fcinfo)
```
## Detailed Description
The  function implements a comprehensive comparison algorithm for PostgreSQL arrays. It performs element-by-element comparison using the appropriate comparison function for the array's element type, following lexicographic ordering principles. When arrays have identical elements up to the length of the shorter array, it applies additional rules based on array dimensionality, bounds, and lower bounds to establish a total ordering.

The function handles NULL values by treating two NULLs as equal and considering NULL greater than any non-NULL value. It uses the type cache system to efficiently look up and cache comparison functions for the array element type.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
## Dependencies
- Functions called/Symbols referenced:
  -  - Get number of dimensions
  -  - Get dimension sizes array
  -  - Get element type OID
  -  - Get lower bounds array
  -  - Calculate total number of items
  -  - Get cached type information
  -  - [Initialize](../I/Initialize.md) array iterator
  -  - Get next array element
  -  - Call element comparison function
  -  - Free detoasted array copies

- Called from (representative examples):
  -  - Array less-than operator
  -  - Array greater-than operator
  -  - Array less-than-or-equal operator
  -  - Array greater-than-or-equal operator
  -  - B-tree comparison support function
  -  - Return larger of two arrays
  -  - Return smaller of two arrays

## Notes and Other Information
- Returns -1 (first array is smaller), 0 (arrays are equal), or 1 (first array is larger)
- Requires arrays to have the same element type; raises error for type mismatches
- Uses cached type information to avoid repeated function lookups during index operations
- Comparison hierarchy: element values → number of items → number of dimensions → dimension sizes → lower bounds
- Handles toasted arrays properly by freeing detoasted copies to prevent memory leaks
- NULL handling follows PostgreSQL's standard semantics where NULL > any non-NULL value

## Simplified Source

```c
static int
array_cmp(FunctionCallInfo fcinfo)
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
    int nitems1 = ArrayGetNItems(ndims1, dims1);
    int nitems2 = ArrayGetNItems(ndims2, dims2);
    Oid element_type = AARR_ELEMTYPE(array1);

    // Validate element types match
    if (element_type != AARR_ELEMTYPE(array2))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("cannot compare arrays of different element types")));

    // Get cached comparison function for element type
    TypeCacheEntry *typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
    if (typentry == NULL || typentry->type_id != element_type)
    {
        typentry = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);
        if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("could not identify a comparison function for type %s",
                                  format_type_be(element_type))));
        fcinfo->flinfo->fn_extra = (void *) typentry;
    }

    // Setup element iteration and comparison
    InitFunctionCallInfoData(*locfcinfo, &typentry->cmp_proc_finfo, 2,
                            collation, NULL, NULL);

    int min_nitems = Min(nitems1, nitems2);
    array_iter it1, it2;
    array_iter_setup(&it1, array1);
    array_iter_setup(&it2, array2);

    // Compare each element pair lexicographically
    for (int i = 0; i < min_nitems; i++)
    {
        bool isnull1, isnull2;
        Datum elt1 = array_iter_next(&it1, &isnull1, i,
                                    typentry->typlen, typentry->typbyval, typentry->typalign);
        Datum elt2 = array_iter_next(&it2, &isnull2, i,
                                    typentry->typlen, typentry->typbyval, typentry->typalign);

        // Handle NULL comparisons: NULL == NULL; NULL > non-NULL
        if (isnull1 && isnull2)
            continue;
        if (isnull1) {
            AARR_FREE_IF_COPY(array1, 0);
            AARR_FREE_IF_COPY(array2, 1);
            return 1;  // array1 > array2
        }
        if (isnull2) {
            AARR_FREE_IF_COPY(array1, 0);
            AARR_FREE_IF_COPY(array2, 1);
            return -1; // array1 < array2
        }

        // Call comparison function for elements
        locfcinfo->args[0].value = elt1;
        locfcinfo->args[0].isnull = false;
        locfcinfo->args[1].value = elt2;
        locfcinfo->args[1].isnull = false;
        int32 cmpresult = DatumGetInt32(FunctionCallInvoke(locfcinfo));

        if (cmpresult != 0) {
            AARR_FREE_IF_COPY(array1, 0);
            AARR_FREE_IF_COPY(array2, 1);
            return (cmpresult < 0) ? -1 : 1;
        }
    }

    // Elements are equal up to shorter array length
    // Compare by: number of items → dimensions → dimension sizes → lower bounds
    int result = 0;
    if (nitems1 != nitems2)
        result = (nitems1 < nitems2) ? -1 : 1;
    else if (ndims1 != ndims2)
        result = (ndims1 < ndims2) ? -1 : 1;
    else {
        // Compare dimension sizes
        for (int i = 0; i < ndims1; i++) {
            if (dims1[i] != dims2[i]) {
                result = (dims1[i] < dims2[i]) ? -1 : 1;
                break;
            }
        }
        // Compare lower bounds if dimensions are identical
        if (result == 0) {
            int *lbound1 = AARR_LBOUND(array1);
            int *lbound2 = AARR_LBOUND(array2);
            for (int i = 0; i < ndims1; i++) {
                if (lbound1[i] != lbound2[i]) {
                    result = (lbound1[i] < lbound2[i]) ? -1 : 1;
                    break;
                }
            }
        }
    }

    AARR_FREE_IF_COPY(array1, 0);
    AARR_FREE_IF_COPY(array2, 1);
    return result;
}
```