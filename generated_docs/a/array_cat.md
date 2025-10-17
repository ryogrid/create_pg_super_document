# array_cat

## Location
[src/backend/utils/adt/array_userfuncs.c:240-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L240-L478)

## Overview
PostgreSQL function that concatenates two n-dimensional arrays to form an n-dimensional array, or pushes an (n-1)-dimensional array onto the end of an n-dimensional array.

## Definition
```c
Datum array_cat(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL array concatenation functionality (|| operator), which combines two arrays into a single array. The function handles multiple concatenation scenarios based on the dimensionality of the input arrays:

1. **Same dimensions (ndims1 == ndims2)**: Concatenates arrays along the first dimension
2. **First array has one less dimension (ndims1 == ndims2 - 1)**: Inserts the first array as an element at the front of the second array
3. **Second array has one less dimension (ndims1 == ndims2 + 1)**: Appends the second array as an element at the end of the first array
4. **Empty array handling**: Returns the non-empty array when one input is empty

The function performs extensive validation to ensure arrays are compatible for concatenation, including element type matching and dimensional consistency. It efficiently handles null bitmaps, data copying, and memory allocation for the result array.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: First array to concatenate (can be null)
  - Argument 1: Second array to concatenate (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_ARRAYTYPE_P
  - PG_RETURN_ARRAYTYPE_P
  - PG_RETURN_NULL
  - ARR_ELEMTYPE
  - ARR_NDIM
  - ARR_LBOUND
  - ARR_DIMS
  - ARR_DATA_PTR
  - ARR_NULLBITMAP
  - ARR_HASNULL
  - ARR_SIZE
  - ARR_DATA_OFFSET
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - SET_VARSIZE
  - [format_type_be](../f/format_type_be.md)
  - [palloc](../p/palloc.md)
  - [palloc0](../p/palloc0.md)
  - memcpy
- Called from (representative examples):
  - SQL array concatenation operations (||)
  - Internal PostgreSQL array operations

## Notes and Other Information
- Supports concatenation of multi-dimensional arrays with complex dimension matching rules
- Provides comprehensive error messages for incompatible array combinations
- Handles null arrays gracefully by treating concatenation with null as a no-op
- Efficiently manages memory allocation and null bitmap copying
- Validates element type compatibility and dimensional constraints
- Supports both arrays with and without null elements
- Critical component of PostgreSQL's array manipulation capabilities
- Returns the result in the standard PostgreSQL ArrayType format

## Simplified Source

```c
Datum array_cat(PG_FUNCTION_ARGS) {
    ArrayType *v1, *v2, *result;
    int ndims1, ndims2, ndims;
    int *dims, *lbs;
    int nitems, ndatabytes, nbytes;
    Oid element_type;

    // Handle null inputs: return the non-null array
    if (PG_ARGISNULL(0)) {
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();
        result = PG_GETARG_ARRAYTYPE_P(1);
        PG_RETURN_ARRAYTYPE_P(result);
    }
    if (PG_ARGISNULL(1)) {
        result = PG_GETARG_ARRAYTYPE_P(0);
        PG_RETURN_ARRAYTYPE_P(result);
    }

    v1 = PG_GETARG_ARRAYTYPE_P(0);
    v2 = PG_GETARG_ARRAYTYPE_P(1);

    // Verify element types match
    if (ARR_ELEMTYPE(v1) != ARR_ELEMTYPE(v2))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("cannot concatenate incompatible arrays")));

    element_type = ARR_ELEMTYPE(v1);
    ndims1 = ARR_NDIM(v1);
    ndims2 = ARR_NDIM(v2);

    // Handle empty arrays: return the non-empty one
    if (ndims1 == 0 && ndims2 > 0)
        PG_RETURN_ARRAYTYPE_P(v2);
    if (ndims2 == 0)
        PG_RETURN_ARRAYTYPE_P(v1);

    // Validate dimension compatibility
    if (ndims1 != ndims2 && ndims1 != ndims2 - 1 && ndims1 != ndims2 + 1)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("cannot concatenate incompatible arrays")));

    // Calculate result dimensions based on input scenarios
    if (ndims1 == ndims2) {
        // Same dimensions: concatenate along first dimension
        ndims = ndims1;
        dims = (int *) palloc(ndims * sizeof(int));
        lbs = (int *) palloc(ndims * sizeof(int));

        dims[0] = ARR_DIMS(v1)[0] + ARR_DIMS(v2)[0];
        lbs[0] = ARR_LBOUND(v1)[0];

        // Copy remaining dimensions (must match)
        for (int i = 1; i < ndims; i++) {
            dims[i] = ARR_DIMS(v1)[i];
            lbs[i] = ARR_LBOUND(v1)[i];
        }
    }
    else if (ndims1 == ndims2 - 1) {
        // Insert v1 as element at front of v2
        ndims = ndims2;
        dims = (int *) palloc(ndims * sizeof(int));
        lbs = (int *) palloc(ndims * sizeof(int));
        memcpy(dims, ARR_DIMS(v2), ndims * sizeof(int));
        memcpy(lbs, ARR_LBOUND(v2), ndims * sizeof(int));
        dims[0] += 1; // Increment outer dimension
    }
    else {
        // Append v2 as element at end of v1
        ndims = ndims1;
        dims = (int *) palloc(ndims * sizeof(int));
        lbs = (int *) palloc(ndims * sizeof(int));
        memcpy(dims, ARR_DIMS(v1), ndims * sizeof(int));
        memcpy(lbs, ARR_LBOUND(v1), ndims * sizeof(int));
        dims[0] += 1; // Increment outer dimension
    }

    // Calculate space needed and build result array
    nitems = ArrayGetNItems(ndims, dims);
    ndatabytes = (ARR_SIZE(v1) - ARR_DATA_OFFSET(v1)) +
                 (ARR_SIZE(v2) - ARR_DATA_OFFSET(v2));

    if (ARR_HASNULL(v1) || ARR_HASNULL(v2)) {
        nbytes = ndatabytes + ARR_OVERHEAD_WITHNULLS(ndims, nitems);
    } else {
        nbytes = ndatabytes + ARR_OVERHEAD_NONULLS(ndims);
    }

    result = (ArrayType *) palloc0(nbytes);
    SET_VARSIZE(result, nbytes);
    result->ndim = ndims;
    result->elemtype = element_type;

    // Copy dimensions, bounds, and data
    memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
    memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));
    memcpy(ARR_DATA_PTR(result), ARR_DATA_PTR(v1), ARR_SIZE(v1) - ARR_DATA_OFFSET(v1));
    memcpy(ARR_DATA_PTR(result) + (ARR_SIZE(v1) - ARR_DATA_OFFSET(v1)),
           ARR_DATA_PTR(v2), ARR_SIZE(v2) - ARR_DATA_OFFSET(v2));

    // Handle null bitmaps if present
    if (ARR_HASNULL(result)) {
        array_bitmap_copy(ARR_NULLBITMAP(result), 0, ARR_NULLBITMAP(v1), 0,
                         ArrayGetNItems(ndims1, ARR_DIMS(v1)));
        array_bitmap_copy(ARR_NULLBITMAP(result), ArrayGetNItems(ndims1, ARR_DIMS(v1)),
                         ARR_NULLBITMAP(v2), 0, ArrayGetNItems(ndims2, ARR_DIMS(v2)));
    }

    PG_RETURN_ARRAYTYPE_P(result);
}
```