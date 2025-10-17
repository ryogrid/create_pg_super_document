# array_set_slice

## Location
[src/backend/utils/adt/arrayfuncs.c:2806-3145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L2806-L3145)

## Overview
Implements slice assignment operations on PostgreSQL arrays, allowing assignment of values to a range of array positions specified by upper and lower bounds.

## Definition

```c
struct_array(srcArray, elmtype, elmlen, elmbyval, elmalign,
						  &dvalues, &dnulls, &nelems);
```
## Detailed Description
This function performs slice assignment operations on PostgreSQL arrays, replacing a range of elements (defined by upper and lower subscript boundaries) with values from a source array. Key features include:

1. **Flexible slicing**: Supports both full and partial boundary specification - omitted bounds default to array limits
2. **Array extension**: For one-dimensional arrays, allows assignment beyond existing bounds, automatically extending the array and filling gaps with NULLs
3. **Multi-dimensional support**: Handles multi-dimensional arrays but restricts extension to prevent complexity
4. **Memory management**: Creates a new array rather than modifying the original, ensuring immutability
5. **Type safety**: Validates array dimensions, bounds, and source array size compatibility

The function handles both ordinary varlena arrays and provides comprehensive bounds checking with overflow protection.

## Parameters / Member Variables
- : The target array to be modified (must not be NULL)
- : Number of subscript dimensions provided
- : Array of upper boundary subscript values for the slice
- : Array of lower boundary subscript values for the slice  
- : Boolean flags indicating which upper boundaries are explicitly provided
- : Boolean flags indicating which lower boundaries are explicitly provided
- : Source array containing the replacement values
- : Indicates whether the source array is NULL (results in no-op)
- : Type length for the array type (pg_type.typlen)
- : Type length for individual array elements
- : Whether array elements are passed by value
- : Alignment requirement for array elements

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - [deconstruct_array](../d/deconstruct_array.md)
  - [construct_md_array](../c/construct_md_array.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [mda_get_range](../m/mda_get_range.md)
  - [array_nelems_size](array_nelems_size.md)
  - [array_slice_size](array_slice_size.md)
  - [array_insert_slice](array_insert_slice.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
- Called from:
  - [array_subscript_assign_slice](array_subscript_assign_slice.md)

## Notes and Other Information
- Fixed-length arrays are not currently supported for slice operations
- Assignment from NULL source arrays is treated as a no-op
- Multi-dimensional arrays cannot be extended during slice assignment (restriction may be lifted in future)
- Uses overflow-safe arithmetic to prevent integer overflow when calculating new array dimensions
- For one-dimensional arrays, automatically fills gaps between existing elements and new slice boundaries with NULLs
- The function assumes it's safe to modify the provided index arrays (lowerIndx[], upperIndx[])
- Index arrays must be sized to MAXDIM even when fewer subscripts are used
- Source array size validation ensures sufficient elements are available for the slice assignment
- Located in src/backend/utils/adt/arrayfuncs.c

## Simplified Source

```c
Datum
array_set_slice(Datum arraydatum, int nSubscripts,
                int *upperIndx, int *lowerIndx,
                bool *upperProvided, bool *lowerProvided,
                Datum srcArrayDatum, bool isNull,
                int arraytyplen, int elmlen, bool elmbyval, char elmalign)
{
    ArrayType *array, *srcArray, *newarray;
    int i, ndim, dim[MAXDIM], lb[MAXDIM], span[MAXDIM];
    bool newhasnulls;
    int nitems, nsrcitems, newsize, overheadlen;
    int addedbefore = 0, addedafter = 0;

    // NULL source array is a no-op
    if (isNull)
        return arraydatum;

    // Fixed-length arrays not supported
    if (arraytyplen > 0)
        ereport(ERROR, /* feature not supported */);

    // Detoast arrays
    array = DatumGetArrayTypeP(arraydatum);
    srcArray = DatumGetArrayTypeP(srcArrayDatum);
    ndim = ARR_NDIM(array);

    // Handle empty array case - create new array with specified dimensions
    if (ndim == 0) {
        Datum *dvalues;
        bool *dnulls;
        int nelems;
        Oid elmtype = ARR_ELEMTYPE(array);

        deconstruct_array(srcArray, elmtype, elmlen, elmbyval, elmalign,
                         &dvalues, &dnulls, &nelems);

        // Validate that all bounds are provided for empty array assignment
        for (i = 0; i < nSubscripts; i++) {
            if (!upperProvided[i] || !lowerProvided[i])
                ereport(ERROR, /* slice boundaries must be fully specified */);

            // Calculate dimension size with overflow checking
            if (pg_sub_s32_overflow(upperIndx[i], lowerIndx[i], &dim[i]) ||
                pg_add_s32_overflow(dim[i], 1, &dim[i]))
                ereport(ERROR, /* array size exceeds maximum */);

            lb[i] = lowerIndx[i];
        }

        // Ensure source has enough elements
        if (nelems < ArrayGetNItems(nSubscripts, dim))
            ereport(ERROR, /* source array too small */);

        return PointerGetDatum(construct_md_array(dvalues, dnulls, nSubscripts,
                                                 dim, lb, elmtype,
                                                 elmlen, elmbyval, elmalign));
    }

    // Validate dimensions
    if (ndim < nSubscripts || ndim <= 0 || ndim > MAXDIM)
        ereport(ERROR, /* wrong number of array subscripts */);

    // Copy dimensions and bounds
    memcpy(dim, ARR_DIMS(array), ndim * sizeof(int));
    memcpy(lb, ARR_LBOUND(array), ndim * sizeof(int));
    newhasnulls = (ARR_HASNULL(array) || ARR_HASNULL(srcArray));

    // Handle subscript validation and array extension (1D only)
    if (ndim == 1) {
        // Fill missing bounds
        if (!lowerProvided[0]) lowerIndx[0] = lb[0];
        if (!upperProvided[0]) upperIndx[0] = dim[0] + lb[0] - 1;

        if (lowerIndx[0] > upperIndx[0])
            ereport(ERROR, /* upper bound cannot be less than lower bound */);

        // Extend array if needed (1D only)
        if (lowerIndx[0] < lb[0]) {
            if (pg_sub_s32_overflow(lb[0], lowerIndx[0], &addedbefore) ||
                pg_add_s32_overflow(dim[0], addedbefore, &dim[0]))
                ereport(ERROR, /* array size exceeds maximum */);
            lb[0] = lowerIndx[0];
            if (addedbefore > 1) newhasnulls = true;
        }

        if (upperIndx[0] >= (dim[0] + lb[0])) {
            if (pg_sub_s32_overflow(upperIndx[0], dim[0] + lb[0], &addedafter) ||
                pg_add_s32_overflow(addedafter, 1, &addedafter) ||
                pg_add_s32_overflow(dim[0], addedafter, &dim[0]))
                ereport(ERROR, /* array size exceeds maximum */);
            if (addedafter > 1) newhasnulls = true;
        }
    } else {
        // Multi-dimensional arrays: no extension allowed
        for (i = 0; i < nSubscripts; i++) {
            if (!lowerProvided[i]) lowerIndx[i] = lb[i];
            if (!upperProvided[i]) upperIndx[i] = dim[i] + lb[i] - 1;

            if (lowerIndx[i] > upperIndx[i])
                ereport(ERROR, /* upper bound cannot be less than lower bound */);
            if (lowerIndx[i] < lb[i] || upperIndx[i] >= (dim[i] + lb[i]))
                ereport(ERROR, /* array subscript out of range */);
        }

        // Fill missing subscript positions
        for (; i < ndim; i++) {
            lowerIndx[i] = lb[i];
            upperIndx[i] = dim[i] + lb[i] - 1;
        }
    }

    // Validate array bounds and source array size
    nitems = ArrayGetNItems(ndim, dim);
    ArrayCheckBounds(ndim, dim, lb);
    mda_get_range(ndim, span, lowerIndx, upperIndx);
    nsrcitems = ArrayGetNItems(ndim, span);

    if (nsrcitems > ArrayGetNItems(ARR_NDIM(srcArray), ARR_DIMS(srcArray)))
        ereport(ERROR, /* source array too small */);

    // Calculate new array size and create result
    if (newhasnulls)
        overheadlen = ARR_OVERHEAD_WITHNULLS(ndim, nitems);
    else
        overheadlen = ARR_OVERHEAD_NONULLS(ndim);

    newsize = overheadlen + /* calculate data size based on old + new elements */;

    newarray = (ArrayType *) palloc0(newsize);
    SET_VARSIZE(newarray, newsize);
    newarray->ndim = ndim;
    newarray->dataoffset = newhasnulls ? overheadlen : 0;
    newarray->elemtype = ARR_ELEMTYPE(array);
    memcpy(ARR_DIMS(newarray), dim, ndim * sizeof(int));
    memcpy(ARR_LBOUND(newarray), lb, ndim * sizeof(int));

    // Insert slice data using appropriate method
    if (ndim > 1) {
        array_insert_slice(newarray, array, srcArray,
                          ndim, dim, lb, lowerIndx, upperIndx,
                          elmlen, elmbyval, elmalign);
    } else {
        // Handle 1D case with data copying and null bitmap management
        /* Copy data and manage null bitmaps */
    }

    return PointerGetDatum(newarray);
}
```