# array_get_slice

## Location
[src/backend/utils/adt/arrayfuncs.c:2030-2200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L2030-L2200)

## Overview
Extracts a slice (subarray) from an array using upper and lower bounds for each dimension, creating a new array structure containing the specified range of elements.

## Definition
```c
Datum array_get_slice(Datum arraydatum,
                     int nSubscripts,
                     int *upperIndx,
                     int *lowerIndx,
                     bool *upperProvided,
                     bool *lowerProvided,
                     int arraytyplen,
                     int elmlen,
                     bool elmbyval,
                     char elmalign)
```

## Detailed Description
The `array_get_slice` function creates a new array containing a specified slice of an existing array. It handles both ordinary varlena arrays and fixed-length arrays (though fixed-length array slicing is currently not implemented and raises an error). The function accepts upper and lower bounds for each dimension, with optional provision flags indicating which bounds are explicitly specified. Missing bounds are replaced with corresponding array limits. The resulting slice has lower bounds reset to 1 for all dimensions. The function performs bounds checking, truncates slices that exceed array limits, and returns empty arrays for invalid ranges.

## Parameters / Member Variables
- `arraydatum`: The source array object (must not be NULL)
- `nSubscripts`: Number of subscripts supplied (must be same for upper/lower bounds)
- `upperIndx[]`: Array of upper subscript values for each dimension
- `lowerIndx[]`: Array of lower subscript values for each dimension
- `upperProvided[]`: Boolean flags indicating which upper subscripts are explicitly provided
- `lowerProvided[]`: Boolean flags indicating which lower subscripts are explicitly provided
- `arraytyplen`: pg_type.typlen for the array type
- `elmlen`: pg_type.typlen for the array's element type
- `elmbyval`: pg_type.typbyval for the array's element type
- `elmalign`: pg_type.typalign for the array's element type

## Dependencies
- Functions called/Symbols referenced:
  - `DatumGetArrayTypeP`: Converts datum to ArrayType pointer
  - `ARR_NDIM`, `ARR_DIMS`, `ARR_LBOUND`, `ARR_ELEMTYPE`: Array metadata accessors
  - `ARR_DATA_PTR`, `ARR_NULLBITMAP`: Array data accessors
  - [construct_empty_array](../c/construct_empty_array.md): Creates empty array for invalid slices
  - [mda_get_range](../m/mda_get_range.md): Calculates span dimensions from bounds
  - [array_slice_size](array_slice_size.md): Calculates required memory for slice data
  - `[ArrayGetNItems](../A/ArrayGetNItems.md)`: Calculates total number of items
  - `ARR_OVERHEAD_WITHNULLS`, `ARR_OVERHEAD_NONULLS`: Array header size calculations
  - [array_extract_slice](array_extract_slice.md): Extracts actual slice data into new array
  - `SET_VARSIZE`: Sets variable-length header size
- Called from (representative examples):
  - [array_subscript_fetch_slice](array_subscript_fetch_slice.md): Array slice subscripting operations
  - [trim_array](../t/trim_array.md): Array trimming operations
  - [array_subscript_fetch_old_slice](array_subscript_fetch_old_slice.md): Legacy slice operations

## Notes and Other Information
- Always returns a valid Datum (never NULL), creating empty arrays for invalid ranges
- Fixed-length array slicing is not implemented and throws a FEATURE_NOT_SUPPORTED error
- Silently truncates slices that exceed current array limits to fit within bounds
- Resulting slice arrays always have lower bounds set to 1 regardless of source array bounds
- Assumes it's safe to modify the provided subscript arrays (lowerIndx and upperIndx)
- Subscript arrays must be of size MAXDIM even when nSubscripts is smaller
- Preserves null bitmap structure from source array in the result
- Critical function for PostgreSQL's array slicing infrastructure
- Located in `src/backend/utils/adt/arrayfuncs.c` at lines 2030-2200

## Simplified Source

```c
Datum
array_get_slice(Datum arraydatum, int nSubscripts,
                int *upperIndx, int *lowerIndx,
                bool *upperProvided, bool *lowerProvided,
                int arraytyplen, int elmlen, bool elmbyval, char elmalign)
{
    ArrayType *array;
    ArrayType *newarray;
    int i, ndim, *dim, *lb, *newlb;
    Oid elemtype;
    char *arraydataptr;
    bits8 *arraynullsptr;
    int32 dataoffset;
    int bytes, span[MAXDIM];

    // Fixed-length arrays not supported
    if (arraytyplen > 0)
        ereport(ERROR, /* feature not supported */);

    // Detoast and extract array metadata
    array = DatumGetArrayTypeP(arraydatum);
    ndim = ARR_NDIM(array);
    dim = ARR_DIMS(array);
    lb = ARR_LBOUND(array);
    elemtype = ARR_ELEMTYPE(array);
    arraydataptr = ARR_DATA_PTR(array);
    arraynullsptr = ARR_NULLBITMAP(array);

    // Validate dimensions and return empty array if invalid
    if (ndim < nSubscripts || ndim <= 0 || ndim > MAXDIM)
        return PointerGetDatum(construct_empty_array(elemtype));

    // Process subscripts and validate bounds
    for (i = 0; i < nSubscripts; i++) {
        // Use array bounds if not provided
        if (!lowerProvided[i] || lowerIndx[i] < lb[i])
            lowerIndx[i] = lb[i];
        if (!upperProvided[i] || upperIndx[i] >= (dim[i] + lb[i]))
            upperIndx[i] = dim[i] + lb[i] - 1;

        // Return empty array for invalid ranges
        if (lowerIndx[i] > upperIndx[i])
            return PointerGetDatum(construct_empty_array(elemtype));
    }

    // Fill missing subscript positions with full array range
    for (; i < ndim; i++) {
        lowerIndx[i] = lb[i];
        upperIndx[i] = dim[i] + lb[i] - 1;
        if (lowerIndx[i] > upperIndx[i])
            return PointerGetDatum(construct_empty_array(elemtype));
    }

    // Calculate slice dimensions
    mda_get_range(ndim, span, lowerIndx, upperIndx);

    // Calculate required memory for slice data
    bytes = array_slice_size(arraydataptr, arraynullsptr,
                            ndim, dim, lb,
                            lowerIndx, upperIndx,
                            elmlen, elmbyval, elmalign);

    // Calculate total array size including overhead
    if (arraynullsptr) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, ArrayGetNItems(ndim, span));
        bytes += dataoffset;
    } else {
        dataoffset = 0;  // no null bitmap
        bytes += ARR_OVERHEAD_NONULLS(ndim);
    }

    // Create new array structure
    newarray = (ArrayType *) palloc0(bytes);
    SET_VARSIZE(newarray, bytes);
    newarray->ndim = ndim;
    newarray->dataoffset = dataoffset;
    newarray->elemtype = elemtype;
    memcpy(ARR_DIMS(newarray), span, ndim * sizeof(int));

    // Set lower bounds to 1 for all dimensions
    newlb = ARR_LBOUND(newarray);
    for (i = 0; i < ndim; i++)
        newlb[i] = 1;

    // Extract slice data into new array
    array_extract_slice(newarray,
                       ndim, dim, lb,
                       arraydataptr, arraynullsptr,
                       lowerIndx, upperIndx,
                       elmlen, elmbyval, elmalign);

    return PointerGetDatum(newarray);
}
```