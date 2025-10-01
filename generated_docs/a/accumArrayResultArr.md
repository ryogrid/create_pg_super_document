# accumArrayResultArr

## Location
[src/backend/utils/adt/arrayfuncs.c:5538-5690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5538-L5690)

## Overview
Accumulates one sub-array into an ArrayBuildStateArr structure, building up data for creating a multi-dimensional array result.

## Definition

```c
ArrayBuildStateArr *
accumArrayResultArr(ArrayBuildStateArr *astate,
					Datum dvalue, bool disnull,
					Oid array_type,
					MemoryContext rcontext)
```
## Detailed Description
This function is the core accumulation function for building arrays from arrays. It takes an input sub-array and adds it to the working state, ensuring all sub-arrays have consistent dimensionality. The function handles memory management, including dynamic expansion of data and null bitmap storage as needed.

On the first call (when astate is NULL), it initializes the working state and establishes the dimensionality pattern that all subsequent inputs must match. For subsequent calls, it validates that new inputs match the established pattern and accumulates their data.

The function manages both the actual array data and null bitmaps, handling cases where some sub-arrays have nulls and others don't. It automatically expands storage as needed using power-of-2 growth for efficiency.

## Parameters / Member Variables
- : Working ArrayBuildStateArr state (can be NULL on first call, will be created)
- : Datum containing the new sub-array to append
- : Boolean indicating if the sub-array value is null (causes error if true)
- : OID of the array type (must be valid varlena array type)
- : Memory context for keeping working state

## Dependencies
- Functions called/Symbols referenced:
  - [initArrayResultArr](../i/initArrayResultArr.md)
  - DatumGetArrayTypeP
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - [repalloc](../r/repalloc.md)
  - ARR_NDIM, ARR_DIMS, ARR_LBOUND, ARR_DATA_PTR
  - ARR_HASNULL, ARR_NULLBITMAP
- Called from (representative examples):
  - [array_agg_array_transfn](array_agg_array_transfn.md)
  - [accumArrayResultAny](accumArrayResultAny.md)

## Notes and Other Information
- **Error conditions**: 
  - Null sub-arrays are not allowed and will cause an error
  - Empty arrays cannot be accumulated
  - All sub-arrays must have identical dimensionality
  - Exceeding MAXDIM dimensions will cause an error
- **Memory management**: Uses power-of-2 growth for both data storage and null bitmap storage
- **Dimensionality**: The output array will have N+1 dimensions where N is the dimensionality of input sub-arrays
- **Performance**: Automatically handles memory expansion and detoasting of input arrays
- **Null handling**: Retrospectively handles null bitmaps when the first array with nulls is encountered
- Part of the three-function API: initArrayResultArr/accumArrayResultArr/makeArrayResultArr

## Simplified Source

```c
ArrayBuildStateArr *accumArrayResultArr(ArrayBuildStateArr *astate,
                                       Datum dvalue, bool disnull,
                                       Oid array_type, MemoryContext rcontext) {
    // Null sub-arrays not allowed
    if (disnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                       errmsg("cannot accumulate null arrays")));

    // Detoast input array
    ArrayType *arg = DatumGetArrayTypeP(dvalue);

    // Initialize state on first call
    if (astate == NULL)
        astate = initArrayResultArr(array_type, InvalidOid, rcontext, true);

    MemoryContext oldcontext = MemoryContextSwitchTo(astate->mcontext);

    // Extract array metadata
    int ndims = ARR_NDIM(arg);
    int *dims = ARR_DIMS(arg);
    int *lbs = ARR_LBOUND(arg);
    char *data = ARR_DATA_PTR(arg);
    int nitems = ArrayGetNItems(ndims, dims);
    int ndatabytes = ARR_SIZE(arg) - ARR_DATA_OFFSET(arg);

    if (astate->ndims == 0) {
        // First input - establish dimensionality pattern
        if (ndims == 0 || ndims + 1 > MAXDIM)
            ereport(ERROR, (...)); // Error for empty/too many dimensions

        // Output array has n+1 dimensions
        astate->ndims = ndims + 1;
        astate->dims[0] = 0;
        memcpy(&astate->dims[1], dims, ndims * sizeof(int));
        astate->lbs[0] = 1;
        memcpy(&astate->lbs[1], lbs, ndims * sizeof(int));

        // Allocate initial data space
        astate->abytes = pg_nextpower2_32(Max(1024, ndatabytes + 1));
        astate->data = (char *) palloc(astate->abytes);
    } else {
        // Subsequent inputs - must match first input's dimensionality
        if (astate->ndims != ndims + 1)
            ereport(ERROR, (...)); // Dimensionality mismatch

        // Validate all dimensions match
        for (int i = 0; i < ndims; i++) {
            if (astate->dims[i + 1] != dims[i] || astate->lbs[i + 1] != lbs[i])
                ereport(ERROR, (...)); // Dimension mismatch
        }

        // Expand data space if needed
        if (astate->nbytes + ndatabytes > astate->abytes) {
            astate->abytes = Max(astate->abytes * 2, astate->nbytes + ndatabytes);
            astate->data = (char *) repalloc(astate->data, astate->abytes);
        }
    }

    // Copy array data
    memcpy(astate->data + astate->nbytes, data, ndatabytes);
    astate->nbytes += ndatabytes;

    // Handle null bitmap if needed
    if (astate->nullbitmap || ARR_HASNULL(arg)) {
        int newnitems = astate->nitems + nitems;

        if (astate->nullbitmap == NULL) {
            // First array with nulls - retrospectively handle previous items
            astate->aitems = pg_nextpower2_32(Max(256, newnitems + 1));
            astate->nullbitmap = (bits8 *) palloc((astate->aitems + 7) / 8);
            array_bitmap_copy(astate->nullbitmap, 0, NULL, 0, astate->nitems);
        } else if (newnitems > astate->aitems) {
            // Expand null bitmap
            astate->aitems = Max(astate->aitems * 2, newnitems);
            astate->nullbitmap = (bits8 *) repalloc(astate->nullbitmap,
                                                   (astate->aitems + 7) / 8);
        }
        array_bitmap_copy(astate->nullbitmap, astate->nitems,
                         ARR_NULLBITMAP(arg), 0, nitems);
    }

    // Update counters
    astate->nitems += nitems;
    astate->dims[0] += 1;

    MemoryContextSwitchTo(oldcontext);

    // Clean up detoasted copy
    if ((Pointer) arg != DatumGetPointer(dvalue))
        pfree(arg);

    return astate;
}
```