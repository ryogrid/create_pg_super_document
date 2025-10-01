# makeArrayResultArr

## Location
[src/backend/utils/adt/arrayfuncs.c:5691-5769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5691-L5769)

## Overview
Produces the final N+1-dimensional array result from an accumulated ArrayBuildStateArr structure.

## Definition

```c
struct_empty_array(astate->element_type);
```
## Detailed Description
This function is the final step in the array-from-arrays building process. It constructs the final ArrayType result from the accumulated data in the ArrayBuildStateArr. The function handles both empty array cases and normal construction, properly managing memory layout including data offset calculations for null bitmaps.

The function performs bounds checking on the array dimensions, computes the required space including overhead for null bitmaps if present, and constructs a properly formatted ArrayType structure. It copies all accumulated data, dimensions, bounds, and null bitmap information into the final result array.

## Parameters / Member Variables
- : Working ArrayBuildStateArr containing accumulated data (must not be NULL)
- : Memory context where the result array should be constructed
- : Whether it's safe to release/delete the working state memory context

## Dependencies
- Functions called/Symbols referenced:
  - [construct_empty_array](../c/construct_empty_array.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [array_bitmap_copy](../a/array_bitmap_copy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - SET_VARSIZE
  - ARR_OVERHEAD_WITHNULLS, ARR_OVERHEAD_NONULLS
  - ARR_DIMS, ARR_LBOUND, ARR_DATA_PTR, ARR_NULLBITMAP
- Called from (representative examples):
  - [array_agg_array_finalfn](../a/array_agg_array_finalfn.md)
  - [makeArrayResultAny](makeArrayResultAny.md)

## Notes and Other Information
- **Empty array handling**: Returns a proper empty array if no inputs were accumulated (ndims == 0)
- **Memory management**: Properly calculates data offset based on presence of null bitmap
- **Bounds checking**: Validates array dimensions don't cause overflow using ArrayCheckBounds
- **Cleanup**: If release=true and astate was created with its own context, deletes the working context
- **Array structure**: Constructs a complete ArrayType with proper VARSIZE, dimensions, bounds, and data
- **Null bitmap**: Copies null bitmap if present, calculating proper overhead
- Part of the three-function API: initArrayResultArr/accumArrayResultArr/makeArrayResultArr
- The resulting array has N+1 dimensions where N is the dimensionality of the input sub-arrays

## Simplified Source

```c
Datum
makeArrayResultArr(ArrayBuildStateArr *astate, MemoryContext rcontext, bool release)
{
    ArrayType *result;
    MemoryContext oldcontext;

    // Switch to result context for array construction
    oldcontext = MemoryContextSwitchTo(rcontext);

    if (astate->ndims == 0) {
        // No inputs - return empty array
        result = construct_empty_array(astate->element_type);
    } else {
        int dataoffset, nbytes;

        // Validate array dimensions don't overflow
        ArrayGetNItems(astate->ndims, astate->dims);
        ArrayCheckBounds(astate->ndims, astate->dims, astate->lbs);

        // Calculate space needed including overhead for nulls
        nbytes = astate->nbytes;
        if (astate->nullbitmap != NULL) {
            dataoffset = ARR_OVERHEAD_WITHNULLS(astate->ndims, astate->nitems);
            nbytes += dataoffset;
        } else {
            dataoffset = 0;
            nbytes += ARR_OVERHEAD_NONULLS(astate->ndims);
        }

        // Allocate and populate result array
        result = (ArrayType *) palloc0(nbytes);
        SET_VARSIZE(result, nbytes);
        result->ndim = astate->ndims;
        result->dataoffset = dataoffset;
        result->elemtype = astate->element_type;

        // Copy dimensions, bounds, and data
        memcpy(ARR_DIMS(result), astate->dims, astate->ndims * sizeof(int));
        memcpy(ARR_LBOUND(result), astate->lbs, astate->ndims * sizeof(int));
        memcpy(ARR_DATA_PTR(result), astate->data, astate->nbytes);

        // Copy null bitmap if present
        if (astate->nullbitmap != NULL)
            array_bitmap_copy(ARR_NULLBITMAP(result), 0, astate->nullbitmap, 0, astate->nitems);
    }

    MemoryContextSwitchTo(oldcontext);

    // Clean up working state if requested
    if (release) {
        Assert(astate->private_cxt);
        MemoryContextDelete(astate->mcontext);
    }

    return PointerGetDatum(result);
}
```