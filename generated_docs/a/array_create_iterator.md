# array_create_iterator

## Location
[src/backend/utils/adt/arrayfuncs.c:4585-4663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4585-L4663)

## Overview
This function creates and initializes an ArrayIterator structure for efficiently traversing through PostgreSQL arrays, supporting both element-by-element and slice-based iteration modes.

## Definition

```c
ArrayIterator
array_create_iterator(ArrayType *arr, int slice_ndim, ArrayMetaState *mstate)
```
## Detailed Description
The  function initializes an iterator object for traversing arrays in PostgreSQL. It supports two iteration modes: element-by-element (when slice_ndim is 0) where individual elements are returned, and slice-based iteration (when slice_ndim > 0) where sub-arrays of the rightmost N dimensions are returned. The function allocates and configures all necessary data structures including workspace for building sub-arrays when operating in slice mode. It also handles type information either from the provided ArrayMetaState or by looking it up from the system catalogs.

## Parameters / Member Variables
- `*arr`: Pointer to the PostgreSQL array to iterate over (must remain valid for iterator lifetime)
- `slice_ndim`: Number of dimensions for slicing (0 for element iteration, 1-ARR_NDIM for slice iteration)
- `*mstate`: Optional pre-computed array metadata state containing type information (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - : Allocates and zeroes memory for the iterator structure
  - : Validates input pointer
  - : Gets number of array dimensions
  - : Gets null bitmap from array
  - : Calculates total number of items in array dimensions
  - : Gets array element type
  - : Retrieves type length, by-value flag, and alignment
  - : Gets array dimension information
  - : Gets array lower bounds
  - : Gets pointer to array data
- Called from (representative examples):
  - : For finding element positions in arrays
  - : For finding all positions of elements in arrays

## Notes and Other Information
- The iterator must be freed using  to prevent memory leaks
- When slice_ndim > 0, the function allocates workspace arrays ( and ) for constructing sub-arrays
- The passed array must remain valid for the entire lifetime of the iterator
- Performs sanity checks on slice_ndim parameter to ensure it's within valid bounds
- Supports both cases where ArrayMetaState is pre-computed (for efficiency) or looked up dynamically
- Sets up all necessary pointers and counters for subsequent iteration via

## Simplified Source

```c
ArrayIterator
array_create_iterator(ArrayType *arr, int slice_ndim, ArrayMetaState *mstate)
{
    ArrayIterator iterator = palloc0(sizeof(ArrayIteratorData));

    // Validate input parameters
    Assert(PointerIsValid(arr));
    if (slice_ndim < 0 || slice_ndim > ARR_NDIM(arr))
        elog(ERROR, "invalid arguments to array_create_iterator");

    // Store basic array information
    iterator->arr = arr;
    iterator->nullbitmap = ARR_NULLBITMAP(arr);
    iterator->nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

    // Get type information from mstate or lookup
    if (mstate != NULL) {
        iterator->typlen = mstate->typlen;
        iterator->typbyval = mstate->typbyval;
        iterator->typalign = mstate->typalign;
    } else {
        get_typlenbyvalalign(ARR_ELEMTYPE(arr),
                           &iterator->typlen,
                           &iterator->typbyval,
                           &iterator->typalign);
    }

    // Setup slicing parameters
    iterator->slice_ndim = slice_ndim;

    if (slice_ndim > 0) {
        // Configure slice dimensions and bounds (rightmost N dimensions)
        iterator->slice_dims = ARR_DIMS(arr) + ARR_NDIM(arr) - slice_ndim;
        iterator->slice_lbound = ARR_LBOUND(arr) + ARR_NDIM(arr) - slice_ndim;
        iterator->slice_len = ArrayGetNItems(slice_ndim, iterator->slice_dims);

        // Allocate workspace for sub-arrays
        iterator->slice_values = palloc(iterator->slice_len * sizeof(Datum));
        iterator->slice_nulls = palloc(iterator->slice_len * sizeof(bool));
    }

    // Initialize iteration pointers
    iterator->data_ptr = ARR_DATA_PTR(arr);
    iterator->current_item = 0;

    return iterator;
}
```