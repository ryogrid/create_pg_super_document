# EA_flatten_into

## Location
[src/backend/utils/adt/array_expanded.c:293-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L293-L351)

## Overview
A method function that flattens an expanded array into its standard serialized PostgreSQL array representation in a pre-allocated memory buffer.

## Definition

```c
static void
EA_flatten_into(ExpandedObjectHeader *eohptr,
				void *result, Size allocated_size)
```
## Detailed Description
This function is part of the ExpandedObjectMethods interface for expanded arrays, responsible for converting an expanded array back into the standard PostgreSQL array format. It works in conjunction with EA_get_flat_size to provide a complete serialization mechanism.

The function optimizes by first checking if a flattened representation already exists and simply copying it if available. Otherwise, it constructs a new ArrayType structure from the deconstructed representation (dvalues/dnulls arrays). The function carefully sets up the array header with proper dimensions, bounds, element type, and data offset, then uses CopyArrayEls to transfer the actual element data with proper alignment and null handling.

The function ensures all padding is zero-filled and handles both arrays with and without null bitmaps appropriately.

## Parameters / Member Variables
- `*eohptr`: Pointer to the ExpandedObjectHeader (cast to ExpandedArrayHeader internally)
- `*result`: Pre-allocated memory buffer where the flattened array will be written
- `allocated_size`: Size of the allocated buffer, must match the size returned by EA_get_flat_size
## Dependencies
- Functions called/Symbols referenced:
  - ARR_SIZE
  - ARR_OVERHEAD_WITHNULLS
  - SET_VARSIZE
  - ARR_DIMS
  - ARR_LBOUND
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - memcpy
  - memset
- Called from (representative examples):
  - Used through EA_methods function pointer table in ExpandedObjectMethods interface

## Notes and Other Information
- This is a static function that implements the flatten_into method in the EA_methods table
- The allocated_size parameter must exactly match the value previously returned by EA_get_flat_size
- Uses memset to zero-fill the entire result buffer, ensuring deterministic output and proper padding
- Handles the data offset calculation correctly for arrays with and without null bitmaps
- The false parameter to CopyArrayEls indicates this is not a construction from individual datums but a reconstruction from an already-deconstructed representation

## Simplified Source

```c
static void EA_flatten_into(ExpandedObjectHeader *eohptr,
                           void *result, Size allocated_size) {
    ExpandedArrayHeader *eah = (ExpandedArrayHeader *) eohptr;
    ArrayType *aresult = (ArrayType *) result;

    // If we already have a flattened version, just copy it
    if (eah->fvalue) {
        memcpy(result, eah->fvalue, allocated_size);
        return;
    }

    // Otherwise, build array from deconstructed elements
    int nelems = eah->nelems;
    int ndims = eah->ndims;

    // Calculate data offset (includes null bitmap if present)
    int32 dataoffset = eah->dnulls ?
        ARR_OVERHEAD_WITHNULLS(ndims, nelems) : 0;

    // Zero-fill the entire result buffer
    memset(aresult, 0, allocated_size);

    // Set up array header
    SET_VARSIZE(aresult, allocated_size);
    aresult->ndim = ndims;
    aresult->dataoffset = dataoffset;
    aresult->elemtype = eah->element_type;

    // Copy dimension and bound information
    memcpy(ARR_DIMS(aresult), eah->dims, ndims * sizeof(int));
    memcpy(ARR_LBOUND(aresult), eah->lbound, ndims * sizeof(int));

    // Copy the actual array elements with proper alignment
    CopyArrayEls(aresult, eah->dvalues, eah->dnulls, nelems,
                 eah->typlen, eah->typbyval, eah->typalign, false);
}
```