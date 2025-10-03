# construct_md_array

## Location
[src/backend/utils/adt/arrayfuncs.c:3482-3567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3482-L3567)

## Overview
Creates a multi-dimensional array object with arbitrary dimensions and support for NULL elements, serving as the core array construction function for complex array operations.

## Definition

```c
ArrayType *
construct_md_array(Datum *elems,
				   bool *nulls,
				   int ndims,
				   int *dims,
				   int *lbs,
				   Oid elmtype, int elmlen, bool elmbyval, char elmalign)
```
## Detailed Description
The construct_md_array function provides the most flexible and comprehensive method for constructing PostgreSQL array objects. It supports arrays with arbitrary numbers of dimensions (up to MAXDIM), custom lower bounds for each dimension, and NULL element values. This function serves as the foundation for other array construction functions like construct_array.

The function performs extensive validation on the input parameters, calculates the required memory allocation size, handles data alignment and toasted values, and creates the complete ArrayType structure with proper headers, dimension information, and element data. It can create zero-dimensional arrays and handles overflow detection for very large arrays.

## Parameters / Member Variables
- `*elems`: Array of Datum items that will become the contents of the constructed array
- `*nulls`: Array of boolean flags indicating which elements are NULL (can be NULL if no null elements)
- `ndims`: Number of dimensions for the array (0 or positive, up to MAXDIM)
- `*dims`: Integer array specifying the size of each dimension
- `*lbs`: Integer array specifying the lower bound of each dimension
- `elmtype`: OID of the data type for the array elements
- `elmlen`: Length of the element data type (-1 for variable-length types)
- `elmbyval`: Boolean indicating whether elements are passed by value or by reference
- `elmalign`: Alignment requirement for the element data type
## Dependencies
- Functions called/Symbols referenced:
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [construct_empty_array](construct_empty_array.md)
  - PG_DETOAST_DATUM
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - SET_VARSIZE
  - ARR_DIMS
  - ARR_LBOUND
  - MAXDIM (constant)
  - MaxAllocSize (constant)
- Called from (representative examples):
  - [construct_array](construct_array.md)
  - [strlist_to_textarray](../s/strlist_to_textarray.md)
  - [ExecEvalArrayExpr](../E/ExecEvalArrayExpr.md)
  - [array_shuffle_n](../a/array_shuffle_n.md)
  - [array_set_element](../a/array_set_element.md)
  - [array_set_slice](../a/array_set_slice.md)
  - [array_iterate](../a/array_iterate.md)
  - [makeMdArrayResult](../m/makeMdArrayResult.md)
  - [percentile_disc_multi_final](../p/percentile_disc_multi_final.md)
  - [build_regexp_match_result](../b/build_regexp_match_result.md)

## Notes and Other Information
- Validates that ndims is between 0 and MAXDIM, raising errors for invalid ranges
- Automatically detours empty arrays (ndims <= 0 or any dimension is 0) to construct_empty_array
- Handles variable-length elements by detoasting them before storage
- Calculates proper memory alignment and detects potential overflow conditions
- Creates null bitmaps only when necessary (when hasnulls is true)
- The resulting array uses palloc0 for zero-initialized memory allocation
- Essential foundation function that other array construction utilities depend upon

## Simplified Source

```c
ArrayType *construct_md_array(Datum *elems, bool *nulls, int ndims, int *dims, int *lbs,
                              Oid elmtype, int elmlen, bool elmbyval, char elmalign) {
    ArrayType *result;
    bool hasnulls;
    int32 nbytes;
    int32 dataoffset;
    int i;
    int nelems;

    // Validate number of dimensions
    if (ndims < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("invalid number of dimensions: %d", ndims)));
    if (ndims > MAXDIM)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                              ndims, MAXDIM)));

    // Calculate total number of elements and validate bounds
    nelems = ArrayGetNItems(ndims, dims);
    ArrayCheckBounds(ndims, dims, lbs);

    // Return empty array if no elements
    if (nelems <= 0)
        return construct_empty_array(elmtype);

    // Calculate required space and check for null elements
    nbytes = 0;
    hasnulls = false;
    for (i = 0; i < nelems; i++) {
        if (nulls && nulls[i]) {
            hasnulls = true;
            continue;
        }
        // Detoast variable-length elements
        if (elmlen == -1)
            elems[i] = PointerGetDatum(PG_DETOAST_DATUM(elems[i]));

        nbytes = att_addlength_datum(nbytes, elmlen, elems[i]);
        nbytes = att_align_nominal(nbytes, elmalign);

        // Check for allocation size overflow
        if (!AllocSizeIsValid(nbytes))
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                           errmsg("array size exceeds the maximum allowed (%d)",
                                  (int) MaxAllocSize)));
    }

    // Calculate total size including array overhead
    if (hasnulls) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nelems);
        nbytes += dataoffset;
    } else {
        dataoffset = 0;  // No null bitmap needed
        nbytes += ARR_OVERHEAD_NONULLS(ndims);
    }

    // Allocate and initialize array structure
    result = (ArrayType *) palloc0(nbytes);
    SET_VARSIZE(result, nbytes);
    result->ndim = ndims;
    result->dataoffset = dataoffset;
    result->elemtype = elmtype;
    memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
    memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

    // Copy element data into the array
    CopyArrayEls(result, elems, nulls, nelems, elmlen, elmbyval, elmalign, false);

    return result;
}
```