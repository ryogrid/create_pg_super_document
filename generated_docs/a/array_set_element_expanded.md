# array_set_element_expanded

## Location
[src/backend/utils/adt/arrayfuncs.c:2501-2805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L2501-L2805)

## Overview
Implements element assignment for expanded arrays, providing optimized performance by modifying arrays in-place without full reconstruction when possible.

## Definition

```c
struct array if we didn't already.  (Someday maybe add a special
	 * case path for fixed-length, no-nulls cases, where we can overwrite an
	 * element in place without ever deconstructing.  But today is not that
	 * day.)
	 */
	deconstruct_expanded_array(eah);
```
## Detailed Description
This function is the specialized implementation of  for expanded arrays. Expanded arrays are PostgreSQL's internal representation that allows efficient in-place modifications without the overhead of complete array reconstruction. The function handles:

1. **Array dimension management**: Can extend single-dimensional arrays by adding elements before or after existing bounds
2. **Memory management**: Safely manages memory contexts and prevents corruption during partial failures
3. **Null handling**: Maintains null bitmaps and properly handles null value assignments
4. **Bounds checking**: Validates subscripts and prevents array size overflow
5. **Storage optimization**: Efficiently reuses existing storage space when possible

The function is designed to be failure-safe, ensuring the expanded array object remains in a consistent state even if operations fail partway through.

## Parameters / Member Variables
- : The expanded array object to modify (as a Datum)
- : Number of array subscripts provided
- : Array of subscript values specifying the target element position
- : The new value to assign to the specified array element
- : Boolean indicating whether the new value is NULL
- : Type length of the array (-1 for variable-length arrays)
- : Length of individual array elements
- : Whether array elements are passed by value
- : Alignment requirement for array elements

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetExpandedArray](../D/DatumGetExpandedArray.md)
  - [deconstruct_expanded_array](../d/deconstruct_expanded_array.md)
  - [datumCopy](../d/datumCopy.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [ArrayGetOffset](../A/ArrayGetOffset.md)
  - [EOHPGetRWDatum](../E/EOHPGetRWDatum.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [repalloc](../r/repalloc.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
- Called from:
  - [array_set_element](array_set_element.md)

## Notes and Other Information
- Only supports extending single-dimensional arrays; multi-dimensional arrays cannot be extended during assignment
- Uses overflow-safe arithmetic to prevent integer overflow when calculating new array dimensions
- Implements copy-on-write semantics for array elements when necessary
- The function maintains the expanded array's internal consistency by deferring irreversible changes until all memory allocations succeed
- Performance is optimized for repeated element assignments by reusing allocated storage space
- Part of PostgreSQL's array manipulation subsystem located in arrayfuncs.c

## Simplified Source

```c
static Datum
array_set_element_expanded(Datum arraydatum,
                          int nSubscripts, int *indx,
                          Datum dataValue, bool isNull,
                          int arraytyplen,
                          int elmlen, bool elmbyval, char elmalign)
{
    ExpandedArrayHeader *eah;
    Datum *dvalues;
    bool *dnulls;
    int ndim, dim[MAXDIM], lb[MAXDIM], offset;
    bool dimschanged = false;

    // Convert to read/write expanded array
    eah = DatumGetExpandedArray(arraydatum);

    // Copy dimension info for local modifications
    ndim = eah->ndims;
    memcpy(dim, eah->dims, ndim * sizeof(int));
    memcpy(lb, eah->lbound, ndim * sizeof(int));

    // Handle empty array case - create dimensions from subscripts
    if (ndim == 0)
    {
        // Allocate dimension arrays
        eah->dims = MemoryContextAllocZero(eah->hdr.eoh_context,
                                          nSubscripts * sizeof(int));
        eah->lbound = MemoryContextAllocZero(eah->hdr.eoh_context,
                                            nSubscripts * sizeof(int));

        // Initialize dimensions
        ndim = nSubscripts;
        for (int i = 0; i < nSubscripts; i++)
        {
            dim[i] = 0;
            lb[i] = indx[i];
        }
        dimschanged = true;
    }
    else if (ndim != nSubscripts)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("wrong number of array subscripts")));

    // Deconstruct array for modification
    deconstruct_expanded_array(eah);

    // Copy new element to array's memory context if needed
    if (!eah->typbyval && !isNull)
    {
        MemoryContext oldcxt = MemoryContextSwitchTo(eah->hdr.eoh_context);
        dataValue = datumCopy(dataValue, false, eah->typlen);
        MemoryContextSwitchTo(oldcxt);
    }

    dvalues = eah->dvalues;
    dnulls = eah->dnulls;

    // Check and adjust array bounds (simplified for 1D case)
    if (ndim == 1)
    {
        // Extend array if subscript is out of bounds
        if (indx[0] < lb[0] || indx[0] >= (dim[0] + lb[0]))
        {
            // Calculate new dimensions with overflow protection
            // (detailed bounds adjustment logic simplified)
            dimschanged = true;
        }
    }
    else
    {
        // Multi-dimensional arrays: validate subscripts are in bounds
        for (int i = 0; i < ndim; i++)
        {
            if (indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i]))
                ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                               errmsg("array subscript out of range")));
        }
    }

    // Calculate target element offset
    offset = ArrayGetOffset(nSubscripts, dim, lb, indx);

    // Ensure storage space is adequate
    if (dim[0] > eah->dvalueslen)
    {
        int newlen = dim[0] + dim[0] / 8;  // Add some extra space
        eah->dvalues = dvalues = repalloc(dvalues, newlen * sizeof(Datum));
        if (dnulls)
            eah->dnulls = dnulls = repalloc(dnulls, newlen * sizeof(bool));
        eah->dvalueslen = newlen;
    }

    // Create null bitmap if needed
    if ((dnulls != NULL || isNull) && dnulls == NULL)
        eah->dnulls = dnulls = MemoryContextAllocZero(eah->hdr.eoh_context,
                                                     eah->dvalueslen * sizeof(bool));

    // Update array metadata
    if (dimschanged)
    {
        eah->ndims = ndim;
        memcpy(eah->dims, dim, ndim * sizeof(int));
        memcpy(eah->lbound, lb, ndim * sizeof(int));
    }

    // Set the new element value
    dvalues[offset] = dataValue;
    if (dnulls)
        dnulls[offset] = isNull;

    // Clean up old element if necessary
    // (memory management details simplified)

    return EOHPGetRWDatum(&eah->hdr);
}
``` 