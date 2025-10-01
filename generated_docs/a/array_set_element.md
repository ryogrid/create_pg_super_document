# array_set_element

## Location
[src/backend/utils/adt/arrayfuncs.c:2201-2500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L2201-L2500)

## Overview
Sets the value of a single array element at specified subscripts, creating a new array with the modified element while preserving the original array structure.

## Definition
```c
Datum array_set_element(Datum arraydatum,
                       int nSubscripts,
                       int *indx,
                       Datum dataValue,
                       bool isNull,
                       int arraytyplen,
                       int elmlen,
                       bool elmbyval,
                       char elmalign)
```

## Detailed Description
The `array_set_element` function creates a new array identical to the input array except for one modified element at the specified subscripts. It handles three array types: fixed-length arrays (1-dimensional, 0-based, no extension allowed), expanded arrays (delegated to `array_set_element_expanded`), and ordinary varlena arrays. For one-dimensional arrays, it supports array extension by assigning to positions outside the existing range, filling gaps with NULLs. Multi-dimensional arrays cannot be extended. The function performs comprehensive bounds checking, handles overflow prevention, manages null bitmaps, and creates optimally-sized result arrays. Unlike fetch operations that return NULL for invalid subscripts, this function throws errors for assignment to invalid positions.

## Parameters / Member Variables
- `arraydatum`: The source array object (must not be NULL)
- `nSubscripts`: Number of subscripts supplied in the indx array
- `indx[]`: Array of subscript values specifying the target element position
- `dataValue`: The datum value to be inserted at the specified position
- `isNull`: Boolean indicating whether dataValue is NULL
- `arraytyplen`: pg_type.typlen for the array type (>0 for fixed-length)
- `elmlen`: pg_type.typlen for the array's element type
- `elmbyval`: pg_type.typbyval for the array's element type
- `elmalign`: pg_type.typalign for the array's element type

## Dependencies
- Functions called/Symbols referenced:
  - `[ArrayCastAndSet](../A/ArrayCastAndSet.md)`: Sets element value in fixed-length arrays
  - `PG_DETOAST_DATUM`: Detoasts varlena elements before insertion
  - `VARATT_IS_EXTERNAL_EXPANDED`: Checks for expanded array format
  - [array_set_element_expanded](array_set_element_expanded.md): Handles expanded array assignments
  - `DatumGetArrayTypeP`: Converts datum to ArrayType pointer
  - `ARR_NDIM`, `ARR_DIMS`, `ARR_LBOUND`, `ARR_ELEMTYPE`: Array metadata
  - [construct_md_array](../c/construct_md_array.md): Creates new arrays from empty arrays
  - `[ArrayGetNItems](../A/ArrayGetNItems.md)`, `ArrayCheckBounds`: Array validation functions
  - `ARR_OVERHEAD_WITHNULLS`, `ARR_OVERHEAD_NONULLS`: Size calculations
  - `[ArrayGetOffset](../A/ArrayGetOffset.md)`: Calculates linear offset from subscripts
  - `[array_seek](array_seek.md)`, `array_get_isnull`: Element access functions
  - `att_addlength_datum`, `att_align_nominal`: Data size/alignment
  - `[array_set_isnull](array_set_isnull.md)`, `array_bitmap_copy`: Null bitmap management
- Called from (representative examples):
  - [array_set](array_set.md): Array assignment operations
  - [array_subscript_assign](array_subscript_assign.md): Subscripted assignment operations
  - [array_append](array_append.md), `array_prepend`: Array modification functions

## Notes and Other Information
- Always returns a new array; never modifies the original (except for writable expanded arrays)
- Supports 1D array extension: assignments beyond bounds extend the array with NULLs filling gaps
- Multi-dimensional arrays cannot be extended; out-of-bounds assignment raises errors
- Fixed-length arrays: 1D only, 0-based indexing, no extension, no NULL assignments allowed
- Handles overflow protection using `pg_sub_s32_overflow` and `pg_add_s32_overflow`
- Maximum array size enforced by `MaxArraySize` constant
- Empty arrays (ndim=0) are converted to new arrays with specified dimensions
- Comprehensive null bitmap management preserves and updates NULL element tracking
- Critical function for PostgreSQL's array assignment infrastructure
- Located in `src/backend/utils/adt/arrayfuncs.c` at lines 2201-2500

## Simplified Source

```c
Datum
array_set_element(Datum arraydatum, int nSubscripts, int *indx,
                  Datum dataValue, bool isNull,
                  int arraytyplen, int elmlen, bool elmbyval, char elmalign)
{
    ArrayType *array, *newarray;
    int ndim, dim[MAXDIM], lb[MAXDIM];
    int oldnitems, newnitems, newsize;
    int offset, lenbefore, lenafter, olditemlen, newitemlen;
    bool newhasnulls;

    // Handle fixed-length arrays (simple case)
    if (arraytyplen > 0) {
        char *resultarray;

        // Validate subscripts and create copy
        if (nSubscripts != 1 || indx[0] < 0 || indx[0] >= arraytyplen / elmlen)
            ereport(ERROR, /* invalid subscripts */);
        if (isNull)
            ereport(ERROR, /* cannot assign NULL to fixed array */);

        // Copy array and set element
        resultarray = (char *) palloc(arraytyplen);
        memcpy(resultarray, DatumGetPointer(arraydatum), arraytyplen);
        ArrayCastAndSet(dataValue, elmlen, elmbyval, elmalign,
                       resultarray + indx[0] * elmlen);
        return PointerGetDatum(resultarray);
    }

    // Validate inputs
    if (nSubscripts <= 0 || nSubscripts > MAXDIM)
        ereport(ERROR, /* wrong number of subscripts */);

    // Detoast input if needed
    if (elmlen == -1 && !isNull)
        dataValue = PointerGetDatum(PG_DETOAST_DATUM(dataValue));

    // Handle expanded arrays separately
    if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(arraydatum)))
        return array_set_element_expanded(/* parameters */);

    array = DatumGetArrayTypeP(arraydatum);
    ndim = ARR_NDIM(array);

    // Create new array from empty array
    if (ndim == 0) {
        for (int i = 0; i < nSubscripts; i++) {
            dim[i] = 1;
            lb[i] = indx[i];
        }
        return PointerGetDatum(construct_md_array(&dataValue, &isNull,
                                                 nSubscripts, dim, lb,
                                                 ARR_ELEMTYPE(array),
                                                 elmlen, elmbyval, elmalign));
    }

    // Validate dimensions match
    if (ndim != nSubscripts)
        ereport(ERROR, /* dimension mismatch */);

    // Copy dimensions and bounds
    memcpy(dim, ARR_DIMS(array), ndim * sizeof(int));
    memcpy(lb, ARR_LBOUND(array), ndim * sizeof(int));
    newhasnulls = (ARR_HASNULL(array) || isNull);

    // Handle array extension for 1D arrays
    int addedbefore = 0, addedafter = 0;
    if (ndim == 1) {
        // Extend before if needed
        if (indx[0] < lb[0]) {
            addedbefore = lb[0] - indx[0];
            dim[0] += addedbefore;
            lb[0] = indx[0];
            if (addedbefore > 1) newhasnulls = true;
        }
        // Extend after if needed
        if (indx[0] >= (dim[0] + lb[0])) {
            addedafter = indx[0] - (dim[0] + lb[0]) + 1;
            dim[0] += addedafter;
            if (addedafter > 1) newhasnulls = true;
        }
    } else {
        // Multi-dimensional: no extension, strict bounds check
        for (int i = 0; i < ndim; i++) {
            if (indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i]))
                ereport(ERROR, /* subscript out of range */);
        }
    }

    // Calculate sizes and positions
    newnitems = ArrayGetNItems(ndim, dim);
    int overheadlen = newhasnulls ? ARR_OVERHEAD_WITHNULLS(ndim, newnitems)
                                  : ARR_OVERHEAD_NONULLS(ndim);

    // Find element position and calculate copy lengths
    if (addedbefore) {
        lenbefore = 0;
        olditemlen = 0;
        lenafter = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
    } else if (addedafter) {
        lenbefore = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
        olditemlen = 0;
        lenafter = 0;
    } else {
        offset = ArrayGetOffset(nSubscripts, dim, lb, indx);
        // Calculate data copy lengths around the target element
        char *elt_ptr = array_seek(ARR_DATA_PTR(array), 0, ARR_NULLBITMAP(array),
                                  offset, elmlen, elmbyval, elmalign);
        lenbefore = (int) (elt_ptr - ARR_DATA_PTR(array));
        olditemlen = array_get_isnull(ARR_NULLBITMAP(array), offset) ? 0 :
                    att_align_nominal(att_addlength_pointer(0, elmlen, elt_ptr), elmalign);
        lenafter = ARR_SIZE(array) - ARR_DATA_OFFSET(array) - lenbefore - olditemlen;
    }

    // Calculate new element size
    newitemlen = isNull ? 0 : att_align_nominal(att_addlength_datum(0, elmlen, dataValue), elmalign);
    newsize = overheadlen + lenbefore + newitemlen + lenafter;

    // Create and initialize new array
    newarray = (ArrayType *) palloc0(newsize);
    SET_VARSIZE(newarray, newsize);
    newarray->ndim = ndim;
    newarray->dataoffset = newhasnulls ? overheadlen : 0;
    newarray->elemtype = ARR_ELEMTYPE(array);
    memcpy(ARR_DIMS(newarray), dim, ndim * sizeof(int));
    memcpy(ARR_LBOUND(newarray), lb, ndim * sizeof(int));

    // Copy data: before + new element + after
    memcpy((char *) newarray + overheadlen,
           (char *) array + ARR_DATA_OFFSET(array), lenbefore);
    if (!isNull)
        ArrayCastAndSet(dataValue, elmlen, elmbyval, elmalign,
                       (char *) newarray + overheadlen + lenbefore);
    memcpy((char *) newarray + overheadlen + lenbefore + newitemlen,
           (char *) array + ARR_DATA_OFFSET(array) + lenbefore + olditemlen,
           lenafter);

    // Update null bitmap if needed
    if (newhasnulls) {
        bits8 *newnullbitmap = ARR_NULLBITMAP(newarray);

        // Set null status for new element
        if (addedafter)
            array_set_isnull(newnullbitmap, newnitems - 1, isNull);
        else
            array_set_isnull(newnullbitmap, offset, isNull);

        // Copy null bitmap sections
        if (addedbefore)
            array_bitmap_copy(newnullbitmap, addedbefore, ARR_NULLBITMAP(array), 0, oldnitems);
        else {
            array_bitmap_copy(newnullbitmap, 0, ARR_NULLBITMAP(array), 0, offset);
            if (addedafter == 0)
                array_bitmap_copy(newnullbitmap, offset + 1, ARR_NULLBITMAP(array),
                                 offset + 1, oldnitems - offset - 1);
        }
    }

    return PointerGetDatum(newarray);
}
```