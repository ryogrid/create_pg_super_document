# PLyList_FromArray_recurse

## Location
[src/pl/plpython/plpy_typeio.c:707-780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L707-L780)

## Overview
Recursively converts PostgreSQL multi-dimensional arrays into nested Python lists, handling NULL values and proper memory alignment during the conversion process.

## Definition

```c
structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
```
## Detailed Description
This function is the core recursive engine for converting PostgreSQL arrays to Python list objects. It handles multi-dimensional arrays by recursively processing each dimension level. For outer dimensions, it creates sublists and recurses deeper. For the innermost dimension, it extracts individual array elements, converts them to Python objects using the provided conversion function, and handles NULL value checking via bitmap masks. The function also manages proper memory alignment and advances data pointers as it processes each element.

## Parameters / Member Variables
- : PLyDatumToOb structure containing type information and conversion function for array elements
- : Array of dimension sizes for each level of the multi-dimensional array
- : Total number of dimensions in the array
- : Current dimension level being processed (0-based index)
- : Pointer to current position in the array data buffer (updated as processing advances)
- : Pointer to NULL value bitmap for tracking which elements are NULL (updated as processing advances)
- : Current bit mask for checking NULL status in the bitmap (updated as processing advances)

## Dependencies
- Functions called/Symbols referenced:
  - [PLyDatumToOb](PLyDatumToOb.md) (type structure)
  - bits8 (type definition)
  - [PLyList_FromArray_recurse](PLyList_FromArray_recurse.md) (recursive self-call)
  - [fetch_att](../f/fetch_att.md) (extracts datum value from data buffer)
  - att_addlength_pointer (advances pointer by attribute length)
  - att_align_nominal (aligns pointer to proper boundary)
- Called from:
  - [PLyList_FromArray](PLyList_FromArray.md) (main array conversion entry point)
  - [PLyList_FromArray_recurse](PLyList_FromArray_recurse.md) (recursive self-calls for multi-dimensional arrays)

## Notes and Other Information
The function uses Python's C API to create list objects and manages reference counting properly. It handles PostgreSQL's internal array representation including NULL bitmaps and memory alignment requirements. The recursive design naturally handles arrays of arbitrary dimensions by processing one dimension level per recursion depth. For performance, it uses PyList_SET_ITEM rather than PyList_SetItem to avoid unnecessary reference count checks on newly created lists.

## Simplified Source

```c
static PyObject *
PLyList_FromArray_recurse(PLyDatumToOb *elm, int *dims, int ndim, int dim,
                          char **dataptr_p, bits8 **bitmap_p, int *bitmask_p)
{
    // Create Python list for current dimension
    PyObject *list = PyList_New(dims[dim]);
    if (!list)
        return NULL;

    if (dim < ndim - 1) {
        // Outer dimension: recurse for each inner slice
        for (int i = 0; i < dims[dim]; i++) {
            PyObject *sublist = PLyList_FromArray_recurse(elm, dims, ndim, dim + 1,
                                                          dataptr_p, bitmap_p, bitmask_p);
            PyList_SET_ITEM(list, i, sublist);
        }
    } else {
        // Innermost dimension: extract actual values
        char *dataptr = *dataptr_p;
        bits8 *bitmap = *bitmap_p;
        int bitmask = *bitmask_p;

        for (int i = 0; i < dims[dim]; i++) {
            // Check for NULL value
            if (bitmap && (*bitmap & bitmask) == 0) {
                Py_INCREF(Py_None);
                PyList_SET_ITEM(list, i, Py_None);
            } else {
                // Extract datum and convert to Python object
                Datum itemvalue = fetch_att(dataptr, elm->typbyval, elm->typlen);
                PyList_SET_ITEM(list, i, elm->func(elm, itemvalue));

                // Advance data pointer with proper alignment
                dataptr = att_addlength_pointer(dataptr, elm->typlen, dataptr);
                dataptr = (char *) att_align_nominal(dataptr, elm->typalign);
            }

            // Advance bitmap if present
            if (bitmap) {
                bitmask <<= 1;
                if (bitmask == 0x100) {
                    bitmap++;
                    bitmask = 1;
                }
            }
        }

        // Update caller's pointers
        *dataptr_p = dataptr;
        *bitmap_p = bitmap;
        *bitmask_p = bitmask;
    }

    return list;
}
```