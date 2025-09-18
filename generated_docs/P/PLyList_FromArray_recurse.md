# PLyList_FromArray_recurse

## Location
src/pl/plpython/plpy_typeio.c: 707 - 780

## Overview
Recursively converts PostgreSQL multi-dimensional arrays into nested Python lists, handling NULL values and proper memory alignment during the conversion process.

## Definition


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
  - PLyDatumToOb (type structure)
  - bits8 (type definition)
  - PLyList_FromArray_recurse (recursive self-call)
  - fetch_att (extracts datum value from data buffer)
  - att_addlength_pointer (advances pointer by attribute length)
  - att_align_nominal (aligns pointer to proper boundary)
- Called from:
  - PLyList_FromArray (main array conversion entry point)
  - PLyList_FromArray_recurse (recursive self-calls for multi-dimensional arrays)

## Notes and Other Information
The function uses Python's C API to create list objects and manages reference counting properly. It handles PostgreSQL's internal array representation including NULL bitmaps and memory alignment requirements. The recursive design naturally handles arrays of arbitrary dimensions by processing one dimension level per recursion depth. For performance, it uses PyList_SET_ITEM rather than PyList_SetItem to avoid unnecessary reference count checks on newly created lists.