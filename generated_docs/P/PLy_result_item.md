# PLy_result_item

## Location
src/pl/plpython/plpy_resultobject.c: 213 - 224

## Overview
Implements Python sequence indexing for PLython result objects, allowing individual row access using bracket notation like result[index].

## Definition


## Detailed Description
This function provides the implementation for Python's item access operation (sq_item) when indexing into a PLython result object. It enables natural Python syntax like result[0] to access the first row, result[1] for the second row, etc. The function delegates to PyList_GetItem() to retrieve the specified row from the internal rows list and properly manages reference counting by incrementing the reference count of the returned object before returning it to the caller.

## Parameters / Member Variables
- : PLyResultObject pointer cast as PyObject, representing the result object being indexed
- : Py_ssize_t index value specifying which row to retrieve (0-based indexing)

## Dependencies
- Functions called/Symbols referenced:
  - PLyResultObject (cast target type)
  - PyList_GetItem (Python list item retrieval)
  - Py_INCREF (Python reference count increment)
- Called from:
  - Python runtime when bracket notation indexing is used on result objects
  - Registered as sq_item in PLyResult sequence methods

## Notes and Other Information
- Returns NULL if the index is out of bounds, following Python's standard IndexError behavior through PyList_GetItem
- Properly manages Python reference counting by incrementing the reference count of returned objects
- Supports negative indexing through PyList_GetItem's built-in handling
- This function enables natural Python idioms like 'first_row = result[0]' to work with database results
- Only handles single item access; slice operations would be handled by a separate sq_slice function if implemented