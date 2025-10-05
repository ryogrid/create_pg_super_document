# PLyList_FromArray

## Location
[src/pl/plpython/plpy_typeio.c:667-706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L667-L706)

## Overview
Converts a PostgreSQL array datum to a Python list, handling multi-dimensional arrays by recursively building nested lists.

## Definition
```c
static PyObject *PLyList_FromArray(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
PLyList_FromArray is a comprehensive array conversion function within PostgreSQL's PL/Python extension that transforms PostgreSQL arrays into Python lists. The function handles arrays of any dimensionality by extracting array metadata (dimensions, null bitmap, data pointer) and delegating the recursive construction to PLyList_FromArray_recurse(). For multi-dimensional arrays, it creates nested Python lists where each dimension becomes a list containing the next inner dimension. The function iterates through array elements in the physical storage order and properly handles NULL values using the array's null bitmap.

## Parameters / Member Variables
- `arg`: PLyDatumToOb pointer containing conversion context information, including element conversion details in arg->u.array.elm
- `d`: Datum containing the PostgreSQL array to be converted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP: Extracts ArrayType structure from datum with potential detoasting
  - ARR_NDIM: Macro to get number of array dimensions
  - ARR_DIMS: Macro to get array dimensions
  - ARR_DATA_PTR: Macro to get pointer to array data
  - ARR_NULLBITMAP: Macro to get array's null bitmap
  - [PLyList_FromArray_recurse](PLyList_FromArray_recurse.md): Recursive helper function to build nested lists
  - PyList_New: Creates empty Python list for zero-dimensional arrays
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for PostgreSQL to Python data conversion

## Notes and Other Information
- This is a static function within the PL/Python type conversion system
- Handles arrays of any dimensionality up to PostgreSQL's MAXDIM limit
- Preserves NULL values from PostgreSQL arrays in the resulting Python list
- Uses recursive approach to properly construct nested list structure for multi-dimensional arrays
- Iterates through array elements in physical storage order for efficiency
- The conversion maintains the logical structure of multi-dimensional arrays as nested lists
- Returns empty list for zero-dimensional arrays
- Properly handles PostgreSQL's variable-length array format including null bitmaps

## Simplified Source

```c
static PyObject *
PLyList_FromArray(PLyDatumToOb *arg, Datum d)
{
    ArrayType *array = DatumGetArrayTypeP(d);
    PLyDatumToOb *elm = arg->u.array.elm;

    // Handle empty arrays
    if (ARR_NDIM(array) == 0)
        return PyList_New(0);

    // Extract array metadata
    int ndim = ARR_NDIM(array);
    int *dims = ARR_DIMS(array);
    char *dataptr = ARR_DATA_PTR(array);
    bits8 *bitmap = ARR_NULLBITMAP(array);
    int bitmask = 1;

    // Recursively build nested Python lists
    return PLyList_FromArray_recurse(elm, dims, ndim, 0,
                                     &dataptr, &bitmap, &bitmask);
}
```