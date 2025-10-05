# PLySequence_ToArray

## Location
[src/pl/plpython/plpy_typeio.c:1133-1192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1133-L1192)

## Overview
Converts a Python sequence (or nested lists) to a PostgreSQL SQL array, handling multi-dimensional arrays through recursive traversal.

## Definition

```c
struct_empty_array(arg->u.array.elmbasetype));
```
## Detailed Description
This function serves as the main entry point for converting Python sequence objects into PostgreSQL array data structures. It validates that the input is a sequence, initializes array dimension tracking, and delegates the recursive traversal to PLySequence_ToArray_recurse. The function supports multi-dimensional arrays by recognizing nested Python lists and maintains PostgreSQL's convention of returning zero-dimensional arrays for empty inputs.

The conversion process involves:
1. Null handling - returns appropriate null datum for Python None
2. Sequence validation - ensures input is a valid Python sequence
3. Dimension initialization - sets up tracking for array dimensions
4. Recursive element collection - traverses nested structures to collect elements
5. Array construction - builds the final PostgreSQL array with proper bounds

## Parameters / Member Variables
- : PLyObToDatum structure containing array element type information and conversion context
- : Python object representing the sequence to be converted
- : Pointer to boolean flag indicating whether the result should be treated as SQL NULL
- : Boolean flag indicating whether this conversion is happening within an array context

## Dependencies
- Functions called/Symbols referenced:
  - [PLySequence_ToArray_recurse](PLySequence_ToArray_recurse.md) (recursive traversal helper)
  - [construct_empty_array](../c/construct_empty_array.md) (creates empty array for zero elements)
  - [makeMdArrayResult](../m/makeMdArrayResult.md) (constructs final multi-dimensional array)
  - PySequence_Check (Python API validation)
  - PySequence_Length (Python API length retrieval)
- Called from (representative examples):
  - [PLy_output_setup_func](PLy_output_setup_func.md) (src/pl/plpython/plpy_typeio.c:359)

## Notes and Other Information
- For historical compatibility, accepts any Python sequence type at the top level, not just lists
- Multi-dimensional arrays are only recognized when using true Python list objects
- Uses depth-first traversal to collect elements into ArrayBuildState
- Follows PostgreSQL convention of returning zero-dimensional arrays for empty inputs
- Maximum array dimensions limited by MAXDIM constant
- Array lower bounds are set to 1 following SQL standard conventions

## Simplified Source

```c
static Datum
PLySequence_ToArray(PLyObToDatum *arg, PyObject *plrv,
                   bool *isnull, bool inarray)
{
    ArrayBuildState *astate = NULL;
    int ndims = 1;
    int dims[MAXDIM];
    int lbs[MAXDIM];

    // Handle Python None -> SQL NULL
    if (plrv == Py_None) {
        *isnull = true;
        return (Datum) 0;
    }
    *isnull = false;

    // Validate input is a sequence
    if (!PySequence_Check(plrv))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("return value must be a Python sequence")));

    // Initialize array dimensions
    memset(dims, 0, sizeof(dims));
    dims[0] = PySequence_Length(plrv);

    // Recursively collect elements from nested structure
    PLySequence_ToArray_recurse(plrv, &astate, &ndims, dims, 1,
                               arg->u.array.elm, arg->u.array.elmbasetype);

    // Handle empty array case
    if (astate == NULL)
        return PointerGetDatum(construct_empty_array(arg->u.array.elmbasetype));

    // Set array lower bounds to 1 (SQL standard)
    for (int i = 0; i < ndims; i++)
        lbs[i] = 1;

    // Build final multi-dimensional array
    return makeMdArrayResult(astate, ndims, dims, lbs,
                            CurrentMemoryContext, true);
}
```