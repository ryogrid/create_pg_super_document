# PLySequence_ToArray_recurse

## Location
[src/pl/plpython/plpy_typeio.c:1193-1280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1193-L1280)

## Overview
Recursively traverses Python nested sequences in depth-first order to extract scalar elements and build a PostgreSQL multi-dimensional array structure.

## Definition

```c
static void
PLySequence_ToArray_recurse(PyObject *obj, ArrayBuildState **astatep,
							int *ndims, int *dims, int cur_depth,
							PLyObToDatum *elm, Oid elmbasetype)
```
## Detailed Description
This helper function performs the core recursive traversal logic for converting Python nested sequences into PostgreSQL arrays. It operates in depth-first order, validating array structure consistency and collecting scalar elements into an ArrayBuildState. The function enforces that multi-dimensional arrays have matching dimensions at each level and prevents mixing of scalars and sub-arrays at the same depth level.

Key operations include:
1. Sequence length validation and iteration through elements
2. Detection of nested lists vs scalar elements
3. Dimensional consistency checking for multi-dimensional arrays
4. Recursive descent for nested structures
5. Scalar element conversion and accumulation into ArrayBuildState
6. Proper Python reference counting with exception safety

## Parameters / Member Variables
- `*obj`: Python sequence object to traverse recursively
- `**astatep`: Pointer to ArrayBuildState pointer, created lazily when first scalar is found
- `*ndims`: Pointer to current number of dimensions discovered
- `*dims`: Array storing the size of each dimension
- `cur_depth`: Current recursion depth (0-based dimension index)
- `*elm`: PLyObToDatum converter for scalar elements
- `elmbasetype`: PostgreSQL OID of the array element base type
## Dependencies
- Functions called/Symbols referenced:
  - PLy_elog (error reporting)
  - PySequence_Length (Python API length retrieval)
  - PySequence_GetItem (Python API element access)
  - PyList_Check (Python type checking)
  - [initArrayResult](../i/initArrayResult.md) (creates ArrayBuildState)
  - [accumArrayResult](../a/accumArrayResult.md) (adds elements to array)
  - PG_TRY/PG_FINALLY/PG_END_TRY (exception handling)
  - Py_XDECREF (Python reference counting)
- Called from (representative examples):
  - [PLySequence_ToArray](PLySequence_ToArray.md) (src/pl/plpython/plpy_typeio.c:1167)
  - [PLySequence_ToArray_recurse](PLySequence_ToArray_recurse.md) (src/pl/plpython/plpy_typeio.c:1240) - recursive self-call

## Notes and Other Information
- Uses lazy initialization of ArrayBuildState to avoid creating it until scalars are found
- Enforces strict dimensional consistency - all sub-arrays at the same level must have identical lengths
- Prevents mixing of scalars and arrays at the same depth level
- Includes comprehensive error checking for dimension limits (MAXDIM)
- Uses PostgreSQL's exception handling macros to ensure Python reference counts are properly decremented
- Recursive calls increment cur_depth to track current dimension being processed
- The function is purely internal to the plpython conversion system

## Simplified Source

```c
static void
PLySequence_ToArray_recurse(PyObject *obj, ArrayBuildState **astatep,
                           int *ndims, int *dims, int cur_depth,
                           PLyObToDatum *elm, Oid elmbasetype)
{
    int len = PySequence_Length(obj);
    if (len < 0)
        PLy_elog(ERROR, "could not determine sequence length");

    for (int i = 0; i < len; i++) {
        PyObject *subobj = PySequence_GetItem(obj, i);

        PG_TRY();
        {
            if (PyList_Check(subobj)) {
                // Handle nested array
                if (i == 0 && *ndims == cur_depth) {
                    // Check for mixing scalars and arrays
                    if (*astatep != NULL)
                        ereport(ERROR, "mismatched array dimensions");

                    // Check dimension limits
                    if (cur_depth >= MAXDIM)
                        ereport(ERROR, "too many array dimensions");

                    // Add new dimension
                    dims[*ndims] = PySequence_Length(subobj);
                    (*ndims)++;
                } else if (cur_depth >= *ndims ||
                          PySequence_Length(subobj) != dims[cur_depth]) {
                    ereport(ERROR, "mismatched array dimensions");
                }

                // Recurse into sub-array
                PLySequence_ToArray_recurse(subobj, astatep, ndims, dims,
                                          cur_depth + 1, elm, elmbasetype);
            } else {
                // Handle scalar element
                if (*ndims != cur_depth)
                    ereport(ERROR, "mismatched array dimensions");

                // Convert Python object to PostgreSQL datum
                bool isnull;
                Datum dat = elm->func(elm, subobj, &isnull, true);

                // Initialize array builder if needed
                if (*astatep == NULL)
                    *astatep = initArrayResult(elmbasetype, CurrentMemoryContext, true);

                // Add element to array
                accumArrayResult(*astatep, dat, isnull, elmbasetype, CurrentMemoryContext);
            }
        }
        PG_FINALLY();
        {
            Py_XDECREF(subobj);
        }
        PG_END_TRY();
    }
}
```