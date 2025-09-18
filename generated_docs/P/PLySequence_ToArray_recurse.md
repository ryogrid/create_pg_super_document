# PLySequence_ToArray_recurse

## Location
src/pl/plpython/plpy_typeio.c: 1193 - 1280

## Overview
Recursively traverses Python nested sequences in depth-first order to extract scalar elements and build a PostgreSQL multi-dimensional array structure.

## Definition


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
- : Python sequence object to traverse recursively
- : Pointer to ArrayBuildState pointer, created lazily when first scalar is found
- : Pointer to current number of dimensions discovered
- : Array storing the size of each dimension
- : Current recursion depth (0-based dimension index)
- : PLyObToDatum converter for scalar elements
- : PostgreSQL OID of the array element base type

## Dependencies
- Functions called/Symbols referenced:
  - PLy_elog (error reporting)
  - PySequence_Length (Python API length retrieval)
  - PySequence_GetItem (Python API element access)
  - PyList_Check (Python type checking)
  - initArrayResult (creates ArrayBuildState)
  - accumArrayResult (adds elements to array)
  - PG_TRY/PG_FINALLY/PG_END_TRY (exception handling)
  - Py_XDECREF (Python reference counting)
- Called from (representative examples):
  - PLySequence_ToArray (src/pl/plpython/plpy_typeio.c:1167)
  - PLySequence_ToArray_recurse (src/pl/plpython/plpy_typeio.c:1240) - recursive self-call

## Notes and Other Information
- Uses lazy initialization of ArrayBuildState to avoid creating it until scalars are found
- Enforces strict dimensional consistency - all sub-arrays at the same level must have identical lengths
- Prevents mixing of scalars and arrays at the same depth level
- Includes comprehensive error checking for dimension limits (MAXDIM)
- Uses PostgreSQL's exception handling macros to ensure Python reference counts are properly decremented
- Recursive calls increment cur_depth to track current dimension being processed
- The function is purely internal to the plpython conversion system