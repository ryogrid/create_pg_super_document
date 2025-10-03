# PLy_result_length

## Location
[src/pl/plpython/plpy_resultobject.c:205-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L205-L212)

## Overview
Implements the Python sequence/mapping length operation for PLython result objects, returning the number of rows in the result set.

## Definition

```c
static Py_ssize_t
PLy_result_length(PyObject *arg)
```
## Detailed Description
This function provides the implementation for Python's len() builtin function when called on a PLython result object. It serves as both a sequence length function (sq_length) and mapping length function (mp_length) in the Python type structure. The function simply delegates to PyList_Size() to get the number of rows stored in the result object's internal rows list, enabling natural Python idioms like len(result) to work with PLython result objects.

## Parameters / Member Variables
- `*arg`: PLyResultObject pointer cast as PyObject, representing the result object instance for which to get the length
## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (cast target type)
  - PyList_Size (Python list size function)
- Called from:
  - Python runtime when len() is called on a result object
  - Used as both sq_length and mp_length in PLyResult type definition

## Notes and Other Information
- Returns the number of rows as a Py_ssize_t, which is Python's standard type for object sizes
- This function enables PLython result objects to be used naturally in Python contexts that expect sequence-like behavior
- The function is assigned to both sequence (sq_length) and mapping (mp_length) slots in the type structure
- No error checking is needed since PyList_Size() handles NULL lists gracefully
- Enables idiomatic Python code like 'if len(result) > 0:' to work with database results