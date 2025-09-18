# PLy_result_ass_subscript

## Location
[src/pl/plpython/plpy_resultobject.c:245-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L245-L250)

## Overview
PLy_result_ass_subscript is a static function that implements the subscript assignment operation for PLyResultObject instances in the PL/Python extension, allowing Python code to modify individual rows using bracket notation assignment.

## Definition
```c
static int
PLy_result_ass_subscript(PyObject *arg, PyObject *item, PyObject *value)
```

## Detailed Description
This function enables subscript assignment to PLyResultObject instances, allowing Python code to modify specific rows in query results using assignment syntax like result[0] = new_row or result[key] = value. The function acts as a thin wrapper around PyObject_SetItem, delegating the actual assignment operation to the rows member of the result object.

This implementation allows PLyResultObject to behave like a mutable Python sequence or mapping, providing the ability to modify query result data through standard Python assignment operations.

## Parameters / Member Variables
- `arg`: A PyObject pointer that should be cast to PLyResultObject, representing the result object being modified
- `item`: A PyObject pointer representing the index or key where the assignment should occur (typically an integer index)
- `value`: A PyObject pointer representing the new value to be assigned at the specified index or key

## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (structure type cast)
  - PyObject_SetItem (Python C API function)
- Called from (representative examples):
  - Used as mp_ass_subscript method in PLyResultAsMapping mapping protocol (indirectly through Python's assignment mechanism)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_resultobject.c file
- The function returns an integer (0 for success, -1 for failure) following Python C API conventions
- The function delegates to PyObject_SetItem on the rows member, which should contain the actual row data as a mutable Python sequence
- This enables natural Python syntax for modifying query results: result[0] = new_row, result[1] = modified_data, etc.
- The function is typically assigned to the mp_ass_subscript slot of the Python mapping protocol for PLyResultObject
- Error handling is delegated to PyObject_SetItem, which will return -1 and set appropriate Python exceptions for invalid operations
- This functionality allows for in-place modification of query result data, which can be useful for data transformation operations in PL/Python procedures