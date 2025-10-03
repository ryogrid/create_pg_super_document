# PLy_result_str

## Location
[src/pl/plpython/plpy_resultobject.c:225-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L225-L236)

## Overview
PLy_result_str is a static function that implements the string representation method (__str__) for PLyResultObject instances in the PL/Python extension, providing a formatted string representation of query result objects.

## Definition

```c
static PyObject *
PLy_result_str(PyObject *arg)
```
## Detailed Description
This function creates a human-readable string representation of a PLyResultObject, which is used when Python code calls str() on a result object or when the object needs to be displayed. The function formats the output to include the object type name, query execution status, number of rows, and the actual row data in a structured format.

The function uses PyUnicode_FromFormat to create a Unicode string with the format "<type_name status=STATUS nrows=NROWS rows=ROWS>", where each component provides insight into the result object's state and contents.

## Parameters / Member Variables
- `*arg`: A PyObject pointer that should be cast to PLyResultObject, representing the result object to be converted to string format
## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (structure type cast)
  - PyUnicode_FromFormat (Python C API function)
  - Py_TYPE (Python C API macro)
- Called from (representative examples):
  - Used as tp_str method in PLyResultType type definition (indirectly through Python's string representation mechanism)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_resultobject.c file
- The function accesses the status, nrows, and rows members of the PLyResultObject structure
- The output format provides a comprehensive view of the result object's state, useful for debugging and logging
- This function is typically assigned to the tp_str slot of the Python type object for PLyResultObject