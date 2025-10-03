# PLy_result_coltypmods

## Location
[src/pl/plpython/plpy_resultobject.c:161-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L161-L186)

## Overview
Returns a list of type modifier values for all columns in a PLython result object, providing information about column constraints like precision and scale.

## Definition

```c
static PyObject *
PLy_result_coltypmods(PyObject *self, PyObject *unused)
```
## Detailed Description
This function is a Python method implementation that extracts type modifier information from all columns in a PostgreSQL result set accessed through PLython. Type modifiers provide additional information about data types, such as precision for numeric types, maximum length for varchar types, etc. The function creates a Python list containing the atttypmod values for each column in the result set's tuple descriptor.

## Parameters / Member Variables
- `*self`: PLyResultObject pointer cast as PyObject, representing the result object instance
- `*unused`: Unused parameter required by Python C API method signature (METH_NOARGS)
## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (cast target type)
  - [PLy_exception_set](PLy_exception_set.md) (error handling)
  - PyList_New (Python list creation)
  - PyList_SET_ITEM (Python list item assignment)
  - PyLong_FromLong (Python integer conversion)
  - TupleDescAttr (PostgreSQL tuple descriptor attribute access)
- Called from:
  - Exposed as 'coltypmods' method on PLython result objects

## Notes and Other Information
- Returns NULL and sets a PLython exception if the command did not produce a result set (no tupdesc)
- Type modifier values are PostgreSQL-specific integers that provide additional type information
- The function is registered as a METH_NOARGS method in the PLyResult method table
- Memory management for the returned list is handled by Python's reference counting system