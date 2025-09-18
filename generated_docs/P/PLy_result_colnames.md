# PLy_result_colnames

## Location
src/pl/plpython/plpy_resultobject.c: 109 - 134

## Overview
Returns a Python list containing the column names of the result set represented by a PLyResultObject.

## Definition
```c
static PyObject *PLy_result_colnames(PyObject *self, PyObject *unused)
```

## Detailed Description
This function extracts the column names from the tuple descriptor of a PLyResultObject and returns them as a Python list of strings. It is exposed as a method on PLyResult objects, allowing Python code to introspect the structure of query results. The function validates that the result object has a valid tuple descriptor (meaning it represents a result set with columns) and raises an exception if called on a result that doesn't have one. The column names are extracted from the PostgreSQL tuple descriptor and converted to Python Unicode strings.

## Parameters / Member Variables
- `self`: PyObject pointer to the PLyResultObject instance
- `unused`: PyObject pointer (not used, as this method takes no arguments)

Returns a Python list where each element is a Unicode string representing a column name, or NULL on error.

## Dependencies
- Functions called/Symbols referenced:
  - PLy_exception_set() (PL/Python exception handling)
  - PyList_New() (Python C API)
  - TupleDescAttr() (PostgreSQL tuple descriptor access)
  - PLyUnicode_FromString() (PL/Python Unicode string conversion)
  - NameStr() (PostgreSQL name string conversion)
  - PLyResultObject (struct type)
- Called from (representative examples):
  - Referenced as "colnames" method in PLy_result_methods at src/pl/plpython/plpy_resultobject.c:39
  - Callable from Python code as result.colnames()

## Notes and Other Information
- This is a static function exposed as a Python method through the PLy_result_methods table
- Raises PLy_exc_error if called on a result object without a tuple descriptor
- The returned list length equals the number of attributes in the tuple descriptor (natts)
- Column names are converted from PostgreSQL's internal Name type to Python Unicode strings
- This method is typically used for introspecting query results in PL/Python functions
- The method takes no arguments (METH_NOARGS in the method definition)
- Part of the public API for PLyResult objects accessible from Python code