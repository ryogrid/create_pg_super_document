# PLy_result_colnames

## Location
[src/pl/plpython/plpy_resultobject.c:109-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L109-L134)

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
  - [PLy_exception_set](PLy_exception_set.md)() (PL/Python exception handling)
  - PyList_New() (Python C API)
  - TupleDescAttr() (PostgreSQL tuple descriptor access)
  - [PLyUnicode_FromString](PLyUnicode_FromString.md)() (PL/Python Unicode string conversion)
  - NameStr() (PostgreSQL name string conversion)
  - [PLyResultObject](PLyResultObject.md) (struct type)
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

## Simplified Source

```c
// Simplified version of PLy_result_colnames
static PyObject *PLy_result_colnames(PyObject *self, PyObject *unused) {
    PLyResultObject *result_obj = (PLyResultObject *) self;

    // Validate result has column information
    if (!result_obj->tupdesc) {
        PLy_exception_set(PLy_exc_error, "command did not produce a result set");
        return NULL;
    }

    // Create Python list for column names
    PyObject *column_list = PyList_New(result_obj->tupdesc->natts);
    if (!column_list)
        return NULL;

    // Extract each column name from tuple descriptor
    for (int i = 0; i < result_obj->tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(result_obj->tupdesc, i);
        PyList_SET_ITEM(column_list, i, PLyUnicode_FromString(NameStr(attr->attname)));
    }

    return column_list;
}
```

Key simplifications made:
- Added descriptive variable names for clarity
- Added explanatory comments for each major step
- Focused on the main logic flow: validation, list creation, name extraction