# PLy_result_coltypes

## Location
[src/pl/plpython/plpy_resultobject.c:135-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L135-L160)

## Overview
Returns a Python list containing the column type OIDs of the result set represented by a PLyResultObject.

## Definition
```c
static PyObject *PLy_result_coltypes(PyObject *self, PyObject *unused)
```

## Detailed Description
This function extracts the column type OIDs from the tuple descriptor of a PLyResultObject and returns them as a Python list of integers. It is exposed as a method on PLyResult objects, allowing Python code to introspect the data types of columns in query results. The function validates that the result object has a valid tuple descriptor (meaning it represents a result set with columns) and raises an exception if called on a result that doesn't have one. Each type OID corresponds to a PostgreSQL data type and can be used to determine how to handle the data in each column.

## Parameters / Member Variables
- `self`: PyObject pointer to the PLyResultObject instance
- `unused`: PyObject pointer (not used, as this method takes no arguments)

Returns a Python list where each element is a Python long integer representing a PostgreSQL type OID, or NULL on error.

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_exception_set](PLy_exception_set.md)() (PL/Python exception handling)
  - PyList_New() (Python C API)
  - TupleDescAttr() (PostgreSQL tuple descriptor access)
  - PyLong_FromLong() (Python C API)
  - [PLyResultObject](PLyResultObject.md) (struct type)
- Called from (representative examples):
  - Referenced as "coltypes" method in PLy_result_methods at src/pl/plpython/plpy_resultobject.c:40
  - Callable from Python code as result.coltypes()

## Notes and Other Information
- This is a static function exposed as a Python method through the PLy_result_methods table
- Raises PLy_exc_error if called on a result object without a tuple descriptor
- The returned list length equals the number of attributes in the tuple descriptor (natts)
- Type OIDs are PostgreSQL's internal identifiers for data types (e.g., 23 for int4, 25 for text)
- This method is typically used for type introspection in PL/Python functions
- The method takes no arguments (METH_NOARGS in the method definition)
- Part of the public API for PLyResult objects accessible from Python code
- Often used in conjunction with colnames() to get both column names and their corresponding types
- The type OIDs can be looked up in PostgreSQL's pg_type system catalog

## Simplified Source

```c
// Simplified version of PLy_result_coltypes
static PyObject *PLy_result_coltypes(PyObject *self, PyObject *unused) {
    PLyResultObject *result_obj = (PLyResultObject *) self;

    // Validate result has column information
    if (!result_obj->tupdesc) {
        PLy_exception_set(PLy_exc_error, "command did not produce a result set");
        return NULL;
    }

    // Create Python list for column type OIDs
    PyObject *type_list = PyList_New(result_obj->tupdesc->natts);
    if (!type_list)
        return NULL;

    // Extract each column type OID from tuple descriptor
    for (int i = 0; i < result_obj->tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(result_obj->tupdesc, i);
        PyList_SET_ITEM(type_list, i, PyLong_FromLong(attr->atttypid));
    }

    return type_list;
}
```

Key simplifications made:
- Added descriptive variable names for clarity
- Added explanatory comments for each major step
- Focused on the main logic flow: validation, list creation, type OID extraction