# PLy_result_nrows

## Location
src/pl/plpython/plpy_resultobject.c: 187 - 195

## Overview
Returns the number of rows affected or returned by a SQL command executed in PLython, providing access to the result set size information.

## Definition


## Detailed Description
This function is a Python method implementation that provides access to the number of rows in a PLython result object. It simply returns the pre-stored nrows value from the PLyResultObject structure, which contains the count of rows affected by the last SQL command (for INSERT/UPDATE/DELETE) or the number of rows in the result set (for SELECT). The function increments the reference count of the nrows Python object before returning it to ensure proper memory management.

## Parameters / Member Variables
- : PLyResultObject pointer cast as PyObject, representing the result object instance
- : Python tuple containing method arguments (currently unused but required for METH_VARARGS signature)

## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (cast target type)  
  - Py_INCREF (Python reference count increment)
- Called from:
  - Exposed as 'nrows' method on PLython result objects

## Notes and Other Information
- The function uses METH_VARARGS calling convention but doesn't actually process any arguments
- Proper reference counting is maintained by incrementing the reference count before returning the nrows object
- The nrows value is set when the PLyResultObject is initially created from a PostgreSQL result
- This provides a Python-accessible way to get row counts without needing to know PostgreSQL's internal result handling