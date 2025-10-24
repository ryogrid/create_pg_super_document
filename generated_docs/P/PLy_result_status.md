# PLy_result_status

## Location
[src/pl/plpython/plpy_resultobject.c:196-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L196-L204)

## Overview
Returns the status string of a SQL command execution result in PLython, indicating the type and outcome of the executed command.

## Definition

```c
static PyObject *
PLy_result_status(PyObject *self, PyObject *args)
```
## Detailed Description
This function is a Python method implementation that provides access to the status information of a PLython result object. The status typically contains a string describing the command that was executed and its outcome, such as 'SELECT 5' for a SELECT query that returned 5 rows, or 'INSERT 0 1' for an INSERT that affected 1 row. The function returns the pre-stored status string from the PLyResultObject structure, properly managing reference counting.

## Parameters / Member Variables
- `*self`: PLyResultObject pointer cast as PyObject, representing the result object instance
- `*args`: Python tuple containing method arguments (currently unused but required for METH_VARARGS signature)
## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (cast target type)
  - Py_INCREF (Python reference count increment)
- Called from:
  - Exposed as 'status' method on PLython result objects

## Notes and Other Information
- The function uses METH_VARARGS calling convention but doesn't actually process any arguments
- Proper reference counting is maintained by incrementing the reference count before returning the status object
- The status value is set when the PLyResultObject is initially created from a PostgreSQL PGresult
- Status strings follow PostgreSQL's standard command tag format (e.g., 'SELECT n', 'INSERT oid n', 'UPDATE n', 'DELETE n')
- This provides Python code with access to the same status information available through PostgreSQL's libpq interface

## Simplified Source

```c
static PyObject *
PLy_result_status(PyObject *self, PyObject *args)
{
    // Cast to PLyResultObject to access stored data
    PLyResultObject *ob = (PLyResultObject *) self;

    // Return status string with proper reference counting
    Py_INCREF(ob->status);
    return ob->status;
}
```