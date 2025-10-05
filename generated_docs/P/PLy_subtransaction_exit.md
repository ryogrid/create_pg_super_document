# PLy_subtransaction_exit

## Location
[src/pl/plpython/plpy_subxactobject.c:137-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_subxactobject.c#L137-L186)

## Overview
Exits an explicit subtransaction in PL/Python, implementing the `__exit__()` method for context manager protocol or direct `exit()` method calls.

## Definition

```c
static PyObject *
PLy_subtransaction_exit(PyObject *self, PyObject *args)
```
## Detailed Description
This function handles the exit from PL/Python subtransactions, following Python's context manager protocol (PEP 343). It accepts exception information as parameters and decides whether to commit or abort the subtransaction based on whether an exception occurred. If exc_type is None, the subtransaction is committed via ReleaseCurrentSubTransaction(); otherwise, it's aborted using RollbackAndReleaseCurrentSubTransaction(). The function performs extensive validation, restores the previous memory context and resource owner, and cleans up the subtransaction data from the explicit_subtransactions list.

## Parameters / Member Variables
- `self`: The PLySubtransactionObject instance  
- `args`: Python tuple containing (exc_type, exc_value, traceback) per context manager protocol
  - `type`: Exception type (None if no exception)
  - `value`: Exception value/instance
  - `traceback`: Exception traceback object

## Dependencies
- Functions called/Symbols referenced:
  - PyArg_ParseTuple
  - [PLy_exception_set](PLy_exception_set.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)  
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - linitial
  - [list_delete_first](../l/list_delete_first.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [pfree](../p/pfree.md)
  - Py_RETURN_NONE
  - [PLySubtransactionData](PLySubtransactionData.md) (struct type)
  - [PLySubtransactionObject](PLySubtransactionObject.md) (struct type)
- Called from:
  - Python method dispatch system (registered as `__exit__` or `exit`)

## Notes and Other Information
- Validates subtransaction state to ensure it was properly entered and not already exited
- Commits subtransaction when no exception (type == Py_None), aborts when exception present
- Restores previous memory context and resource owner from saved subtransaction data  
- Removes and frees subtransaction data from explicit_subtransactions list
- Supports Python's `with` statement through context manager protocol
- Returns Py_None as required by context manager protocol
- Located in src/pl/plpython/plpy_subxactobject.c:137-186

## Simplified Source

```c
static PyObject *PLy_subtransaction_exit(PyObject *self, PyObject *args) {
    PyObject *type, *value, *traceback;
    PLySubtransactionData *subxactdata;
    PLySubtransactionObject *subxact = (PLySubtransactionObject *) self;

    // Parse arguments (exc_type, exc_value, traceback)
    if (!PyArg_ParseTuple(args, "OOO", &type, &value, &traceback))
        return NULL;

    // Validate subtransaction state
    if (!subxact->started) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has not been entered");
        return NULL;
    }
    if (subxact->exited) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has already been exited");
        return NULL;
    }
    if (explicit_subtransactions == NIL) {
        PLy_exception_set(PyExc_ValueError, "there is no subtransaction to exit from");
        return NULL;
    }

    subxact->exited = true;

    // Commit or abort based on exception status
    if (type != Py_None) {
        RollbackAndReleaseCurrentSubTransaction();  // Exception occurred
    } else {
        ReleaseCurrentSubTransaction();  // Success
    }

    // Restore previous context and cleanup
    subxactdata = (PLySubtransactionData *) linitial(explicit_subtransactions);
    explicit_subtransactions = list_delete_first(explicit_subtransactions);

    MemoryContextSwitchTo(subxactdata->oldcontext);
    CurrentResourceOwner = subxactdata->oldowner;
    pfree(subxactdata);

    Py_RETURN_NONE;
}
```