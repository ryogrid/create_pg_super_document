# PLy_subtransaction_enter

## Location
[src/pl/plpython/plpy_subxactobject.c:84-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_subxactobject.c#L84-L136)

## Overview
Starts an explicit subtransaction in PL/Python, corresponding to the `__enter__()` method or `enter()` method of a subtransaction object.

## Definition

```c
static PyObject *
PLy_subtransaction_enter(PyObject *self, PyObject *unused)
```
## Detailed Description
This function implements the entry point for PL/Python subtransactions, allowing Python code to start explicit subtransactions within stored procedures. It validates that the subtransaction hasn't already been started or exited, then creates the necessary data structures and calls BeginInternalSubTransaction() to start the actual database subtransaction. The function manages memory contexts and resource owners to ensure proper cleanup, and maintains a list of active explicit subtransactions. SPI calls within an explicit subtransaction will not start another subtransaction, providing atomic execution control.

## Parameters / Member Variables
- `self`: The PLySubtransactionObject instance
- `unused`: Unused parameter for method signature compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_exception_set](PLy_exception_set.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [lcons](../l/lcons.md)
  - Py_INCREF
  - [PLySubtransactionData](PLySubtransactionData.md) (struct type)
  - [PLySubtransactionObject](PLySubtransactionObject.md) (struct type)
- Called from:
  - Python method dispatch system (registered as `__enter__` or `enter`)

## Notes and Other Information
- Validates subtransaction state to prevent multiple entries or entry after exit
- Creates PLySubtransactionData in TopTransactionContext for longevity
- Manages memory context switching to preserve caller's context
- Adds subtransaction data to global explicit_subtransactions list
- Returns self with incremented reference count for Python context manager protocol
- Part of Python's context manager protocol (`with` statement support)
- Located in src/pl/plpython/plpy_subxactobject.c:84-136

## Simplified Source

```c
static PyObject *PLy_subtransaction_enter(PyObject *self, PyObject *unused) {
    PLySubtransactionData *subxactdata;
    MemoryContext oldcontext;
    PLySubtransactionObject *subxact = (PLySubtransactionObject *) self;

    // Validate subtransaction state
    if (subxact->started) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has already been entered");
        return NULL;
    }
    if (subxact->exited) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has already been exited");
        return NULL;
    }

    // Mark as started and save current context
    subxact->started = true;
    oldcontext = CurrentMemoryContext;

    // Create subtransaction data in long-lived context
    subxactdata = (PLySubtransactionData *)
        MemoryContextAlloc(TopTransactionContext, sizeof(PLySubtransactionData));
    subxactdata->oldcontext = oldcontext;
    subxactdata->oldowner = CurrentResourceOwner;

    // Start the database subtransaction
    BeginInternalSubTransaction(NULL);

    // Add to explicit subtransactions list
    MemoryContextSwitchTo(TopTransactionContext);
    explicit_subtransactions = lcons(subxactdata, explicit_subtransactions);
    MemoryContextSwitchTo(oldcontext);

    // Return self for context manager protocol
    Py_INCREF(self);
    return self;
}
```