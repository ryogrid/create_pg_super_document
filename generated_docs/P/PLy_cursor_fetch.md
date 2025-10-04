# PLy_cursor_fetch

## Location
[src/pl/plpython/plpy_cursorobject.c:366-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_cursorobject.c#L366-L466)

## Overview
Fetches a specified number of rows from a PostgreSQL cursor and returns them as a PLyResultObject containing a Python list of row data.

## Definition
```c
static PyObject *PLy_cursor_fetch(PyObject *self, PyObject *args)
```

## Detailed Description
PLy_cursor_fetch is a static function that implements the "fetch" method for PLyCursor objects in PostgreSQL's PL/Python extension. It accepts a count parameter specifying how many rows to fetch from the cursor and returns a PLyResultObject containing the fetched rows as a Python list. The function performs extensive validation, including checking that the cursor is not closed and that the associated portal remains valid.

The function uses PostgreSQL's SPI (Server Programming Interface) to perform the actual cursor fetch operation within a subtransaction for safety. It handles the conversion of PostgreSQL tuple data to Python objects, with special consideration for Python list size limitations. The returned PLyResultObject includes status information, row count, and the actual row data formatted as Python objects.

## Parameters / Member Variables
- `self`: PyObject pointer to the PLyCursorObject instance
- `args`: Python tuple containing the fetch count parameter (parsed as integer)

## Dependencies
- Functions called/Symbols referenced:
  - PyArg_ParseTuple
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - [PLy_exception_set](PLy_exception_set.md)
  - [GetPortalByName](../G/GetPortalByName.md)
  - PortalIsValid
  - [PLy_result_new](PLy_result_new.md)
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md)
  - PyLong_FromLong
  - PyLong_FromUnsignedLongLong
  - PyList_New
  - [PLy_input_setup_tuple](PLy_input_setup_tuple.md)
  - [PLy_input_from_tuple](PLy_input_from_tuple.md)
  - PyList_SetItem
  - [SPI_freetuptable](../S/SPI_freetuptable.md)
  - [PLy_spi_subtransaction_commit](PLy_spi_subtransaction_commit.md)
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md)
- Called from (representative examples):
  - Registered as "fetch" method in PLy_cursor_methods array

## Notes and Other Information
- Enforces Python list size limitations (PY_SSIZE_T_MAX) for large result sets
- Returns a PLyResultObject with status SPI_OK_FETCH and row count information
- Uses subtransactions for transactional safety during fetch operations
- Memory management includes proper reference counting for Python objects
- Error handling covers closed cursors and invalid portals
- The function is exposed to Python as the "fetch" method on cursor objects

## Simplified Source

```c
static PyObject *PLy_cursor_fetch(PyObject *self, PyObject *args) {
    PLyCursorObject *cursor;
    int count;
    PLyResultObject *ret;
    PLyExecutionContext *exec_ctx = PLy_current_execution_context();
    volatile MemoryContext oldcontext = CurrentMemoryContext;
    volatile ResourceOwner oldowner = CurrentResourceOwner;
    Portal portal;

    if (!PyArg_ParseTuple(args, "i:fetch", &count))
        return NULL;

    cursor = (PLyCursorObject *) self;

    // Validate cursor state
    if (cursor->closed) {
        PLy_exception_set(PyExc_ValueError, "fetch from a closed cursor");
        return NULL;
    }

    portal = GetPortalByName(cursor->portalname);
    if (!PortalIsValid(portal)) {
        PLy_exception_set(PyExc_ValueError,
                         "iterating a cursor in an aborted subtransaction");
        return NULL;
    }

    // Create result object
    ret = (PLyResultObject *) PLy_result_new();
    if (ret == NULL)
        return NULL;

    PLy_spi_subtransaction_begin(oldcontext, oldowner);

    PG_TRY();
    {
        // Fetch specified number of rows
        SPI_cursor_fetch(portal, true, count);

        // Set result status and row count
        Py_DECREF(ret->status);
        ret->status = PyLong_FromLong(SPI_OK_FETCH);
        Py_DECREF(ret->nrows);
        ret->nrows = PyLong_FromUnsignedLongLong(SPI_processed);

        if (SPI_processed != 0) {
            // Check for Python list size limits
            if (SPI_processed > (uint64) PY_SSIZE_T_MAX)
                ereport(ERROR,
                        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                         errmsg("query result has too many rows to fit in a Python list")));

            // Create list and populate with rows
            Py_DECREF(ret->rows);
            ret->rows = PyList_New(SPI_processed);
            if (!ret->rows) {
                Py_DECREF(ret);
                ret = NULL;
            } else {
                PLy_input_setup_tuple(&cursor->result, SPI_tuptable->tupdesc, exec_ctx->curr_proc);

                for (uint64 i = 0; i < SPI_processed; i++) {
                    PyObject *row = PLy_input_from_tuple(&cursor->result,
                                                        SPI_tuptable->vals[i],
                                                        SPI_tuptable->tupdesc,
                                                        true);
                    PyList_SetItem(ret->rows, i, row);
                }
            }
        }

        SPI_freetuptable(SPI_tuptable);
        PLy_spi_subtransaction_commit(oldcontext, oldowner);
    }
    PG_CATCH();
    {
        PLy_spi_subtransaction_abort(oldcontext, oldowner);
        return NULL;
    }
    PG_END_TRY();

    return (PyObject *) ret;
}
```