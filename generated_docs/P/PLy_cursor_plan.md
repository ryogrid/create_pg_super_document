# PLy_cursor_plan

## Location
[src/pl/plpython/plpy_cursorobject.c:141-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_cursorobject.c#L141-L276)

## Overview
Creates a PL/Python cursor object from a prepared plan object and optional parameter arguments for parameterized query execution.

## Definition

```c
PyObject *
PLy_cursor_plan(PyObject *ob, PyObject *args)
```
## Detailed Description
PLy_cursor_plan creates a cursor object for executing prepared SQL plans with parameters. This function is more complex than PLy_cursor_query as it must handle parameter validation, conversion, and binding. It validates that the number of provided arguments matches the plan's expected parameter count, converts Python objects to PostgreSQL Datum values, and opens a cursor using the prepared plan.

The function performs comprehensive argument validation, ensuring the second parameter is a sequence (if provided) and that the argument count matches the plan's requirements. It creates a temporary memory context for parameter conversion to ensure proper cleanup, converts each Python argument to the appropriate PostgreSQL type using PLy_output_convert(), and binds these values when opening the cursor portal.

The entire operation is wrapped in a subtransaction for proper error handling and resource management, with careful cleanup of both the cursor object and temporary contexts in case of failure.

## Parameters / Member Variables
- `*ob`: PLyPlanObject containing the prepared plan to execute
- `*args`: Python sequence containing parameter values for the prepared plan (optional)
## Dependencies
- Functions called/Symbols referenced:
  - PySequence_Check (Python C API)
  - PyUnicode_Check (Python C API)
  - [PLy_exception_set](PLy_exception_set.md)
  - PySequence_Length (Python C API)
  - PLy_elog
  - [PLyUnicode_AsString](PLyUnicode_AsString.md)
  - [PLy_exception_set_plural](PLy_exception_set_plural.md)
  - PyObject_New (Python C API)
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - AllocSetContextCreate
  - [PLy_input_setup_func](PLy_input_setup_func.md)
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - PySequence_GetItem (Python C API)
  - [PLy_output_convert](PLy_output_convert.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [PinPortal](PinPortal.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [PLy_spi_subtransaction_commit](PLy_spi_subtransaction_commit.md)
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md)
- Called from (representative examples):
  - [PLy_cursor](PLy_cursor.md)
  - [PLy_plan_cursor](PLy_plan_cursor.md)

## Notes and Other Information
- Validates argument types and counts before proceeding with cursor creation
- Uses a temporary memory context for parameter conversion to ensure proper cleanup
- Supports parameterized queries through proper Datum conversion and null handling
- Implements comprehensive error handling with automatic cleanup of allocated resources
- Creates the same cursor infrastructure as PLy_cursor_query but with parameter binding
- Uses PLy_exception_set_plural() for proper singular/plural error message formatting
- The temporary context is automatically cleaned up during subtransaction abort if an error occurs
- Parameter conversion uses PLy_output_convert() which handles type conversion from Python to PostgreSQL types

## Simplified Source

```c
PyObject *PLy_cursor_plan(PyObject *ob, PyObject *args) {
    PLyCursorObject *cursor;
    int nargs;
    PLyPlanObject *plan = (PLyPlanObject *) ob;
    PLyExecutionContext *exec_ctx = PLy_current_execution_context();
    volatile MemoryContext oldcontext = CurrentMemoryContext;
    volatile ResourceOwner oldowner = CurrentResourceOwner;

    // Validate arguments
    if (args) {
        if (!PySequence_Check(args) || PyUnicode_Check(args)) {
            PLy_exception_set(PyExc_TypeError, "plpy.cursor takes a sequence as its second argument");
            return NULL;
        }
        nargs = PySequence_Length(args);
    } else {
        nargs = 0;
    }

    // Check argument count matches plan
    if (nargs != plan->nargs) {
        char *sv;
        PyObject *so = PyObject_Str(args);
        if (!so) PLy_elog(ERROR, "could not execute plan");
        sv = PLyUnicode_AsString(so);
        PLy_exception_set_plural(PyExc_TypeError,
                                "Expected sequence of %d argument, got %d: %s",
                                "Expected sequence of %d arguments, got %d: %s",
                                plan->nargs, plan->nargs, nargs, sv);
        Py_DECREF(so);
        return NULL;
    }

    // Create cursor object
    if ((cursor = PyObject_New(PLyCursorObject, &PLy_CursorType)) == NULL)
        return NULL;

    cursor->portalname = NULL;
    cursor->closed = false;
    cursor->mcxt = AllocSetContextCreate(TopMemoryContext,
                                        "PL/Python cursor context",
                                        ALLOCSET_DEFAULT_SIZES);

    // Set up tuple conversion
    PLy_input_setup_func(&cursor->result, cursor->mcxt, RECORDOID, -1, exec_ctx->curr_proc);

    PLy_spi_subtransaction_begin(oldcontext, oldowner);

    PG_TRY();
    {
        Portal portal;
        MemoryContext tmpcontext;
        Datum *values = NULL;
        char *nulls = NULL;

        // Create temporary context for parameter conversion
        tmpcontext = AllocSetContextCreate(CurTransactionContext,
                                          "PL/Python temporary context",
                                          ALLOCSET_SMALL_SIZES);
        MemoryContextSwitchTo(tmpcontext);

        // Convert parameters
        if (nargs > 0) {
            values = (Datum *) palloc(nargs * sizeof(Datum));
            nulls = (char *) palloc(nargs * sizeof(char));

            for (int j = 0; j < nargs; j++) {
                PLyObToDatum *arg = &plan->args[j];
                PyObject *elem = PySequence_GetItem(args, j);
                bool isnull;

                values[j] = PLy_output_convert(arg, elem, &isnull);
                nulls[j] = isnull ? 'n' : ' ';
                Py_DECREF(elem);
            }
        }

        MemoryContextSwitchTo(oldcontext);

        // Open cursor with prepared plan
        portal = SPI_cursor_open(NULL, plan->plan, values, nulls, exec_ctx->curr_proc->fn_readonly);
        if (portal == NULL)
            elog(ERROR, "SPI_cursor_open() failed: %s", SPI_result_code_string(SPI_result));

        cursor->portalname = MemoryContextStrdup(cursor->mcxt, portal->name);
        PinPortal(portal);

        MemoryContextDelete(tmpcontext);
        PLy_spi_subtransaction_commit(oldcontext, oldowner);
    }
    PG_CATCH();
    {
        Py_DECREF(cursor);
        PLy_spi_subtransaction_abort(oldcontext, oldowner);
        return NULL;
    }
    PG_END_TRY();

    return (PyObject *) cursor;
}
```