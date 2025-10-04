# PLy_cursor_query

## Location
[src/pl/plpython/plpy_cursorobject.c:78-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_cursorobject.c#L78-L140)

## Overview
Creates a PL/Python cursor object from a SQL query string by preparing the query and opening a PostgreSQL portal.

## Definition

```c
static PyObject *
PLy_cursor_query(const char *query)
```
## Detailed Description
PLy_cursor_query creates a cursor object for executing and iterating through the results of a SQL query string. The function performs several key operations: allocates and initializes a PLyCursorObject, creates a dedicated memory context for the cursor, prepares the SQL query using SPI_prepare(), opens a portal using SPI_cursor_open(), and sets up the necessary infrastructure for converting PostgreSQL tuples to Python objects.

The function operates within a subtransaction to ensure proper error handling and resource cleanup. It validates the query string encoding, prepares the query plan, and creates a named portal that can be used for incremental result fetching. The portal is pinned to prevent premature cleanup and the portal name is stored in the cursor object for later access.

## Parameters / Member Variables
- `*query`: SQL query string to be executed through the cursor
## Dependencies
- Functions called/Symbols referenced:
  - PyObject_New (Python C API)
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - AllocSetContextCreate
  - [PLy_input_setup_func](PLy_input_setup_func.md)
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)
  - [pg_verifymbstr](../p/pg_verifymbstr.md)
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [SPI_freeplan](../S/SPI_freeplan.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [PinPortal](PinPortal.md)
  - [PLy_spi_subtransaction_commit](PLy_spi_subtransaction_commit.md)
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md)
- Called from (representative examples):
  - [PLy_cursor](PLy_cursor.md)

## Notes and Other Information
- Creates a dedicated memory context "PL/Python cursor context" for cursor-related allocations
- Uses PostgreSQL's portal mechanism for efficient streaming of large result sets
- Operates within a subtransaction to provide proper exception handling and cleanup
- Validates query string encoding using pg_verifymbstr() before preparation
- The portal is pinned to prevent automatic cleanup and must be explicitly unpinned when the cursor is closed
- Sets up result tuple conversion infrastructure using PLy_input_setup_func() for RECORDOID type
- Returns NULL on any error, with appropriate error handling through the subtransaction mechanism

## Simplified Source

```c
static PyObject *PLy_cursor_query(const char *query) {
    PLyCursorObject *cursor;
    PLyExecutionContext *exec_ctx = PLy_current_execution_context();
    volatile MemoryContext oldcontext = CurrentMemoryContext;
    volatile ResourceOwner oldowner = CurrentResourceOwner;

    // Create and initialize cursor object
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
        SPIPlanPtr plan;
        Portal portal;

        // Validate and prepare query
        pg_verifymbstr(query, strlen(query), false);
        plan = SPI_prepare(query, 0, NULL);
        if (plan == NULL)
            elog(ERROR, "SPI_prepare failed: %s", SPI_result_code_string(SPI_result));

        // Open cursor portal
        portal = SPI_cursor_open(NULL, plan, NULL, NULL, exec_ctx->curr_proc->fn_readonly);
        SPI_freeplan(plan);

        if (portal == NULL)
            elog(ERROR, "SPI_cursor_open() failed: %s", SPI_result_code_string(SPI_result));

        // Store portal name and pin it
        cursor->portalname = MemoryContextStrdup(cursor->mcxt, portal->name);
        PinPortal(portal);

        PLy_spi_subtransaction_commit(oldcontext, oldowner);
    }
    PG_CATCH();
    {
        PLy_spi_subtransaction_abort(oldcontext, oldowner);
        return NULL;
    }
    PG_END_TRY();

    return (PyObject *) cursor;
}
```