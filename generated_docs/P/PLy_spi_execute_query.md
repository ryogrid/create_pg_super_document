# PLy_spi_execute_query

## Location
[src/pl/plpython/plpy_spi.c:298-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.c#L298-L339)

## Overview
PLy_spi_execute_query executes a direct SQL query string without parameters, handling the execution within a subtransaction and processing the results for return to Python.

## Definition

```c
static PyObject *
PLy_spi_execute_query(char *query, long limit)
```
## Detailed Description
This static function provides the implementation for executing raw SQL query strings in PL/Python. It validates the query string encoding, executes it through PostgreSQL's SPI (Server Programming Interface), and processes the results. The function operates within a subtransaction to ensure proper error handling and resource cleanup.

The function respects the read-only status of the current procedure context and applies any specified row limit. It handles all aspects of query execution including encoding validation, SPI execution, result processing, and error reporting. Unlike PLy_spi_execute_plan, this function works with direct SQL strings rather than prepared statements.

## Parameters / Member Variables
- `*query`: NULL-terminated SQL query string to execute
- `limit`: Maximum number of rows to return (0 for no limit)
## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current execution context and procedure info
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)/commit/abort: Manages subtransaction lifecycle
  - [pg_verifymbstr](../p/pg_verifymbstr.md): Validates string encoding
  - [SPI_execute](../S/SPI_execute.md): PostgreSQL SPI function for direct query execution
  - [PLy_spi_execute_fetch_result](PLy_spi_execute_fetch_result.md): Processes execution results into Python objects
  - [PLy_exception_set](PLy_exception_set.md): Sets Python exceptions for error conditions
  - [SPI_result_code_string](../S/SPI_result_code_string.md): Converts SPI result codes to readable strings
- Called from (representative examples):
  - [PLy_spi_execute](PLy_spi_execute.md): When executing string queries through plpy.execute()

## Notes and Other Information
- This is a static function, only used internally within the plpy_spi.c module
- Validates query string encoding using pg_verifymbstr before execution
- Executes within a subtransaction for atomic error handling and resource management
- Respects the fn_readonly flag of the current procedure to enforce read-only constraints
- Handles both successful execution and error conditions with appropriate cleanup
- Uses PLy_spi_execute_fetch_result to convert SPI results into Python-accessible format
- Provides detailed error messages including SPI result codes when execution fails
- Does not support parameterized queries - parameters must be embedded in the query string
- Memory management is handled through the subtransaction mechanism

## Simplified Source

```c
static PyObject *PLy_spi_execute_query(char *query, long limit) {
    // Set up subtransaction for atomic execution
    volatile MemoryContext oldcontext = CurrentMemoryContext;
    volatile ResourceOwner oldowner = CurrentResourceOwner;
    PLy_spi_subtransaction_begin(oldcontext, oldowner);

    PyObject *ret = NULL;

    PG_TRY();
    {
        // Get current execution context to check read-only status
        PLyExecutionContext *exec_ctx = PLy_current_execution_context();

        // Validate query string encoding
        pg_verifymbstr(query, strlen(query), false);

        // Execute the SQL query through SPI
        int rv = SPI_execute(query, exec_ctx->curr_proc->fn_readonly, limit);

        // Process results into Python objects
        ret = PLy_spi_execute_fetch_result(SPI_tuptable, SPI_processed, rv);

        PLy_spi_subtransaction_commit(oldcontext, oldowner);
    }
    PG_CATCH();
    {
        // Abort subtransaction on error
        PLy_spi_subtransaction_abort(oldcontext, oldowner);
        return NULL;
    }
    PG_END_TRY();

    // Check for SPI execution errors
    if (rv < 0) {
        Py_XDECREF(ret);
        PLy_exception_set(PLy_exc_spi_error, "SPI_execute failed: %s",
                          SPI_result_code_string(rv));
        return NULL;
    }

    return ret;
}
```