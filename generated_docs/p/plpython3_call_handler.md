# plpython3_call_handler

## Location
[src/pl/plpython/plpy_main.c:191-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L191-L262)

## Overview
Main entry point for executing PL/Python functions and triggers, handling the complete execution lifecycle including context management, error handling, and SPI connection setup.

## Definition

```c
structure pops this for us again at exit, so we needn't do that
		 * explicitly, nor do we risk the callback getting called after we've
		 * destroyed the exec_ctx.
		 */
		plerrcontext.callback = plpython_error_callback;
```
## Detailed Description
This function serves as the primary execution handler for PL/Python procedures and triggers within PostgreSQL. It manages the complete execution context, including SPI connections, execution context stack management, error handling, and proper cleanup. The function distinguishes between trigger and regular function calls, setting up appropriate execution environments for each type. It implements robust error handling using PostgreSQL's PG_TRY/PG_CATCH mechanism and ensures proper cleanup of Python execution contexts.

## Parameters / Member Variables
- : Boolean flag indicating whether the function runs in non-atomic context
- : Return value of the executed function
- : PL/Python execution context for the current call
- : Error context callback structure for enhanced error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_initialize](../P/PLy_initialize.md) (initialize PL/Python environment)
  - IsA (PostgreSQL type checking macro)
  - castNode (PostgreSQL type casting)
  - SPI_connect_ext (establish SPI connection with options)
  - elog (error logging)
  - [PLy_push_execution_context](../P/PLy_push_execution_context.md) (create execution context)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling)
  - [plpython_error_callback](plpython_error_callback.md) (error callback function)
  - CALLED_AS_TRIGGER (trigger detection macro)
  - [PLy_procedure_get](../P/PLy_procedure_get.md) (retrieve/compile procedure)
  - [PLy_exec_trigger](../P/PLy_exec_trigger.md) (execute trigger function)
  - [PLy_exec_function](../P/PLy_exec_function.md) (execute regular function)
  - [PointerGetDatum](../P/PointerGetDatum.md) (convert pointer to Datum)
  - [PLy_pop_execution_context](../P/PLy_pop_execution_context.md) (cleanup execution context)
  - PyErr_Clear (clear Python errors)
  - PG_RE_THROW (re-throw PostgreSQL exceptions)
- Called from (representative examples):
  - PostgreSQL's function call system when PL/Python functions are invoked

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c at lines 190-260
- Returns Datum type following PostgreSQL function call conventions
- Handles both regular functions and trigger functions with appropriate execution paths
- Implements comprehensive error handling with proper cleanup of execution contexts
- Uses SPI_OPT_NONATOMIC flag for functions that don't require atomicity
- Part of PostgreSQL's procedural language execution infrastructure
- Manages execution context stack to support nested PL/Python calls
- Includes detailed error context setup for better debugging and error reporting