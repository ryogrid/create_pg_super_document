# PLy_exec_function

## Location
[src/pl/plpython/plpy_exec.c:55-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L55-L320)

## Overview
PLy_exec_function is the main entry point for executing PL/Python functions and procedures, handling both regular functions and set-returning functions (SRFs) with comprehensive error handling and memory management.

## Definition

```c
Datum
PLy_exec_function(FunctionCallInfo fcinfo, PLyProcedure *proc)
```
## Detailed Description
This function serves as the core execution handler for PL/Python functions and procedures. It manages the complete lifecycle of function execution including:

1. **Argument Management**: Handles recursive function calls by pushing/popping arguments on a global stack
2. **Set-Returning Function Support**: Implements iterator-based processing for functions that return sets of values
3. **Memory Context Management**: Properly manages memory contexts for both regular and SRF execution
4. **Error Handling**: Provides comprehensive error handling with proper cleanup of Python objects and PostgreSQL resources
5. **Type Conversion**: Handles conversion between Python objects and PostgreSQL Datums, including special handling for void, record, and null values
6. **SPI Integration**: Manages SPI (Server Programming Interface) connections for database access

The function uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to ensure proper cleanup in case of errors, and implements a state machine approach for set-returning functions to maintain iteration state across multiple calls.

## Parameters / Member Variables
- : FunctionCallInfo structure containing function call context, arguments, and return information
- : PLyProcedure structure containing the compiled Python procedure information and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_global_args_push](PLy_global_args_push.md)/PLy_global_args_pop
  - [PLy_function_build_args](PLy_function_build_args.md)
  - [PLy_procedure_call](PLy_procedure_call.md)
  - [PLy_function_save_args](PLy_function_save_args.md)/PLy_function_restore_args
  - [PLy_output_convert](PLy_output_convert.md)/PLy_output_setup_record
  - SRF_* macros for set-returning function management
  - SPI_finish for database connection cleanup
  - PyIter_Next, PyObject_GetIter for Python iteration
- Called from (representative examples):
  - [plpython3_call_handler](../p/plpython3_call_handler.md) (main function handler)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md) (inline code handler)

## Notes and Other Information
- Supports both regular functions and set-returning functions through a unified interface
- Implements proper cleanup callbacks for SRF state to prevent memory leaks
- Handles special cases for void return types and procedure vs function semantics  
- Uses iterator protocol for efficient handling of large result sets in SRFs
- Maintains argument state across SRF calls to handle interleaved function evaluations
- Integrates with PostgreSQL's error context system for better error reporting
- File location: src/pl/plpython/plpy_exec.c:55-320