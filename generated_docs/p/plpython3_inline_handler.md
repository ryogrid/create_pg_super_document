# plpython3_inline_handler

## Location
[src/pl/plpython/plpy_main.c:263-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L263-L338)

## Overview
The plpython3_inline_handler function is responsible for executing inline Python code blocks (DO blocks) in PostgreSQL's PL/Python language handler.

## Definition
Datum plpython3_inline_handler(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the entry point for executing inline Python code blocks submitted via PostgreSQL's DO statement. It creates a temporary procedure context, compiles the Python source code, and executes it within a properly managed execution environment. The function handles both atomic and non-atomic execution contexts depending on the inline code block's requirements.

The function performs comprehensive setup including SPI connection establishment, memory context management, error handling setup, and proper cleanup of resources after execution. It uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to ensure proper cleanup even when errors occur during execution.

## Parameters / Member Variables
- The InlineCodeBlock contains:
  - : The Python code to execute
  - : Language identifier for the Python language handler  
  - : Boolean indicating whether the block should execute atomically

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_initialize](../P/PLy_initialize.md): Initializes the Python language environment
  - [SPI_connect_ext](../S/SPI_connect_ext.md): Establishes SPI connection for database access
  - AllocSetContextCreate: Creates memory context for the procedure
  - [PLy_push_execution_context](../P/PLy_push_execution_context.md): Sets up execution context stack
  - [PLy_procedure_compile](../P/PLy_procedure_compile.md): Compiles the Python source code
  - [PLy_exec_function](../P/PLy_exec_function.md): Executes the compiled Python procedure
  - [PLy_pop_execution_context](../P/PLy_pop_execution_context.md): Cleans up execution context
  - [PLy_procedure_delete](../P/PLy_procedure_delete.md): Deallocates procedure resources
  - [plpython_inline_error_callback](plpython_inline_error_callback.md): Error callback for inline execution
- Called from (representative examples):
  - Called directly by PostgreSQL's function call mechanism for DO blocks

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c:263-338
- Creates a temporary procedure named "__plpython_inline_block" for execution
- Uses SPI_OPT_NONATOMIC flag for non-atomic blocks to allow transaction control
- Implements proper exception handling to ensure cleanup of Python state and memory contexts
- The function always returns void (PG_RETURN_VOID) as inline blocks don't produce return values
- Note in code mentions that SPI_finish() happens in plpy_exec.c, which is described as "dubious design"