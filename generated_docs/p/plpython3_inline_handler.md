# plpython3_inline_handler

## Location
src/pl/plpython/plpy_main.c: 263 - 338

## Overview
The plpython3_inline_handler function is responsible for executing inline Python code blocks (DO blocks) in PostgreSQL's PL/Python language handler.

## Definition
Datum plpython3_inline_handler(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the entry point for executing inline Python code blocks submitted via PostgreSQL's DO statement. It creates a temporary procedure context, compiles the Python source code, and executes it within a properly managed execution environment. The function handles both atomic and non-atomic execution contexts depending on the inline code block's requirements.

The function performs comprehensive setup including SPI connection establishment, memory context management, error handling setup, and proper cleanup of resources after execution. It uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to ensure proper cleanup even when errors occur during execution.

## Parameters / Member Variables
- Takes a single parameter through PG_FUNCTION_ARGS macro, which contains an InlineCodeBlock pointer as the first argument
- The InlineCodeBlock contains:
  - : The Python code to execute
  - : Language identifier for the Python language handler  
  - : Boolean indicating whether the block should execute atomically

## Dependencies
- Functions called/Symbols referenced:
  - PLy_initialize: Initializes the Python language environment
  - SPI_connect_ext: Establishes SPI connection for database access
  - AllocSetContextCreate: Creates memory context for the procedure
  - PLy_push_execution_context: Sets up execution context stack
  - PLy_procedure_compile: Compiles the Python source code
  - PLy_exec_function: Executes the compiled Python procedure
  - PLy_pop_execution_context: Cleans up execution context
  - PLy_procedure_delete: Deallocates procedure resources
  - plpython_inline_error_callback: Error callback for inline execution
- Called from (representative examples):
  - Called directly by PostgreSQL's function call mechanism for DO blocks

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c:263-338
- Creates a temporary procedure named "__plpython_inline_block" for execution
- Uses SPI_OPT_NONATOMIC flag for non-atomic blocks to allow transaction control
- Implements proper exception handling to ensure cleanup of Python state and memory contexts
- The function always returns void (PG_RETURN_VOID) as inline blocks don't produce return values
- Note in code mentions that SPI_finish() happens in plpy_exec.c, which is described as "dubious design"