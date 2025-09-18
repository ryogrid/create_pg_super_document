# PLyExecutionContext

## Location
[src/pl/plpython/plpy_main.h:18-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.h#L18-L23)

## Overview
PLyExecutionContext is a structure that represents a stack-based execution context for PL/Python procedure calls. It maintains the execution state and scratch memory context for each level of nested Python function calls within PostgreSQL.

## Definition


## Detailed Description
PLyExecutionContext implements a stack-based execution model for PL/Python functions within PostgreSQL. Each time user-defined Python code is invoked, a new execution context is created and pushed onto the stack. When the Python code returns, the context is destroyed and popped from the stack. This design allows for proper handling of nested function calls, recursive calls, and ensures proper memory management and cleanup of resources associated with each execution level.

The structure serves as a crucial component in the PL/Python language handler, providing isolation between different execution levels and maintaining the necessary state information for each active Python procedure call. It supports the PostgreSQL memory context system by providing a dedicated scratch context for temporary allocations during type conversions and other operations.

## Parameters / Member Variables
- : Pointer to the PLyProcedure structure representing the currently executing Python procedure, containing compiled code, metadata, and cached information
- : A PostgreSQL MemoryContext used for temporary allocations during the execution of the current procedure, particularly for type input/output operations
- : Pointer to the previous PLyExecutionContext in the stack, creating a linked list structure that represents the call stack of nested Python procedures

## Dependencies
- Functions called/Symbols referenced:
  - [PLyProcedure](PLyProcedure.md) (procedure metadata structure)
  - [PLyExecutionContext](PLyExecutionContext.md) (self-reference for stack linking)
  - [MemoryContext](../M/MemoryContext.md) (PostgreSQL memory management)

- Called from (representative examples):
  - [PLy_push_execution_context](PLy_push_execution_context.md) (context stack management)
  - [PLy_pop_execution_context](PLy_pop_execution_context.md) (context stack management)
  - [PLy_get_scratch_context](PLy_get_scratch_context.md) (memory context access)
  - [plpython3_call_handler](../p/plpython3_call_handler.md) (main function call handler)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md) (inline code execution)
  - [PLy_cursor_query](PLy_cursor_query.md) (cursor operations)
  - [PLy_spi_prepare](PLy_spi_prepare.md) (SPI prepared statements)
  - [PLy_input_convert](PLy_input_convert.md) (type conversion operations)

## Notes and Other Information
- The execution context stack is essential for proper error handling and cleanup in nested Python function calls
- The scratch_ctx member provides a convenient memory context for temporary allocations that should be cleaned up when the function returns
- This structure is fundamental to the PL/Python implementation's ability to handle recursive function calls and maintain proper isolation between different execution levels
- The stack-based design ensures that each level of execution has its own isolated environment while maintaining access to the overall execution hierarchy
- Memory contexts associated with each execution level are properly managed through PostgreSQL's memory management system