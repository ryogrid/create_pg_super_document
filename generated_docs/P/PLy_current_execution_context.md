# PLy_current_execution_context

## Location
[src/pl/plpython/plpy_main.c:367-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L367-L375)

## Overview
PLy_current_execution_context is a utility function that returns the current PL/Python execution context, providing access to the active execution state information.

## Definition
PLyExecutionContext *PLy_current_execution_context(void)

## Detailed Description
This function provides access to the current execution context for PL/Python functions and procedures. It serves as a centralized way to retrieve the active execution context, which contains important state information such as the current procedure, execution environment settings, and other context-specific data needed during Python code execution.

The function performs validation to ensure that there is indeed an active Python execution context before returning it. If called when no Python function is executing, it raises an error to prevent undefined behavior.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PLy_execution_contexts: Global variable tracking the execution context stack
  - elog: PostgreSQL logging/error function
- Called from (representative examples):
  - [PLy_cursor_query](PLy_cursor_query.md): For cursor operations requiring execution context
  - [PLy_spi_prepare](PLy_spi_prepare.md): For SPI operation context management
  - [PLy_traceback](PLy_traceback.md): For error reporting and traceback generation
  - [PLy_commit](PLy_commit.md)/PLy_rollback: For transaction control operations
  - Various type conversion functions in plpy_typeio.c

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c:367-375
- Returns a pointer to PLyExecutionContext structure containing execution state
- Validates that execution context exists before returning, raising ERROR if none is active
- Widely used throughout the PL/Python codebase for accessing current execution state
- Part of the execution context management system that maintains a stack of contexts
- Essential for operations that need access to current procedure information, memory contexts, and execution environment settings
- The function provides a safe way to access execution context with built-in error checking