# plpython_return_error_callback

## Location
src/pl/plpython/plpy_exec.c: 695 - 704

## Overview
Error context callback function that provides additional context information when errors occur during PLpython function return value creation.

## Definition
```c
static void plpython_return_error_callback(void *arg)
```

## Detailed Description
This callback function is registered with PostgreSQL's error handling system to provide more specific error context when PLpython functions encounter errors during return value processing. When an error occurs, it checks if the currently executing procedure is a function (not a procedure) and adds the context message "while creating return value" to help users understand where the error occurred. This enhances error reporting by providing more precise information about the failure point in PLpython function execution.

## Parameters / Member Variables
- `arg`: void pointer argument (currently unused in the function implementation)

## Dependencies
- Functions called/Symbols referenced:
  - PLy_current_execution_context (gets current PLpython execution context)
  - errcontext (PostgreSQL error context reporting function)
  - PLyExecutionContext (execution context structure type)
- Called from (representative examples):
  - PLy_exec_function (registered as error callback at src/pl/plpython/plpy_exec.c:194)

## Notes and Other Information
- This is a static function internal to plpy_exec.c
- Registered as an error context callback using PostgreSQL's error handling infrastructure
- Only adds context for functions, not procedures (checked via is_procedure flag)
- Provides user-friendly error messages by indicating the specific phase where the error occurred
- Part of PLpython's integration with PostgreSQL's comprehensive error reporting system
- Helps distinguish between errors in function execution vs. errors in return value processing
- The context message helps users debug issues with return value conversion or formatting