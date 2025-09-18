# plpython_trigger_error_callback

## Location
[src/pl/plpython/plpy_exec.c:1052-1061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L1052-L1061)

## Overview
An error context callback function that provides contextual information when errors occur during PL/Python trigger row modification operations.

## Definition


## Detailed Description
This function serves as an error context callback specifically for PL/Python trigger operations. It is registered with PostgreSQL's error context system to provide additional context information when errors occur during trigger execution, particularly during row modification operations. When an error occurs, this callback checks if there is an active PL/Python execution context with a current procedure, and if so, adds the contextual message "while modifying trigger row" to the error report. This helps developers and administrators understand that the error occurred specifically during the trigger row modification phase of PL/Python execution.

## Parameters / Member Variables
- : Unused argument parameter (required by error callback function signature)

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](../P/PLy_current_execution_context.md)
  - [PLyExecutionContext](../P/PLyExecutionContext.md)
  - errcontext
- Called from (representative examples):
  - [PLy_modify_tuple](../P/PLy_modify_tuple.md) (as error callback)

## Notes and Other Information
This function is designed to be used as a callback in PostgreSQL's error context stack system. It is set up as the callback function in an ErrorContextCallback structure before potentially error-prone operations and is automatically invoked by PostgreSQL's error reporting system when an error occurs. The function follows the standard error callback pattern of checking for an active execution context before adding contextual information. The contextual message helps distinguish trigger row modification errors from other types of PL/Python errors, improving debugging and error diagnosis.