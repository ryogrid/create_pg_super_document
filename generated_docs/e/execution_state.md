# execution_state

## Location
src/backend/executor/functions.c: 65 - 73

## Overview
The execution_state structure represents the execution state of individual SQL statements within a SQL function, maintaining information about statement status, result handling, and execution context.

## Definition


## Detailed Description
The execution_state structure is a fundamental component of PostgreSQL's SQL function execution system, defined in src/backend/executor/functions.c:65-73. It serves as a node in a linked list that tracks the execution state of each SQL statement within a SQL function body. This structure enables PostgreSQL to manage complex SQL functions that contain multiple statements, handling their sequential execution, result management, and lazy evaluation strategies.

Each execution_state node represents one parsed and planned SQL statement from the function body. The structure supports both eager and lazy evaluation modes, allowing PostgreSQL to optimize function execution based on the specific requirements of each statement and the overall function context.

## Parameters / Member Variables
- : Pointer to the next execution_state in the linked list, allowing chaining of multiple SQL statements within a function
- : Current execution status of the statement, tracked using ExecStatus enumeration values
- : Boolean flag indicating whether this particular query produces the function's final result value
- : Boolean flag controlling evaluation strategy - when true, the statement fetches one row at a time rather than materializing all results
- : Pointer to the PlannedStmt containing the optimized execution plan for this SQL statement
- : Pointer to QueryDesc structure containing runtime execution context; remains null unless the statement status is RUN

## Dependencies
- Functions called/Symbols referenced:
  - ExecStatus
  - PlannedStmt
  - QueryDesc

- Called from (representative examples):
  - init_execution_state
  - postquel_start
  - postquel_getnext
  - postquel_end
  - fmgr_sql
  - sql_exec_error_callback
  - ShutdownSQLFunction

## Notes and Other Information
The execution_state structure is primarily used within the SQL function execution framework in PostgreSQL. It's part of a linked list architecture that allows SQL functions to contain multiple statements while maintaining proper execution order and state management. The structure supports PostgreSQL's lazy evaluation optimization, which can significantly improve performance for functions that may not need to fully execute all statements depending on control flow and result requirements. The lazyEval flag is particularly important for set-returning functions and functions with conditional logic that may not require full result materialization.