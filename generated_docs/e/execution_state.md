# execution_state

## Location
[src/backend/executor/functions.c:65-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L65-L73)

## Overview
The execution_state structure represents the execution state of individual SQL statements within a SQL function, maintaining information about statement status, result handling, and execution context.

## Definition

```c
typedef struct execution_state
{
	struct execution_state *next;
	ExecStatus	status;
	bool		setsResult;		/* true if this query produces func's result */
	bool		lazyEval;		/* true if should fetch one row at a time */
	PlannedStmt *stmt;			/* plan for this query */
	QueryDesc  *qd;				/* null unless status == RUN */
} execution_state;
```
## Detailed Description
The execution_state structure is a fundamental component of PostgreSQL's SQL function execution system, defined in src/backend/executor/functions.c:65-73. It serves as a node in a linked list that tracks the execution state of each SQL statement within a SQL function body. This structure enables PostgreSQL to manage complex SQL functions that contain multiple statements, handling their sequential execution, result management, and lazy evaluation strategies.

Each execution_state node represents one parsed and planned SQL statement from the function body. The structure supports both eager and lazy evaluation modes, allowing PostgreSQL to optimize function execution based on the specific requirements of each statement and the overall function context.

## Parameters / Member Variables
- `*next`: Pointer to the next execution_state in the linked list, allowing chaining of multiple SQL statements within a function
- `status`: Current execution status of the statement, tracked using ExecStatus enumeration values
- `setsResult`: Boolean flag indicating whether this particular query produces the function's final result value
- `lazyEval`: Boolean flag controlling evaluation strategy - when true, the statement fetches one row at a time rather than materializing all results
- `*stmt`: Pointer to the PlannedStmt containing the optimized execution plan for this SQL statement
- `*qd`: Pointer to QueryDesc structure containing runtime execution context; remains null unless the statement status is RUN
## Dependencies
- Functions called/Symbols referenced:
  - [ExecStatus](../E/ExecStatus.md)
  - [PlannedStmt](../P/PlannedStmt.md)
  - QueryDesc

- Called from (representative examples):
  - [init_execution_state](../i/init_execution_state.md)
  - [postquel_start](../p/postquel_start.md)
  - [postquel_getnext](../p/postquel_getnext.md)
  - [postquel_end](../p/postquel_end.md)
  - [fmgr_sql](../f/fmgr_sql.md)
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md)
  - [ShutdownSQLFunction](../S/ShutdownSQLFunction.md)

## Notes and Other Information
The execution_state structure is primarily used within the SQL function execution framework in PostgreSQL. It's part of a linked list architecture that allows SQL functions to contain multiple statements while maintaining proper execution order and state management. The structure supports PostgreSQL's lazy evaluation optimization, which can significantly improve performance for functions that may not need to fully execute all statements depending on control flow and result requirements. The lazyEval flag is particularly important for set-returning functions and functions with conditional logic that may not require full result materialization.