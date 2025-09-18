# pgoutput_row_filter_exec_expr

## Location
src/backend/replication/pgoutput/pgoutput.c: 850 - 872

## Overview
Executes row filter expressions to determine whether a change should be replicated, treating NULL results as false to exclude the change from replication.

## Definition


## Detailed Description
This function evaluates row filter expressions in the context of logical replication to determine if a particular row change should be transmitted to subscribers. It uses PostgreSQL's expression evaluation infrastructure to execute the compiled filter expression within the provided expression context. The function implements specific semantics for NULL handling: if the expression evaluates to NULL, it is treated as false, meaning the change will not be replicated. This follows SQL's three-valued logic where NULL in a boolean context typically means 'unknown' but is treated as false for filtering purposes.

## Parameters / Member Variables
- : ExprState pointer representing the compiled expression to evaluate
- : ExprContext pointer providing the execution context including tuple data and variable values

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExprSwitchContext
  - DatumGetBool (macro)
  - elog
  - Assert (macro)
  - DEBUG3 (log level constant)
  - ExprState (type)
  - ExprContext (type)
  - Datum (type)
- Called from (representative examples):
  - pgoutput_row_filter (multiple times for different filtering scenarios)

## Notes and Other Information
The function includes debug logging at level DEBUG3 to help troubleshoot row filtering behavior, showing both the evaluation result and whether it was NULL. The NULL-as-false semantics are important for replication consistency - when a filter expression cannot be evaluated (returns NULL), the safest approach is to exclude the change rather than risk replicating inappropriate data. The function uses ExecEvalExprSwitchContext which handles memory context switching for safe expression evaluation in the replication context.