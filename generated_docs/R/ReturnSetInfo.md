# ReturnSetInfo

## Location
src/include/nodes/execnodes.h: 330 - 343

## Overview
ReturnSetInfo is a communication structure passed to functions that return multiple rows, allowing the function to communicate return status and result data back to the caller.

## Definition


## Detailed Description
ReturnSetInfo serves as the communication protocol between PostgreSQL's function call mechanism and Set Returning Functions (SRFs). When a function is called that might return multiple rows, this structure is passed as fcinfo->resultinfo to coordinate the return of result sets. The structure supports different return modes: ValuePerCall (where the function is called multiple times, once per returned row), and Materialize (where the function returns all rows at once in a tuplestore). The caller sets up the expected result format and supported modes, while the function fills in the actual results and status information.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : Pointer to ExprContext providing the execution context for the function call
- : TupleDesc describing the tuple format expected by the caller
- : Bitmask indicating which SetFunctionReturnMode values the caller can handle
- : SetFunctionReturnMode indicating the actual return mode chosen by the function
- : ExprDoneCond status flag used in ValuePerCall mode to indicate completion state
- : Tuplestorestate holding the complete set of returned tuples in Materialize mode
- : TupleDesc describing the actual format of returned tuples (may differ from expectedDesc)

## Dependencies
- Functions called/Symbols referenced:
  - SetFunctionReturnMode (enumeration of function return modes)
  - ExprDoneCond (enumeration for expression completion status)
  - Tuplestorestate (structure for storing tuple sets)
  - ExprContext (expression evaluation context)
- Called from (representative examples):
  - ExecMakeTableFunctionResult (table function execution)
  - ExecMakeFunctionResultSet (set returning function execution)
  - fmgr_sql (SQL function manager)
  - Various built-in set returning functions (pg_stat_*, pg_ls_dir, etc.)

## Notes and Other Information
- Functions returning sets must raise an error if no ReturnSetInfo is provided in fcinfo->resultinfo
- The structure supports both per-call and materialized return modes for flexibility and performance
- In ValuePerCall mode, the function is called repeatedly until isDone indicates completion
- In Materialize mode, the function fills setResult with all tuples and returns immediately
- The expectedDesc and setDesc may differ if the function needs to return a different tuple format than expected
- This mechanism is fundamental to PostgreSQL's support for table functions and set returning functions
- Used extensively by system administration functions, statistical functions, and user-defined set returning functions