# fmgr_sql

## Location
src/backend/executor/functions.c: 1029 - 1405

## Overview
The primary function call manager for executing SQL-language functions, handling both scalar and set-returning functions with comprehensive execution state management, parameter substitution, snapshot handling, and result processing.

## Definition


## Detailed Description
fmgr_sql serves as the central entry point for executing SQL functions in PostgreSQL. It manages the complete lifecycle of SQL function execution, including cache validation and initialization, parameter conversion, execution state management, snapshot handling for transaction consistency, and result processing for both scalar and set-returning functions. The function supports both lazy evaluation (returning one result at a time) and materialized evaluation (returning all results at once) for set-returning functions. It handles complex scenarios like multi-statement functions, proper cleanup through error context callbacks, and manages memory contexts to ensure proper resource cleanup.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which expands to  parameter containing function call information, arguments, and execution context

## Dependencies
- Functions called/Symbols referenced:
  - sql_exec_error_callback
  - init_sql_fcache  
  - postquel_sub_params
  - postquel_start
  - postquel_getnext
  - postquel_end
  - postquel_get_single_result
  - SubTransactionIsActive
  - tuplestore_begin_heap
  - RegisterExprContextCallback
  - UnregisterExprContextCallback
  - CreateTupleDescCopy
- Called from (representative examples):
  - Function manager system via fmgr_info_cxt_security

## Notes and Other Information
- Implements comprehensive error handling with sql_exec_error_callback for better error reporting
- Manages function cache validation based on transaction IDs to ensure cache consistency
- Handles both set-returning and scalar functions with different execution paths
- Supports lazy evaluation for set-returning functions, allowing suspension and resumption of execution
- Manages PostgreSQL snapshots to ensure proper transaction isolation during function execution
- Uses tuplestore for buffering results, especially important for set-returning functions
- Implements proper cleanup registration/deregistration to handle premature termination scenarios
- Handles multi-statement functions by iterating through execution states until completion or suspension
- Memory context switching ensures proper lifetime management of function-related data