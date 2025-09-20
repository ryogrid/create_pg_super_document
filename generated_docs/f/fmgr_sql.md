# fmgr_sql

## Location
[src/backend/executor/functions.c:1029-1405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L1029-L1405)

## Overview
The primary function call manager for executing SQL-language functions, handling both scalar and set-returning functions with comprehensive execution state management, parameter substitution, snapshot handling, and result processing.

## Definition

```c
Datum
fmgr_sql(PG_FUNCTION_ARGS)
```
## Detailed Description
fmgr_sql serves as the central entry point for executing SQL functions in PostgreSQL. It manages the complete lifecycle of SQL function execution, including cache validation and initialization, parameter conversion, execution state management, snapshot handling for transaction consistency, and result processing for both scalar and set-returning functions. The function supports both lazy evaluation (returning one result at a time) and materialized evaluation (returning all results at once) for set-returning functions. It handles complex scenarios like multi-statement functions, proper cleanup through error context callbacks, and manages memory contexts to ensure proper resource cleanup.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)  
  - [postquel_sub_params](../p/postquel_sub_params.md)
  - [postquel_start](../p/postquel_start.md)
  - [postquel_getnext](../p/postquel_getnext.md)
  - [postquel_end](../p/postquel_end.md)
  - [postquel_get_single_result](../p/postquel_get_single_result.md)
  - [SubTransactionIsActive](../S/SubTransactionIsActive.md)
  - tuplestore_begin_heap
  - [RegisterExprContextCallback](../R/RegisterExprContextCallback.md)
  - [UnregisterExprContextCallback](../U/UnregisterExprContextCallback.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
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