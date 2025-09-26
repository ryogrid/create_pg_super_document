# init_sql_fcache

## Location
[src/backend/executor/functions.c:583-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L583-L813)

## Overview
Initializes the SQLFunctionCache structure for a SQL function, handling parsing, planning, and validation of function body queries.

## Definition
```c
static void
init_sql_fcache(FunctionCallInfo fcinfo, Oid collation, bool lazyEvalOK)
```

## Detailed Description
This function performs comprehensive initialization of a SQL function's cache structure. It creates a dedicated memory context, retrieves function metadata from the system catalog, resolves polymorphic types, and processes the function body. The function handles both traditional prosrc text and newer prosqlbody stored parse trees. It validates query statements, sets up result type handling with junk filtering, and creates execution states for all queries. The cache enables efficient repeated execution of SQL functions.

## Parameters / Member Variables
- `fcinfo`: Function call information containing function OID and execution context
- `collation`: Collation to use for parameter symbol resolution in function parsing  
- `lazyEvalOK`: Boolean indicating whether lazy evaluation optimization is permitted for SELECT statements

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md)
  - [pg_parse_query](../p/pg_parse_query.md)
  - [pg_analyze_and_rewrite_withcb](../p/pg_analyze_and_rewrite_withcb.md)
  - [sql_fn_parser_setup](../s/sql_fn_parser_setup.md)
  - [check_sql_fn_statements](../c/check_sql_fn_statements.md)
  - [check_sql_fn_retval](../c/check_sql_fn_retval.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecInitJunkFilter](../E/ExecInitJunkFilter.md)
  - [ExecInitJunkFilterConversion](../E/ExecInitJunkFilterConversion.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - [init_execution_state](init_execution_state.md)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md)

## Notes and Other Information
- Creates dedicated memory context for function cache to ensure proper cleanup
- Handles both prosrc (text) and prosqlbody (stored parse trees) function bodies
- Resolves polymorphic types using actual call arguments
- Sets up junk filtering for result type coercion and dropped column handling
- Forces lazy evaluation for rowtype results returned as scalars to avoid materialization issues
- Marks cache with transaction IDs for validity checking
- All parsing and planning occurs in the function context, creating persistent cruft until module uses plancache.c