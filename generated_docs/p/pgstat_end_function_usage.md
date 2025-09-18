# pgstat_end_function_usage

## Location
[src/backend/utils/activity/pgstat_function.c:146-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L146-L192)

## Overview
Finalizes function call usage tracking after function execution, calculating timing statistics and updating counters while properly handling recursion and set-returning functions.

## Definition
```c
void pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)
```

## Detailed Description
This function completes the statistics tracking for a function call that was previously initialized with `pgstat_init_function_usage`. It performs sophisticated timing calculations to accurately measure both total execution time and self time (excluding time spent in nested function calls). The function handles recursive calls by using saved timing information to avoid double-counting. For set-returning functions that run in value-per-call mode, this function may be called multiple times for a single logical function call, with the `finalize` parameter indicating the final call. It updates both per-function statistics (call count, total time, self time) and the backend-wide total function time.

## Parameters / Member Variables
- `fcu`: Function call usage structure containing timing and statistics information that was set up during function initialization
- `finalize`: Boolean flag indicating whether this is the final call for this function invocation (important for set-returning functions and call counting)

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SUBTRACT
  - INSTR_TIME_ADD
  - [instr_time](../i/instr_time.md) (type)
  - PgStat_FunctionCounts (type)
- Called from (representative examples):
  - [ExecEvalFuncExprFusage](../E/ExecEvalFuncExprFusage.md) (in src/backend/executor/execExprInterp.c:2466)
  - [fmgr_security_definer](../f/fmgr_security_definer.md) (in src/backend/utils/fmgr/fmgr.c:753)
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md) (in src/backend/executor/execSRF.c:236)

## Notes and Other Information
- Handles complex timing calculations to avoid double-counting time spent in recursive function calls
- Updates call count only when finalize=true to properly handle set-returning functions
- Calculates both total time (including nested calls) and self time (excluding nested calls)
- Maintains backend-wide function timing statistics for overall performance monitoring
- Works in conjunction with pgstat_init_function_usage to provide complete function performance tracking
- Located in src/backend/utils/activity/pgstat_function.c:146-192
- Essential for accurate function performance analysis and PostgreSQL's query optimization