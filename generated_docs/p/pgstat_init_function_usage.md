# pgstat_init_function_usage

## Location
[src/backend/utils/activity/pgstat_function.c:72-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L72-L145)

## Overview
Initializes function call usage tracking before a function execution, setting up timing and statistics collection while handling edge cases like concurrent function deletion.

## Definition
```c
void pgstat_init_function_usage(FunctionCallInfo fcinfo, PgStat_FunctionCallUsage *fcu)
```

## Detailed Description
This function initializes the statistics tracking infrastructure before executing a user-defined function. It checks if function statistics should be tracked based on the `pgstat_track_functions` setting and the function's statistics level. If tracking is enabled, it prepares a pending statistics entry and handles the complex case where a function might have been dropped concurrently. The function also sets up timing infrastructure by recording the current time and saving existing statistics for recursion compensation. It includes sophisticated logic to detect and handle functions that have been deleted while a statement calling them is still being executed.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing metadata about the function being called, including its OID and statistics tracking level
- `fcu`: Function call usage structure that will be populated with timing and statistics tracking information for the duration of the function call

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_pending_entry](pgstat_prep_pending_entry.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - SearchSysCacheExists1
  - pgstat_drop_entry
  - INSTR_TIME_SET_CURRENT
  - PGSTAT_KIND_FUNCTION
- Called from (representative examples):
  - [ExecEvalFuncExprFusage](../E/ExecEvalFuncExprFusage.md) (in src/backend/executor/execExprInterp.c:2459)
  - [fmgr_security_definer](../f/fmgr_security_definer.md) (in src/backend/utils/fmgr/fmgr.c:745)
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md) (in src/backend/executor/execSRF.c:230)

## Notes and Other Information
- Performs concurrent deletion detection by accepting invalidation messages when creating new statistics entries
- Handles function recursion by saving previous statistics state for later compensation
- Only tracks statistics if pgstat_track_functions setting allows it for the specific function's statistics level
- Coordinates with pgstat_drop_function to ensure reliable transaction-aware statistics management
- Located in src/backend/utils/activity/pgstat_function.c:72-145
- Critical for accurate function performance monitoring and prevents statistics corruption in edge cases