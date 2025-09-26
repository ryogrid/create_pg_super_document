# PgStat_FunctionCallUsage

## Location
[src/include/pgstat.h:117-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L117-L128)

## Overview
PgStat_FunctionCallUsage is a working state structure used to accumulate per-function-call timing statistics during function execution in PostgreSQL.

## Definition
```c
typedef struct PgStat_FunctionCallUsage
{
    /* Link to function's hashtable entry (must still be there at exit!) */
    /* NULL means we are not tracking the current function call */
    PgStat_FunctionCounts *fs;
    /* Total time previously charged to function, as of function start */
    instr_time    save_f_total_time;
    /* Backend-wide total time as of function start */
    instr_time    save_total;
    /* system clock as of function start */
    instr_time    start;
} PgStat_FunctionCallUsage;
```

## Detailed Description
This structure serves as temporary working state to accurately measure and accumulate timing statistics for individual function calls. It maintains timing snapshots from the beginning of function execution that are used to calculate the actual time spent in the function when the call completes. The structure enables PostgreSQL to track both total execution time (including nested function calls) and self time (excluding nested calls) by preserving baseline measurements.

The fs pointer links to the function's entry in the statistics hash table, and when NULL, indicates that the current function call is not being tracked for statistics purposes.

## Parameters / Member Variables
- `*fs`: Pointer to the function's hashtable entry in PgStat_FunctionCounts; NULL indicates no tracking for current call
- `save_f_total_time`: Snapshot of total time previously charged to this function at the start of current call
- `save_total`: Snapshot of backend-wide total execution time at function start
- `start`: System clock timestamp marking when the function call began
## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_FunctionCounts](PgStat_FunctionCounts.md)
  - [instr_time](../i/instr_time.md)
- Called from (representative examples):
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md) (initialize function call tracking)
  - [pgstat_end_function_usage](../p/pgstat_end_function_usage.md) (finalize function call tracking)
  - [ExecEvalFuncExprFusage](../E/ExecEvalFuncExprFusage.md) (expression evaluation with function usage tracking)
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md) (CALL statement execution)
  - [ExecCallTriggerFunc](../E/ExecCallTriggerFunc.md) (trigger function execution)

## Notes and Other Information
- Used for precise timing measurement during function execution
- The fs pointer must remain valid throughout the function call duration
- Enables calculation of both inclusive and exclusive execution times
- Critical component of PostgreSQL's function performance monitoring system
- Located at src/include/pgstat.h:117-128