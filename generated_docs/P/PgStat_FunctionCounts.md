# PgStat_FunctionCounts

## Location
[src/include/pgstat.h:107-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L107-L112)

## Overview
PgStat_FunctionCounts is a structure that stores per-function execution statistics in backend local memory while accumulating counts during function execution.

## Definition

```c
typedef struct PgStat_FunctionCounts
{
	PgStat_Counter numcalls;
	instr_time	total_time;
	instr_time	self_time;
} PgStat_FunctionCounts;
```
## Detailed Description
This structure is designed to hold actual event counters for function execution statistics in the backend's local memory. It serves as an accumulation buffer for function call statistics before they are flushed to the statistics collector. The structure contains only actual event counters to enable efficient zero-detection through memcmp operations to determine if there are pending statistics to be reported.

The time counters are stored in instr_time format within this structure and are converted to microseconds in PgStat_Counter format when the pending statistics are flushed out to the statistics collector.

## Parameters / Member Variables
- : Counter tracking the number of times the function has been called
- : Total time spent in function execution, including time spent in called functions (instr_time format)
- : Time spent in the function itself, excluding time spent in called functions (instr_time format)

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter
  - instr_time
- Called from (representative examples):
  - pgstat_init_function_usage (statistics initialization)
  - pgstat_end_function_usage (statistics finalization)
  - pgstat_function_flush_cb (statistics flushing)
  - PgStat_FunctionCallUsage (embedded in function call usage structure)

## Notes and Other Information
- This structure is optimized for performance by containing only actual counters
- Uses memcmp against zeroes for efficient pending statistics detection
- Time values are stored in high-precision instr_time format and converted during flushing
- Part of PostgreSQL's statistics collection system for monitoring function performance
- Located at src/include/pgstat.h:107-112