# TrackFunctionsLevel

## Location
[src/include/pgstat.h:66-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L66-L67)

## Overview
TrackFunctionsLevel is an enumeration that defines the possible values for the track_functions GUC (Grand Unified Configuration) parameter, controlling which function calls are tracked for statistics collection in PostgreSQL.

## Definition


## Detailed Description
TrackFunctionsLevel controls the granularity of function call statistics collection in PostgreSQL. The enum values form a hierarchy where higher numeric values include broader tracking scope. This enumeration is used by the track_functions GUC parameter to determine which function calls should be monitored and have their execution statistics recorded. The order of values is significant as it allows for numeric comparisons to determine tracking levels.

## Parameters / Member Variables
- : Disables function call tracking entirely - no statistics are collected for any function calls
- : Enables tracking only for procedural language (PL) functions such as PL/pgSQL, PL/Python, etc., but not built-in SQL functions
- : Enables comprehensive tracking for all function calls, including both procedural language functions and built-in SQL functions

## Dependencies
- Functions called/Symbols referenced:
  - Used as type for pgstat_track_functions global variable
- Called from (representative examples):
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md) (for tracking level comparison)
  - Function execution paths in executor/execExpr.c
  - GUC configuration system in guc_tables.c

## Notes and Other Information
- The enumeration order is explicitly documented as significant because it allows numeric comparisons (e.g., pgstat_track_functions <= flinfo->fn_stats)
- This setting can significantly impact performance when set to TRACK_FUNC_ALL as it tracks every function call
- Commonly used in performance monitoring and query optimization scenarios
- The global variable pgstat_track_functions of this type is declared in pgstat_function.c
- Function statistics are only collected when the tracking level is appropriate for the function type being called