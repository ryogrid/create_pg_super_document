# IsQueryIdEnabled

## Location
[src/include/nodes/queryjumble.h:77-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/queryjumble.h#L77-L86)

## Overview
IsQueryIdEnabled is a static inline function that determines whether query identifier computation is currently enabled based on the compute_query_id GUC setting and module-specific enablement.

## Definition

```c
static inline bool
IsQueryIdEnabled(void)
```
## Detailed Description
This function provides a centralized way to check if query ID computation should be performed. It evaluates the compute_query_id GUC parameter and returns the appropriate boolean value:

- If compute_query_id is set to COMPUTE_QUERY_ID_OFF, it always returns false
- If compute_query_id is set to COMPUTE_QUERY_ID_ON, it always returns true  
- For COMPUTE_QUERY_ID_AUTO or COMPUTE_QUERY_ID_REGRESS modes, it defers to the query_id_enabled global variable, which can be set by modules like pg_stat_statements

The function is defined as a static inline in the header file for optimal performance since it's called frequently during query processing.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - compute_query_id (global GUC variable)
  - query_id_enabled (global boolean variable)
  - COMPUTE_QUERY_ID_OFF (enum constant)
  - COMPUTE_QUERY_ID_ON (enum constant)
- Called from (representative examples):
  - analyze.c:123 (in parse_analyze)
  - analyze.c:165 (in parse_analyze_fixedparams)
  - analyze.c:202 (in parse_analyze_varparams)
  - explain.c:309 (in ExplainOnePlan)
  - queryjumblefuncs.c:109 (assertion check)

## Notes and Other Information
- This function should be used instead of directly checking compute_query_id to ensure proper handling of the AUTO mode
- The function is performance-critical as it's called during every query analysis
- When compute_query_id is COMPUTE_QUERY_ID_AUTO, modules like pg_stat_statements can enable query ID computation by setting query_id_enabled to true
- Located in src/include/nodes/queryjumble.h:77-84