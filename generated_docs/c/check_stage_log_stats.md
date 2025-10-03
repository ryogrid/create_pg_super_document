# check_stage_log_stats

## Location
[src/backend/tcop/postgres.c:3654-3667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3654-L3667)

## Overview
A GUC check hook function that validates log_parser_stats, log_planner_stats, and log_executor_stats configuration parameters, preventing them from being enabled when log_statement_stats is already true.

## Definition

```c
bool
check_stage_log_stats(bool *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for PostgreSQL's stage-specific logging statistics parameters (log_parser_stats, log_planner_stats, and log_executor_stats). It implements a mutual exclusion constraint with the log_statement_stats parameter - these stage-specific parameters cannot be enabled when log_statement_stats is already enabled, as they would provide redundant or conflicting information.

The function includes a note acknowledging that this is a "hack" implementation that may not work perfectly in all scenarios (such as when applying pg_db_role_setting values), but is tolerated because these are legacy settings with limited production usage.

## Parameters / Member Variables
- `*newval`: Pointer to the new boolean value being set for the stage log stats parameter
- `**extra`: Pointer to extra data (unused in this function)
- `source`: The source of the configuration change (GucSource enumeration)
## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail
  - log_statement_stats (global variable check)
  - GucSource
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H (src/include/utils/guc_hooks.h:139)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- Shared by multiple configuration parameters: log_parser_stats, log_planner_stats, log_executor_stats
- Implements mutual exclusion with log_statement_stats to prevent conflicting logging configurations
- Acknowledged as a legacy "hack" implementation with known limitations
- Works in conjunction with check_log_stats to maintain consistent logging parameter states
- Returns true if validation passes, false if the configuration conflicts with log_statement_stats
- May fail in complex configuration scenarios but is acceptable due to limited production usage