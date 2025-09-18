# check_log_stats

## Location
[src/backend/tcop/postgres.c:3668-3682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3668-L3682)

## Overview
A GUC check hook function that validates the log_statement_stats configuration parameter, preventing it from being enabled when any stage-specific logging parameters are already true.

## Definition


## Detailed Description
This function serves as a validation hook for PostgreSQL's log_statement_stats configuration parameter. It implements the complementary side of a mutual exclusion constraint with the stage-specific logging parameters (log_parser_stats, log_planner_stats, and log_executor_stats). When attempting to enable log_statement_stats, this function checks if any of the individual stage logging parameters are already enabled, and prevents the configuration if so.

This works in conjunction with check_stage_log_stats to ensure that statement-level logging and stage-specific logging cannot be enabled simultaneously, as they would provide redundant or conflicting statistical information.

## Parameters / Member Variables
- : Pointer to the new boolean value being set for log_statement_stats
- : Pointer to extra data (unused in this function)
- : The source of the configuration change (GucSource enumeration)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail
  - log_parser_stats (global variable check)
  - log_planner_stats (global variable check)
  - log_executor_stats (global variable check)
  - GucSource
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H (src/include/utils/guc_hooks.h:80)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- Specifically validates the log_statement_stats parameter
- Implements mutual exclusion with stage-specific logging parameters to prevent conflicting configurations
- Works as the counterpart to check_stage_log_stats for maintaining consistent logging parameter states
- Returns true if validation passes, false if the configuration conflicts with existing stage-specific logging
- Part of the legacy logging statistics system with limited production usage
- Provides detailed error messages listing all conflicting parameters when validation fails