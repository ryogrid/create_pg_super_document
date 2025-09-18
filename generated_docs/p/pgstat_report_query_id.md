# pgstat_report_query_id

## Location
src/backend/utils/activity/backend_status.c: 613 - 652

## Overview
Updates the top-level query identifier in the backend status entry, enabling query tracking and correlation across PostgreSQL's statistics collection system.

## Definition


## Detailed Description
This function updates the query identifier stored in the backend's status entry in shared memory. It implements a policy of only tracking top-level query identifiers to avoid noise from nested or sub-queries. The function uses the same change-counting protocol as other backend status updates to ensure atomic modifications.

The function enforces a "top-level only" policy where once a query ID is set, subsequent calls are ignored unless the force flag is true or the query ID was previously reset to zero. This prevents nested queries, stored procedures, or other sub-operations from overwriting the main query identifier that users and monitoring tools are typically interested in tracking.

Query identifiers are automatically reset when a backend transitions to STATE_RUNNING via pgstat_report_activity(), preparing for the next top-level command.

## Parameters / Member Variables
- : The 64-bit query identifier to be stored (typically computed from the query text hash)
- : Boolean flag to override the top-level-only policy and force an update even if a query ID is already set

## Dependencies
- Functions called/Symbols referenced:
  - PGSTAT_BEGIN_WRITE_ACTIVITY
  - PGSTAT_END_WRITE_ACTIVITY
- Called from (representative examples):
  - ExecutorStart
  - parse_analyze_fixedparams
  - parse_analyze_varparams
  - parse_analyze_withcb
  - exec_simple_query
  - exec_bind_message
  - exec_execute_message

## Notes and Other Information
- Only reports top-level query identifiers to avoid confusion from nested operations
- Uses volatile pointers and atomic update protocol to ensure thread safety
- Returns early if activity tracking is disabled (pgstat_track_activities = false)
- The stored query_id is reset to 0 when pgstat_report_activity(STATE_RUNNING) is called
- Query identifiers enable correlation between pg_stat_statements and pg_stat_activity views
- Part of PostgreSQL's query monitoring infrastructure for performance analysis and debugging