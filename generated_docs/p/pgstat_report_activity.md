# pgstat_report_activity

## Location
[src/backend/utils/activity/backend_status.c:503-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L503-L612)

## Overview
Reports the current activity state and command string of a backend process, updating the shared backend status entry with state transitions and timing information.

## Definition


## Detailed Description
This function is the primary interface for reporting backend activity status to PostgreSQL's statistics collection system. It updates the backend's status entry in shared memory with the current state and command string being executed. The function handles state transitions, tracks timing information for different activity phases, and maintains consistency through a change-counting protocol.

Key behaviors include:
- Tracking state transitions and their durations (active time vs. idle-in-transaction time)
- Updating the command string being executed (truncated if necessary)
- Handling cases where activity tracking is disabled
- Resetting query identifiers when new queries start
- Using volatile pointers and atomic operations to ensure thread safety

The function implements a protocol where all status updates are bracketed by incrementing a change counter before and after modifications, allowing readers to detect concurrent updates.

## Parameters / Member Variables
- : The new BackendState (e.g., STATE_RUNNING, STATE_IDLE, STATE_IDLEINTRANSACTION)
- : The SQL command string being executed (can be NULL for certain cases)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentStatementStartTimestamp](../G/GetCurrentStatementStartTimestamp.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifference](../T/TimestampDifference.md)
  - pgstat_count_conn_active_time
  - pgstat_count_conn_txn_idle_time
  - PGSTAT_BEGIN_WRITE_ACTIVITY
  - PGSTAT_END_WRITE_ACTIVITY
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_parse_message](../e/exec_parse_message.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [exec_execute_message](../e/exec_execute_message.md)
  - [PostgresMain](../P/PostgresMain.md)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md)

## Notes and Other Information
- Called from tcop/postgres.c to report what the backend is actually doing
- Uses volatile pointers to prevent compiler optimizations that could break the change-counting protocol
- Includes DTrace/SystemTap tracing support via TRACE_POSTGRESQL_STATEMENT_STATUS
- [Command](../C/Command.md) strings are truncated to pgstat_track_activity_query_size - 1 characters for performance
- When activity tracking is disabled, the function performs a final cleanup to set STATE_DISABLED
- State duration tracking helps with performance analysis by measuring time spent in different backend states
- Part of PostgreSQL's comprehensive statistics collection framework for monitoring database activity