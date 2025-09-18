# PortalRun

## Location
src/backend/tcop/pquery.c: 686 - 864

## Overview
Executes a portal's query or queries, handling different execution strategies and managing global context during execution.

## Definition
bool PortalRun(Portal portal, long count, bool isTopLevel, bool run_once, DestReceiver *dest, DestReceiver *altdest, QueryCompletion *qc)

## Detailed Description
PortalRun is the main execution function for portals, responsible for running queries according to the portal's execution strategy. It handles four main strategies: PORTAL_ONE_SELECT for simple SELECT statements, PORTAL_ONE_RETURNING and PORTAL_ONE_MOD_WITH for statements with RETURNING clauses, PORTAL_UTIL_SELECT for utility statements, and PORTAL_MULTI_QUERY for multiple statements.

The function manages global context carefully to support utility commands like VACUUM and CLUSTER that internally start and commit transactions. It uses exception handling to ensure proper cleanup and restoration of global state variables in case of errors. For single-query strategies, it delegates to PortalRunSelect to fetch the desired results, while for multi-query strategies it uses PortalRunMulti.

## Parameters / Member Variables
- portal: The Portal to execute, must be in PORTAL_READY status
- count: Maximum number of rows to fetch; FETCH_ALL means all rows, count <= 0 is a no-op
- isTopLevel: true if query is being executed directly from a client command message
- run_once: ignored parameter, present only to avoid API break in stable branches
- dest: DestReceiver for output of primary (canSetTag) query
- altdest: DestReceiver for output of non-primary queries
- qc: QueryCompletion structure to store command completion status data, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - MarkPortalActive
  - FillPortalStore
  - PortalRunSelect
  - PortalRunMulti
  - MarkPortalDone
  - MarkPortalFailed
  - InitializeQueryCompletion
  - CopyQueryCompletion
- Called from (representative examples):
  - ExecuteQuery
  - exec_simple_query
  - exec_execute_message

## Notes and Other Information
- Returns true if portal execution is complete, false if suspended due to count exhaustion
- Count parameter is ignored in multi-query situations where portal always runs to completion
- Handles special case of utility commands that internally start/commit transactions
- Uses extensive exception handling to restore global state on errors
- Logs executor statistics when log_executor_stats is enabled
- For non-PORTAL_ONE_SELECT strategies, results may be stored in portal's tuplestore via FillPortalStore
- Located in src/backend/tcop/pquery.c:686-864