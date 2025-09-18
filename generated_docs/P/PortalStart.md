# PortalStart

## Location
src/backend/tcop/pquery.c: 433 - 622

## Overview
Prepares a portal for execution by setting up the execution environment, determining strategy, and initializing the executor for various types of SQL statements.

## Definition
void PortalStart(Portal portal, ParamListInfo params, int eflags, Snapshot snapshot)

## Detailed Description
PortalStart is a critical function that transitions a portal from the PORTAL_DEFINED state to the PORTAL_READY state, making it ready to accept PortalRun calls. The function determines the appropriate execution strategy based on the statement type and sets up the necessary execution context.

The function handles four main portal strategies: PORTAL_ONE_SELECT for simple SELECT statements, PORTAL_ONE_RETURNING and PORTAL_ONE_MOD_WITH for statements with RETURNING clauses, PORTAL_UTIL_SELECT for utility statements that return tuples, and PORTAL_MULTI_QUERY for multiple statements. For each strategy, it performs different initialization steps including snapshot management, executor setup, and tuple descriptor preparation.

The function uses exception handling to ensure proper cleanup if errors occur during initialization, marking the portal as failed and restoring global state variables.

## Parameters / Member Variables
- portal: The Portal to be started, must be in PORTAL_DEFINED status
- params: ParamListInfo containing query parameters, can be NULL if no parameters needed
- eflags: Execution flags to pass to ExecutorStart, mostly honored for PORTAL_ONE_SELECT portals
- snapshot: Optional snapshot to use; pass InvalidSnapshot for normal behavior of setting a new snapshot

## Dependencies
- Functions called/Symbols referenced:
  - [ChoosePortalStrategy](../C/ChoosePortalStrategy.md)
  - PushActiveSnapshot
  - GetTransactionSnapshot
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - [ExecutorStart](../E/ExecutorStart.md)
  - PortalGetPrimaryStmt
  - [ExecCleanTypeFromTL](../E/ExecCleanTypeFromTL.md)
  - UtilityTupleDescriptor
  - MarkPortalFailed
- Called from (representative examples):
  - [PerformCursorOpen](PerformCursorOpen.md)
  - [ExecuteQuery](../E/ExecuteQuery.md)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md)
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_bind_message](../e/exec_bind_message.md)

## Notes and Other Information
- Caller must have already created the portal and called PortalDefineQuery
- Sets up global portal context pointers including ActivePortal, CurrentResourceOwner, and PortalContext
- For scrollable cursors, automatically adds EXEC_FLAG_REWIND and EXEC_FLAG_BACKWARD flags
- The snapshot parameter is currently only used for PORTAL_ONE_SELECT portals
- After successful completion, portal status changes to PORTAL_READY and result tuple descriptor is available
- Located in src/backend/tcop/pquery.c:433-622