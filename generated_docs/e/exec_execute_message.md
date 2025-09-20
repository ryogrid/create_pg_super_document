# exec_execute_message

## Location
[src/backend/tcop/postgres.c:2101-2367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2101-L2367)

## Overview
Processes an "Execute" message for a portal, running the actual query execution and returning results to the client in PostgreSQL's extended query protocol.

## Definition

```c
static void
exec_execute_message(const char *portal_name, long max_rows)
```
## Detailed Description
This function implements the Execute phase of PostgreSQL's extended query protocol. It executes a previously bound portal (created via Parse and Bind messages) and returns query results to the client. The function handles both complete execution and partial execution with row limits.

Key responsibilities include:
- Locating the specified portal and validating its existence
- Handling transaction command detection and processing
- Setting up appropriate destination receivers for result output
- Executing the portal with specified row limits
- Managing transaction state and command completion
- Comprehensive logging for both statement execution and duration
- Supporting portal suspension for partial result fetching

The function differentiates between complete portal execution and fetch operations (re-execution of existing portals) and handles transaction control statements specially by committing them immediately.

## Parameters / Member Variables
- : Name of the portal to execute (empty string for unnamed portal)
- : Maximum number of rows to return (0 or negative means fetch all rows)

## Dependencies
- Functions called/Symbols referenced:
  - GetPortalByName (locate portal by name)
  - PortalIsValid (validate portal existence)
  - [IsTransactionStmtList](../I/IsTransactionStmtList.md) (detect transaction control statements)
  - [PortalRun](../P/PortalRun.md) (execute the portal)
  - [CreateDestReceiver](../C/CreateDestReceiver.md) (create result destination receiver)
  - [BeginCommand](../B/BeginCommand.md)/EndCommand (command lifecycle management)
  - [check_log_statement](../c/check_log_statement.md) (statement logging policy check)
  - [finish_xact_command](../f/finish_xact_command.md) (transaction command completion)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (activity monitoring)
  - [check_log_duration](../c/check_log_duration.md) (duration logging)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main message processing loop)

## Notes and Other Information
- Supports row-limited execution via max_rows parameter, enabling cursor-like behavior
- Handles empty query responses for null command portals
- Implements special handling for transaction control statements (COMMIT/ROLLBACK/etc.)
- Manages pipelining flags to optimize transaction batching
- Supports portal suspension when partial results are requested
- Integrates comprehensive error handling with parameter logging
- Validates transaction state, rejecting non-transaction-exit commands in aborted transactions  
- Automatically disables statement timeouts after successful execution
- Sends appropriate completion messages (CommandComplete or PortalSuspended) based on execution status
- Distinguishes between initial execution and fetch operations for accurate logging