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
  - [GetPortalByName](../G/GetPortalByName.md) (locate portal by name)
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

## Simplified Source

```c
// Simplified version of exec_execute_message
static void
exec_execute_message(const char *portal_name, long max_rows)
{
    Portal portal;
    bool completed;
    QueryCompletion qc;
    DestReceiver *receiver;
    bool is_xact_command;
    bool execute_is_fetch;

    // Step 1: Set up destination for output
    CommandDest dest = (whereToSendOutput == DestRemote) ? DestRemoteExecute : whereToSendOutput;

    // Step 2: Find and validate the portal
    portal = GetPortalByName(portal_name);
    if (!PortalIsValid(portal))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
                       errmsg("portal \"%s\" does not exist", portal_name)));

    // Step 3: Handle empty query case
    if (portal->commandTag == CMDTAG_UNKNOWN) {
        NullCommand(dest);
        return;
    }

    // Step 4: Check if this is a transaction command
    is_xact_command = IsTransactionStmtList(portal->stmts);

    // Step 5: Set up monitoring and logging
    debug_query_string = portal->sourceText;
    pgstat_report_activity(STATE_RUNNING, portal->sourceText);
    BeginCommand(portal->commandTag, dest);

    // Step 6: Create result receiver and start transaction
    receiver = CreateDestReceiver(dest);
    if (dest == DestRemoteExecute)
        SetRemoteDestReceiverParams(receiver, portal);
    start_xact_command();

    // Step 7: Determine if this is a fetch operation
    execute_is_fetch = !portal->atStart;

    // Step 8: Log statement if required
    if (check_log_statement(portal->stmts)) {
        ereport(LOG, (errmsg("%s %s: %s",
                            execute_is_fetch ? "execute fetch from" : "execute",
                            portal->name ? portal->name : "<unnamed>",
                            portal->sourceText)));
    }

    // Step 9: Validate transaction state
    if (IsAbortedTransactionBlockState() && !IsTransactionExitStmtList(portal->stmts))
        ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                       errmsg("current transaction is aborted")));

    // Step 10: Execute the portal
    if (max_rows <= 0)
        max_rows = FETCH_ALL;

    completed = PortalRun(portal, max_rows, true, true, receiver, receiver, &qc);
    receiver->rDestroy(receiver);

    // Step 11: Handle completion and transaction state
    if (completed) {
        if (is_xact_command || (MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT)) {
            // Commit transaction commands immediately
            finish_xact_command();
        } else {
            // Increment command counter for non-transaction commands
            CommandCounterIncrement();
            MyXactFlags |= XACT_FLAGS_PIPELINING;
            disable_statement_timeout();
        }
        EndCommand(&qc, dest, false);
    } else {
        // Portal suspended - send appropriate message
        if (whereToSendOutput == DestRemote)
            pq_putemptymessage(PqMsg_PortalSuspended);
        MyXactFlags |= XACT_FLAGS_PIPELINING;
    }

    // Step 12: Clean up
    debug_query_string = NULL;
}
```

Key simplifications made:
- Removed detailed error handling context setup for clarity
- Consolidated parameter logging and duration tracking
- Abstracted complex monitoring and statistics collection
- Simplified string handling and memory management
- Focused on the main execution flow
- Removed platform-specific debugging code
- Consolidated similar conditional branches