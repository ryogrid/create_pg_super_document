# exec_simple_query

## Location
[src/backend/tcop/postgres.c:1017-1394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L1017-L1394)

## Overview
Executes a "simple Query" protocol message, handling the complete SQL query processing pipeline from parsing through execution for PostgreSQL's simple query protocol.

## Definition

```c
structing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);
```
## Detailed Description
This function is the main entry point for PostgreSQL's simple query protocol, implementing the complete query processing pipeline from raw SQL text to result delivery. It handles multiple SQL statements within a single query string and manages transaction boundaries appropriately.

The function operates through several key phases:

1. **Initialization and Monitoring**: Sets up query monitoring, activity reporting, and performance statistics collection.

2. **Transaction Management**: Starts transaction commands and manages implicit transaction blocks for multi-statement queries.

3. **Parse Processing**: Converts the raw SQL string into parse trees using .

4. **Statement Processing Loop**: For each parsed statement:
   - **Command Analysis**: Creates command tags and handles process status display
   - **Transaction State Validation**: Ensures queries can execute in current transaction state
   - **Snapshot Management**: Sets up appropriate snapshots for analysis and planning
   - **Query Processing**: Performs analysis, rewriting, and planning via  and 
   - **Portal Creation**: Creates an unnamed portal for query execution
   - **Execution**: Runs the query through  with appropriate output formatting
   - **Cleanup**: Manages memory contexts and transaction boundaries

5. **Transaction Finalization**: Handles implicit transaction blocks and transaction command completion.

6. **Performance Reporting**: Logs duration and performance statistics as configured.

The function includes extensive error handling, memory management through dedicated contexts, and comprehensive monitoring integration.

## Parameters / Member Variables
- : Raw SQL query string to be executed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_query](../p/pg_parse_query.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - [pg_plan_queries](../p/pg_plan_queries.md)
  - [start_xact_command](../s/start_xact_command.md)
  - [finish_xact_command](../f/finish_xact_command.md)
  - [CreatePortal](../C/CreatePortal.md)
  - [PortalDefineQuery](../P/PortalDefineQuery.md)
  - [PortalStart](../P/PortalStart.md)
  - [PortalRun](../P/PortalRun.md)
  - [PortalDrop](../P/PortalDrop.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [CreateCommandTag](../C/CreateCommandTag.md)
  - [BeginCommand](../B/BeginCommand.md)
  - [EndCommand](../E/EndCommand.md)
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md)
  - [BeginImplicitTransactionBlock](../B/BeginImplicitTransactionBlock.md)
  - [EndImplicitTransactionBlock](../E/EndImplicitTransactionBlock.md)
  - Many memory management and utility functions
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (multiple call sites)

## Notes and Other Information
- Core function in PostgreSQL's simple query protocol implementation
- Manages complex transaction semantics for multi-statement queries using implicit transaction blocks
- Includes comprehensive performance monitoring and debugging support
- Handles both regular DML/DDL statements and utility commands
- Located in src/backend/tcop/postgres.c:1017-1394
- Implements proper memory context management to prevent memory leaks
- Supports extensive logging and tracing capabilities through various PostgreSQL subsystems
- Critical for PostgreSQL's compliance with the PostgreSQL wire protocol
- The function ensures that COMMIT/ROLLBACK statements properly separate transaction boundaries even within multi-statement queries
- Includes special handling for binary cursor operations and output formatting

## Simplified Source

```c
// Simplified version of exec_simple_query
static void exec_simple_query(const char *query_string) {
    CommandDest dest = whereToSendOutput;
    MemoryContext oldcontext;
    List *parsetree_list;
    ListCell *parsetree_item;
    bool use_implicit_block;

    // Setup monitoring and activity reporting
    debug_query_string = query_string;
    pgstat_report_activity(STATE_RUNNING, query_string);

    // Initialize transaction command
    start_xact_command();

    // Clean up any existing unnamed statement
    drop_unnamed_stmt();

    // Parse the SQL query string into parse trees
    oldcontext = MemoryContextSwitchTo(MessageContext);
    parsetree_list = pg_parse_query(query_string);

    // Log the statement if required by configuration
    if (check_log_statement(parsetree_list)) {
        ereport(LOG, (errmsg("statement: %s", query_string)));
    }

    MemoryContextSwitchTo(oldcontext);

    // Use implicit transaction block for multiple statements
    use_implicit_block = (list_length(parsetree_list) > 1);

    // Process each parsed statement
    foreach(parsetree_item, parsetree_list) {
        RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
        CommandTag commandTag;
        QueryCompletion qc;
        List *querytree_list, *plantree_list;
        Portal portal;
        DestReceiver *receiver;

        // Get command tag and update process status
        commandTag = CreateCommandTag(parsetree->stmt);
        set_ps_display_with_command_tag(commandTag);
        BeginCommand(commandTag, dest);

        // Reject commands in aborted transaction (except COMMIT/ABORT)
        if (IsAbortedTransactionBlockState() &&
            !IsTransactionExitStmt(parsetree->stmt)) {
            ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                           errmsg("current transaction is aborted")));
        }

        start_xact_command();

        // Start implicit transaction block if needed
        if (use_implicit_block) {
            BeginImplicitTransactionBlock();
        }

        // Set up snapshot for analysis if needed
        bool snapshot_set = false;
        if (analyze_requires_snapshot(parsetree)) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        // Switch to appropriate memory context for query processing
        if (lnext(parsetree_list, parsetree_item) != NULL) {
            // Create separate context for non-final statements
            MemoryContext per_parsetree_context =
                AllocSetContextCreate(MessageContext, "per-parsetree context",
                                    ALLOCSET_DEFAULT_SIZES);
            oldcontext = MemoryContextSwitchTo(per_parsetree_context);
        } else {
            oldcontext = MemoryContextSwitchTo(MessageContext);
        }

        // Analyze, rewrite, and plan the query
        querytree_list = pg_analyze_and_rewrite_fixedparams(parsetree,
                                                           query_string,
                                                           NULL, 0, NULL);
        plantree_list = pg_plan_queries(querytree_list, query_string,
                                       CURSOR_OPT_PARALLEL_OK, NULL);

        // Clean up analysis snapshot
        if (snapshot_set) {
            PopActiveSnapshot();
        }

        // Create portal for execution
        portal = CreatePortal("", true, true);
        portal->visible = false;
        PortalDefineQuery(portal, NULL, query_string, commandTag,
                         plantree_list, NULL);
        PortalStart(portal, NULL, 0, InvalidSnapshot);

        // Set output format (text by default, binary for binary cursors)
        int16 format = 0; // TEXT format
        if (IsA(parsetree->stmt, FetchStmt)) {
            FetchStmt *stmt = (FetchStmt *) parsetree->stmt;
            if (!stmt->ismove) {
                Portal fportal = GetPortalByName(stmt->portalname);
                if (PortalIsValid(fportal) &&
                    (fportal->cursorOptions & CURSOR_OPT_BINARY)) {
                    format = 1; // BINARY format
                }
            }
        }
        PortalSetResultFormat(portal, 1, &format);

        // Create destination receiver and execute query
        receiver = CreateDestReceiver(dest);
        if (dest == DestRemote) {
            SetRemoteDestReceiverParams(receiver, portal);
        }

        MemoryContextSwitchTo(oldcontext);

        // Execute the query through the portal
        PortalRun(portal, FETCH_ALL, true, true, receiver, receiver, &qc);

        // Clean up portal and receiver
        receiver->rDestroy(receiver);
        PortalDrop(portal, false);

        // Handle transaction boundaries between statements
        if (lnext(parsetree_list, parsetree_item) == NULL) {
            // Last statement: close implicit block and finish transaction
            if (use_implicit_block) {
                EndImplicitTransactionBlock();
            }
            finish_xact_command();
        } else if (IsA(parsetree->stmt, TransactionStmt)) {
            // Transaction control statement: commit and start new transaction
            finish_xact_command();
        } else {
            // Regular statement: increment command counter
            CommandCounterIncrement();
            disable_statement_timeout();
        }

        // Send completion message to client
        EndCommand(&qc, dest, false);

        // Clean up per-statement memory context if created
        if (per_parsetree_context) {
            MemoryContextDelete(per_parsetree_context);
        }
    }

    // Final transaction cleanup
    finish_xact_command();

    // Handle empty query case
    if (!parsetree_list) {
        NullCommand(dest);
    }

    // Performance logging and cleanup
    check_log_duration_and_report();
    TRACE_POSTGRESQL_QUERY_DONE(query_string);
    debug_query_string = NULL;
}
```

Key simplifications made:
- Removed detailed error handling and edge case management for clarity
- Consolidated memory context switching into clearer sections
- Abstracted complex logging and statistics collection into single calls
- Simplified snapshot management while preserving the core logic
- Focused on the main execution path rather than all error conditions
- Reduced variable declarations to essential ones only
- Streamlined the transaction management logic
- Removed detailed performance monitoring code while keeping the structure