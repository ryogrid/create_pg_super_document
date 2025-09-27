# exec_parse_message

## Location
[src/backend/tcop/postgres.c:1395-1629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L1395-L1629)

## Overview
Executes a "Parse" protocol message, which parses SQL query strings and creates prepared statements for later execution in the PostgreSQL frontend/backend protocol.

## Definition

```c
structing parsetrees.
	 *
	 * We have two strategies depending on whether the prepared statement is
	 * named or not.  For a named prepared statement, we do parsing in
	 * MessageContext and copy the finished trees into the prepared
	 * statement's plancache entry;
```
## Detailed Description
This function implements the Parse phase of PostgreSQL's extended query protocol. It parses the provided SQL query string and creates a cached plan source that can be executed later via Bind and Execute messages. The function handles both named and unnamed prepared statements with different memory management strategies:

- **Named prepared statements**: Parsing occurs in MessageContext and the finished parse trees are copied into the prepared statement's plancache entry
- **Unnamed prepared statements**: Creates a dedicated memory context for parsing to avoid copying overhead since these statements typically have shorter lifespans

The function performs comprehensive validation including transaction state checks, parameter validation, and ensures only single statements are allowed in prepared statements. It integrates with PostgreSQL's monitoring, logging, and statistics systems throughout the parsing process.

## Parameters / Member Variables
- : The SQL query string to be parsed and prepared
- : Name for the prepared statement (empty string for unnamed statements)
- : Array of parameter type OIDs for parameterized queries
- : Number of parameters in the query

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_query](../p/pg_parse_query.md) (basic SQL parsing)
  - [CreateCachedPlan](../C/CreateCachedPlan.md) (creates cached plan source)
  - [pg_analyze_and_rewrite_varparams](../p/pg_analyze_and_rewrite_varparams.md) (semantic analysis and query rewriting)
  - [CompleteCachedPlan](../C/CompleteCachedPlan.md) (finalizes the cached plan)
  - [StorePreparedStatement](../S/StorePreparedStatement.md) (stores named prepared statements)
  - [check_log_duration](../c/check_log_duration.md) (duration logging)
  - [start_xact_command](../s/start_xact_command.md) (transaction management)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (activity reporting)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main message processing loop)

## Notes and Other Information
- Only allows single SQL statements per prepared statement to keep the protocol simple
- Rejects commands in aborted transaction states except for COMMIT/ROLLBACK
- Handles empty query strings as legal input
- Integrates with PostgreSQL's monitoring facilities including debug logging, statistics collection, and process display updates
- Sends ParseComplete message to client upon successful completion
- Memory management differs significantly between named and unnamed prepared statements for performance optimization

## Simplified Source

```c
// Simplified version of exec_parse_message
static void
exec_parse_message(const char *query_string,    /* string to execute */
                   const char *stmt_name,       /* name for prepared stmt */
                   Oid *paramTypes,            /* parameter types */
                   int numParams)              /* number of parameters */
{
    MemoryContext unnamed_stmt_context = NULL;
    MemoryContext oldcontext;
    List *parsetree_list;
    RawStmt *raw_parse_tree;
    List *querytree_list;
    CachedPlanSource *psrc;
    bool is_named;

    // Setup monitoring and logging
    debug_query_string = query_string;
    pgstat_report_activity(STATE_RUNNING, query_string);
    set_ps_display("PARSE");

    // Start transaction if needed
    start_xact_command();

    // Choose memory management strategy based on statement type
    is_named = (stmt_name[0] != '\0');
    if (is_named) {
        // Named statement: use MessageContext for parsing
        oldcontext = MemoryContextSwitchTo(MessageContext);
    } else {
        // Unnamed statement: create dedicated context
        drop_unnamed_stmt();
        unnamed_stmt_context = AllocSetContextCreate(MessageContext,
                                                   "unnamed prepared statement",
                                                   ALLOCSET_DEFAULT_SIZES);
        oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);
    }

    // Parse the SQL query
    parsetree_list = pg_parse_query(query_string);

    // Validate single statement per prepared statement
    if (list_length(parsetree_list) > 1)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("cannot insert multiple commands into a prepared statement")));

    if (parsetree_list != NIL) {
        raw_parse_tree = linitial_node(RawStmt, parsetree_list);

        // Check transaction state for non-exit statements
        if (IsAbortedTransactionBlockState() &&
            !IsTransactionExitStmt(raw_parse_tree->stmt))
            ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                    errmsg("current transaction is aborted, commands ignored until end of transaction block")));

        // Create cached plan source
        psrc = CreateCachedPlan(raw_parse_tree, query_string,
                              CreateCommandTag(raw_parse_tree->stmt));

        // Setup snapshot if needed for analysis
        bool snapshot_set = false;
        if (analyze_requires_snapshot(raw_parse_tree)) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        // Analyze and rewrite the query
        querytree_list = pg_analyze_and_rewrite_varparams(raw_parse_tree,
                                                        query_string,
                                                        &paramTypes,
                                                        &numParams,
                                                        NULL);

        // Clean up snapshot
        if (snapshot_set)
            PopActiveSnapshot();
    } else {
        // Handle empty query string
        raw_parse_tree = NULL;
        psrc = CreateCachedPlan(raw_parse_tree, query_string, CMDTAG_UNKNOWN);
        querytree_list = NIL;
    }

    // Fix memory context hierarchy for unnamed statements
    if (unnamed_stmt_context)
        MemoryContextSetParent(psrc->context, MessageContext);

    // Complete the cached plan
    CompleteCachedPlan(psrc, querytree_list, unnamed_stmt_context,
                      paramTypes, numParams, NULL, NULL,
                      CURSOR_OPT_PARALLEL_OK, true);

    // Check for cancellation
    CHECK_FOR_INTERRUPTS();

    // Store the prepared statement
    if (is_named) {
        StorePreparedStatement(stmt_name, psrc, false);
    } else {
        SaveCachedPlan(psrc);
        unnamed_stmt_psrc = psrc;
    }

    // Restore memory context
    MemoryContextSwitchTo(oldcontext);

    // Update command counter and send completion message
    CommandCounterIncrement();
    if (whereToSendOutput == DestRemote)
        pq_putemptymessage(PqMsg_ParseComplete);

    // Cleanup
    debug_query_string = NULL;
}
```

Key simplifications made:
- Removed detailed error handling and logging complexity
- Consolidated duration logging logic into brief comments
- Abstracted low-level memory operations with descriptive comments
- Simplified variable declarations and initialization
- Focused on the main execution path
- Removed performance statistics collection details
- Maintained essential algorithm flow and error checking
- Preserved transaction management and protocol compliance