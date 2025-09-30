# ExecuteQuery

## Location
[src/backend/commands/prepare.c:147-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L147-L277)

## Overview
Implements the 'EXECUTE' utility statement and supports CREATE TABLE ... AS EXECUTE, executing a previously prepared statement with optional parameters and directing output to the specified destination.

## Definition

```c
void
ExecuteQuery(ParseState *pstate,
			 ExecuteStmt *stmt, IntoClause *intoClause,
			 ParamListInfo params,
			 DestReceiver *dest, QueryCompletion *qc)
```
## Detailed Description
ExecuteQuery retrieves and executes a prepared statement by name, handling parameter evaluation, portal creation, and query execution. It supports both regular EXECUTE statements and CREATE TABLE ... AS EXECUTE constructs. The function validates that the prepared statement exists and has a fixed result type, evaluates any parameters using the current execution context, creates a portal for query execution, and runs the query through the portal interface. For CREATE TABLE ... AS EXECUTE, it performs additional validation to ensure the statement is a SELECT query.

## Parameters / Member Variables
- : Parse state containing parsing context information
- : ExecuteStmt node containing the prepared statement name and parameter values
- : IntoClause for CREATE TABLE ... AS EXECUTE (NULL for regular EXECUTE)
- : Parameter list information from outer query contexts
- : Destination receiver for query results
- : Query completion information structure

## Dependencies
- Functions called/Symbols referenced:
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (retrieves prepared statement)
  - [CreateExecutorState](../C/CreateExecutorState.md) (creates execution state for parameter evaluation)
  - [EvaluateParams](EvaluateParams.md) (evaluates parameter expressions)
  - [CreateNewPortal](../C/CreateNewPortal.md) (creates portal for execution)
  - [GetCachedPlan](../G/GetCachedPlan.md) (gets cached plan for execution)
  - [PortalDefineQuery](../P/PortalDefineQuery.md) (defines query in portal)
  - [PortalStart](../P/PortalStart.md) (starts portal execution)
  - [PortalRun](../P/PortalRun.md) (runs portal to completion or specified count)
  - [PortalDrop](../P/PortalDrop.md) (cleans up portal)
  - [FreeExecutorState](../F/FreeExecutorState.md) (releases execution state)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)
  - [ExecCreateTableAs](ExecCreateTableAs.md) (CREATE TABLE AS execution)

## Notes and Other Information
- Validates that prepared statements have fixed result types to prevent variable-result plan execution
- Creates invisible portals for internal use that don't appear in pg_cursors
- Supports parameter evaluation with proper memory management to handle pass-by-reference parameters
- For CREATE TABLE ... AS EXECUTE, enforces that only SELECT statements are allowed
- Handles WITH NO DATA option for CREATE TABLE ... AS EXECUTE by setting fetch count to 0
- Manages plan reference counts carefully to prevent memory leaks between GetCachedPlan and PortalDefineQuery
- Uses portal interface for consistent query execution and resource management

## Simplified Source

```c
void ExecuteQuery(ParseState *pstate, ExecuteStmt *stmt, IntoClause *intoClause,
                 ParamListInfo params, DestReceiver *dest, QueryCompletion *qc)
{
    PreparedStatement *entry;
    CachedPlan *cplan;
    List *plan_list;
    ParamListInfo paramLI = NULL;
    EState *estate = NULL;
    Portal portal;
    char *query_string;
    int eflags;
    long count;

    // Look up prepared statement by name
    entry = FetchPreparedStatement(stmt->name, true);

    // Validate prepared statement supports EXECUTE
    if (!entry->plansource->fixed_result)
        elog(ERROR, "EXECUTE does not support variable-result cached plans");

    // Evaluate parameters if the prepared statement has them
    if (entry->plansource->num_params > 0)
    {
        estate = CreateExecutorState();
        estate->es_param_list_info = params;
        paramLI = EvaluateParams(pstate, entry, stmt->params, estate);
    }

    // Create portal for execution
    portal = CreateNewPortal();
    portal->visible = false;  // Internal use only

    // Copy query string to portal memory context
    query_string = MemoryContextStrdup(portal->portalContext,
                                      entry->plansource->query_string);

    // Get cached plan and statement list
    cplan = GetCachedPlan(entry->plansource, paramLI, NULL, NULL);
    plan_list = cplan->stmt_list;

    // Define query in portal (critical: no errors between GetCachedPlan and here)
    PortalDefineQuery(portal, NULL, query_string, entry->plansource->commandTag,
                     plan_list, cplan);

    // Handle CREATE TABLE ... AS EXECUTE special case
    if (intoClause)
    {
        PlannedStmt *pstmt;

        // Validate it's a single SELECT statement
        if (list_length(plan_list) != 1)
            ereport(ERROR, /* prepared statement is not a SELECT */);

        pstmt = linitial_node(PlannedStmt, plan_list);
        if (pstmt->commandType != CMD_SELECT)
            ereport(ERROR, /* prepared statement is not a SELECT */);

        // Set appropriate execution flags
        eflags = GetIntoRelEFlags(intoClause);
        count = intoClause->skipData ? 0 : FETCH_ALL;
    }
    else
    {
        // Regular EXECUTE
        eflags = 0;
        count = FETCH_ALL;
    }

    // Execute the query through portal interface
    PortalStart(portal, paramLI, eflags, GetActiveSnapshot());
    (void) PortalRun(portal, count, false, true, dest, dest, qc);

    // Cleanup
    PortalDrop(portal, false);
    if (estate)
        FreeExecutorState(estate);
}
```