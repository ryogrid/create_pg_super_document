# ExecCreateTableAs

## Location
[src/backend/commands/createas.c:221-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L221-L367)

## Overview
Main entry point function for executing CREATE TABLE AS and CREATE MATERIALIZED VIEW statements, handling the complete workflow from query planning to relation creation and data population.

## Definition

```c
ObjectAddress
ExecCreateTableAs(ParseState *pstate, CreateTableAsStmt *stmt,
				  ParamListInfo params, QueryEnvironment *queryEnv,
				  QueryCompletion *qc)
```
## Detailed Description
The  function serves as the primary execution handler for both CREATE TABLE AS SELECT and CREATE MATERIALIZED VIEW statements. It orchestrates the entire process including relation existence checking, query rewriting, planning, execution, and data population.

The function handles different execution paths based on the statement type and options:
- For EXECUTE statements within CREATE TABLE AS, it delegates to ExecuteQuery
- For WITH NO DATA scenarios, it uses create_ctas_nodata for efficient relation creation without query execution
- For regular CREATE TABLE AS, it performs complete query rewriting, planning, and execution
- For materialized views, it creates the relation with skipData=true initially, then uses REFRESH logic for secure data population

The function manages snapshot handling, query descriptor creation, executor lifecycle, and proper cleanup. For materialized views, it implements a two-phase approach to leverage the security restrictions of the REFRESH operation.

## Parameters / Member Variables
- `*pstate`: ParseState containing parsing context and source text information
- `*stmt`: CreateTableAsStmt node containing the parsed statement structure
- `params`: ParamListInfo for handling parameterized queries
- `*queryEnv`: QueryEnvironment for query execution context
- `*qc`: QueryCompletion structure for returning execution statistics
## Dependencies
- Functions called/Symbols referenced:
  - [CreateTableAsRelExists](../C/CreateTableAsRelExists.md)
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md)
  - [ExecuteQuery](ExecuteQuery.md)
  - [create_ctas_nodata](../c/create_ctas_nodata.md)
  - [QueryRewrite](../Q/QueryRewrite.md)
  - [pg_plan_query](../p/pg_plan_query.md)
  - [PushCopiedSnapshot](../P/PushCopiedSnapshot.md)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [UpdateActiveSnapshotCommandId](../U/UpdateActiveSnapshotCommandId.md)
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - [GetIntoRelEFlags](../G/GetIntoRelEFlags.md)
  - [ExecutorStart](ExecutorStart.md)
  - [ExecutorRun](ExecutorRun.md)
  - [ExecutorFinish](ExecutorFinish.md)
  - [ExecutorEnd](ExecutorEnd.md)
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- This is a public function exposed through the createas.h header
- Implements sophisticated execution flow control based on statement characteristics
- Handles both regular tables and materialized views with different strategies
- Uses snapshot management to ensure consistency during query execution
- For materialized views, leverages REFRESH logic to maintain security restrictions
- Properly manages executor lifecycle with appropriate startup, run, finish, and end phases
- Returns ObjectAddress for the created relation for further processing by callers
- Implements error handling for unexpected rewrite results and validation scenarios

## Simplified Source

```c
ObjectAddress
ExecCreateTableAs(ParseState *pstate, CreateTableAsStmt *stmt,
                  ParamListInfo params, QueryEnvironment *queryEnv,
                  QueryCompletion *qc)
{
    Query *query = castNode(Query, stmt->query);
    IntoClause *into = stmt->into;
    bool is_matview = (into->viewQuery != NULL);
    bool do_refresh = false;
    DestReceiver *dest;
    ObjectAddress address;

    // Check if relation already exists
    if (CreateTableAsRelExists(stmt))
        return InvalidObjectAddress;

    // Create destination receiver for new relation
    dest = CreateIntoRelDestReceiver(into);

    // Handle EXECUTE statement case
    if (query->commandType == CMD_UTILITY &&
        IsA(query->utilityStmt, ExecuteStmt))
    {
        ExecuteStmt *estmt = castNode(ExecuteStmt, query->utilityStmt);
        ExecuteQuery(pstate, estmt, into, params, dest, qc);
        address = ((DR_intorel *) dest)->reladdr;
        return address;
    }

    // For materialized views, create without data first
    if (is_matview)
    {
        do_refresh = !into->skipData;
        into->skipData = true;
    }

    if (into->skipData)
    {
        // Create relation without executing query
        address = create_ctas_nodata(query->targetList, into);
    }
    else
    {
        // Full query execution path
        List *rewritten = QueryRewrite(query);
        query = linitial_node(Query, rewritten);

        // Plan and execute the query
        PlannedStmt *plan = pg_plan_query(query, pstate->p_sourcetext,
                                         CURSOR_OPT_PARALLEL_OK, params);

        // Setup snapshot and query descriptor
        PushCopiedSnapshot(GetActiveSnapshot());
        UpdateActiveSnapshotCommandId();

        QueryDesc *queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
                                              GetActiveSnapshot(), InvalidSnapshot,
                                              dest, params, queryEnv, 0);

        // Execute query
        ExecutorStart(queryDesc, GetIntoRelEFlags(into));
        ExecutorRun(queryDesc, ForwardScanDirection, 0, true);

        // Collect results and cleanup
        if (qc)
            SetQueryCompletion(qc, CMDTAG_SELECT, queryDesc->estate->es_processed);

        address = ((DR_intorel *) dest)->reladdr;

        ExecutorFinish(queryDesc);
        ExecutorEnd(queryDesc);
        FreeQueryDesc(queryDesc);
        PopActiveSnapshot();
    }

    // For materialized views, refresh with data if needed
    if (do_refresh)
    {
        RefreshMatViewByOid(address.objectId, false, false,
                           pstate->p_sourcetext, NULL, qc);
        if (qc)
            qc->commandTag = CMDTAG_SELECT;
    }

    return address;
}
```