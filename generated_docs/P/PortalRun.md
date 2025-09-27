# PortalRun

## Location
[src/backend/tcop/pquery.c:686-864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L686-L864)

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
  - [MarkPortalActive](../M/MarkPortalActive.md)
  - [FillPortalStore](../F/FillPortalStore.md)
  - [PortalRunSelect](PortalRunSelect.md)
  - [PortalRunMulti](PortalRunMulti.md)
  - [MarkPortalDone](../M/MarkPortalDone.md)
  - [MarkPortalFailed](../M/MarkPortalFailed.md)
  - [InitializeQueryCompletion](../I/InitializeQueryCompletion.md)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md)
- Called from (representative examples):
  - [ExecuteQuery](../E/ExecuteQuery.md)
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_execute_message](../e/exec_execute_message.md)

## Notes and Other Information
- Returns true if portal execution is complete, false if suspended due to count exhaustion
- Count parameter is ignored in multi-query situations where portal always runs to completion
- Handles special case of utility commands that internally start/commit transactions
- Uses extensive exception handling to restore global state on errors
- Logs executor statistics when log_executor_stats is enabled
- For non-PORTAL_ONE_SELECT strategies, results may be stored in portal's tuplestore via FillPortalStore
- Located in src/backend/tcop/pquery.c:686-864

## Simplified Source

```c
// Simplified version of PortalRun
bool PortalRun(Portal portal, long count, bool isTopLevel, bool run_once,
               DestReceiver *dest, DestReceiver *altdest, QueryCompletion *qc) {
    bool result;
    uint64 nprocessed;

    // Save current global state for restoration
    Portal saveActivePortal = ActivePortal;
    ResourceOwner saveResourceOwner = CurrentResourceOwner;
    MemoryContext saveMemoryContext = CurrentMemoryContext;

    // Initialize completion data
    if (qc)
        InitializeQueryCompletion(qc);

    // Mark portal as active and set up global context
    MarkPortalActive(portal);

    // Exception handling block for proper cleanup
    PG_TRY();
    {
        // Set up portal context
        ActivePortal = portal;
        if (portal->resowner)
            CurrentResourceOwner = portal->resowner;
        MemoryContextSwitchTo(portal->portalContext);

        // Execute based on portal strategy
        switch (portal->strategy) {
            case PORTAL_ONE_SELECT:
            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:
            case PORTAL_UTIL_SELECT:
                // Fill portal store if needed (except for PORTAL_ONE_SELECT)
                if (portal->strategy != PORTAL_ONE_SELECT && !portal->holdStore)
                    FillPortalStore(portal, isTopLevel);

                // Fetch the requested rows
                nprocessed = PortalRunSelect(portal, true, count, dest);

                // Copy completion data if requested
                if (qc && portal->qc.commandTag != CMDTAG_UNKNOWN) {
                    CopyQueryCompletion(qc, &portal->qc);
                    qc->nprocessed = nprocessed;
                }

                // Mark portal ready and check if we're at the end
                portal->status = PORTAL_READY;
                result = portal->atEnd;
                break;

            case PORTAL_MULTI_QUERY:
                // Execute multiple queries
                PortalRunMulti(portal, isTopLevel, false, dest, altdest, qc);
                MarkPortalDone(portal);
                result = true;  // Always complete for multi-query
                break;

            default:
                elog(ERROR, "unrecognized portal strategy: %d", portal->strategy);
                result = false;
                break;
        }
    }
    PG_CATCH();
    {
        // Handle errors: mark portal failed and restore state
        MarkPortalFailed(portal);
        MemoryContextSwitchTo(saveMemoryContext);
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Restore global state
    MemoryContextSwitchTo(saveMemoryContext);
    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;

    return result;
}
```

Key simplifications made:
- Removed detailed comments about transaction handling complexity
- Simplified global state saving (only kept essential variables)
- Removed logging and statistics code
- Consolidated error handling logic
- Removed complex memory context restoration logic in favor of simpler approach
- Focused on the main execution flow and strategy switching
- Abstracted away the detailed transaction resource owner management