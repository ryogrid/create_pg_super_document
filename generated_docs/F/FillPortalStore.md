# FillPortalStore

## Location
[src/backend/tcop/pquery.c:998-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L998-L1058)

## Overview
Runs a query and loads result tuples into the portal's tuple store for cases that require holding results in memory before delivery to the client.

## Definition

```c
static void
FillPortalStore(Portal portal, bool isTopLevel)
```
## Detailed Description
FillPortalStore is a specialized function used to execute queries and store their results in a portal's tuple store rather than streaming them directly to the client. This function is specifically designed for three portal strategies: PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT. It creates a tuplestore destination receiver, configures it to write to the portal's hold store, and then executes the appropriate query execution path based on the portal's strategy. The function ensures that query results are captured and held in memory for later retrieval, which is essential for certain query patterns that require result buffering.

## Parameters / Member Variables
- : The Portal structure containing the query to execute and where results will be stored
- : Boolean flag indicating whether this is a top-level query execution

## Dependencies
- Functions called/Symbols referenced:
  - [InitializeQueryCompletion](../I/InitializeQueryCompletion.md)
  - [PortalCreateHoldStore](../P/PortalCreateHoldStore.md)
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - [SetTuplestoreDestReceiverParams](../S/SetTuplestoreDestReceiverParams.md)
  - [PortalRunMulti](../P/PortalRunMulti.md)
  - [PortalRunUtility](../P/PortalRunUtility.md)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md)
- Called from (representative examples):
  - [PortalRun](../P/PortalRun.md)
  - [PortalRunFetch](../P/PortalRunFetch.md)

## Notes and Other Information
- This function is static and only used within pquery.c
- It handles three specific portal strategies, throwing an error for unsupported strategies
- The function sets up a tuplestore destination receiver to capture query output
- For PORTAL_ONE_RETURNING and PORTAL_ONE_MOD_WITH, it delegates to PortalRunMulti
- For PORTAL_UTIL_SELECT, it delegates to PortalRunUtility
- [Query](../Q/Query.md) completion information is preserved and copied to the portal's completion data
- The destination receiver is properly destroyed after use to prevent memory leaks

## Simplified Source

```c
// Simplified version of FillPortalStore
static void FillPortalStore(Portal portal, bool isTopLevel) {
    QueryCompletion qc;

    // Initialize completion tracking and create tuple store
    InitializeQueryCompletion(&qc);
    PortalCreateHoldStore(portal);

    // Create destination receiver for tuple store
    DestReceiver *treceiver = CreateDestReceiver(DestTuplestore);
    SetTuplestoreDestReceiverParams(treceiver, portal->holdStore,
                                  portal->holdContext, false, NULL, NULL);

    // Execute query based on portal strategy
    switch (portal->strategy) {
        case PORTAL_ONE_RETURNING:
        case PORTAL_ONE_MOD_WITH:
            // Run multi-statement portal with tuplestore output
            PortalRunMulti(portal, isTopLevel, true, treceiver, None_Receiver, &qc);
            break;

        case PORTAL_UTIL_SELECT:
            // Run utility statement
            PortalRunUtility(portal, linitial_node(PlannedStmt, portal->stmts),
                           isTopLevel, true, treceiver, &qc);
            break;

        default:
            elog(ERROR, "unsupported portal strategy: %d", (int) portal->strategy);
    }

    // Update portal with actual completion data
    if (qc.commandTag != CMDTAG_UNKNOWN) {
        CopyQueryCompletion(&portal->qc, &qc);
    }

    // Clean up destination receiver
    treceiver->rDestroy(treceiver);
}
```

Key simplifications made:
- Consolidated variable declarations
- Simplified comments while preserving essential information
- Maintained the switch statement structure for clarity
- Preserved error handling and cleanup logic
- Focused on the core workflow: setup store, execute query, update completion data, cleanup