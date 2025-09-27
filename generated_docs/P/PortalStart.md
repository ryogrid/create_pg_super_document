# PortalStart

## Location
[src/backend/tcop/pquery.c:433-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L433-L622)

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
  - [PushActiveSnapshot](PushActiveSnapshot.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - [ExecutorStart](../E/ExecutorStart.md)
  - [PortalGetPrimaryStmt](PortalGetPrimaryStmt.md)
  - [ExecCleanTypeFromTL](../E/ExecCleanTypeFromTL.md)
  - [UtilityTupleDescriptor](../U/UtilityTupleDescriptor.md)
  - [MarkPortalFailed](../M/MarkPortalFailed.md)
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

## Simplified Source

```c
// Simplified version of PortalStart
void PortalStart(Portal portal, ParamListInfo params, int eflags, Snapshot snapshot) {
    // Validate portal is in correct state
    Assert(PortalIsValid(portal));
    Assert(portal->status == PORTAL_DEFINED);

    // Save current global state for restoration later
    Portal saveActivePortal = ActivePortal;
    ResourceOwner saveResourceOwner = CurrentResourceOwner;
    MemoryContext savePortalContext = PortalContext;

    PG_TRY();
    {
        // Set up execution context
        ActivePortal = portal;
        if (portal->resowner)
            CurrentResourceOwner = portal->resowner;
        PortalContext = portal->portalContext;

        // Switch to portal's memory context
        oldContext = MemoryContextSwitchTo(PortalContext);

        // Store parameters and determine execution strategy
        portal->portalParams = params;
        portal->strategy = ChoosePortalStrategy(portal->stmts);

        // Initialize based on portal strategy
        switch (portal->strategy) {
            case PORTAL_ONE_SELECT:
                // Set up snapshot for SELECT queries
                if (snapshot)
                    PushActiveSnapshot(snapshot);
                else
                    PushActiveSnapshot(GetTransactionSnapshot());

                // Create query descriptor
                queryDesc = CreateQueryDesc(
                    linitial_node(PlannedStmt, portal->stmts),
                    portal->sourceText,
                    GetActiveSnapshot(),
                    InvalidSnapshot,
                    None_Receiver,
                    params,
                    portal->queryEnv,
                    0);

                // Set execution flags for scrollable cursors
                if (portal->cursorOptions & CURSOR_OPT_SCROLL)
                    myeflags = eflags | EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD;
                else
                    myeflags = eflags;

                // Start the executor
                ExecutorStart(queryDesc, myeflags);

                // Store query descriptor and tuple descriptor
                portal->queryDesc = queryDesc;
                portal->tupDesc = queryDesc->tupDesc;

                // Reset cursor position
                portal->atStart = true;
                portal->atEnd = false;
                portal->portalPos = 0;

                PopActiveSnapshot();
                break;

            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:
                // Set up tuple descriptor for RETURNING clauses
                PlannedStmt *pstmt = PortalGetPrimaryStmt(portal);
                portal->tupDesc = ExecCleanTypeFromTL(pstmt->planTree->targetlist);

                // Reset cursor position
                portal->atStart = true;
                portal->atEnd = false;
                portal->portalPos = 0;
                break;

            case PORTAL_UTIL_SELECT:
                // Set up tuple descriptor for utility statements
                PlannedStmt *pstmt = PortalGetPrimaryStmt(portal);
                portal->tupDesc = UtilityTupleDescriptor(pstmt->utilityStmt);

                // Reset cursor position
                portal->atStart = true;
                portal->atEnd = false;
                portal->portalPos = 0;
                break;

            case PORTAL_MULTI_QUERY:
                // No special setup needed for multi-query portals
                portal->tupDesc = NULL;
                break;
        }
    }
    PG_CATCH();
    {
        // Handle errors: mark portal as failed and restore state
        MarkPortalFailed(portal);
        ActivePortal = saveActivePortal;
        CurrentResourceOwner = saveResourceOwner;
        PortalContext = savePortalContext;
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Restore previous context and global state
    MemoryContextSwitchTo(oldContext);
    ActivePortal = saveActivePortal;
    CurrentResourceOwner = saveResourceOwner;
    PortalContext = savePortalContext;

    // Mark portal as ready for execution
    portal->status = PORTAL_READY;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic flow comments
- Consolidated similar cursor position reset code across cases
- Simplified variable declarations by moving them closer to usage
- Abstracted complex memory context operations with high-level descriptions
- Focused on the main execution path for each portal strategy
- Maintained the essential error handling pattern with PG_TRY/PG_CATCH