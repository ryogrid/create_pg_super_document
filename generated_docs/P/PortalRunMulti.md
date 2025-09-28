# PortalRunMulti

## Location
[src/backend/tcop/pquery.c:1188-1379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L1188-L1379)

## Overview
Executes a portal's queries in the general case, handling multiple queries or non-SELECT-like queries with proper snapshot management and destination routing.

## Definition

```c
static void
PortalRunMulti(Portal portal,
			   bool isTopLevel, bool setHoldSnapshot,
			   DestReceiver *dest, DestReceiver *altdest,
			   QueryCompletion *qc)
```
## Detailed Description
PortalRunMulti is the most comprehensive portal execution function, designed to handle complex scenarios involving multiple statements or utility commands within a single portal. The function iterates through all statements in the portal, distinguishing between plannable queries and utility statements. For plannable queries, it manages transaction snapshots, handles command counter increments, and routes output to the primary destination receiver. For utility statements, it delegates to PortalRunUtility. The function handles destination receiver adjustments for remote execution scenarios, manages memory context cleanup between statements, and supports snapshot holding for later use. It also handles special cases like internal COMMIT/ROLLBACK operations that may reset the portal's statement list.

## Parameters / Member Variables
- : The Portal structure containing the statements to execute
- : Boolean indicating whether this is a top-level execution
- : Boolean indicating whether to register and hold snapshots for later use
- : Primary DestReceiver for statement output (typically for tag-setting statements)
- : Alternative DestReceiver for auxiliary statements (typically for non-tag-setting statements)
- : QueryCompletion structure to record execution results

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [ResetUsage](../R/ResetUsage.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [RegisterSnapshot](../R/RegisterSnapshot.md)
  - [PushCopiedSnapshot](PushCopiedSnapshot.md)
  - [UpdateActiveSnapshotCommandId](../U/UpdateActiveSnapshotCommandId.md)
  - [ProcessQuery](ProcessQuery.md)
  - [ShowUsage](../S/ShowUsage.md)
  - [PortalRunUtility](PortalRunUtility.md)
  - [MemoryContextDeleteChildren](../M/MemoryContextDeleteChildren.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [PopActiveSnapshot](PopActiveSnapshot.md)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md)
- Called from (representative examples):
  - [PortalRun](PortalRun.md)
  - [FillPortalStore](../F/FillPortalStore.md)

## Notes and Other Information
- This function is static and only used within pquery.c
- Handles both plannable queries and utility statements in a single portal
- Manages snapshot lifecycle including registration, activation, and cleanup
- Switches DestRemoteExecute destinations to DestNone to prevent unexpected tuple delivery
- Increments command counter between statements to ensure proper transaction semantics
- Cleans up subsidiary memory contexts between statements to prevent memory leaks
- Handles edge cases like portal statement list reset during CALL/DO operations
- Uses different destination receivers for tag-setting vs. auxiliary statements
- Supports performance monitoring through executor statistics logging
- Ensures proper query completion tag propagation from portal to caller

## Simplified Source

```c
// Simplified version of PortalRunMulti
static void PortalRunMulti(Portal portal, bool isTopLevel, bool setHoldSnapshot,
                          DestReceiver *dest, DestReceiver *altdest, QueryCompletion *qc) {
    bool active_snapshot_set = false;
    ListCell *stmtlist_item;

    // Adjust destination receivers for remote execution
    if (dest->mydest == DestRemoteExecute)
        dest = None_Receiver;
    if (altdest->mydest == DestRemoteExecute)
        altdest = None_Receiver;

    // Execute each statement in the portal
    foreach(stmtlist_item, portal->stmts) {
        PlannedStmt *pstmt = lfirst_node(PlannedStmt, stmtlist_item);

        // Check for cancellation signals
        CHECK_FOR_INTERRUPTS();

        if (pstmt->utilityStmt == NULL) {
            // Handle plannable queries

            // Set up snapshot for first query or update for subsequent ones
            if (!active_snapshot_set) {
                Snapshot snapshot = GetTransactionSnapshot();

                if (setHoldSnapshot) {
                    snapshot = RegisterSnapshot(snapshot);
                    portal->holdSnapshot = snapshot;
                }

                PushCopiedSnapshot(snapshot);
                active_snapshot_set = true;
            } else {
                UpdateActiveSnapshotCommandId();
            }

            // Execute the query with appropriate destination
            if (pstmt->canSetTag) {
                ProcessQuery(pstmt, portal->sourceText, portal->portalParams,
                           portal->queryEnv, dest, qc);
            } else {
                ProcessQuery(pstmt, portal->sourceText, portal->portalParams,
                           portal->queryEnv, altdest, NULL);
            }
        } else {
            // Handle utility statements
            if (pstmt->canSetTag) {
                PortalRunUtility(portal, pstmt, isTopLevel, false, dest, qc);
            } else {
                PortalRunUtility(portal, pstmt, isTopLevel, false, altdest, NULL);
            }
        }

        // Clean up memory between statements
        MemoryContextDeleteChildren(portal->portalContext);

        // Handle case where statements were reset (CALL/DO with COMMIT/ROLLBACK)
        if (portal->stmts == NIL)
            break;

        // Increment command counter between statements
        if (lnext(portal->stmts, stmtlist_item) != NULL)
            CommandCounterIncrement();
    }

    // Clean up snapshot if we set one
    if (active_snapshot_set)
        PopActiveSnapshot();

    // Copy completion information if needed
    if (qc && qc->commandTag == CMDTAG_UNKNOWN &&
        portal->qc.commandTag != CMDTAG_UNKNOWN) {
        CopyQueryCompletion(qc, &portal->qc);
    }
}
```

Key simplifications made:
- Removed detailed comments and performance monitoring code
- Consolidated snapshot management logic
- Simplified the plannable vs utility statement handling
- Preserved essential error checking and cleanup logic
- Focused on core workflow: setup, execute each statement, cleanup
- Maintained critical safety mechanisms like interrupt checking and memory cleanup