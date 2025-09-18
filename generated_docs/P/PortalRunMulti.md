# PortalRunMulti

## Location
src/backend/tcop/pquery.c: 1188 - 1379

## Overview
Executes a portal's queries in the general case, handling multiple queries or non-SELECT-like queries with proper snapshot management and destination routing.

## Definition


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
  - ResetUsage
  - GetTransactionSnapshot
  - RegisterSnapshot
  - PushCopiedSnapshot
  - UpdateActiveSnapshotCommandId
  - ProcessQuery
  - ShowUsage
  - PortalRunUtility
  - MemoryContextDeleteChildren
  - CommandCounterIncrement
  - PopActiveSnapshot
  - CopyQueryCompletion
- Called from (representative examples):
  - PortalRun
  - FillPortalStore

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