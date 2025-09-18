# PortalRunUtility

## Location
src/backend/tcop/pquery.c: 1125 - 1187

## Overview
Executes a utility statement inside a portal, handling snapshot management and delegating the actual execution to ProcessUtility.

## Definition


## Detailed Description
PortalRunUtility is responsible for executing utility statements (non-DML commands like DDL, administrative commands, etc.) within the context of a portal. The function manages transaction snapshots appropriately based on whether the utility statement requires one, handles snapshot registration for hold scenarios, and ensures proper cleanup after execution. It calls ProcessUtility to perform the actual command execution and manages memory context switches that may occur during utility command execution. The function is designed to handle the complexities of snapshot management in utility commands, including cases where commands may modify or pop snapshots from the stack.

## Parameters / Member Variables
- : The Portal structure containing the utility statement to execute
- : The PlannedStmt containing the utility statement details
- : Boolean indicating whether this is a top-level command execution
- : Boolean indicating whether to register and hold the snapshot for later use
- : DestReceiver that will handle any output from the utility command
- : QueryCompletion structure to record execution results

## Dependencies
- Functions called/Symbols referenced:
  - PlannedStmtRequiresSnapshot
  - GetTransactionSnapshot
  - RegisterSnapshot
  - PushActiveSnapshotWithLevel
  - GetActiveSnapshot
  - ProcessUtility
  - ActiveSnapshotSet
  - PopActiveSnapshot
- Called from (representative examples):
  - FillPortalStore
  - PortalRunMulti

## Notes and Other Information
- This function is static and only used within pquery.c
- Handles snapshot management for utility statements that require them
- Supports holding snapshots for later use when setHoldSnapshot is true
- Manages portal snapshot references and ensures proper cleanup
- Handles cases where utility commands may modify the active snapshot stack
- Switches back to portal context after utility execution to handle context changes
- Used for executing utility statements in various portal execution strategies
- Properly handles snapshot lifecycle including registration, activation, and cleanup