# ExecutorRewind

## Location
src/backend/executor/execMain.c: 526 - 571

## Overview
Rewinds an open query descriptor to the start of execution, allowing the query to be re-executed from the beginning.

## Definition


## Detailed Description
The  function provides the capability to reset a query execution to its initial state, enabling the same query to be executed multiple times without recreating the entire execution environment. This is particularly useful for holdable cursors and portal operations where the same result set needs to be accessed repeatedly.

The function performs a rescan operation on the plan state, which recursively resets all plan nodes in the execution tree to their initial conditions. It operates within the per-query memory context and includes safety checks to ensure it's only used with SELECT operations, as rewinding UPDATE/INSERT/DELETE operations would not be semantically meaningful.

## Parameters / Member Variables
- : Pointer to the QueryDesc structure containing the execution context and plan state to be rewound

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [ExecReScan](ExecReScan.md)
  - CMD_SELECT (assertion check)
- Called from (representative examples):
  - [PersistHoldablePortal](../P/PersistHoldablePortal.md)
  - [DoPortalRewind](../D/DoPortalRewind.md)

## Notes and Other Information
- Only works with SELECT operations (assertion enforces queryDesc->operation == CMD_SELECT)
- Rewinds the execution to the very beginning, not to an arbitrary position
- Used primarily for holdable portals and cursor operations
- Operates within the per-query memory context for proper memory management
- Does not recreate the execution state, only resets existing plan nodes
- Essential for implementing SQL cursor SCROLL functionality
- Should not be used with updating queries (INSERT/UPDATE/DELETE) as it's not semantically meaningful