# ExecReScanMaterial

## Location
src/backend/executor/nodeMaterial.c: 313 - 363

## Overview
ExecReScanMaterial rescans a materialized relation node, handling both cases where materialization has occurred and pass-through scenarios. It manages the state of stored tuples and coordinates with the underlying subplan for efficient re-execution.

## Definition


## Detailed Description
This function implements the rescan operation for Material executor nodes in PostgreSQL's execution engine. The Material node can operate in two primary modes:

1. **Materialization mode (eflags != 0)**: The node stores tuples from its subplan in a tuplestore for repeated access
2. **Pass-through mode (eflags == 0)**: The node directly passes through results from its subplan without storing them

The function intelligently handles different rescan scenarios:
- If materialization hasn't occurred yet, it returns early and lets the subplan handle rescanning
- If the subplan's parameters have changed (chgParam != NULL) or the node doesn't support rewind operations, it destroys the existing tuplestore and forces rematerialization
- If the stored data is still valid, it simply rewinds the tuplestore to the beginning
- In pass-through mode, it delegates the rescan to the underlying subplan

## Parameters / Member Variables
- : Pointer to MaterialState containing the execution state for the Material node, including:
  - : The result tuple slot that gets cleared
  - : Execution flags indicating the node's operational mode
  - : The tuplestore containing materialized tuples (if any)
  - : Flag indicating whether the underlying subplan has reached EOF

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the outer plan state
  - : Clears the result tuple slot
  - : Destroys the tuplestore
  - : Rescans the outer subplan
  - : Rewinds the tuplestore to the beginning
  - : Flag indicating rewind capability
- Called from (representative examples):
  - : Generic rescan dispatcher in execAmi.c:253

## Notes and Other Information
- The function optimizes performance by avoiding unnecessary re-materialization when the stored data remains valid
- The  mechanism allows the executor to detect when subplan parameters have changed, requiring fresh data
- The  flag is crucial for determining whether the tuplestore can be reused
- In pass-through mode, the Material node acts as a transparent wrapper around its subplan
- The  flag is reset to false to ensure proper iteration behavior after rescan