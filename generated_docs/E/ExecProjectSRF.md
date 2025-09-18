# ExecProjectSRF

## Location
src/backend/executor/nodeProjectSet.c: 139 - 226

## Overview
ExecProjectSRF projects a targetlist containing one or more set-returning functions, handling the complex evaluation state of SRFs.

## Definition
```c
static TupleTableSlot *ExecProjectSRF(ProjectSetState *node, bool continuing)
```

## Detailed Description
ExecProjectSRF is the core function responsible for evaluating target list expressions that contain set-returning functions. It handles the intricate state management required for SRFs, including:

1. **State Management**: Tracks which SRFs have finished producing results (`ExprEndResult`) vs. those still producing (`ExprMultipleResult`)
2. **Continuation Logic**: When `continuing` is true, it handles previously started SRF evaluation
3. **Mixed Expression Types**: Handles both SRFs (SetExprState) and regular expressions in the same target list
4. **Memory Context Management**: Evaluates expressions in the appropriate per-tuple memory context

The function iterates through all elements in the target list, evaluating each according to its type and current state, and assembles the results into a virtual tuple slot.

## Parameters / Member Variables
- `node`: The ProjectSetState containing evaluation state and target list information
- `continuing`: Boolean indicating whether to continue projecting from the same input tuple (true) or start fresh with a new input tuple (false)

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [ExecMakeFunctionResultSet](ExecMakeFunctionResultSet.md)
  - ExecEvalExpr
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)
- Called from (representative examples):
  - [ExecProjectSet](ExecProjectSet.md) (both for continuing and new evaluations)

## Notes and Other Information
- Uses assertions to ensure ProjectSet nodes only contain SRFs (hassrf must be true)
- Returns NULL when all SRFs have finished producing results
- Carefully manages memory contexts to prevent leaks during SRF evaluation
- The elemdone array tracks the completion state of each target list element