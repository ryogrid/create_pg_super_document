# ExecReScanAppend

## Location
src/backend/executor/nodeAppend.c: 406 - 483

## Overview
Resets an Append node to restart execution from the beginning, handling parameter changes, partition pruning state, and asynchronous execution cleanup.

## Definition


## Detailed Description
ExecReScanAppend is the rescan function for PostgreSQL's Append node executor, responsible for resetting the node's execution state so it can be re-executed from the beginning. This is commonly needed when the Append node is part of a nested loop or when parameter values change that affect subplan selection.

The function handles several critical aspects:

1. **Parameter Change Detection**: Checks if any PARAM_EXEC parameters used in partition pruning have changed, requiring recomputation of valid subplans
2. **Pruning State Reset**: Clears cached valid subplan information when parameters change
3. **Subplan Propagation**: Propagates parameter changes to all subplans using UpdateChangedParamSet
4. **Selective Rescanning**: Only rescans subplans that don't have parameter changes (others will be rescanned automatically on next execution)
5. **Async State Reset**: Cleans up all asynchronous execution state including pending requests and results
6. **Execution State Reset**: Returns the node to its initial state ready for fresh execution

## Parameters / Member Variables
- : The AppendState containing the execution state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md) (for checking parameter intersection)
  - [bms_free](../b/bms_free.md) (for cleaning up bitmap sets)
  - [bms_next_member](../b/bms_next_member.md) (for iterating async plans)
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md) (for parameter change propagation)
  - [ExecReScan](ExecReScan.md) (for recursive subplan rescanning)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (main executor rescan dispatcher)

## Notes and Other Information
- The function intelligently avoids double-rescanning subplans that will be rescanned automatically due to parameter changes
- Partition pruning state is only reset when relevant parameters actually change, preserving performance
- Async execution state is completely reset to ensure clean restart
- The function uses UpdateChangedParamSet (one of the processed symbols) to propagate parameter changes efficiently
- Parameter change detection uses bitmap overlap operations for efficiency
- The rescan operation prepares the node for potential re-pruning of partitions if parameters affecting pruning have changed