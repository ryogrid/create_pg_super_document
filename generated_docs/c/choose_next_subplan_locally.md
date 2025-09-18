# choose_next_subplan_locally

## Location
src/backend/executor/nodeAppend.c: 554 - 619

## Overview
Selects the next synchronous subplan for execution in a non-parallel-aware Append node, supporting both forward and backward scan directions.

## Definition
```c
static bool choose_next_subplan_locally(AppendState *node)
```

## Detailed Description
This function implements the local subplan selection logic for Append nodes that do not use parallel execution. It maintains state to track which subplan is currently being executed and determines the next valid subplan based on runtime partition pruning results and scan direction. The function handles both forward and backward scans, supports runtime partition pruning by identifying valid subplans when needed, and properly manages the synchronization state for mixed async/sync execution scenarios.

The function uses a bitmapset (`as_valid_subplans`) to track which subplans are valid after runtime partition pruning, and advances through them in the appropriate direction based on the scan direction.

## Parameters
- `node`: Pointer to the AppendState structure containing the current execution state and subplan information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFindMatchingSubPlans](../E/ExecFindMatchingSubPlans.md)
  - ScanDirectionIsForward
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_prev_member](../b/bms_prev_member.md)
  - INVALID_SUBPLAN_INDEX (constant)
- Called from (representative examples):
  - [ExecInitAppend](../E/ExecInitAppend.md)

## Notes and Other Information
- Returns `true` if a next subplan was found and selected, `false` if no more subplans are available
- Handles the initial call case by setting `whichplan` to -1 and identifying valid subplans if not already done
- For async-enabled Append nodes (`as_nasyncplans > 0`), valid subplans are pre-identified during initialization
- Sets `as_syncdone` to true when no more sync subplans are available in async mode
- Supports bidirectional scanning using `bms_next_member` for forward scans and `bms_prev_member` for backward scans
- The function is static and only used within the nodeAppend.c file for local (non-parallel) execution
- Runtime partition pruning integration allows dynamic exclusion of subplans that don't match current parameter values