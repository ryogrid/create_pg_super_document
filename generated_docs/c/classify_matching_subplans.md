# classify_matching_subplans

## Location
src/backend/executor/nodeAppend.c: 1173 - 1205

## Overview
Classifies the node's valid subplans into synchronous and asynchronous categories, separating them into appropriate bitmapsets for execution control.

## Definition
```c
static void classify_matching_subplans(AppendState *node)
```

## Detailed Description
This function performs the critical task of separating valid subplans into synchronous and asynchronous categories within an AppendState node. It takes the set of valid subplans and splits them based on which ones are configured for asynchronous execution. The synchronous subplans remain in the as_valid_subplans bitmapset, while the asynchronous ones are moved to as_valid_asyncplans.

The function handles edge cases where no valid subplans exist or where no valid asynchronous subplans exist, setting appropriate state flags and counters. This classification is essential for the Append node's execution strategy, determining how to process subplans concurrently versus sequentially.

## Parameters / Member Variables
- `node`: AppendState structure containing subplan classification information, including valid subplans, async subplan definitions, and execution state flags

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - bms_overlap
  - bms_intersect
  - bms_del_members
  - Assert (assertion macro)
- Called from (representative examples):
  - ExecInitAppend
  - ExecAppendAsyncBegin

## Notes and Other Information
- Requires that as_valid_subplans_identified is true before execution
- Asserts that as_valid_asyncplans is initially NULL to ensure clean state
- Sets as_syncdone to true when no valid subplans exist, indicating synchronous execution completion
- Uses bitmapset operations to efficiently manage subplan classification
- The separation enables mixed execution strategies where some subplans run synchronously while others execute asynchronously
- Updates as_nasyncremain counter based on whether async subplans are present
- Critical for proper initialization of the Append node's dual execution modes