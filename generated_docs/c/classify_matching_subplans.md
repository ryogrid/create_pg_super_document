# classify_matching_subplans

## Location
[src/backend/executor/nodeAppend.c:1173-1205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L1173-L1205)

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
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_intersect](../b/bms_intersect.md)
  - [bms_del_members](../b/bms_del_members.md)
  - Assert (assertion macro)
- Called from (representative examples):
  - [ExecInitAppend](../E/ExecInitAppend.md)
  - [ExecAppendAsyncBegin](../E/ExecAppendAsyncBegin.md)

## Notes and Other Information
- Requires that as_valid_subplans_identified is true before execution
- Asserts that as_valid_asyncplans is initially NULL to ensure clean state
- Sets as_syncdone to true when no valid subplans exist, indicating synchronous execution completion
- Uses bitmapset operations to efficiently manage subplan classification
- The separation enables mixed execution strategies where some subplans run synchronously while others execute asynchronously
- Updates as_nasyncremain counter based on whether async subplans are present
- Critical for proper initialization of the Append node's dual execution modes

## Simplified Source

```c
static void classify_matching_subplans(AppendState *node) {
    Bitmapset *valid_asyncplans;

    // Early exit if no valid subplans exist
    if (bms_is_empty(node->as_valid_subplans)) {
        node->as_syncdone = true;
        node->as_nasyncremain = 0;
        return;
    }

    // Early exit if no async subplans overlap with valid ones
    if (!bms_overlap(node->as_valid_subplans, node->as_asyncplans)) {
        node->as_nasyncremain = 0;
        return;
    }

    // Separate valid subplans into sync and async categories
    valid_asyncplans = bms_intersect(node->as_asyncplans, node->as_valid_subplans);
    node->as_valid_subplans = bms_del_members(node->as_valid_subplans, valid_asyncplans);
    node->as_valid_asyncplans = valid_asyncplans;
}
```