# ExecAppendAsyncBegin

## Location
[src/backend/executor/nodeAppend.c:862-913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L862-L913)

## Overview
Initiates asynchronous execution of valid async-capable subplans in an Append node, setting up the initial state and making requests for all available async subplans to begin concurrent execution.

## Definition

```c
static void
ExecAppendAsyncBegin(AppendState *node)
```
## Detailed Description
This function is the entry point for asynchronous execution in Append nodes, enabling PostgreSQL to execute multiple subplans concurrently rather than sequentially. It is part of PostgreSQL's asynchronous query execution infrastructure that allows for improved performance when dealing with multiple data sources or partitions.

The function performs several key initialization steps:
1. **Runtime Pruning**: If not already done, determines which subplans are valid using runtime partition pruning
2. **Classification**: Classifies the matching subplans to identify which ones support asynchronous execution
3. **State Initialization**: Sets up tracking variables for synchronous completion and remaining async operations
4. **Request Submission**: Issues asynchronous requests for all valid async-capable subplans

The async execution model allows the database to initiate I/O operations or remote queries on multiple subplans simultaneously, then collect results as they become available, significantly improving performance for queries that access multiple partitions or foreign tables.

## Parameters / Member Variables
- `*node`: Pointer to AppendState containing the append node's execution state, async subplan information, and request tracking structures
## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward (direction validation)
  - [ExecFindMatchingSubPlans](ExecFindMatchingSubPlans.md) (runtime pruning support)
  - [classify_matching_subplans](../c/classify_matching_subplans.md) (async capability classification)
  - bms_is_empty (bitmap emptiness check)
  - [bms_num_members](../b/bms_num_members.md) (async subplan counting)
  - [bms_next_member](../b/bms_next_member.md) (bitmap iteration)
  - [ExecAsyncRequest](ExecAsyncRequest.md) (async request submission)
- Called from (representative examples):
  - [ExecAppend](ExecAppend.md) (main append execution function)

## Notes and Other Information
- Only supports forward scans (backward scans not supported for async execution)
- Requires at least one async-capable subplan to be meaningful
- Early returns if no valid async subplans are found after pruning
- Critical for performance when dealing with foreign tables, partitioned tables, or other async-capable data sources
- Part of PostgreSQL's push towards more concurrent and parallel execution models
- Works in conjunction with the async request/response infrastructure
- Essential for modern workloads involving distributed data or multiple storage systems

## Simplified Source

```c
static void
ExecAppendAsyncBegin(AppendState *node)
{
    // Validation: async append only supports forward scans
    Assert(ScanDirectionIsForward(node->ps.state->es_direction));
    Assert(node->as_nplans > 0);
    Assert(node->as_nasyncplans > 0);

    // Determine valid subplans if not already done
    if (!node->as_valid_subplans_identified)
    {
        // Use runtime pruning to find valid subplans
        node->as_valid_subplans = ExecFindMatchingSubPlans(node->as_prune_state, false);
        node->as_valid_subplans_identified = true;

        // Classify which subplans support async execution
        classify_matching_subplans(node);
    }

    // Initialize state tracking variables
    node->as_syncdone = bms_is_empty(node->as_valid_subplans);
    node->as_nasyncremain = bms_num_members(node->as_valid_asyncplans);

    // Early return if no async subplans are valid
    if (node->as_nasyncremain == 0)
        return;

    // Submit async requests for all valid async subplans
    int i = -1;
    while ((i = bms_next_member(node->as_valid_asyncplans, i)) >= 0)
    {
        AsyncRequest *areq = node->as_asyncrequests[i];

        Assert(areq->request_index == i);
        Assert(!areq->callback_pending);

        // Initiate async execution for this subplan
        ExecAsyncRequest(areq);
    }
}
```