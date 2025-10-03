# ExecAppendAsyncRequest

## Location
[src/backend/executor/nodeAppend.c:963-1016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L963-L1016)

## Overview
Processes asynchronous tuple requests for Append nodes, either returning already-available results from previous async operations or initiating new async requests for subplans that need them.

## Definition

```c
static bool
ExecAppendAsyncRequest(AppendState *node, TupleTableSlot **result)
```
## Detailed Description
This function serves as the central coordinator for asynchronous tuple retrieval in Append nodes, implementing a two-phase approach to maximize efficiency and minimize latency. It handles both the consumption of previously completed async results and the initiation of new async operations when needed.

The function operates through several key phases:
1. **Early Return Check**: If no subplans need new requests, returns false immediately to avoid unnecessary work
2. **Result Cache Check**: If previously completed async operations have produced results that haven't been consumed yet, returns one of those results immediately
3. **Request Batch Processing**: For subplans that need new requests, issues new async operations in batch to maximize concurrency
4. **Immediate Result Check**: After issuing new requests, checks if any operations completed immediately and returns a result if available

This design optimizes for both latency (by returning cached results immediately) and throughput (by batching new requests). The function maintains the as_needrequest bitmap to track which subplans require new async operations and manages the as_asyncresults array as a stack of completed but unconsummed results.

## Parameters / Member Variables
- `*node`: Pointer to AppendState containing async execution state, request tracking bitmaps, and result caching structures
- `**result`: Output parameter that receives a tuple slot if one is available, or remains unchanged if no tuple is ready
## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty (bitmap emptiness checking)
  - [bms_next_member](../b/bms_next_member.md) (bitmap iteration for request processing)
  - [bms_free](../b/bms_free.md) (bitmap memory management)
  - [ExecAsyncRequest](ExecAsyncRequest.md) (actual async operation initiation)
- Called from (representative examples):
  - [ExecAppendAsyncGetNext](ExecAppendAsyncGetNext.md) (async tuple retrieval coordination)

## Notes and Other Information
- Returns true when a tuple is available, false when no tuple is ready
- Manages result caching through as_asyncresults array operated as a stack (LIFO)
- Clears as_needrequest bitmap and processes all pending requests in a single batch
- Critical for minimizing async operation latency and maximizing concurrent execution
- Part of PostgreSQL's async execution infrastructure for foreign tables and parallel operations
- Enables efficient pipeline processing where new requests can be issued while previous results are being consumed
- Essential for achieving high throughput in scenarios with multiple async-capable data sources

## Simplified Source

```c
static bool
ExecAppendAsyncRequest(AppendState *node, TupleTableSlot **result)
{
    Bitmapset *needrequest;
    int i;

    // Nothing to do if no async subplans need requests
    if (bms_is_empty(node->as_needrequest)) {
        Assert(node->as_nasyncresults == 0);
        return false;
    }

    // Return cached result if available
    if (node->as_nasyncresults > 0) {
        --node->as_nasyncresults;
        *result = node->as_asyncresults[node->as_nasyncresults];
        return true;
    }

    // Issue new requests for all subplans that need them
    needrequest = node->as_needrequest;
    node->as_needrequest = NULL;

    i = -1;
    while ((i = bms_next_member(needrequest, i)) >= 0) {
        AsyncRequest *areq = node->as_asyncrequests[i];
        ExecAsyncRequest(areq);
    }
    bms_free(needrequest);

    // Return any immediately available result
    if (node->as_nasyncresults > 0) {
        --node->as_nasyncresults;
        *result = node->as_asyncresults[node->as_nasyncresults];
        return true;
    }

    return false;
}
```