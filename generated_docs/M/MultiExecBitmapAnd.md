# MultiExecBitmapAnd

## Location
[src/backend/executor/nodeBitmapAnd.c:110-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapAnd.c#L110-L177)

## Overview
MultiExecBitmapAnd executes a BitmapAnd node by combining bitmaps from multiple subplans using AND logic, producing a single merged bitmap of qualifying tuple identifiers.

## Definition

```c
Node *
MultiExecBitmapAnd(BitmapAndState *node)
```
## Detailed Description
MultiExecBitmapAnd is the core execution function for BitmapAnd nodes in PostgreSQL's bitmap scan optimization. It iterates through all child subplans, executes each one to obtain a TIDBitmap, and then performs bitwise AND operations to intersect the results.

The function optimizes execution by short-circuiting when any intermediate result becomes completely empty, since ANDing additional bitmaps cannot change an empty result. This optimization is enhanced by the query planner's ordering of subplans by selectivity, making early termination more likely.

The function handles its own instrumentation for performance monitoring, calling InstrStartNode and InstrStopNode to track execution time. Each subplan is executed via MultiExecProcNode, and the resulting bitmaps are intersected using tbm_intersect. Memory management is handled by freeing intermediate bitmaps after intersection, keeping only the final result.

## Parameters / Member Variables
- `*node`: Pointer to the BitmapAndState containing the initialized subplans to execute and combine
## Dependencies
- Functions called/Symbols referenced:
  - [InstrStartNode](../I/InstrStartNode.md) (for performance instrumentation)
  - [MultiExecProcNode](MultiExecProcNode.md) (to execute each subplan)
  - [tbm_intersect](../t/tbm_intersect.md) (to perform bitmap intersection)
  - [tbm_free](../t/tbm_free.md) (to free intermediate bitmaps)
  - [tbm_is_empty](../t/tbm_is_empty.md) (to check for optimization opportunities)
  - [InstrStopNode](../I/InstrStopNode.md) (to complete performance instrumentation)
  - IsA (for type checking)
  - elog (for error reporting)
- Called from (representative examples):
  - [MultiExecProcNode](MultiExecProcNode.md) (general multi-execution dispatcher)

## Notes and Other Information
- This is the actual execution function for BitmapAnd nodes, unlike ExecBitmapAnd which only throws an error
- Implements early termination optimization when intermediate results become empty
- Relies on query planner's selectivity-based ordering of subplans for optimal performance
- Returns a TIDBitmap containing tuple identifiers that satisfy ALL subplan conditions
- Part of PostgreSQL's bitmap index scan infrastructure for efficient multi-index queries
- Handles its own performance instrumentation since it bypasses standard executor interfaces
- Located in src/backend/executor/nodeBitmapAnd.c:110-177

## Simplified Source

```c
Node *
MultiExecBitmapAnd(BitmapAndState *node)
{
    TIDBitmap *result = NULL;

    // Start performance instrumentation if enabled
    if (node->ps.instrument)
        InstrStartNode(node->ps.instrument);

    // Get subplan information
    PlanState **bitmapplans = node->bitmapplans;
    int nplans = node->nplans;

    // Execute each subplan and AND their bitmaps together
    for (int i = 0; i < nplans; i++)
    {
        // Execute the subplan to get its bitmap
        TIDBitmap *subresult = (TIDBitmap *) MultiExecProcNode(bitmapplans[i]);

        if (!subresult || !IsA(subresult, TIDBitmap))
            elog(ERROR, "unrecognized result from subplan");

        if (result == NULL)
            result = subresult;  // First subplan result
        else
        {
            // Intersect with previous results
            tbm_intersect(result, subresult);
            tbm_free(subresult);
        }

        // Early termination optimization: if empty, no need to continue
        if (tbm_is_empty(result))
            break;
    }

    if (result == NULL)
        elog(ERROR, "BitmapAnd doesn't support zero inputs");

    // Stop performance instrumentation
    if (node->ps.instrument)
        InstrStopNode(node->ps.instrument, 0);

    return (Node *) result;
}
```