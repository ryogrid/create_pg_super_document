# MultiExecBitmapOr

## Location
[src/backend/executor/nodeBitmapOr.c:111-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapOr.c#L111-L195)

## Overview
MultiExecBitmapOr executes a BitmapOr node by combining the bitmap results from all child subplans using logical OR operations to produce a unified TID bitmap.

## Definition

```c
union step for each child: just pass down the current result
		 * bitmap and let the child OR directly into it.
		 */
		if (IsA(subnode, BitmapIndexScanState))
		{
			if (result == NULL) /* first subplan */
			{
				/* XXX should we use less than work_mem for this? */
				result = tbm_create(work_mem * 1024L,
									((BitmapOr *) node->ps.plan)->isshared ?
									node->ps.state->es_query_dsa : NULL);
			}

			((BitmapIndexScanState *) subnode)->biss_result = result;

			subresult = (TIDBitmap *) MultiExecProcNode(subnode);

			if (subresult != result)
				elog(ERROR, "unrecognized result from subplan");
		}
		else
		{
			/* standard implementation */
			subresult = (TIDBitmap *) MultiExecProcNode(subnode);

			if (!subresult || !IsA(subresult, TIDBitmap))
				elog(ERROR, "unrecognized result from subplan");

			if (result == NULL)
				result = subresult; /* first subplan */
			else
			{
				tbm_union(result, subresult);
				tbm_free(subresult);
			}
		}
	}

	/* We could return an empty result set here? */
	if (result == NULL)
		elog(ERROR, "BitmapOr doesn't support zero inputs");
```
## Detailed Description
MultiExecBitmapOr is the core execution function for BitmapOr nodes, implementing the actual bitmap OR logic. The function iterates through all child subplans, executes each one to obtain a TID (Tuple Identifier) bitmap, and combines these bitmaps using logical OR operations to create a unified result bitmap.

The function includes an important optimization for BitmapIndexScan children: instead of performing explicit tbm_union operations, it passes the current result bitmap directly to BitmapIndexScan nodes, allowing them to OR their results directly into the target bitmap. This reduces memory allocation and copying overhead.

For non-BitmapIndexScan children, the function uses the standard approach of executing each subplan independently and then using tbm_union to combine the results. The function handles instrumentation for performance monitoring and includes error checking to ensure all subplans return valid TID bitmaps.

## Parameters / Member Variables
- : Pointer to the BitmapOrState containing the execution context and child plan states

## Dependencies
- Functions called/Symbols referenced:
  - [InstrStartNode](../I/InstrStartNode.md)/InstrStopNode (performance instrumentation)
  - [tbm_create](../t/tbm_create.md) (creates initial TID bitmap)
  - [MultiExecProcNode](MultiExecProcNode.md) (executes child subplans)
  - [tbm_union](../t/tbm_union.md) (combines bitmaps using OR operation)
  - [tbm_free](../t/tbm_free.md) (deallocates temporary bitmaps)
  - IsA (type checking for optimization)
  - elog (error reporting)
  - work_mem (global variable for memory limits)

- Called from (representative examples):
  - [MultiExecProcNode](MultiExecProcNode.md) (part of the multi-execution dispatch system)

## Notes and Other Information
- Uses work_mem to determine initial bitmap size allocation
- Supports shared bitmap allocation through query-level DSA (Dynamic Shared Area)
- Implements special optimization for BitmapIndexScan children to avoid extra bitmap copies
- Requires at least one input subplan (errors if zero inputs provided)
- Returns TIDBitmap rather than tuple slots, following the multi-execution interface pattern
- Provides manual instrumentation support since it doesn't use standard execution framework
- All child bitmaps except the first are freed after union operation to manage memory

## Simplified Source

```c
Node *
MultiExecBitmapOr(BitmapOrState *node)
{
    TIDBitmap *result = NULL;

    // Start performance instrumentation
    if (node->ps.instrument)
        InstrStartNode(node->ps.instrument);

    // Get subplan information
    PlanState **bitmapplans = node->bitmapplans;
    int nplans = node->nplans;

    // Execute each subplan and OR their bitmaps together
    for (int i = 0; i < nplans; i++)
    {
        PlanState *subnode = bitmapplans[i];
        TIDBitmap *subresult;

        // Optimization for BitmapIndexScan: let it OR directly into result
        if (IsA(subnode, BitmapIndexScanState))
        {
            if (result == NULL)
            {
                // Create initial bitmap
                result = tbm_create(work_mem * 1024L,
                                   ((BitmapOr *) node->ps.plan)->isshared ?
                                   node->ps.state->es_query_dsa : NULL);
            }

            // Pass result to child so it can OR directly into it
            ((BitmapIndexScanState *) subnode)->biss_result = result;
            subresult = (TIDBitmap *) MultiExecProcNode(subnode);

            if (subresult != result)
                elog(ERROR, "unrecognized result from subplan");
        }
        else
        {
            // Standard implementation for other node types
            subresult = (TIDBitmap *) MultiExecProcNode(subnode);

            if (!subresult || !IsA(subresult, TIDBitmap))
                elog(ERROR, "unrecognized result from subplan");

            if (result == NULL)
                result = subresult;  // First subplan
            else
            {
                // Union with previous results
                tbm_union(result, subresult);
                tbm_free(subresult);
            }
        }
    }

    if (result == NULL)
        elog(ERROR, "BitmapOr doesn't support zero inputs");

    // Stop instrumentation
    if (node->ps.instrument)
        InstrStopNode(node->ps.instrument, 0);

    return (Node *) result;
}
```