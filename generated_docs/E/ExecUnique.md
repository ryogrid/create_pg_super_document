# ExecUnique

## Location
[src/backend/executor/nodeUnique.c:46-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeUnique.c#L46-L113)

## Overview
ExecUnique is the main execution function for the UNIQUE plan node that eliminates duplicate tuples from a sorted input stream, returning only the first occurrence of each distinct tuple.

## Definition

```c
static TupleTableSlot *			/* return: a tuple or NULL */
ExecUnique(PlanState *pstate)
```
## Detailed Description
ExecUnique implements duplicate elimination by processing tuples from its outer subplan in a loop. The function assumes that input tuples arrive in sorted order, which allows for efficient duplicate detection by comparing consecutive tuples. When a new tuple is fetched from the subplan, it is compared against the previously returned tuple using equality functions. If the tuples are identical, the new tuple is discarded and the next tuple is fetched. If they differ, or if this is the first tuple, it is saved as the result and returned to the caller. The function handles the end-of-input condition by returning NULL when the outer subplan is exhausted.

## Parameters / Member Variables
- : The PlanState structure containing execution state information for the UNIQUE node, cast to UniqueState internally

## Dependencies
- Functions called/Symbols referenced:
  - castNode: Cast pstate to UniqueState
  - outerPlanState: Get the outer plan state
  - [ExecProcNode](ExecProcNode.md): Execute the outer subplan to get next tuple
  - TupIsNull: Check if tuple slot is empty
  - [ExecClearTuple](ExecClearTuple.md): Clear the result tuple slot
  - [ExecQualAndReset](ExecQualAndReset.md): Execute equality comparison between tuples
  - [ExecCopySlot](ExecCopySlot.md): Copy tuple from source to result slot
- Called from:
  - [ExecInitUnique](ExecInitUnique.md): During node initialization to set up the execution function

## Notes and Other Information
- Requires input tuples to be sorted for correct duplicate elimination
- Only returns the first tuple of each group of duplicates
- Uses equality functions (eqfunction) stored in UniqueState for tuple comparison
- Must copy result tuples because the source subplan may reuse tuple slots
- Handles interrupts via CHECK_FOR_INTERRUPTS() for query cancellation support

## Simplified Source

```c
static TupleTableSlot *
ExecUnique(PlanState *pstate)
{
    UniqueState *node = castNode(UniqueState, pstate);
    TupleTableSlot *resultSlot = node->ps.ps_ResultTupleSlot;
    TupleTableSlot *slot;
    PlanState *outerPlan = outerPlanState(node);

    // Main loop: eliminate duplicates from sorted input
    for (;;)
    {
        // Get next tuple from outer subplan
        slot = ExecProcNode(outerPlan);
        if (TupIsNull(slot))
            return NULL;  // End of input

        // Always return first tuple
        if (TupIsNull(resultSlot))
            break;

        // Compare with previous tuple - if identical, skip it
        node->ps.ps_ExprContext->ecxt_innertuple = slot;
        node->ps.ps_ExprContext->ecxt_outertuple = resultSlot;
        if (!ExecQualAndReset(node->eqfunction, node->ps.ps_ExprContext))
            break;  // Different tuple found
    }

    // Copy and return the new distinct tuple
    return ExecCopySlot(resultSlot, slot);
}
```

This function eliminates duplicates by:
1. Fetching tuples from the outer subplan in sorted order
2. Comparing each new tuple with the previously returned tuple
3. Skipping identical tuples and returning only distinct ones
4. Copying result tuples to ensure they remain valid