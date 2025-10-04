# spgInnerTest

## Location
[src/backend/access/spgist/spgscan.c:667-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L667-L745)

## Overview
Tests an inner tuple using the opclass-specific inner_consistent method and creates search items for child nodes that should be visited during SP-GiST index traversal.

## Definition

```c
enum SpGistSpecialOffsetNumbers
{
	SpGistBreakOffsetNumber = InvalidOffsetNumber,
	SpGistRedirectOffsetNumber = MaxOffsetNumber + 1,
	SpGistErrorOffsetNumber = MaxOffsetNumber + 2,
};
```
## Detailed Description
This function is the core inner tuple processing mechanism in SP-GiST scanning. It calls the opclass-specific inner_consistent method to determine which child nodes of an inner tuple should be visited. The function handles both NULL and non-NULL inner tuples, manages memory contexts appropriately, and creates search items for qualifying child nodes.

For non-NULL inner tuples, it calls the user-defined inner_consistent function with properly initialized input parameters. For NULL inner tuples, it forces all children to be visited since NULL values can appear anywhere in the tree structure.

The function includes validation for allTheSame inner tuples, ensuring that the inner_consistent method returns consistent results (either all nodes or no nodes should match). It then creates search items for each qualifying child node and adds them to the search queue for continued traversal.

## Parameters
- : SpGistScanOpaque - The scan state containing configuration, contexts, and method function pointers
- : SpGistSearchItem * - The current search item representing the inner tuple being processed
- : SpGistInnerTuple - The inner tuple containing child node information and structure
- : bool - Whether this inner tuple represents a NULL value path

## Dependencies
- Functions called/Symbols referenced:
  - [spgInitInnerConsistentIn](spgInitInnerConsistentIn.md) - Initializes input parameters for inner_consistent call
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) - Calls the opclass inner_consistent method
  - SGITITERATE - Iterates through child nodes in the inner tuple
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md) - Validates child node pointers
  - [spgMakeInnerItem](spgMakeInnerItem.md) - Creates search items for qualifying child nodes
  - [spgAddSearchItemToQueue](spgAddSearchItemToQueue.md) - Adds child items to the search queue
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) - Manages memory contexts during processing
- Called from:
  - [spgWalk](spgWalk.md) - Main scanning function that processes inner tuples during traversal

## Notes and Other Information
- Memory context management is crucial - temporary context for inner_consistent calls, traversal context for creating search items
- NULL inner tuple handling forces visitation of all children since NULLs can appear anywhere
- The allTheSame validation prevents inconsistent behavior from poorly implemented inner_consistent methods
- Distance handling defaults to infinity distances when the inner_consistent method doesn't provide them
- Child nodes with invalid tuple IDs are skipped during processing
- The function properly handles arrays returned by inner_consistent (nodeNumbers, levelAdds, reconstructedValues, traversalValues, distances)
- Error checking ensures that array indices are within valid bounds before accessing child nodes

## Simplified Source

```c
static void
spgInnerTest(SpGistScanOpaque so, SpGistSearchItem *item,
             SpGistInnerTuple innerTuple, bool isnull)
{
    MemoryContext oldCxt = MemoryContextSwitchTo(so->tempCxt);
    spgInnerConsistentOut out;
    int nNodes = innerTuple->nNodes;
    int i;

    memset(&out, 0, sizeof(out));

    if (!isnull) {
        // Test inner tuple using opclass-specific inner_consistent method
        spgInnerConsistentIn in;
        spgInitInnerConsistentIn(&in, so, item, innerTuple);

        // Call user-defined inner consistent method
        FunctionCall2Coll(&so->innerConsistentFn,
                         so->indexCollation,
                         PointerGetDatum(&in),
                         PointerGetDatum(&out));
    } else {
        // For NULL inner tuples, visit all children
        out.nNodes = nNodes;
        out.nodeNumbers = (int *) palloc(sizeof(int) * nNodes);
        for (i = 0; i < nNodes; i++)
            out.nodeNumbers[i] = i;
    }

    // Validate allTheSame consistency
    if (innerTuple->allTheSame && out.nNodes != 0 && out.nNodes != nNodes)
        elog(ERROR, "inconsistent inner_consistent results for allTheSame inner tuple");

    if (out.nNodes) {
        // Collect all child node pointers
        SpGistNodeTuple node;
        SpGistNodeTuple *nodes = (SpGistNodeTuple *) palloc(sizeof(SpGistNodeTuple) * nNodes);

        SGITITERATE(innerTuple, i, node) {
            nodes[i] = node;
        }

        MemoryContextSwitchTo(so->traversalCxt);

        // Create search items for each qualifying child node
        for (i = 0; i < out.nNodes; i++) {
            int nodeN = out.nodeNumbers[i];
            SpGistSearchItem *innerItem;
            double *distances;

            node = nodes[nodeN];

            // Skip invalid child nodes
            if (!ItemPointerIsValid(&node->t_tid))
                continue;

            // Use provided distances or default to infinity
            distances = out.distances ? out.distances[i] : so->infDistances;

            // Create search item and add to queue
            innerItem = spgMakeInnerItem(so, item, node, &out, i, isnull, distances);
            spgAddSearchItemToQueue(so, innerItem);
        }
    }

    MemoryContextSwitchTo(oldCxt);
}
```