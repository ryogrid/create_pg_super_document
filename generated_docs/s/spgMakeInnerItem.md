# spgMakeInnerItem

## Location
[src/backend/access/spgist/spgscan.c:629-666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L629-L666)

## Overview
Creates a new SpGistSearchItem for an inner node during SP-GiST index traversal, incorporating results from the inner_consistent method to guide further tree navigation.

## Definition

```c
structed values are of type leafType) */
	item->value = out->reconstructedValues
		? datumCopy(out->reconstructedValues[i],
					so->state.attLeafType.attbyval,
					so->state.attLeafType.attlen)
		: (Datum) 0;
```
## Detailed Description
This function constructs a SpGistSearchItem specifically for inner (non-leaf) nodes during SP-GiST index scanning. It processes the output from an inner_consistent method call to create search items for child nodes that should be visited. The function handles level calculation, reconstructed value copying, and traversal value management.

The function is responsible for:
- Allocating a new search item for the child node
- Setting the correct tree level (potentially adjusted by levelAdds)
- Copying reconstructed values from temporary to persistent memory context
- Preserving traversal values for use in subsequent inner_consistent calls
- Setting appropriate flags for inner node processing

## Parameters
- : SpGistScanOpaque - The scan state containing configuration and context information
- : SpGistSearchItem * - The parent search item from which this child is derived
- : SpGistNodeTuple - The node tuple representing the child to be visited
- : spgInnerConsistentOut * - Output structure from inner_consistent containing processing results
- : int - Index of this particular child node in the output arrays
- : bool - Whether this child represents a NULL value path
- : double * - Array of distance values for ordered scans

## Dependencies
- Functions called/Symbols referenced:
  - [spgAllocSearchItem](spgAllocSearchItem.md) - Allocates a new search item in queue context
  - [datumCopy](../d/datumCopy.md) - Creates a proper copy of the reconstructed datum value
- Called from:
  - [spgInnerTest](spgInnerTest.md) - Creates inner items for child nodes selected by inner_consistent

## Notes and Other Information
- The level calculation supports opclass-defined level adjustments via levelAdds array
- Reconstructed values are copied using the leafType specification, not the attribute type
- Traversal values are assumed to be already allocated in the long-lived traversal memory context
- Inner items never have recheck flags set (unlike leaf items)
- The leafTuple field is always NULL for inner items since they don't correspond to heap tuples
- The function handles both ordered and non-ordered scans through the distances parameter
- Memory context management is crucial - reconstructed values must be copied out of temporary context

## Simplified Source

```c
static SpGistSearchItem *
spgMakeInnerItem(SpGistScanOpaque so,
                 SpGistSearchItem *parentItem,
                 SpGistNodeTuple tuple,
                 spgInnerConsistentOut *out, int i, bool isnull,
                 double *distances)
{
    // Allocate new search item for inner node
    SpGistSearchItem *item = spgAllocSearchItem(so, isnull, distances);

    // Set basic properties from node tuple
    item->heapPtr = tuple->t_tid;

    // Calculate level (with optional adjustment from opclass)
    item->level = out->levelAdds ? parentItem->level + out->levelAdds[i]
                                 : parentItem->level;

    // Copy reconstructed value from temp context to persistent context
    item->value = out->reconstructedValues
        ? datumCopy(out->reconstructedValues[i],
                   so->state.attLeafType.attbyval,
                   so->state.attLeafType.attlen)
        : (Datum) 0;

    // Set traversal value for next inner_consistent call
    item->traversalValue = out->traversalValues ? out->traversalValues[i] : NULL;

    // Initialize inner node flags
    item->leafTuple = NULL;
    item->isLeaf = false;
    item->recheck = false;
    item->recheckDistances = false;

    return item;
}
```