# checkAllTheSame

## Location
[src/backend/access/spgist/spgdoinsert.c:599-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L599-L676)

## Overview
This function detects when a user-defined picksplit function fails to properly distribute leaf tuples across multiple nodes and randomly redistributes them to enable "allTheSame" mode in SPGiST.

## Definition

```c
static bool
checkAllTheSame(spgPickSplitIn *in, spgPickSplitOut *out, bool tooBig,
				bool *includeNew)
```
## Detailed Description
This function serves as a failsafe mechanism for SPGiST index operations when the user-defined picksplit function produces inadequate node distribution. It detects the problematic case where all tuples are assigned to the same node, which would not achieve the desired space-partitioning effect.

When such a situation is detected, the function:
1. Overrides the picksplit function's decisions
2. Creates 8 arbitrary child nodes
3. Randomly distributes tuples across these nodes using modulo assignment
4. Preserves node labels if they exist by duplicating the original label
5. Returns true to signal that "allTheSame" mode should be used

The function includes special handling for cases where the tuple set is too large to fit on one page, excluding the new (incoming) tuple from the distribution check to avoid infinite loops.

## Parameters / Member Variables
- `*in`: Input structure containing the tuples to be split and related information
- `*out`: Output structure that will be modified to contain the new node assignments
- `tooBig`: Boolean indicating if the tuple set is too large to fit on one page
- `*includeNew`: Output parameter indicating whether the new tuple should be included in the split
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation for node labels array)
- Called from (representative examples):
  - [doPickSplit](../d/doPickSplit.md) (at src/backend/access/spgist/spgdoinsert.c:900)

## Notes and Other Information
- Uses an arbitrary number of 8 child nodes for redistribution
- Random distribution is achieved using modulo operator (i % out->nNodes)
- Special case handling for scenarios where all existing tuples fit in one node but adding the new tuple would exceed page capacity
- When tooBig is true and the new tuple is in its own node, it's excluded from the split (*includeNew = false)
- Preserves existing node labels by duplicating them across all new nodes
- Does not modify prefix or leaf tuple datum assignments from the original picksplit output
- This mechanism prevents infinite loops that could occur with poorly implemented picksplit functions
- Also used to forcibly select allTheSame mode for null values
- Location: src/backend/access/spgist/spgdoinsert.c:599-676

## Simplified Source

```c
static bool checkAllTheSame(spgPickSplitIn *in, spgPickSplitOut *out,
                           bool tooBig, bool *includeNew)
{
    int theNode;
    int limit;
    int i;

    // Assume we can include the new tuple initially
    *includeNew = true;

    // Need at least 2 tuples to check distribution
    if (in->nTuples <= 1)
        return false;

    // If set is too big, exclude new tuple from distribution check
    limit = tooBig ? in->nTuples - 1 : in->nTuples;

    // Check if all tuples are assigned to the same node
    theNode = out->mapTuplesToNodes[0];
    for (i = 1; i < limit; i++)
    {
        if (out->mapTuplesToNodes[i] != theNode)
            return false;  // Distribution is okay, use original split
    }

    // All tuples in same node - override picksplit function's decision

    // Special case: if new tuple is separate and set is too big, exclude it
    if (tooBig && out->mapTuplesToNodes[in->nTuples - 1] != theNode)
        *includeNew = false;

    // Create 8 arbitrary child nodes
    out->nNodes = 8;

    // Randomly distribute tuples across nodes using modulo
    for (i = 0; i < in->nTuples; i++)
        out->mapTuplesToNodes[i] = i % out->nNodes;

    // Duplicate node labels if opclass uses them
    if (out->nodeLabels)
    {
        Datum theLabel = out->nodeLabels[theNode];
        out->nodeLabels = (Datum *) palloc(sizeof(Datum) * out->nNodes);
        for (i = 0; i < out->nNodes; i++)
            out->nodeLabels[i] = theLabel;
    }

    return true;  // Signal allTheSame mode should be used
}
```