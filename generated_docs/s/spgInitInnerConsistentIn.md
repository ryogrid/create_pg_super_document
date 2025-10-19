# spgInitInnerConsistentIn

## Location
[src/backend/access/spgist/spgscan.c:606-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L606-L628)

## Overview
Initializes the input structure for calling an SP-GiST opclass inner_consistent method by populating all required fields from scan state and current search context.

## Definition

```c
structedValue would be wrong type */
	in->reconstructedValue = item->value;
```
## Detailed Description
This function serves as a bundle initializer that prepares the spgInnerConsistentIn structure with all necessary information before calling an opclass-specific inner_consistent method. It consolidates data from the scan state, current search item, and inner tuple into a single structure that the inner_consistent method can use to determine which child nodes should be visited.

The function extracts key information including scan keys, order-by clauses, reconstructed values from the traversal path, tuple metadata, and node labels. It performs validation to ensure the current item is not a leaf (since that would indicate incorrect traversal state).

## Parameters
- : spgInnerConsistentIn * - The structure to initialize with input data for inner_consistent
- : SpGistScanOpaque - The scan state containing keys, contexts, and configuration
- : SpGistSearchItem * - The current search item containing traversal state and reconstructed values
- : SpGistInnerTuple - The inner tuple being processed, containing node structure and prefix

## Dependencies
- Functions called/Symbols referenced:
  - SGITDATUM - Extracts the prefix datum from the inner tuple
  - [spgExtractNodeLabels](spgExtractNodeLabels.md) - Extracts node labels from the inner tuple structure
- Called from:
  - [spgInnerTest](spgInnerTest.md) - Uses this to initialize input before calling inner_consistent methods

## Notes and Other Information
- The function includes an assertion to verify that the current item is not a leaf, preventing traversal errors
- The hasPrefix field is derived by checking if prefixSize > 0, providing a boolean convenience flag
- The traversalMemoryContext is passed to allow the inner_consistent method to allocate persistent data
- The nodeLabels extraction handles the complex inner tuple structure to provide easy access to child node information
- All scan keys and order-by data are passed through to allow the opclass method full access to query constraints
- The returnData flag indicates whether the caller needs reconstructed tuple values for result construction

## Simplified Source

```c
static void spgInitInnerConsistentIn(spgInnerConsistentIn *in,
                                     SpGistScanOpaque so,
                                     SpGistSearchItem *item,
                                     SpGistInnerTuple innerTuple) {
    // Copy scan keys and order-by information
    in->scankeys = so->keyData;
    in->orderbys = so->orderByData;
    in->nkeys = so->numberOfKeys;
    in->norderbys = so->numberOfNonNullOrderBys;

    // Set traversal state from current search item
    Assert(!item->isLeaf);  // Must be inner node
    in->reconstructedValue = item->value;
    in->traversalMemoryContext = so->traversalCxt;
    in->traversalValue = item->traversalValue;
    in->level = item->level;

    // Set scan configuration
    in->returnData = so->want_itup;

    // Extract inner tuple information
    in->allTheSame = innerTuple->allTheSame;
    in->hasPrefix = (innerTuple->prefixSize > 0);
    in->prefixDatum = SGITDATUM(innerTuple, &so->state);
    in->nNodes = innerTuple->nNodes;
    in->nodeLabels = spgExtractNodeLabels(&so->state, innerTuple);
}
```