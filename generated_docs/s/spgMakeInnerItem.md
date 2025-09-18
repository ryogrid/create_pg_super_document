# spgMakeInnerItem

## Location
src/backend/access/spgist/spgscan.c: 629 - 666

## Overview
Creates a new SpGistSearchItem for an inner node during SP-GiST index traversal, incorporating results from the inner_consistent method to guide further tree navigation.

## Definition


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
  - spgAllocSearchItem - Allocates a new search item in queue context
  - datumCopy - Creates a proper copy of the reconstructed datum value
- Called from:
  - spgInnerTest - Creates inner items for child nodes selected by inner_consistent

## Notes and Other Information
- The level calculation supports opclass-defined level adjustments via levelAdds array
- Reconstructed values are copied using the leafType specification, not the attribute type
- Traversal values are assumed to be already allocated in the long-lived traversal memory context
- Inner items never have recheck flags set (unlike leaf items)
- The leafTuple field is always NULL for inner items since they don't correspond to heap tuples
- The function handles both ordered and non-ordered scans through the distances parameter
- Memory context management is crucial - reconstructed values must be copied out of temporary context