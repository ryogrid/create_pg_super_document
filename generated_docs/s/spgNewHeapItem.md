# spgNewHeapItem

## Location
src/backend/access/spgist/spgscan.c: 463 - 515

## Overview
Creates a new SpGistSearchItem for a leaf tuple in an SP-GiST index scan, storing all necessary information for heap tuple retrieval and result reconstruction.

## Definition


## Detailed Description
This function constructs a SpGistSearchItem specifically for leaf tuples during SP-GiST index scanning. It serves as a constructor for search items that represent actual heap tuples found during the scan. The function handles the proper copying of leaf values and tuple data from temporary memory contexts to the queue context for safe storage throughout the scan process.

The function is responsible for:
- Allocating a new search item in the queue context
- Setting up the heap pointer for tuple retrieval
- Copying the reconstructed value if needed (handling type safety)
- Storing the complete leaf tuple if INCLUDE attributes are present
- Setting flags for result processing (recheck, distance rechecking)

## Parameters
- : SpGistScanOpaque - The scan state containing configuration and context information
- : int - The tree level where this leaf was found
- : SpGistLeafTuple - The leaf tuple containing the heap pointer and data
- : Datum - The reconstructed value from the leaf_consistent method
- : bool - Whether the result needs to be rechecked against the original condition
- : bool - Whether distance calculations need to be rechecked
- : bool - Whether the leaf value is NULL
- : double * - Array of distance values for ordering results

## Dependencies
- Functions called/Symbols referenced:
  - spgAllocSearchItem - Allocates a new search item in queue context
  - datumCopy - Creates a proper copy of the datum value
  - palloc - Allocates memory for leaf tuple storage
  - memcpy - Copies leaf tuple data
- Called from:
  - spgLeafTest - Creates heap items for qualifying leaf tuples

## Notes and Other Information
- The function carefully handles memory context issues by copying data from temporary context to queue context
- It includes type safety checks, noting that leaf_consistent methods might supply wrong-typed values
- The function handles INCLUDE attributes by storing the complete leaf tuple when multiple attributes are present
- All search items created are marked as leaf items (isLeaf = true)
- The traversalValue is always set to NULL for leaf items since no further traversal is needed