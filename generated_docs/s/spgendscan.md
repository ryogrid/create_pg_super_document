# spgendscan

## Location
src/backend/access/spgist/spgscan.c: 429 - 462

## Overview
Terminates an SP-GiST index scan and frees all associated memory and resources allocated during the scan lifetime.

## Definition
```c
void spgendscan(IndexScanDesc scan)
```

## Detailed Description
This function performs cleanup and resource deallocation for an SP-GiST index scan that is being terminated. It systematically frees all memory contexts, data structures, and arrays that were allocated during the scan's initialization and operation.

The function deletes the temporary and traversal memory contexts that were created for the scan, frees scan key data, cleans up tuple descriptors if they were specially allocated, and handles cleanup for distance-ordered scans by freeing all associated arrays. Finally, it frees the main SpGistScanOpaque structure itself.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure representing the index scan to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [pfree](../p/pfree.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - RelationGetDescr (for comparison)
- Called from:
  - [spghandler](spghandler.md) (src/backend/access/spgist/spgutils.c:88)

## Dependencies
- Types used:
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - SpGistScanOpaque

## Notes and Other Information
- This function is the cleanup counterpart to spgbeginscan
- Deletes both tempCxt and traversalCxt memory contexts that were created during scan initialization
- Only frees the leafTupDesc if it was specially created (not the same as the relation's descriptor)
- For distance-ordered scans, frees multiple arrays: orderByTypes, nonNullOrderByOffsets, zeroDistances, infDistances
- Also frees order-by related arrays from the scan descriptor: xs_orderbyvals and xs_orderbynulls
- Ensures complete cleanup to prevent memory leaks when ending SP-GiST index scans
- Should be called when the scan is no longer needed to free all associated resources