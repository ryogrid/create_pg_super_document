# SpGistLeafTuple

## Location
src/include/access/spgist_private.h: 132 - 134

## Overview
SpGistLeafTuple is a pointer type to SpGistLeafTupleData that represents leaf-level tuples in SP-GiST (Space-Partitioned Generalized Search Tree) indexes, carrying leaf data and heap tuple references.

## Definition


## Detailed Description
SpGistLeafTuple is a typedef for a pointer to SpGistLeafTupleData structure. It represents the fundamental storage unit for leaf nodes in SP-GiST indexes. These tuples store the actual indexed values (or derived forms like suffixes) along with pointers to the corresponding heap tuples. The structure supports optional included columns and efficiently handles NULL values through a bitmap mechanism.

The design allows for flexible data representation where the leaf datum might be the original indexed value, a compressed suffix, or other delta information that can reconstruct the full value given the path traversed through the tree. This flexibility is crucial for SP-GiST's space-partitioning approach.

## Parameters / Member Variables
The underlying SpGistLeafTupleData structure contains:
- : 2-bit field indicating tuple status (LIVE/REDIRECT/DEAD/PLACEHOLDER)
- : 30-bit field storing tuple size (must be MAXALIGN multiple and >= SGDTSIZE)
- : 16-bit field containing nextOffset (14 bits) plus flag bits for nulls bitmap and other purposes
- : ItemPointerData pointing to the corresponding heap tuple
- Variable-length data follows: nulls bitmap (if needed), leaf datum, and included columns

## Dependencies
- Functions called/Symbols referenced:
  - SpGistLeafTupleData (underlying structure)
  - ItemPointerData (for heap tuple references)

- Called from (representative examples):
  - addLeafTuple (spgdoinsert.c:203)
  - spgFormLeafTuple (spgutils.c:866)
  - spgDeformLeafTuple (spgutils.c:1107)
  - vacuumLeafPage (spgvacuum.c:149)
  - spgLeafTest (spgscan.c:517)

## Notes and Other Information
- The size field is intentionally wider than needed for on-disk storage to support temporary large datums during tuple formation
- The nextOffset field chains tuples belonging to the same parent node, except on root pages where chaining is not used
- NULL handling is optimized: nulls bitmap only exists when there are included columns with NULL values
- Tuple size must accommodate potential conversion to REDIRECT status during splits
- Critical for SP-GiST's space-partitioning strategy, allowing efficient storage and retrieval of spatial and hierarchical data