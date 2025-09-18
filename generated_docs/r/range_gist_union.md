# range_gist_union

## Location
[src/backend/utils/adt/rangetypes_gist.c:324-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L324-L361)

## Overview
Implements the GiST union operation for range types, computing the bounding range that encompasses all input ranges from a set of index entries.

## Definition


## Detailed Description
The  function is a core component of the GiST operator class for range types. It computes the union (bounding range) of multiple range values, which is essential for maintaining the GiST tree structure. This function is called during index construction and maintenance operations to create parent nodes that represent the spatial bounds of their child nodes.

The function iterates through a vector of GiST entries, each containing a range value, and uses  to progressively compute the smallest range that encompasses all input ranges. This bounding range becomes the key for internal nodes in the GiST tree.

## Parameters / Member Variables
- : Vector of GiST entries containing range values to be unified

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - GiST index access method during index construction and node splitting operations

## Notes and Other Information
- This function is fundamental to GiST tree maintenance, creating bounding ranges for internal nodes
- The result range may contain gaps and span beyond the actual ranges if they are non-contiguous
- Uses  which handles cases where ranges may not be adjacent or overlapping
- Part of the standard GiST operator class interface for range types
- The function processes entries sequentially, building up the union incrementally
- Located in src/backend/utils/adt/rangetypes_gist.c:324-361