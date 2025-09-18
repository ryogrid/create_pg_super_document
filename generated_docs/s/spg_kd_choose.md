# spg_kd_choose

## Location
[src/backend/access/spgist/spgkdtreeproc.c:54-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgkdtreeproc.c#L54-L77)

## Overview
SP-GiST choose function for k-dimensional trees that determines which child node to follow when descending the tree during insertion or search operations.

## Definition


## Detailed Description
This function implements the choose logic for SP-GiST k-d tree operations. When traversing the tree during insertion or search, it determines which of the two child nodes to follow based on the spatial relationship between the input point and the splitting coordinate. The function uses alternating dimensions (X and Y coordinates) at different tree levels to create a balanced spatial partitioning. It extracts the splitting coordinate from the prefix datum and compares it against the appropriate coordinate of the input point using the  helper function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to:
  - Input argument 0:  - Contains input point, prefix datum, tree level, and node information
  - Input argument 1:  - Output structure to specify which child node to follow

## Dependencies
- Functions called/Symbols referenced:
  - [spgChooseIn](spgChooseIn.md) (structure type)
  - [spgChooseOut](spgChooseOut.md) (structure type)  
  - [Point](../P/Point.md) (structure type)
  - [DatumGetPointP](../D/DatumGetPointP.md) (PostgreSQL datum conversion macro)
  - [DatumGetFloat8](../D/DatumGetFloat8.md) (PostgreSQL datum conversion macro)
  - spgMatchNode (SP-GiST result type constant)
  - [getSide](../g/getSide.md) (helper function for coordinate comparison)
  - [PointPGetDatum](../P/PointPGetDatum.md) (PostgreSQL datum conversion macro)
  - PG_RETURN_VOID (PostgreSQL return macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses alternating dimensions based on tree level () to determine whether to compare X or Y coordinates
- Always expects exactly 2 child nodes () as k-d trees are binary trees
- Returns node 0 if the input point is on the "greater" side, node 1 otherwise
- Includes error handling for the  condition which should not occur in k-d trees
- Sets  to increment the tree level for the next descent step
- Part of the SP-GiST framework for spatial indexing in PostgreSQL
- Located in src/backend/access/spgist/spgkdtreeproc.c:54-77