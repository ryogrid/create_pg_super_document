# spgInnerConsistentOut

## Location
[src/include/access/spgist.h:154-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L154-L162)

## Overview
spgInnerConsistentOut is a structure that defines the output results from SP-GiST inner node consistency checking operations, specifying which child nodes should be visited and providing associated traversal information.

## Definition
```c
typedef struct spgInnerConsistentOut
{
    int         nNodes;             /* number of child nodes to be visited */
    int        *nodeNumbers;        /* their indexes in the node array */
    int        *levelAdds;          /* increment level by this much for each */
    Datum      *reconstructedValues; /* associated reconstructed values */
    void      **traversalValues;    /* opclass-specific traverse values */
    double    **distances;          /* associated distances */
} spgInnerConsistentOut;
```

## Detailed Description
The spgInnerConsistentOut structure is used as an output parameter for SP-GiST inner node consistency checking functions. After an inner_consistent method determines which child nodes should be visited during index traversal, it populates this structure with the selection results and associated metadata. This structure provides not only the list of child nodes to visit, but also the context information needed for continued traversal, including reconstructed values, traversal state, and distance calculations for nearest-neighbor queries.

## Parameters / Member Variables
- `nNodes`: The number of child nodes that should be visited (length of all the following arrays)
- `nodeNumbers`: Array of integers specifying the indexes of child nodes to visit (corresponds to positions in the inner tuple's node array)
- `levelAdds`: Array of integers specifying how much to increment the tree level for each corresponding child node (usually 1, but can vary for compressed paths)
- `reconstructedValues`: Array of Datum values representing the data values reconstructed for each child node path
- `traversalValues`: Array of void pointers to opclass-specific traversal state information for each child node
- `distances`: Array of double pointers to distance values for each child node (used in nearest-neighbor searches, NULL for non-ordered scans)

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
  - void (standard C type)
  - double (standard C type)
- Called from (representative examples):
  - [spg_kd_inner_consistent](spg_kd_inner_consistent.md) (src/backend/access/spgist/spgkdtreeproc.c:163)
  - [spg_quad_inner_consistent](spg_quad_inner_consistent.md) (src/backend/access/spgist/spgquadtreeproc.c:230)
  - [spgMakeInnerItem](spgMakeInnerItem.md) (src/backend/access/spgist/spgscan.c:632)
  - [spgInnerTest](spgInnerTest.md) (src/backend/access/spgist/spgscan.c:671)
  - [spg_text_inner_consistent](spg_text_inner_consistent.md) (src/backend/access/spgist/spgtextproc.c:429)
  - [spg_box_quad_inner_consistent](spg_box_quad_inner_consistent.md) (src/backend/utils/adt/geo_spgist.c:556)
  - [inet_spg_inner_consistent](../i/inet_spg_inner_consistent.md) (src/backend/utils/adt/network_spgist.c:242)
  - [spg_range_quad_inner_consistent](spg_range_quad_inner_consistent.md) (src/backend/utils/adt/rangetypes_spgist.c:303)

## Notes and Other Information
- All arrays in this structure must have the same length (nNodes elements)
- The nodeNumbers array contains indexes that reference child nodes in the current inner tuple
- The levelAdds array allows for compressed tree paths where multiple logical levels are skipped
- The distances array is only populated for nearest-neighbor queries and remains NULL for regular searches
- Memory allocation for the arrays is typically handled by the inner_consistent method using the provided memory context
- The traversalValues array allows operator classes to maintain different state information for different child paths
- If no child nodes should be visited (search condition eliminates all possibilities), nNodes should be set to 0