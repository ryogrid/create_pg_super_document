# spgInnerConsistentIn

## Location
src/include/access/spgist.h: 132 - 152

## Overview
spgInnerConsistentIn is a structure that provides input parameters for SP-GiST inner node consistency checking operations, containing scan keys, traversal context, and inner tuple information needed to determine which child nodes to visit during index traversal.

## Definition
```c
typedef struct spgInnerConsistentIn
{
    ScanKey     scankeys;           /* array of operators and comparison values */
    ScanKey     orderbys;           /* array of ordering operators and comparison values */
    int         nkeys;              /* length of scankeys array */
    int         norderbys;          /* length of orderbys array */

    Datum       reconstructedValue; /* value reconstructed at parent */
    void       *traversalValue;     /* opclass-specific traverse value */
    MemoryContext traversalMemoryContext; /* put new traverse values here */
    int         level;              /* current level (counting from zero) */
    bool        returnData;         /* original data must be returned? */

    /* Data from current inner tuple */
    bool        allTheSame;         /* tuple is marked all-the-same? */
    bool        hasPrefix;          /* tuple has a prefix? */
    Datum       prefixDatum;        /* if so, the prefix value */
    int         nNodes;             /* number of nodes in the inner tuple */
    Datum      *nodeLabels;         /* node label values (NULL if none) */
} spgInnerConsistentIn;
```

## Detailed Description
The spgInnerConsistentIn structure is used as an input parameter for SP-GiST inner node consistency checking functions. When traversing an SP-GiST index during a search operation, this structure provides all the necessary information for the operator class's inner_consistent method to determine which child nodes should be visited. It contains the search conditions (scan keys), ordering requirements, traversal state, and detailed information about the current inner tuple being examined.

## Parameters / Member Variables
- `scankeys`: Array of ScanKey structures containing search operators and comparison values for the query
- `orderbys`: Array of ScanKey structures for ordering operations (used in nearest-neighbor searches)
- `nkeys`: Number of elements in the scankeys array
- `norderbys`: Number of elements in the orderbys array
- `reconstructedValue`: The data value reconstructed from the path taken from the root to the current node
- `traversalValue`: Opaque pointer to operator class-specific data maintained during traversal
- `traversalMemoryContext`: Memory context where new traversal values should be allocated
- `level`: Current depth level in the tree (root is level 0)
- `returnData`: Boolean flag indicating whether original indexed data must be returned (affects optimization decisions)
- `allTheSame`: Boolean flag indicating if the current inner tuple is marked as "all-the-same" (optimization for identical values)
- `hasPrefix`: Boolean flag indicating whether the current inner tuple has a prefix value
- `prefixDatum`: The prefix value stored in the inner tuple (valid only if hasPrefix is true)
- `nNodes`: Number of child nodes in the current inner tuple
- `nodeLabels`: Array of Datum values representing labels for each child node (NULL if nodes have no labels)

## Dependencies
- Functions called/Symbols referenced:
  - ScanKey (PostgreSQL scan key structure)
  - Datum (PostgreSQL data type)
  - MemoryContext (PostgreSQL memory management)
- Called from (representative examples):
  - spg_kd_inner_consistent (src/backend/access/spgist/spgkdtreeproc.c:162)
  - spg_quad_inner_consistent (src/backend/access/spgist/spgquadtreeproc.c:229)
  - spgInitInnerConsistentIn (src/backend/access/spgist/spgscan.c:606)
  - spgInnerTest (src/backend/access/spgist/spgscan.c:679)
  - spg_text_inner_consistent (src/backend/access/spgist/spgtextproc.c:428)
  - spg_box_quad_inner_consistent (src/backend/utils/adt/geo_spgist.c:555)
  - inet_spg_inner_consistent (src/backend/utils/adt/network_spgist.c:241)
  - spg_range_quad_inner_consistent (src/backend/utils/adt/rangetypes_spgist.c:302)

## Notes and Other Information
- This structure is read-only from the perspective of the inner_consistent method - it should not modify the provided data
- The traversalValue field allows operator classes to maintain state across different levels of the tree traversal
- The reconstructedValue field contains the cumulative result of following the path from root to the current node
- Different data types implement their own inner consistency logic but all use this common input structure
- The allTheSame flag is an optimization for cases where all values in a subtree are identical