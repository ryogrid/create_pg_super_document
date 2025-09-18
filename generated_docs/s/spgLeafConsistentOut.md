# spgLeafConsistentOut

## Location
src/include/access/spgist.h: 183 - 189

## Overview
spgLeafConsistentOut is a structure that defines the output results from SP-GiST leaf node consistency checking operations, providing the reconstructed leaf value, recheck flags, and distance information for matching leaf tuples.

## Definition
```c
typedef struct spgLeafConsistentOut
{
    Datum       leafValue;          /* reconstructed original data, if any */
    bool        recheck;            /* set true if operator must be rechecked */
    bool        recheckDistances;   /* set true if distances must be rechecked */
    double     *distances;          /* associated distances */
} spgLeafConsistentOut;
```

## Detailed Description
The spgLeafConsistentOut structure is used as an output parameter for SP-GiST leaf node consistency checking functions. When a leaf_consistent method determines that a leaf tuple matches the search criteria, it populates this structure with the results and associated metadata. This structure provides the reconstructed original data value, flags indicating whether additional checking is needed at higher levels, and distance calculations for nearest-neighbor queries.

## Parameters / Member Variables
- `leafValue`: The reconstructed original data value for the leaf tuple (may differ from the stored datum if lossy compression is used)
- `recheck`: Boolean flag indicating whether the search operator must be rechecked at a higher level (used when the index provides approximate results)
- `recheckDistances`: Boolean flag indicating whether distance calculations must be rechecked at a higher level (used in nearest-neighbor searches with approximate distances)
- `distances`: Array of double values representing distances for nearest-neighbor queries (NULL for non-ordered scans, must match the number of ordering operators)

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
  - bool (standard boolean type)
  - double (standard floating-point type)
- Called from (representative examples):
  - spg_quad_leaf_consistent (src/backend/access/spgist/spgquadtreeproc.c:410)
  - spgLeafTest (src/backend/access/spgist/spgscan.c:539)
  - spg_text_leaf_consistent (src/backend/access/spgist/spgtextproc.c:577)
  - spg_box_quad_leaf_consistent (src/backend/utils/adt/geo_spgist.c:744)
  - inet_spg_leaf_consistent (src/backend/utils/adt/network_spgist.c:326)
  - spg_range_quad_leaf_consistent (src/backend/utils/adt/rangetypes_spgist.c:920)
  - spgist_name_leaf_consistent (src/test/modules/spgist_name_ops/spgist_name_ops.c:402)

## Notes and Other Information
- The leafValue field should contain the original indexed data, which may need to be reconstructed if the index stores compressed or transformed values
- The recheck flag is crucial for indexes that provide approximate results - when set to true, PostgreSQL will re-evaluate the search condition using the original operator on the heap tuple
- The recheckDistances flag is used in nearest-neighbor searches when the index can only provide approximate distance calculations
- The distances array must have the same length as the number of ordering operators in the query (norderbys from the input structure)
- If no distance calculation is needed (non-ordered scan), the distances field should be set to NULL
- Memory allocation for the distances array is typically handled by the leaf_consistent method
- Different data types may set these flags differently based on their indexing precision and capabilities