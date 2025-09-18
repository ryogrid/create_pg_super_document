# spgLeafConsistentIn

## Location
[src/include/access/spgist.h:167-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L167-L181)

## Overview
spgLeafConsistentIn is a structure that provides input parameters for SP-GiST leaf node consistency checking operations, containing scan keys, traversal context, and the actual leaf datum needed to determine if a leaf tuple matches the search criteria.

## Definition
```c
typedef struct spgLeafConsistentIn
{
    ScanKey     scankeys;           /* array of operators and comparison values */
    ScanKey     orderbys;           /* array of ordering operators and comparison values */
    int         nkeys;              /* length of scankeys array */
    int         norderbys;          /* length of orderbys array */

    Datum       reconstructedValue; /* value reconstructed at parent */
    void       *traversalValue;     /* opclass-specific traverse value */
    int         level;              /* current level (counting from zero) */
    bool        returnData;         /* original data must be returned? */

    Datum       leafDatum;          /* datum in leaf tuple */
} spgLeafConsistentIn;
```

## Detailed Description
The spgLeafConsistentIn structure is used as an input parameter for SP-GiST leaf node consistency checking functions. When the index traversal reaches a leaf tuple, this structure provides all the necessary information for the operator class's leaf_consistent method to determine whether the leaf tuple satisfies the search conditions. It contains the search criteria, traversal state accumulated during the tree descent, and the actual data value stored in the leaf tuple being examined.

## Parameters / Member Variables
- `scankeys`: Array of ScanKey structures containing search operators and comparison values for the query
- `orderbys`: Array of ScanKey structures for ordering operations (used in nearest-neighbor searches)
- `nkeys`: Number of elements in the scankeys array
- `norderbys`: Number of elements in the orderbys array
- `reconstructedValue`: The data value reconstructed from the path taken from the root to reach this leaf
- `traversalValue`: Opaque pointer to operator class-specific data maintained during traversal
- `level`: Current depth level in the tree where this leaf resides (root is level 0)
- `returnData`: Boolean flag indicating whether original indexed data must be returned (affects optimization decisions)
- `leafDatum`: The actual data value stored in the leaf tuple being tested for consistency

## Dependencies
- Functions called/Symbols referenced:
  - ScanKey (PostgreSQL scan key structure)
  - Datum (PostgreSQL data type)
- Called from (representative examples):
  - [spg_quad_leaf_consistent](spg_quad_leaf_consistent.md) (src/backend/access/spgist/spgquadtreeproc.c:409)
  - [spgLeafTest](spgLeafTest.md) (src/backend/access/spgist/spgscan.c:538)
  - [spg_text_leaf_consistent](spg_text_leaf_consistent.md) (src/backend/access/spgist/spgtextproc.c:576)
  - [spg_box_quad_leaf_consistent](spg_box_quad_leaf_consistent.md) (src/backend/utils/adt/geo_spgist.c:743)
  - [inet_spg_leaf_consistent](../i/inet_spg_leaf_consistent.md) (src/backend/utils/adt/network_spgist.c:325)
  - [spg_range_quad_leaf_consistent](spg_range_quad_leaf_consistent.md) (src/backend/utils/adt/rangetypes_spgist.c:919)
  - [spgist_name_leaf_consistent](spgist_name_leaf_consistent.md) (src/test/modules/spgist_name_ops/spgist_name_ops.c:401)

## Notes and Other Information
- This structure is read-only from the perspective of the leaf_consistent method - it should not modify the provided data
- The leafDatum field contains the actual indexed value that needs to be tested against the search conditions
- The reconstructedValue may be different from leafDatum in cases where the tree structure performs lossy compression
- The traversalValue field carries state information accumulated during the descent from root to this leaf
- Different data types implement their own leaf consistency logic but all use this common input structure
- The leaf_consistent method typically compares the leafDatum against the search conditions specified in scankeys
- For nearest-neighbor queries, the method may also need to calculate distances using the orderbys criteria