# spg_range_quad_picksplit

## Location
[src/backend/utils/adt/rangetypes_spgist.c:200-299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L200-L299)

## Overview
SP-GiST picksplit function that divides a collection of ranges into child nodes by selecting a centroid range and distributing ranges according to quadrants.

## Definition
Datum spg_range_quad_picksplit(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the node splitting logic for SP-GiST quadtree indexing of range types. When a node becomes full, it analyzes all ranges in the node to construct a "centroid" range based on median values of the lower and upper bounds. It then distributes the ranges into quadrant-based child nodes according to their spatial relationship with the centroid. The function handles special cases including all-empty ranges and root-level splitting, creating appropriate node structures for efficient quadtree navigation.

## Parameters / Member Variables
- in: spgPickSplitIn structure containing ranges to split and node information
- out: spgPickSplitOut structure populated with splitting results
- nonEmptyCount: Count of non-empty ranges in the input set
- centroid: The calculated centroid range used for quadrant determination
- lowerBounds, upperBounds: Arrays of range bounds extracted from input ranges
- typcache: Type cache entry for range operations

## Dependencies
- Functions called/Symbols referenced:
  - [spgPickSplitIn](spgPickSplitIn.md), spgPickSplitOut (structure types)
  - RangeBound (structure type)
  - [range_get_typcache](../r/range_get_typcache.md) (type cache retrieval)
  - RangeTypeGetOid, DatumGetRangeTypeP (range type operations)
  - [range_deserialize](../r/range_deserialize.md), range_serialize (range serialization)
  - [palloc](../p/palloc.md) (memory allocation)
  - qsort_arg (sorting with context)
  - [bound_cmp](../b/bound_cmp.md) (comparison function for sorting)
  - [getQuadrant](../g/getQuadrant.md) (quadrant determination)
  - [RangeTypePGetDatum](../R/RangeTypePGetDatum.md) (range to datum conversion)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - SP-GiST index splitting operations when nodes become full

## Notes and Other Information
- Constructs centroid from median lower and upper bounds of non-empty ranges
- Creates 2 nodes for all-empty case: node 0 for empty ranges, node 1 for future non-empty
- Creates 5 nodes at root level (including empty range node), 4 nodes otherwise
- Maps ranges to nodes based on quadrant (1-4 becomes nodes 0-3)
- Handles memory allocation for node mapping and leaf tuple storage
- Preserves original range data in leaf nodes for exact retrieval
- Essential for maintaining balanced quadtree structure during index growth
- Located in src/backend/utils/adt/rangetypes_spgist.c:200-299

## Simplified Source

```c
Datum spg_range_quad_picksplit(PG_FUNCTION_ARGS)
{
    spgPickSplitIn *in = (spgPickSplitIn *) PG_GETARG_POINTER(0);
    spgPickSplitOut *out = (spgPickSplitOut *) PG_GETARG_POINTER(1);

    // Get type cache for range operations
    TypeCacheEntry *typcache = range_get_typcache(fcinfo,
        RangeTypeGetOid(DatumGetRangeTypeP(in->datums[0])));

    // Extract bounds from all input ranges
    RangeBound *lowerBounds = palloc(sizeof(RangeBound) * in->nTuples);
    RangeBound *upperBounds = palloc(sizeof(RangeBound) * in->nTuples);

    int nonEmptyCount = 0;
    for (int i = 0; i < in->nTuples; i++) {
        bool empty;
        range_deserialize(typcache, DatumGetRangeTypeP(in->datums[i]),
                         &lowerBounds[nonEmptyCount], &upperBounds[nonEmptyCount], &empty);
        if (!empty)
            nonEmptyCount++;
    }

    // Handle case where all ranges are empty
    if (nonEmptyCount == 0) {
        out->nNodes = 2;
        out->hasPrefix = false;
        out->prefixDatum = PointerGetDatum(NULL);

        // Put all empty ranges in node 0
        out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
        out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);
        for (int i = 0; i < in->nTuples; i++) {
            out->leafTupleDatums[i] = in->datums[i];
            out->mapTuplesToNodes[i] = 0;
        }
        PG_RETURN_VOID();
    }

    // Sort bounds to find medians for centroid
    qsort_arg(lowerBounds, nonEmptyCount, sizeof(RangeBound), bound_cmp, typcache);
    qsort_arg(upperBounds, nonEmptyCount, sizeof(RangeBound), bound_cmp, typcache);

    // Create centroid range from median bounds
    RangeType *centroid = range_serialize(typcache,
                                         &lowerBounds[nonEmptyCount / 2],
                                         &upperBounds[nonEmptyCount / 2],
                                         false, NULL);
    out->hasPrefix = true;
    out->prefixDatum = RangeTypePGetDatum(centroid);

    // Create 5 nodes at root (including empty), 4 nodes otherwise
    out->nNodes = (in->level == 0) ? 5 : 4;
    out->nodeLabels = NULL;

    // Assign ranges to quadrant-based nodes
    out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
    out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

    for (int i = 0; i < in->nTuples; i++) {
        RangeType *range = DatumGetRangeTypeP(in->datums[i]);
        int16 quadrant = getQuadrant(typcache, centroid, range);

        out->leafTupleDatums[i] = RangeTypePGetDatum(range);
        out->mapTuplesToNodes[i] = quadrant - 1;  // Convert 1-4 to 0-3
    }

    PG_RETURN_VOID();
}
```