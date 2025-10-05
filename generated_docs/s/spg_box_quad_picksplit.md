# spg_box_quad_picksplit

## Location
[src/backend/utils/adt/geo_spgist.c:441-507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L441-L507)

## Overview
The SP-GiST pick-split function for box geometric types that partitions a collection of boxes into quadrants by calculating a central 4D point as the median of all box coordinates.

## Definition
```c
Datum spg_box_quad_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "pick-split" operation for SP-GiST indexes on PostgreSQL's BOX geometric type using a quadtree partitioning strategy. When an internal node becomes full and needs to be split, this function determines how to partition the existing boxes into 16 quadrants based on a calculated centroid.

The function operates by:
1. Extracting coordinate arrays (lowXs, highXs, lowYs, highYs) from all input boxes
2. Sorting each coordinate array to find median values
3. Creating a centroid box using the median coordinates
4. Assigning each input box to one of 16 quadrants relative to the centroid
5. Setting up the output structure with the centroid as prefix and quadrant assignments

The 4D coordinate system considers each box as having four coordinates: low.x, high.x, low.y, high.y, allowing for precise spatial partitioning.

## Parameters / Member Variables
- `in` (spgPickSplitIn*): Input structure containing the array of datums (boxes) to be split and the number of tuples
- `out` (spgPickSplitOut*): Output structure where split results are stored including prefix, node mappings, and leaf data
- `centroid` (BOX*): The calculated median box that serves as the splitting point
- `median` (int): Index position of the median value in sorted arrays
- `lowXs/highXs/lowYs/highYs` (float8*): Arrays holding sorted coordinate values for median calculation

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [DatumGetBoxP](../D/DatumGetBoxP.md) (converts Datum to BOX pointer)
  - qsort (standard library sorting function)
  - [compareDoubles](../c/compareDoubles.md) (comparator function for sorting coordinates)
  - [getQuadrant](../g/getQuadrant.md) (determines quadrant based on centroid and box)
  - [BoxPGetDatum](../B/BoxPGetDatum.md) (converts BOX pointer to Datum)
  - PG_RETURN_VOID (PostgreSQL function return macro)
- Called from (representative examples):
  - SP-GiST index split operations
  - Spatial index rebalancing routines

## Notes and Other Information
- Creates 16 child nodes corresponding to all possible quadrant combinations in 4D space
- Uses median-based splitting strategy to ensure balanced tree structure
- Does not use node labels (nodeLabels set to NULL)
- Memory allocation uses PostgreSQL's palloc for automatic cleanup
- Located in src/backend/utils/adt/geo_spgist.c:441-507
- The split strategy considers boxes as 4D points to enable more precise spatial partitioning than traditional 2D approaches

## Simplified Source

```c
/* SP-GiST pick-split function - splits boxes into quadrants using median centroid */
Datum
spg_box_quad_picksplit(PG_FUNCTION_ARGS)
{
    spgPickSplitIn *in = (spgPickSplitIn *) PG_GETARG_POINTER(0);
    spgPickSplitOut *out = (spgPickSplitOut *) PG_GETARG_POINTER(1);

    // Allocate arrays for all 4D coordinates
    float8 *lowXs = palloc(sizeof(float8) * in->nTuples);
    float8 *highXs = palloc(sizeof(float8) * in->nTuples);
    float8 *lowYs = palloc(sizeof(float8) * in->nTuples);
    float8 *highYs = palloc(sizeof(float8) * in->nTuples);

    // Extract coordinates from all input boxes
    for (int i = 0; i < in->nTuples; i++) {
        BOX *box = DatumGetBoxP(in->datums[i]);
        lowXs[i] = box->low.x;
        highXs[i] = box->high.x;
        lowYs[i] = box->low.y;
        highYs[i] = box->high.y;
    }

    // Sort coordinates to find medians
    qsort(lowXs, in->nTuples, sizeof(float8), compareDoubles);
    qsort(highXs, in->nTuples, sizeof(float8), compareDoubles);
    qsort(lowYs, in->nTuples, sizeof(float8), compareDoubles);
    qsort(highYs, in->nTuples, sizeof(float8), compareDoubles);

    // Create centroid using median coordinates
    int median = in->nTuples / 2;
    BOX *centroid = palloc(sizeof(BOX));
    centroid->low.x = lowXs[median];
    centroid->high.x = highXs[median];
    centroid->low.y = lowYs[median];
    centroid->high.y = highYs[median];

    // Set up output structure with 16 nodes
    out->hasPrefix = true;
    out->prefixDatum = BoxPGetDatum(centroid);
    out->nNodes = 16;
    out->nodeLabels = NULL;
    out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
    out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

    // Assign each box to its appropriate quadrant
    for (int i = 0; i < in->nTuples; i++) {
        BOX *box = DatumGetBoxP(in->datums[i]);
        uint8 quadrant = getQuadrant(centroid, box);
        out->leafTupleDatums[i] = BoxPGetDatum(box);
        out->mapTuplesToNodes[i] = quadrant;
    }

    PG_RETURN_VOID();
}
```