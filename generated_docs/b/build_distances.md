# build_distances

## Location
[src/backend/access/brin/brin_minmax_multi.c:1329-1385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1329-L1385)

## Overview
Computes the distances (gaps) between consecutive expanded ranges in a BRIN minmax-multi index structure to identify which ranges can be efficiently merged.

## Definition

```c
static DistanceValue *
build_distances(FmgrInfo *distanceFn, Oid colloid,
				ExpandedRange *eranges, int neranges)
```
## Detailed Description
This function analyzes an array of expanded ranges and calculates the size of gaps between each consecutive pair of ranges. For n ranges, it computes (n-1) gap distances. The function uses a provided distance function to calculate the difference between the maximum value of one range and the minimum value of the next range. These distance calculations are used later by the range merging logic to determine which ranges should be combined to optimize storage efficiency in BRIN indexes.

The computed distances are sorted in descending order so that the largest gaps appear first, allowing the calling code to prioritize merging ranges with smaller gaps while preserving larger gaps that provide better selectivity.

## Parameters / Member Variables
- `*distanceFn`: Function pointer to the distance calculation function for the specific data type
- `colloid`: Collation identifier for proper comparison of values
- `*eranges`: Array of expanded ranges to analyze
- `neranges`: Number of ranges in the array
## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - qsort
  - [compare_distances](../c/compare_distances.md)
  - [palloc0](../p/palloc0.md)
- Types referenced:
  - [ExpandedRange](../E/ExpandedRange.md)
  - [DistanceValue](../D/DistanceValue.md)
- Called from:
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [compactify_ranges](../c/compactify_ranges.md)
  - [brin_minmax_multi_union](brin_minmax_multi_union.md)

## Notes and Other Information
- Returns NULL if only a single range is provided (no gaps to calculate)
- The function is static and used internally within the BRIN minmax-multi access method
- Distance calculations may be expensive depending on the data type, so they are performed once and cached
- The resulting distances array is sorted to optimize the range merging process
- Part of PostgreSQL's BRIN (Block Range INdex) implementation for handling multiple values per range

## Simplified Source

```c
static DistanceValue *
build_distances(FmgrInfo *distanceFn, Oid colloid,
                ExpandedRange *eranges, int neranges)
{
    // Single range has no gaps to calculate
    if (neranges == 1)
        return NULL;

    int ndistances = neranges - 1;
    DistanceValue *distances = (DistanceValue *) palloc0(sizeof(DistanceValue) * ndistances);

    // Calculate distance between each consecutive pair of ranges
    for (int i = 0; i < ndistances; i++) {
        Datum maxval = eranges[i].maxval;        // End of current range
        Datum minval = eranges[i + 1].minval;    // Start of next range

        // Compute gap size using distance function
        Datum r = FunctionCall2Coll(distanceFn, colloid, maxval, minval);

        distances[i].index = i;                   // Remember which gap this is
        distances[i].value = DatumGetFloat8(r);  // Store distance value
    }

    // Sort distances in descending order (largest gaps first)
    qsort(distances, ndistances, sizeof(DistanceValue), compare_distances);

    return distances;
}
```