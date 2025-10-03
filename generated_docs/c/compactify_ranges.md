# compactify_ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:1788-1858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1788-L1858)

## Overview
This function generates a compact range representation from data collected during "batch mode" operations, handling unsorted values and performing range combination when necessary.

## Definition

```c
static void
compactify_ranges(BrinDesc *bdesc, Ranges *ranges, int max_values)
```
## Detailed Description
The function serves as a compaction mechanism for BRIN minmax-multi ranges during serialization phase. Unlike reduce_expanded_ranges, it cannot assume that values are pre-sorted and must handle potential duplicate values. 

The function first determines if compaction is actually needed by checking two conditions: whether the total storage requirement (2*nranges + nvalues) exceeds max_values, or whether there are unsorted values present. If neither condition is met, it returns early without modifications.

When compaction is required, it follows a similar pattern to ensure_free_space_in_buffer:
1. Creates a temporary memory context for distance calculations
2. Builds expanded range representation from current ranges and values
3. Calculates distances between adjacent ranges
4. Reduces the number of ranges until storage requirements are under max_values
5. Converts back to standard range representation

Unlike ensure_free_space_in_buffer, this function doesn't apply a load factor since it's used during serialization when no immediate further insertions are expected.

## Parameters / Member Variables
- : BRIN descriptor containing index metadata and operator procedures
- : Ranges structure to be compactified in-place
- : Maximum number of values allowed in the compacted representation

## Dependencies
- Functions called/Symbols referenced:
  - [minmax_multi_get_strategy_procinfo](../m/minmax_multi_get_strategy_procinfo.md)
  - [minmax_multi_get_procinfo](../m/minmax_multi_get_procinfo.md)
  - AllocSetContextCreate
  - [build_expanded_ranges](../b/build_expanded_ranges.md)
  - [build_distances](../b/build_distances.md)
  - [reduce_expanded_ranges](../r/reduce_expanded_ranges.md)
  - [count_values](count_values.md)
  - [store_expanded_ranges](../s/store_expanded_ranges.md)
  - [AssertCheckRanges](../A/AssertCheckRanges.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from:
  - [brin_minmax_multi_serialize](../b/brin_minmax_multi_serialize.md)

## Notes and Other Information
- Unlike other compaction functions, this does not use a load factor buffer since it's used during serialization
- Handles both sorted and unsorted value collections, making it suitable for batch processing scenarios
- Uses temporary memory contexts to prevent memory leaks during potentially expensive distance function calls
- Early termination optimization avoids unnecessary work when compaction is not needed
- Critical for preparing BRIN minmax-multi data for persistent storage by ensuring it fits within space constraints
- The function modifies the ranges structure in-place rather than returning a new structure
- Includes comprehensive assertions to validate the final compacted ranges maintain all invariants

## Simplified Source

```c
static void
compactify_ranges(BrinDesc *bdesc, Ranges *ranges, int max_values)
{
    // Early exit if compaction not needed
    if ((ranges->nranges * 2 + ranges->nvalues <= max_values) &&
        (ranges->nsorted == ranges->nvalues))
        return;

    // Get required functions for compaction
    FmgrInfo *cmpFn = minmax_multi_get_strategy_procinfo(bdesc, ranges->attno,
                                                        ranges->typid,
                                                        BTLessStrategyNumber);
    FmgrInfo *distanceFn = minmax_multi_get_procinfo(bdesc, ranges->attno,
                                                    PROCNUM_DISTANCE);

    // Create temporary memory context for distance calculations
    MemoryContext ctx = AllocSetContextCreate(CurrentMemoryContext,
                                            "minmax-multi context",
                                            ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldctx = MemoryContextSwitchTo(ctx);

    // Build expanded ranges from current data
    ExpandedRange *eranges;
    int neranges;
    eranges = build_expanded_ranges(cmpFn, ranges->colloid, ranges, &neranges);

    // Calculate distances between adjacent ranges
    DistanceValue *distances = build_distances(distanceFn, ranges->colloid,
                                              eranges, neranges);

    // Combine ranges until under max_values limit
    neranges = reduce_expanded_ranges(eranges, neranges, distances,
                                    max_values, cmpFn, ranges->colloid);

    // Convert back to standard range format
    store_expanded_ranges(ranges, eranges, neranges);

    // Clean up temporary memory
    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(ctx);
}
```