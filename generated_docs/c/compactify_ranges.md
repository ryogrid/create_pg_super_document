# compactify_ranges

## Location
src/backend/access/brin/brin_minmax_multi.c: 1788 - 1858

## Overview
This function generates a compact range representation from data collected during "batch mode" operations, handling unsorted values and performing range combination when necessary.

## Definition


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
  - minmax_multi_get_strategy_procinfo
  - minmax_multi_get_procinfo
  - AllocSetContextCreate
  - build_expanded_ranges
  - build_distances
  - reduce_expanded_ranges
  - count_values
  - store_expanded_ranges
  - AssertCheckRanges
  - MemoryContextDelete
- Called from:
  - brin_minmax_multi_serialize

## Notes and Other Information
- Unlike other compaction functions, this does not use a load factor buffer since it's used during serialization
- Handles both sorted and unsorted value collections, making it suitable for batch processing scenarios
- Uses temporary memory contexts to prevent memory leaks during potentially expensive distance function calls
- Early termination optimization avoids unnecessary work when compaction is not needed
- Critical for preparing BRIN minmax-multi data for persistent storage by ensuring it fits within space constraints
- The function modifies the ranges structure in-place rather than returning a new structure
- Includes comprehensive assertions to validate the final compacted ranges maintain all invariants