# brin_minmax_multi_union

## Location
src/backend/access/brin/brin_minmax_multi.c: 2735 - 2862

## Overview
Union operation for BRIN minmax-multi operator class that merges two BrinValues summary structures into a single combined summary.

## Definition


## Detailed Description
This function implements the union operation for BRIN minmax-multi operator class, which is used during index maintenance operations like VACUUM to combine multiple summary values. The function takes two BrinValues containing serialized range summaries and merges them into a single consolidated summary in the first BrinValues structure.

The union process involves several steps:
1. Deserialize both input range summaries into internal Ranges structures
2. Expand all ranges into a unified ExpandedRange array
3. Sort and merge overlapping ranges
4. Apply range reduction if needed to stay within storage limits
5. Serialize the result back into the first BrinValues structure

The function uses a temporary memory context to manage allocations during the potentially memory-intensive distance calculations and range operations.

## Parameters / Member Variables
- : BrinDesc pointer - BRIN index descriptor containing metadata
- : BrinValues pointer - First summary value (updated with union result)
- : BrinValues pointer - Second summary value (left unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - brin_range_deserialize
  - fill_expanded_ranges
  - sort_expanded_ranges  
  - merge_overlapping_ranges
  - minmax_multi_get_strategy_procinfo
  - minmax_multi_get_procinfo
  - build_distances
  - reduce_expanded_ranges
  - store_expanded_ranges
  - brin_range_serialize
- Called from (representative examples):
  - No direct references found (likely called through function manager)

## Notes and Other Information
- Both input BrinValues must have the same attribute number and neither can be all-nulls
- The function modifies only the first BrinValues parameter (col_a)
- Memory management is carefully handled using a temporary context to avoid leaks during distance calculations
- The union operation maintains the maxvalues limit from the first range summary
- Range reduction is applied to ensure the result fits within storage constraints