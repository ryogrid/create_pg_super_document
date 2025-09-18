# range_deduplicate_values

## Location
src/backend/access/brin/brin_minmax_multi.c: 516 - 575

## Overview
range_deduplicate_values is an optimization function that removes duplicate values from the unsorted portion of a Ranges structure, improving storage efficiency and query performance in BRIN minmax-multi indexes.

## Definition


## Detailed Description
This function performs in-place deduplication of values in the unsorted portion of a Ranges structure. It serves as a lightweight optimization strategy that improves storage efficiency without the computational overhead of more expensive range consolidation operations.

The function operates through a multi-step process:

1. **Early termination**: If all values are already sorted (nsorted == nvalues), the function returns immediately
2. **Sorting**: Sorts all values (both previously sorted and unsorted) using qsort_arg with the appropriate comparison context
3. **Deduplication**: Performs a single-pass deduplication by comparing consecutive values and compacting the array
4. **State update**: Updates the Ranges structure to reflect that all values are now sorted and deduplicated
5. **Validation**: Calls AssertCheckRanges to verify structural integrity

The function is designed to be more efficient than full range consolidation because it avoids calling potentially expensive distance functions and doesn't attempt to merge values into ranges. It assumes that values don't duplicate with existing ranges since this is checked before values are added.

## Parameters / Member Variables
- : Pointer to the Ranges structure to deduplicate

## Dependencies
- Functions called/Symbols referenced:
  - qsort_arg
  - compare_values
  - AssertCheckRanges
- Data structures referenced:
  - Ranges
  - compare_context
- Called from (representative examples):
  - brin_range_serialize
  - ensure_free_space_in_buffer

## Notes and Other Information
- Operates only on the values portion, leaving range boundaries untouched
- Uses in-place deduplication to minimize memory usage
- The function includes a comment about potential future optimization using merge sort to leverage pre-sorted portions
- Critical for maintaining storage efficiency in BRIN minmax-multi indexes
- Assumes values don't duplicate with existing ranges due to pre-insertion validation
- Located in src/backend/access/brin/brin_minmax_multi.c:516-575
- Updates both nvalues and nsorted to reflect the new state after deduplication