# mcelem_array_contain_overlap_selec

## Location
src/backend/utils/adt/array_selfuncs.c: 521 - 695

## Overview
Estimates selectivity for array containment (@>) and overlap (&&) operators based on most common element statistics, assuming independent element occurrences.

## Definition


## Detailed Description
This function calculates selectivity estimates for array containment and overlap operations by analyzing the most common elements (MCELEM) statistics from the array column. It processes each element in the constant array and computes the probability that rows will satisfy the given operator condition.

For containment (@>), it starts with selectivity 1.0 and multiplies by each element's selectivity (intersection probability). For overlap (&&), it starts with selectivity 0.0 and uses the union probability formula: selec = selec + elem_selec - selec * elem_selec.

The function uses either binary search or linear scan to find matching elements in the MCELEM array, depending on which approach is more efficient based on the array sizes.

## Parameters / Member Variables
- : Array of most common element values from statistics (presorted)
- : Number of elements in mcelem array
- : Array of frequency values corresponding to mcelem elements
- : Number of elements in numbers array (should be nmcelem + 3)
- : Elements from the constant array being compared (presorted)
- : Number of elements in array_data
- : The array operator being used (OID_ARRAY_CONTAINS_OP for @>, others for &&)
- : Type cache entry for element comparison functions

## Dependencies
- Functions called/Symbols referenced:
  - [floor_log2](../f/floor_log2.md)
  - [element_compare](../e/element_compare.md)
  - [find_next_mcelem](../f/find_next_mcelem.md)
  - DEFAULT_CONTAIN_SEL
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [scalararraysel_containment](../s/scalararraysel_containment.md)
  - [mcelem_array_selec](mcelem_array_selec.md)

## Notes and Other Information
- Assumes element occurrences are independent, which may not always be accurate in practice
- Uses binary search optimization when nitems * floor_log2(nmcelem) < nmcelem + nitems
- Falls back to default selectivity estimates when elements are not found in MCELEM statistics
- TODO comment suggests potential improvement by using distinct elements count histogram
- The function expects numbers array to have exactly nmcelem + 3 elements (frequencies + min/max/null stats)