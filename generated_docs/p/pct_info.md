# pct_info

## Location
src/backend/utils/adt/orderedsetaggs.c: 634 - 645

## Overview
pct_info is a structure used in PostgreSQL's ordered-set aggregates to handle arrays of percentiles, particularly for multi-percentile functions like percentile_disc and percentile_cont when processing multiple percentile values simultaneously.

## Definition


## Detailed Description
The pct_info structure is a support data structure designed specifically for handling arrays of percentiles efficiently. It is used when PostgreSQL needs to compute multiple percentile values from the same sorted dataset, allowing for optimized processing by organizing the required sampling information.

Each pct_info entry represents one percentile calculation within a multi-percentile operation. The structure stores the necessary information to extract the correct value(s) from the sorted data, including which row(s) to sample and any interpolation needed. This approach allows PostgreSQL to process all percentiles in a single pass through the sorted data rather than requiring separate passes for each percentile value.

## Parameters / Member Variables
- : The primary row number to sample from the sorted dataset (0-based indexing)
- : The secondary row number for interpolation, typically equal to first_row or first_row + 1
- : The interpolation fraction used when the percentile falls between two discrete values (0.0 to 1.0)
- : The index position of this percentile in the original input array, used for maintaining correspondence with the output array

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references - this is a data structure)
- Called from (representative examples):
  - [pct_info_cmp](pct_info_cmp.md) (comparison function for sorting pct_info arrays)
  - [setup_pct_info](../s/setup_pct_info.md) (function that populates pct_info arrays)
  - [percentile_disc_multi_final](percentile_disc_multi_final.md) (uses pct_info for discrete percentiles)
  - [percentile_cont_multi_final_common](percentile_cont_multi_final_common.md) (uses pct_info for continuous percentiles)

## Notes and Other Information
- The constraint that second_row should be equal to or exactly one more than first_row ensures efficient interpolation logic
- The structure is typically used in arrays that are sorted by row numbers to enable efficient sequential processing of the sorted dataset
- For discrete percentiles (percentile_disc), the proportion is typically 0.0 since no interpolation is needed
- For continuous percentiles (percentile_cont), the proportion determines how to interpolate between first_row and second_row values
- The idx field is crucial for maintaining the correct output order when multiple percentiles are requested, as the pct_info array may be sorted by row numbers rather than original order