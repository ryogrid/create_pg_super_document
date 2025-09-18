# store_expanded_ranges

## Location
src/backend/access/brin/brin_minmax_multi.c: 1558 - 1600

## Overview
Converts processed expanded ranges back into the compact Ranges storage format, separating regular ranges from collapsed (single-point) ranges for efficient storage in BRIN minmax-multi indexes.

## Definition


## Detailed Description
This function performs the final step in range processing by converting an array of ExpandedRange structures back into the compact Ranges format used for persistent storage. The function separates the expanded ranges into two categories: regular ranges (requiring two boundary values each) and collapsed ranges (single-point ranges requiring one value each). Regular ranges are stored first with their min/max pairs, followed by the collapsed ranges stored as individual values.

The function ensures proper organization of the output data structure by setting appropriate counts for ranges and values, and marking all values as sorted since the expanded ranges have been processed and sorted during prior operations. This conversion is essential for maintaining the compact storage format required by the BRIN index structure.

## Parameters / Member Variables
- : Output structure to store the converted ranges
- : Input array of expanded ranges to convert
- : Number of expanded ranges in the input array

## Dependencies
- Functions called/Symbols referenced:
  - count_values (for assertion validation)
- Types referenced:
  - Ranges
  - ExpandedRange
- Called from:
  - ensure_free_space_in_buffer
  - compactify_ranges
  - brin_minmax_multi_union

## Notes and Other Information
- Modifies the ranges structure in-place to contain the converted data
- Regular ranges are stored before collapsed ranges in the values array
- All values are marked as sorted since processing maintains sort order
- Includes assertions to validate that the conversion preserves all boundary values
- The function is static and used internally within the BRIN minmax-multi implementation
- Essential for the final step of range compaction and storage in BRIN indexes
- Maintains the separation between multi-value ranges and single-point values for optimal storage efficiency