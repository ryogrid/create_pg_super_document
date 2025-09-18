# count_values

## Location
src/backend/access/brin/brin_minmax_multi.c: 1415 - 1475

## Overview
Calculates the total number of boundary values required to store an array of expanded ranges in a BRIN minmax-multi index structure.

## Definition


## Detailed Description
This utility function determines how many individual values are needed to represent an array of expanded ranges in their stored form. The calculation depends on whether each range is collapsed (single-point) or represents an actual range. Collapsed ranges require only one boundary value to store, while regular ranges need two values (minimum and maximum). This count is essential for memory allocation and storage planning when persisting range data in BRIN index pages.

The function performs a simple linear scan through the expanded ranges array, examining the collapsed flag of each range to determine the appropriate count contribution.

## Parameters / Member Variables
- : Array of expanded ranges to count values for
- : Number of ranges in the array

## Dependencies
- Functions called/Symbols referenced:
  - (none - uses only basic operations)
- Types referenced:
  - [ExpandedRange](../E/ExpandedRange.md)
- Called from:
  - [store_expanded_ranges](../s/store_expanded_ranges.md)
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [compactify_ranges](compactify_ranges.md)

## Notes and Other Information
- Returns the total count of boundary values needed for storage
- Collapsed ranges contribute 1 value each, regular ranges contribute 2 values each
- The function is static and used internally within the BRIN minmax-multi implementation
- Essential for determining storage requirements before serializing range data
- Simple O(n) algorithm that examines each range's collapsed property
- Used in memory allocation decisions and storage space calculations