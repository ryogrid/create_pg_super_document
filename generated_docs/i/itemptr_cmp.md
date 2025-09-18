# itemptr_cmp

## Location
src/test/modules/test_tidstore/test_tidstore.c: 53 - 85

## Overview
A static comparison function used for sorting ItemPointer structures (TIDs) in ascending order based on block number first, then offset number within the block.

## Definition


## Detailed Description
This function implements a comparator routine for ItemPointer structures, which are used in PostgreSQL to identify the physical location of tuples within heap files. The function performs lexicographic comparison where block numbers are compared first, and if they are equal, offset numbers are compared. This ordering ensures that ItemPointers are sorted in the physical order they appear on disk, which is important for efficient storage and retrieval operations in the tidstore test module.

The function follows the standard C library qsort comparator interface, returning:
- Negative value if left < right
- Zero if left == right  
- Positive value if left > right

## Parameters / Member Variables
- `left`: Pointer to the first ItemPointer to compare
- `right`: Pointer to the second ItemPointer to compare

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [check_set_block_offsets](../c/check_set_block_offsets.md) (used with qsort for sorting ItemPointer arrays)

## Notes and Other Information
- This is a static function within the test_tidstore module, used specifically for testing tidstore functionality
- The comparison logic ensures consistent ordering of TIDs across different operations
- Used primarily with qsort() to sort arrays of ItemPointers for verification and testing purposes
- The function handles the two-level hierarchy of PostgreSQL's tuple identification system (block + offset)