# purge_from_verification_array

## Location
[src/test/modules/test_tidstore/test_tidstore.c:157-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L157-L169)

## Overview
A static utility function that removes all ItemPointers belonging to a specific block number from the verification array used in tidstore testing.

## Definition

```c
static void
purge_from_verification_array(BlockNumber blkno)
```
## Detailed Description
This function performs an in-place removal of ItemPointers that belong to a specified block number from the insert_tids verification array. It uses a two-pointer technique where:

1.  iterates through all existing ItemPointers in the array
2.  tracks the position where the next valid (non-matching) ItemPointer should be placed
3. Only ItemPointers that do NOT match the specified block number are copied to the destination position

This approach efficiently compacts the array by removing all TIDs from the specified block while preserving the order of remaining elements. The function updates  to reflect the new array size after purging.

The operation is typically used when testing block-level operations on the tidstore, where certain blocks need to be excluded from verification arrays to ensure test accuracy.

## Parameters / Member Variables
- : The block number whose associated ItemPointers should be removed from the verification array

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md) (to extract block number from ItemPointers)
- Called from (representative examples):
  - [do_set_block_offsets](../d/do_set_block_offsets.md)

## Notes and Other Information
- This is a static helper function used internally within the test_tidstore module
- Operates on the global  array and  counter
- Uses efficient in-place array compaction with O(n) time complexity
- Essential for maintaining accurate verification arrays during block-specific tidstore operations
- The function preserves the relative order of remaining ItemPointers
- No memory reallocation is performed; the array capacity remains unchanged
- Used in testing scenarios where entire blocks are cleared or modified in the tidstore