# intset_is_member

## Location
src/backend/lib/integerset.c: 554 - 623

## Overview
A membership test function that efficiently determines whether a given 64-bit integer value exists in an IntegerSet, searching both buffered values and the compressed B-tree structure.

## Definition


## Detailed Description
The  function provides fast membership testing for the IntegerSet data structure. It implements a two-phase search strategy to handle the hybrid nature of the IntegerSet, which maintains both buffered uncompressed values and compressed values in a B-tree structure.

The function operates in the following phases:
1. **Buffer search**: First checks if the value might be in the buffer of recently added values using binary search
2. **B-tree traversal**: If not found in the buffer, traverses the B-tree from root to leaf, using binary search at each internal node to find the correct path
3. **Leaf search**: Performs binary search on the leaf node's items to locate the correct leaf_item
4. **Compressed search**: Uses Simple-8b decompression to check if the value exists within the compressed codeword

This approach ensures optimal performance by checking the most likely locations first (recent additions in buffer) before performing more expensive B-tree traversal and decompression operations.

## Parameters
- : Pointer to the IntegerSet structure to search
- : The 64-bit unsigned integer value to search for

## Dependencies
- Functions called/Symbols referenced:
  - intset_binsrch_uint64
  - intset_binsrch_leaf
  - simple8b_contains
  - intset_node
  - intset_leaf_node
  - intset_internal_node
  - leaf_item
- Called from (representative examples):
  - gistvacuum_delete_empty_pages
  - test_pattern
  - test_single_value
  - check_with_filler
  - test_empty
  - test_huge_distances

## Notes and Other Information
- Returns  if the value is found,  otherwise
- Optimized for common access patterns by checking buffered values first
- Handles edge cases gracefully, including empty sets and values outside the set's range
- Uses specialized binary search functions optimized for the specific data structures
- The function can handle values both in the uncompressed buffer and in the compressed B-tree nodes
- Particularly efficient for recently added values due to buffer-first search strategy