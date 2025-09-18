# intset_binsrch_leaf

## Location
[src/backend/lib/integerset.c:747-820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L747-L820)

## Overview
A specialized binary search function for arrays of leaf_item structures, used to efficiently locate compressed integer sequences within IntegerSet's B-tree leaf nodes.

## Definition


## Detailed Description
This function is a variant of the standard binary search algorithm, specifically designed to work with arrays of  structures. Each  contains a compressed sequence of integers, and this function searches by comparing against the  field of each item, which represents the first (smallest) integer in that compressed sequence.

The function is nearly identical to  but operates on  structures instead of raw uint64 values. It uses the same two-mode behavior controlled by the  parameter:

1. When  is false: Returns the position of an item whose  value equals the search key, or the insertion point
2. When  is true: Returns the position immediately after an item whose  value equals the search key, or the insertion point

This enables efficient navigation through B-tree leaf nodes to find the appropriate compressed sequence that might contain a target integer.

## Parameters / Member Variables
- : The uint64 value to search for
- : Pointer to the sorted array of leaf_item structures to search in
- : Number of leaf_item elements in the array
- : Boolean flag controlling behavior when equal keys are found

## Dependencies
- Functions called/Symbols referenced:
  - : Structure type representing compressed integer sequences in leaf nodes
- Called from (representative examples):
  -  operations: Used internally during set operations on B-tree leaf nodes
  - : Used to locate the appropriate leaf item when checking membership

## Notes and Other Information
- This is a static function, only accessible within the integerset.c file
- Searches based on the  field of leaf_item structures, not the entire compressed sequence
- Uses overflow-safe midpoint calculation like 
- Essential for efficient navigation through compressed integer sequences in B-tree leaf nodes
- Time complexity is O(log n) where n is the number of leaf items in the array
- The returned position can be used to identify which compressed sequence might contain the target value