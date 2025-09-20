# intset_binsrch_leaf

## Location
[src/backend/lib/integerset.c:747-820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L747-L820)

## Overview
A specialized binary search function for arrays of leaf_item structures, used to efficiently locate compressed integer sequences within IntegerSet's B-tree leaf nodes.

## Definition

```c
struct simple8b_mode
{
	uint8		bits_per_int;
	uint8		num_ints;
}			simple8b_modes[17] =

{
	{0, 240},					/* mode  0: 240 zeroes */
	{0, 120},					/* mode  1: 120 zeroes */
	{1, 60},					/* mode  2: sixty 1-bit integers */
	{2, 30},					/* mode  3: thirty 2-bit integers */
	{3, 20},					/* mode  4: twenty 3-bit integers */
	{4, 15},					/* mode  5: fifteen 4-bit integers */
	{5, 12},					/* mode  6: twelve 5-bit integers */
	{6, 10},					/* mode  7: ten 6-bit integers */
	{7, 8},						/* mode  8: eight 7-bit integers (four bits
								 * are wasted) */
	{8, 7},						/* mode  9: seven 8-bit integers (four bits
								 * are wasted) */
	{10, 6},					/* mode 10: six 10-bit integers */
	{12, 5},					/* mode 11: five 12-bit integers */
	{15, 4},					/* mode 12: four 15-bit integers */
	{20, 3},					/* mode 13: three 20-bit integers */
	{30, 2},					/* mode 14: two 30-bit integers */
	{60, 1},					/* mode 15: one 60-bit integer */

	{0, 0}						/* sentinel value */
};
```
## Detailed Description
This function is a variant of the standard binary search algorithm, specifically designed to work with arrays of  structures. Each  contains a compressed sequence of integers, and this function searches by comparing against the  field of each item, which represents the first (smallest) integer in that compressed sequence.

The function is nearly identical to  but operates on  structures instead of raw uint64 values. It uses the same two-mode behavior controlled by the  parameter:

1. When  is false: Returns the position of an item whose  value equals the search key, or the insertion point
2. When  is true: Returns the position immediately after an item whose  value equals the search key, or the insertion point

This enables efficient navigation through B-tree leaf nodes to find the appropriate compressed sequence that might contain a target integer.

## Parameters
- `item`: The uint64 value to search for
- `arr`: Pointer to the sorted array of leaf_item structures to search in
- `count`: Number of leaf_item elements in the array
- `nextkey`: Boolean flag controlling behavior when equal keys are found

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