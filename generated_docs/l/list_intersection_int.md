# list_intersection_int

## Location
src/backend/nodes/list.c: 1200 - 1236

## Overview
Creates a new list containing the intersection of two integer lists, preserving elements that exist in both input lists.

## Definition
```c
List *list_intersection_int(const List *list1, const List *list2)
```

## Detailed Description
This function performs a set intersection operation on two lists containing integer values. It creates a new list that contains only the integer elements that appear in both input lists. The function iterates through the first list and includes elements in the result only if they are also present in the second list, using `list_member_int()` for membership testing.

Key behavioral characteristics:
- If either input list is NIL, the function returns NIL immediately
- Duplicate entries in list1 are not suppressed in the result
- The function preserves the order of elements as they appear in list1
- Time complexity is O(n*m) where n and m are the sizes of the input lists

## Parameters / Member Variables
- `list1`: The first input list of integers (const List *)
- `list2`: The second input list of integers (const List *)

## Dependencies
- Functions called/Symbols referenced:
  - IsIntegerList (validation)
  - list_member_int (check integer membership)
  - lfirst_int (extract integer values)
  - lappend_int (append integer values)
  - check_list_invariants (validation)
- Called from (representative examples):
  - parseCheckAggregates (parse_agg.c:1124)
  - forfive (pg_list.h:651)

## Notes and Other Information
- Both input lists must contain only integer values, enforced by assertions
- Returns NIL if either input list is NIL
- The function allocates a new list; callers are responsible for memory management
- Preserves duplicates from list1 if they exist in list2
- Specialized variant of `list_intersection` optimized for integer comparison
- More efficient than generic `list_intersection` for integer lists due to direct integer comparison