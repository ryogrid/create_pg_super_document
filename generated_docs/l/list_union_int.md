# list_union_int

## Location
src/backend/nodes/list.c: 1113 - 1135

## Overview
Creates a new list containing the union of two integer lists, eliminating duplicate values.

## Definition


## Detailed Description
This function performs a set union operation on two lists containing integer values. It creates a new list that contains all unique integer values from both input lists. The function starts by copying the first list, then iterates through the second list and appends any integers that are not already present in the result. This ensures no duplicate values exist in the final union.

The function includes assertions to verify that both input lists contain only integer values using . After construction, it validates the result using  to ensure list consistency.

## Parameters / Member Variables
- : The first input list of integers (const List *)
- : The second input list of integers (const List *)

## Dependencies
- Functions called/Symbols referenced:
  - IsIntegerList (validation)
  - list_copy (copy first list)
  - list_member_int (check membership)
  - lfirst_int (extract integer values)
  - lappend_int (append integer values)
  - check_list_invariants (validation)
- Called from (representative examples):
  - expand_grouping_sets (parse_agg.c:1838, 1855)

## Notes and Other Information
- Both input lists must contain only integer values, enforced by assertions
- The function allocates a new list; callers are responsible for memory management
- Order of elements follows list1 first, then unique elements from list2
- Time complexity is O(n*m) where n and m are the sizes of the input lists due to membership testing