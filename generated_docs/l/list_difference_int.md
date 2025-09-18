# list_difference_int

## Location
src/backend/nodes/list.c: 1288 - 1312

## Overview
Returns a new list containing integer elements from the first list that are not present in the second list, using integer value equality for membership comparison.

## Definition
```c
List *list_difference_int(const List *list1, const List *list2)
```

## Detailed Description
This function creates a new list containing all integer elements from `list1` that are not found in `list2`. It is a specialized variant of `list_difference()` that operates specifically on lists of integers, using integer value comparison to determine list membership.

The function iterates through each integer element in `list1` and checks if that integer value exists in `list2`. If not found, the integer is appended to the result list. The original lists remain unchanged. This variant is optimized for integer lists and uses the appropriate integer-specific list operations.

## Parameters / Member Variables
- `list1`: The source list of integers from which elements will be selected
- `list2`: The list of integers containing elements to be excluded from the result (can be NIL)

## Dependencies
- Functions called/Symbols referenced:
  - `IsIntegerList` - Asserts that both input lists are integer lists
  - `list_copy` - Creates a copy of list1 when list2 is NIL
  - `list_member_int` - Checks integer membership in list2
  - `lfirst_int` - Extracts integer values from list cells
  - `lappend_int` - Appends integer elements to the result list
  - `check_list_invariants` - Validates the final result list
- Called from (representative examples):
  - `reorder_grouping_sets` (src/backend/optimizer/plan/planner.c:3201)

## Notes and Other Information
- Both input lists must be integer lists (verified by assertions)
- Returns a copy of list1 if list2 is NIL (empty)
- Uses integer value equality for membership testing
- The result list maintains the original order of elements from list1
- Uses integer-specific list functions for better performance with integer data
- Memory for the result list is newly allocated and should be freed when no longer needed