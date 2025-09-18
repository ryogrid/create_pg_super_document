# list_difference

## Location
src/backend/nodes/list.c: 1237 - 1262

## Overview
Creates a new list containing elements from the first list that are not present in the second list (set difference operation).

## Definition
```c
List *list_difference(const List *list1, const List *list2)
```

## Detailed Description
This function performs a set difference operation on two lists containing pointer values. It creates a new list that contains only the elements from list1 that do not appear in list2. The function iterates through the first list and includes elements in the result only if they are not present in the second list, using `equal()` for membership testing.

Key behavioral characteristics:
- If list2 is NIL, the function returns a copy of list1
- The result contains pointers to the same objects as in list1 (shallow copy)
- Maintains the order of elements as they appear in list1
- Time complexity is O(n*m) where n and m are the sizes of the input lists

## Parameters / Member Variables
- `list1`: The source list of pointers (const List *)
- `list2`: The list of pointers to exclude (const List *)

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList (validation)
  - [list_copy](list_copy.md) (copy entire list when list2 is NIL)
  - [list_member](list_member.md) (check membership using equal())
  - lfirst (extract pointer values)
  - lappend (append pointer values)
  - [check_list_invariants](../c/check_list_invariants.md) (validation)
- Called from (representative examples):
  - [get_useful_group_keys_orderings](../g/get_useful_group_keys_orderings.md) (pathkeys.c:534)
  - [create_tidscan_plan](../c/create_tidscan_plan.md) (createplan.c:3609)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md) (createplan.c:4502)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md) (createplan.c:4804)
  - [process_duplicate_ors](../p/process_duplicate_ors.md) (prepqual.c:628)
  - [infer_arbiter_indexes](../i/infer_arbiter_indexes.md) (plancat.c:920)

## Notes and Other Information
- Both input lists must contain only pointer values, enforced by assertions
- Returns a copy of list1 if list2 is NIL (optimization)
- The function allocates a new list; callers are responsible for memory management
- Performance warning: time complexity is O(n*m), consider other data structures for large lists
- Uses `equal()` function for element comparison rather than pointer equality
- Widely used in PostgreSQL's query optimizer for plan generation and optimization