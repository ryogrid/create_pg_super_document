# list_intersection

## Location
[src/backend/nodes/list.c:1174-1199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1174-L1199)

## Overview
Creates a new list containing the intersection of two pointer lists, preserving elements that exist in both input lists.

## Definition
```c
List *list_intersection(const List *list1, const List *list2)
```

## Detailed Description
This function performs a set intersection operation on two lists containing pointer values. It creates a new list that contains only the elements that appear in both input lists. The function iterates through the first list and includes elements in the result only if they are also present in the second list, using `equal()` for membership testing.

Important behavioral notes:
- If either input list is NIL, the function returns NIL immediately
- Duplicate entries in list1 are not suppressed in the result
- The function is only a true "intersection" if list1 contains unique elements
- The result contains pointers to the same objects as in the input lists (shallow copy)
- Time complexity is O(n*m) where n and m are the sizes of the input lists

## Parameters / Member Variables
- `list1`: The first input list of pointers (const List *)
- `list2`: The second input list of pointers (const List *)

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList (validation)
  - [list_member](list_member.md) (check membership using equal())
  - lfirst (extract pointer values)
  - [lappend](lappend.md) (append pointer values)
  - [check_list_invariants](../c/check_list_invariants.md) (validation)
- Called from (representative examples):
  - forfive (pg_list.h:650)

## Notes and Other Information
- Both input lists must contain only pointer values, enforced by assertions
- Returns NIL if either input list is NIL
- The function allocates a new list; callers are responsible for memory management
- Preserves duplicates from list1 if they exist in list2
- Performance warning: time complexity is O(n*m), consider other data structures for large lists
- Uses `equal()` function for element comparison rather than pointer equality