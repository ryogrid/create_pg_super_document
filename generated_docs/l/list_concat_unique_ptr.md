# list_concat_unique_ptr

## Location
[src/backend/nodes/list.c:1427-1447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1427-L1447)

## Overview
Concatenates two pointer lists by appending unique elements from the second list to the first, using simple pointer equality for membership testing instead of equal() function.

## Definition
```c
List *list_concat_unique_ptr(List *list1, const List *list2)
```

## Detailed Description
This function is a specialized variant of list_concat_unique() that uses simple pointer equality rather than the equal() function to determine list membership. This makes it significantly faster when working with lists where pointer identity is sufficient for uniqueness testing, such as when dealing with unique object references or when the same data structure instances should not be duplicated.

The function modifies list1 in-place by appending each element from list2 that is not already present based on pointer comparison. This approach is more efficient than value-based equality testing and is particularly useful in scenarios where the same pointer values represent identical entities, such as in query optimization where the same plan nodes or path structures are referenced.

## Parameters / Member Variables
- `list1`: The target List structure that will be modified in-place by appending unique pointer elements
- `list2`: The source List structure whose elements will be checked and potentially appended based on pointer equality

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList
  - [list_member_ptr](list_member_ptr.md)
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [group_keys_reorder_by_pathkeys](../g/group_keys_reorder_by_pathkeys.md)
  - forfive

## Notes and Other Information
- Uses pointer equality (==) instead of equal() function for membership testing
- More efficient than list_concat_unique() when pointer identity is sufficient
- Modifies list1 in-place rather than creating a new list
- Both input lists must be pointer lists (verified by assertions)
- Maintains list invariants through check_list_invariants()
- Commonly used in query optimization where the same object instances need to be handled uniquely
- Preserves the relative order of non-duplicate elements from list2
- Particularly useful when working with plan nodes, expressions, or other structures where pointer identity matters

## Simplified Source

```c
List *list_concat_unique_ptr(List *list1, const List *list2)
{
    ListCell *cell;

    Assert(IsPointerList(list1));
    Assert(IsPointerList(list2));

    // Add each element from list2 that's not already in list1 (by pointer equality)
    foreach(cell, list2) {
        if (!list_member_ptr(list1, lfirst(cell)))
            list1 = lappend(list1, lfirst(cell));
    }

    check_list_invariants(list1);
    return list1;
}
```