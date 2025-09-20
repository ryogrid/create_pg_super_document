# list_difference_ptr

## Location
[src/backend/nodes/list.c:1263-1287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1263-L1287)

## Overview
Returns a new list containing elements from the first list that are not present in the second list, using simple pointer equality for membership comparison.

## Definition

```c
List *
list_difference_ptr(const List *list1, const List *list2)
```
## Detailed Description
This function creates a new list containing all elements from  that are not found in . It is a specialized variant of  that uses simple pointer equality (==) rather than deep comparison to determine list membership. This makes it more efficient when working with lists of pointers where identity matters more than content equality.

The function iterates through each element in  and checks if that exact pointer value exists in . If not found, the element is appended to the result list. The original lists remain unchanged.

## Parameters / Member Variables
- : The source list from which elements will be selected
- : The list containing elements to be excluded from the result (can be NIL)

## Dependencies
- Functions called/Symbols referenced:
  -  - Asserts that both input lists are pointer lists
  -  - Creates a copy of list1 when list2 is NIL
  -  - Checks pointer membership in list2
  -  - Appends elements to the result list
  -  - Validates the final result list
- Called from (representative examples):
  -  (src/backend/commands/tablecmds.c:2288)
  -  (src/backend/optimizer/path/pathkeys.c:535)
  -  (src/backend/optimizer/plan/createplan.c:3284)

## Notes and Other Information
- Both input lists must be pointer lists (verified by assertions)
- Returns a copy of list1 if list2 is NIL (empty)
- Uses pointer equality (==) for membership testing, not content comparison
- The result list maintains the original order of elements from list1
- Memory for the result list is newly allocated and should be freed when no longer needed