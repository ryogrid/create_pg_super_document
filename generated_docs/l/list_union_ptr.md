# list_union_ptr

## Location
[src/backend/nodes/list.c:1090-1112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1090-L1112)

## Overview
Creates a new list containing the union of two PostgreSQL pointer lists, using simple pointer comparison to determine duplicates rather than deep equality checking.

## Definition

```c
List *
list_union_ptr(const List *list1, const List *list2)
```
## Detailed Description
This function is a variant of list_union() that performs the same basic union operation but uses a different method for determining duplicate elements. Instead of using the equal() function for deep comparison, it uses simple pointer comparison via list_member_ptr().

The function operates by first copying list1 and then iterating through list2, adding elements that don't already exist in the result list based on pointer identity. This makes it more efficient than list_union() when you only care about pointer identity rather than deep object equality.

Like list_union(), this function creates a completely new list structure with shallow copy semantics - the pointed-to objects are not copied, but the list structure itself is newly allocated. It also has the same limitation that it doesn't remove pre-existing duplicates in list1.

The performance characteristics are similar to list_union() (O(n*m) complexity), but the individual comparison operations are faster since they only compare pointer values rather than calling equal() functions.

## Parameters / Member Variables
- : The first PostgreSQL List (must be a pointer list). This forms the base of the union.
- : The second PostgreSQL List (must be a pointer list). Elements from this list are added if their pointers are not already present in list1.

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList: Validates that both input lists are pointer lists (called twice)
  - list_copy: Creates a copy of list1 as the starting point for the result
  - list_member_ptr: Checks if a pointer from list2 already exists in the result list using pointer comparison
  - lappend: Adds unique elements from list2 to the result list
  - lfirst: Extracts the data pointer from list cells during iteration
  - check_list_invariants: Validates the final result list structure
- Called from (representative examples):
  - Limited usage found in codebase - primarily available through foreach macros

## Notes and Other Information
- Both input lists must be pointer lists (not integer or OID lists)
- Uses pointer identity comparison instead of deep equality (equal() function)
- More efficient than list_union() for pointer-based duplicate detection
- Returns a newly-allocated list with shallow copy semantics
- Does not remove pre-existing duplicates in list1
- Performance: O(n*m) complexity but with faster individual comparisons
- Maintains list structure invariants through check_list_invariants()
- Useful when working with lists of objects where pointer identity is sufficient for uniqueness
- The same memory management and performance considerations as list_union() apply
- Less commonly used than list_union() - specialized for pointer identity use cases