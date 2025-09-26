# list_union

## Location
[src/backend/nodes/list.c:1066-1089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1066-L1089)

## Overview
Creates a new list containing the union of two PostgreSQL pointer lists, adding elements from the second list that are not already present in the first list.

## Definition

```c
List *
list_union(const List *list1, const List *list2)
```
## Detailed Description
This function generates the union of two PostgreSQL Lists by first copying list1 and then adding all unique members from list2 that aren't already present in the copied list. The uniqueness check is performed using the equal() function for element comparison.

The function creates a completely new list structure, though the pointed-to objects themselves are not copied (shallow copy semantics). This means the resulting list shares the same data objects as the input lists but has its own list structure.

An important limitation is that the function does not remove duplicates that may already exist in list1 - it only ensures that elements from list2 are not duplicated. Therefore, it only performs a true "union" operation if list1 is known to be unique to begin with.

The performance complexity is O(n*m) where n and m are the lengths of the two lists, making it potentially expensive for large lists. The documentation suggests using other data structures if this becomes a performance bottleneck.

## Parameters / Member Variables
- : The first PostgreSQL List (must be a pointer list). This forms the base of the union.
- : The second PostgreSQL List (must be a pointer list). Elements from this list are added if not already present in list1.

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList: Validates that both input lists are pointer lists (called twice)
  - [list_copy](list_copy.md): Creates a copy of list1 as the starting point for the result
  - [list_member](list_member.md): Checks if an element from list2 already exists in the result list
  - [lappend](lappend.md): Adds unique elements from list2 to the result list
  - lfirst: Extracts the data pointer from list cells during iteration
  - [check_list_invariants](../c/check_list_invariants.md): Validates the final result list structure
- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md): Constraint management during table operations
  - [process_duplicate_ors](../p/process_duplicate_ors.md): Query optimization for OR clause processing

## Notes and Other Information
- Both input lists must be pointer lists (not integer or OID lists)
- Returns a newly-allocated list with shallow copy semantics
- Does not remove pre-existing duplicates in list1
- Performance warning: O(n*m) complexity - consider alternative data structures for large lists
- For the pattern "x = list_union(x, y)", consider using list_concat_unique() instead to avoid memory waste
- The function maintains list structure invariants through check_list_invariants()
- Uses equal() function for element comparison, allowing for proper object equality testing
- Commonly used in catalog operations and query optimization where set operations are needed
- The foreach() macro is used for efficient iteration over list2