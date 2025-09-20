# list_copy_deep

## Location
[src/backend/nodes/list.c:1639-1673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1639-L1673)

## Overview
Creates a deep copy of a PostgreSQL list structure, recursively copying both the list container and all data elements it contains.

## Definition

```c
typedef int (*qsort_comparator) (const void *a, const void *b);
```
## Detailed Description
The  function creates a deep copy of a PostgreSQL List structure, meaning it copies not only the list container and element pointers, but also recursively copies all the actual data objects pointed to by the list elements. This is accomplished using  for each element, which provides PostgreSQL's standard deep-copy semantics for node structures.

The function includes an assertion to ensure it's only called on pointer Lists (T_List), as deep copying only makes sense for lists containing pointers to objects. The deep copy operation is significantly more expensive than shallow copying but ensures complete independence between the original and copied structures.

The comment notes that this function's concept of "deep" copying is more thorough than what  considers deep, highlighting the distinction between different levels of copying operations in PostgreSQL.

## Parameters / Member Variables
- : The source List to be deep copied. Must be a T_List type (pointer list). Can be NIL, in which case NIL is returned.

## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md)
  - [copyObjectImpl](../c/copyObjectImpl.md)
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [copyObjectImpl](../c/copyObjectImpl.md) (recursive copying within the copy framework)

## Notes and Other Information
- This is a deep copy operation - both the list structure and all contained data objects are recursively duplicated
- Only works with T_List (pointer lists) - includes an assertion to enforce this requirement
- Uses  for deep copying of individual elements, ensuring proper handling of complex PostgreSQL node structures
- Much more expensive than shallow copy operations due to recursive copying
- The copied list and all its elements are completely independent from the original
- Safe to call with NIL input
- Primarily used within PostgreSQL's general object copying framework
- The "deep" semantics here are deeper than those used by 