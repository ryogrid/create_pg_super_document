# list_copy

## Location
[src/backend/nodes/list.c:1573-1592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1573-L1592)

## Overview
Creates a shallow copy of a PostgreSQL list structure, duplicating only the list container and its element pointers but not the actual data elements themselves.

## Definition

```c
List *
list_copy(const List *oldlist)
```
## Detailed Description
The  function creates a shallow copy of a PostgreSQL List structure. It allocates a new List with the same type and length as the original, then copies all element pointers using . This is a shallow copy operation, meaning only the list structure and pointers are duplicated - the actual data elements pointed to by the list cells remain the same objects in memory.

The function handles the special case where the input list is  (null) by returning  directly without allocation. After copying, it validates the new list structure using  to ensure consistency.

This function is widely used throughout PostgreSQL for creating working copies of lists that can be modified without affecting the original list structure, while still sharing the underlying data elements.

## Parameters / Member Variables
- : The source List to be copied. Can be NIL (null), in which case NIL is returned.

## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md)
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [list_concat](list_concat.md)
  - [list_concat_copy](list_concat_copy.md)  
  - list_union
  - [list_difference](list_difference.md)
  - [copyObjectImpl](../c/copyObjectImpl.md)
  - [get_foreign_key_join_selectivity](../g/get_foreign_key_join_selectivity.md)
  - [preprocess_groupclause](../p/preprocess_groupclause.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)

## Notes and Other Information
- This is a shallow copy operation - only the list structure is duplicated, not the data elements
- The function is safe to call with NIL input
- The copied list maintains the same type (T_List, T_IntList, T_OidList) as the original
- Memory allocation for the new list is handled by the  function
- Used extensively throughout the query planner, parser, and various PostgreSQL subsystems for creating working copies of lists