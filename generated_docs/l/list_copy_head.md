# list_copy_head

## Location
[src/backend/nodes/list.c:1593-1612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1593-L1612)

## Overview
Creates a shallow copy of a PostgreSQL list containing only the first 'len' elements of the original list.

## Definition

```c
List *
list_copy_head(const List *oldlist, int len)
```
## Detailed Description
The  function creates a shallow copy of the first portion of a PostgreSQL List structure. It copies only the first 'len' elements from the source list, or the entire list if it contains fewer than 'len' elements. Like , this performs a shallow copy operation where only the list structure and element pointers are duplicated.

The function includes several safety checks: it returns NIL if the input list is NIL or if len is zero or negative. It uses  to ensure that the requested length doesn't exceed the actual list length, preventing buffer overruns.

This function is particularly useful in query optimization and planning where only a subset of a list (typically the most significant elements) is needed for processing.

## Parameters / Member Variables
- `*oldlist`: The source List from which to copy the head elements. Can be NIL.
- `len`: The maximum number of elements to copy from the beginning of the list. If negative or zero, NIL is returned.
## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md)
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [get_object_address_relobject](../g/get_object_address_relobject.md)
  - [expand_indexqual_rowcompare](../e/expand_indexqual_rowcompare.md)
  - [group_keys_reorder_by_pathkeys](../g/group_keys_reorder_by_pathkeys.md)
  - [truncate_useless_pathkeys](../t/truncate_useless_pathkeys.md)
  - [create_append_plan](../c/create_append_plan.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)

## Notes and Other Information
- This is a shallow copy operation - only the list structure is duplicated, not the data elements
- Safe to call with NIL input or non-positive len values
- Uses  macro to prevent copying beyond the list boundary  
- Commonly used in query planning to extract the most relevant portion of pathkey lists
- The copied list maintains the same type as the original
- Memory allocation is handled by the  function

## Simplified Source

```c
List *
list_copy_head(const List *oldlist, int len)
{
    List *newlist;

    // Return NIL for empty list or invalid length
    if (oldlist == NIL || len <= 0)
        return NIL;

    // Don't copy more elements than exist
    len = Min(oldlist->length, len);

    // Create new list and copy first 'len' elements
    newlist = new_list(oldlist->type, len);
    memcpy(newlist->elements, oldlist->elements, len * sizeof(ListCell));

    check_list_invariants(newlist);
    return newlist;
}
```