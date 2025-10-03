# insert_new_cell

## Location
[src/backend/nodes/list.c:415-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L415-L438)

## Overview
Creates space for a new cell at a specified position within a PostgreSQL List, returning the address of the newly created cell.

## Definition

```c
static ListCell *
insert_new_cell(List *list, int pos)
```
## Detailed Description
The  function is a low-level, static utility function that creates space for a new cell at a specific position within an existing PostgreSQL List. Unlike the append functions, this function can insert at any valid position within the list, including the beginning or middle. It handles the memory management aspects of list insertion, including enlarging the array if necessary and shifting existing elements to make room for the new cell.

The function assumes the input list is non-NIL and validates that the position is within valid bounds (0 <= pos <= list length). After ensuring sufficient space is available by potentially enlarging the list, it uses memmove to shift existing elements and increments the list length. The data in the newly created cell is left undefined and must be filled by the caller.

This is an internal implementation function used by the public list insertion APIs.

## Parameters / Member Variables
- `*list`: The non-NIL List to insert into (must be a valid existing list)
- `pos`: The zero-based position where the new cell should be inserted (0 <= pos <= list length)
## Dependencies
- Functions called/Symbols referenced:
  - [enlarge_list](../e/enlarge_list.md) (expands list capacity when current space is insufficient)
- Called from (representative examples):
  - [list_insert_nth](../l/list_insert_nth.md) (public API for inserting pointer values at specific positions)
  - [list_insert_nth_int](../l/list_insert_nth_int.md) (public API for inserting integer values at specific positions)
  - [list_insert_nth_oid](../l/list_insert_nth_oid.md) (public API for inserting OID values at specific positions)

## Notes and Other Information
- Static function, not part of the public API - used internally by list insertion functions
- Performs bounds checking with assertions to ensure position validity
- Uses memmove for safe memory copying when elements need to be shifted
- Returns pointer to new cell, but cell data is uninitialized and must be set by caller
- Automatically handles list expansion when capacity is exceeded
- More complex than append operations due to need for element shifting

## Simplified Source

```c
static ListCell *
insert_new_cell(List *list, int pos)
{
    // Validate position is within bounds
    Assert(pos >= 0 && pos <= list->length);

    // Expand list capacity if needed
    if (list->length >= list->max_length)
        enlarge_list(list, list->length + 1);

    // Shift existing elements to make room
    if (pos < list->length)
        memmove(&list->elements[pos + 1], &list->elements[pos],
                (list->length - pos) * sizeof(ListCell));

    // Update list length and return new cell
    list->length++;
    return &list->elements[pos];
}
```