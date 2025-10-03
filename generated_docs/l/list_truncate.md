# list_truncate

## Location
[src/backend/nodes/list.c:631-660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L631-L660)

## Overview
The  function modifies a list in-place to contain no more than a specified number of elements, effectively shortening the list by removing elements from the end.

## Definition

```c
List *
list_truncate(List *list, int new_size)
```
## Detailed Description
This function provides an efficient way to reduce the length of a list without deallocating memory or moving list cells. It operates by simply adjusting the list's length field when the requested size is smaller than the current length. If the requested size is greater than or equal to the current length, no modification occurs.

The function handles the special case of truncating to zero length by returning NIL. Importantly, the cells that are "removed" by truncation are not actually freed or wiped from memory - they remain in the list's storage but are considered beyond the list's effective boundary. This design choice maintains compatibility with the old cons-cell-based implementation and ensures that existing pointers to list cells remain valid.

The in-place modification approach means the function doesn't invalidate pointers to the remaining cells, making it safe to use in contexts where other code might hold references to list elements.

## Parameters / Member Variables
- `*list`: The list to truncate (modified in-place)
- `new_size`: The maximum number of elements the list should contain after truncation
## Dependencies
- Functions called/Symbols referenced:
  -  - Gets the current length of the list for comparison
  
- Called from (representative examples):
  -  (src/backend/nodes/list.c:971)
  -  (src/backend/optimizer/path/indxpath.c:1471)
  -  (src/backend/optimizer/path/joinpath.c:1603)
  -  (src/backend/parser/parse_agg.c:128)
  -  (src/backend/parser/parse_func.c:701)

## Notes and Other Information
- This is a destructive operation that modifies the input list in-place
- Callers should use the returned pointer as it may differ from the input pointer
- Removed cells are NOT deallocated ('d), remaining in the list's storage
- No memory movement occurs, preserving validity of existing cell pointers
- Returns NIL when truncating to zero or negative length
- More efficient than repeatedly calling list deletion functions
- The function maintains backward compatibility with legacy cons-cell behavior
- Used primarily in query parsing, optimization, and list manipulation contexts where precise list sizing is needed

## Simplified Source

```c
List *
list_truncate(List *list, int new_size)
{
    // Handle empty list case
    if (new_size <= 0)
        return NIL;

    // Only truncate if new size is smaller than current length
    if (new_size < list_length(list))
        list->length = new_size;

    // Note: Removed cells are not freed - they remain in memory
    // but are beyond the list's effective boundary

    return list;
}
```