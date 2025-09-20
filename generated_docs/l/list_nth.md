# list_nth

## Location
[src/include/nodes/pg_list.h:299-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L299-L309)

## Overview
Returns the pointer value contained in the n-th element of a List, providing indexed access to list elements starting from position 0.

## Definition

```c
static inline void *
list_nth(const List *list, int n)
```
## Detailed Description
The `list_nth` function provides indexed access to PostgreSQL List elements, returning the void pointer stored at the specified position. It uses zero-based indexing, where the first element is at position 0. The function works by first obtaining the ListCell at position n using `list_nth_cell`, then extracting the pointer value using `lfirst`.

This function is one of the core list access utilities in PostgreSQL, providing O(1) access time due to the array-based implementation of PostgreSQL's List structure. It includes type checking to ensure the provided parameter is actually a List structure.

## Parameters / Member Variables
- `list`: A const pointer to the List structure from which to retrieve the element
- `n`: Zero-based index of the element to retrieve (must be within bounds: 0 <= n < list->length)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for type checking)
  - list_nth_cell (to get the cell at position n)
  - lfirst (to extract the pointer value from the cell)
- Called from (representative examples):
  - [gistfinishsplit](../g/gistfinishsplit.md) (GiST index operations)
  - [ExecInitAppend](../E/ExecInitAppend.md) (append node initialization)
  - [transformInsertRow](../t/transformInsertRow.md) (SQL INSERT statement processing)
  - [get_rte_attribute_name](../g/get_rte_attribute_name.md) (relation attribute name retrieval)
  - rt_fetch (range table access macro)
  - Many optimizer and executor functions

## Notes and Other Information
- This is a static inline function for optimal performance
- Uses zero-based indexing (first element is at index 0)
- No bounds checking is performed beyond the assertion in list_nth_cell
- Returns a generic void* pointer that typically needs to be cast to the appropriate type
- Extensively used throughout PostgreSQL's query processing pipeline
- Part of the fundamental list manipulation API that abstracts PostgreSQL's array-based list implementation
- Calling with an invalid index will result in undefined behavior or assertion failure