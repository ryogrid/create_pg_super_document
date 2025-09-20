# list_delete_cell

## Location
[src/backend/nodes/list.c:841-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L841-L852)

## Overview
Deletes a specific cell from a PostgreSQL list by reference, freeing the entire list if it becomes empty.

## Definition

```c
List *
list_delete_cell(List *list, ListCell *cell)
```
## Detailed Description
This function removes a specific  from a  by calculating its position relative to the list's elements array and delegating to . It's a convenience function that allows deletion by cell reference rather than by index position. The function takes time proportional to the distance from the deleted cell to the end of the list, since all following entries must be moved to fill the gap.

If the deleted cell was the last remaining member of the list, the entire list structure is freed and  is returned, which is the canonical representation of an empty list in PostgreSQL. The function does not touch any data that the cell might have been pointing to - only the list structure itself is modified.

## Parameters / Member Variables
- : The List from which to delete the cell
- : Pointer to the specific ListCell to be removed from the list

## Dependencies
- Functions called/Symbols referenced:
  - [list_delete_nth_cell](list_delete_nth_cell.md)
- Called from (representative examples):
  - [transformGenericOptions](../t/transformGenericOptions.md) (src/backend/commands/foreigncmds.c:161)
  - [list_delete](list_delete.md) (src/backend/nodes/list.c:863) 
  - [list_delete_ptr](list_delete_ptr.md) (src/backend/nodes/list.c:882)
  - [list_delete_int](list_delete_int.md) (src/backend/nodes/list.c:901)
  - [list_delete_oid](list_delete_oid.md) (src/backend/nodes/list.c:920)
  - [remove_useless_joins](../r/remove_useless_joins.md) (src/backend/optimizer/plan/analyzejoins.c:103)

## Notes and Other Information
- The function performs bounds checking through the underlying  call
- Time complexity is O(n) where n is the number of elements after the deleted cell
- Memory management is handled automatically - the list is freed if it becomes empty
- This is part of PostgreSQL's generic list implementation used throughout the codebase
- The cell parameter must be a valid cell that actually belongs to the specified list