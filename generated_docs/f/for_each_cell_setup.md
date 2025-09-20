# for_each_cell_setup

## Location
[src/include/nodes/pg_list.h:447-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L447-L468)

## Overview
Initializes a ForEachState structure for the for_each_cell macro, enabling iteration through a PostgreSQL List starting from a specified ListCell.

## Definition

```c
static inline ForEachState
for_each_cell_setup(const List *lst, const ListCell *initcell)
```
## Detailed Description
The `for_each_cell_setup` function is a helper function that initializes the state required for the `for_each_cell` macro. It creates and returns a `ForEachState` structure that contains the list pointer and the starting index derived from the provided initial cell. The function handles the case where the initial cell might be NULL by setting the index to the list length (effectively making iteration start at the end).

When a valid initial cell is provided, the function uses `list_cell_number` to convert the cell pointer to its corresponding index within the list. This allows the `for_each_cell` macro to iterate starting from that specific cell position.

## Parameters / Member Variables
- `lst`: A pointer to the PostgreSQL List structure to iterate over
- `initcell`: A pointer to the ListCell from which to start iteration (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ForEachState](../F/ForEachState.md) (struct type)
  - [list_cell_number](../l/list_cell_number.md) (to convert cell pointer to index)
  - list_length (to get list length when initcell is NULL)
- Called from (representative examples):
  - for_each_cell (macro)

## Notes and Other Information
- This is an inline function for performance optimization
- Used internally by the `for_each_cell` macro, not typically called directly by user code
- If initcell is NULL, the starting index is set to the list length, effectively creating an empty iteration
- Part of PostgreSQL's list iteration infrastructure that provides convenient cell-based iteration
- Returns a ForEachState struct by value, containing the list pointer and calculated starting index
- The function relies on `list_cell_number` for bounds checking of the initial cell