# for_both_cell_setup

## Location
[src/include/nodes/pg_list.h:549-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L549-L562)

## Overview
Initializes a ForBothCellState structure for the for_both_cell macro, enabling parallel iteration through two PostgreSQL Lists starting from specified ListCells in each.

## Definition

```c
static inline ForBothCellState
for_both_cell_setup(const List *list1, const ListCell *initcell1,
					const List *list2, const ListCell *initcell2)
```
## Detailed Description
The `for_both_cell_setup` function is a helper function that initializes the state required for the `for_both_cell` macro. It creates and returns a `ForBothCellState` structure that contains pointers to both lists and their corresponding starting indices derived from the provided initial cells. This enables synchronized iteration through two lists simultaneously, starting from specific positions in each list.

The function handles NULL initial cells by setting the corresponding index to the list length, effectively making iteration start at the end for that list. When valid initial cells are provided, the function uses `list_cell_number` to convert each cell pointer to its corresponding index within its respective list.

## Parameters / Member Variables
- `list1`: A pointer to the first PostgreSQL List structure to iterate over
- `initcell1`: A pointer to the ListCell in the first list from which to start iteration (can be NULL)
- `list2`: A pointer to the second PostgreSQL List structure to iterate over  
- `initcell2`: A pointer to the ListCell in the second list from which to start iteration (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ForBothCellState](../F/ForBothCellState.md) (struct type)
  - [list_cell_number](../l/list_cell_number.md) (to convert cell pointers to indices)
  - [list_length](../l/list_length.md) (to get list lengths when initcells are NULL)
- Called from (representative examples):
  - for_both_cell (macro)

## Notes and Other Information
- This is an inline function for performance optimization
- Used internally by the `for_both_cell` macro, not typically called directly by user code
- If either initcell is NULL, the corresponding starting index is set to that list's length
- Part of PostgreSQL's list iteration infrastructure that provides synchronized dual-list iteration
- Returns a ForBothCellState struct by value, containing both list pointers and calculated starting indices
- The macro using this setup function stops iteration when either list runs out of elements
- Commonly used when processing paired data structures that need to be traversed in parallel

## Simplified Source

```c
static inline ForBothCellState for_both_cell_setup(const List *list1, const ListCell *initcell1,
                                                   const List *list2, const ListCell *initcell2) {
    ForBothCellState r = {
        list1, list2,
        // Set starting indices for both lists
        initcell1 ? list_cell_number(list1, initcell1) : list_length(list1),
        initcell2 ? list_cell_number(list2, initcell2) : list_length(list2)
    };

    return r;
}
```

This function initializes a ForBothCellState structure for synchronized iteration through two lists. For each list, if an initial cell is provided, it calculates the starting index; otherwise it sets the index to the list length (creating empty iteration for that list).