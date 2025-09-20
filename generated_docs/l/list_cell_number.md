# list_cell_number

## Location
[src/include/nodes/pg_list.h:333-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L333-L342)

## Overview
Returns the zero-based index position of a given ListCell within its containing List, providing reverse lookup from cell pointer to index.

## Definition

```c
static inline int
list_cell_number(const List *l, const ListCell *c)
```
## Detailed Description
The `list_cell_number` function performs pointer arithmetic to determine the index position of a specific ListCell within its containing List. It calculates the index by subtracting the base address of the list's elements array from the given cell pointer, effectively performing reverse indexing.

This function includes bounds checking through an assertion that verifies the cell pointer falls within the valid range of the list's elements array. The function is particularly useful in scenarios where you have a ListCell pointer (typically from iteration) and need to determine its position for indexing or comparison purposes.

## Parameters / Member Variables
- `l`: A const pointer to the List structure that contains the cell
- `c`: A const pointer to the ListCell whose index position is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for bounds checking to ensure cell is within list boundaries)
  - Direct access to List structure fields (elements array and length)
- Called from (representative examples):
  - [SyncPostCheckpoint](../S/SyncPostCheckpoint.md) (synchronous checkpoint processing)
  - [push_ancestor_plan](../p/push_ancestor_plan.md) (rule decompilation utilities)
  - [for_each_cell_setup](../f/for_each_cell_setup.md) (list iteration macro setup)
  - [for_both_cell_setup](../f/for_both_cell_setup.md) (dual list iteration macro setup)

## Notes and Other Information
- This is a static inline function for optimal performance
- Uses pointer arithmetic to calculate the index position
- Performs O(1) constant-time calculation
- Includes runtime bounds checking to ensure the cell pointer is valid
- The cell must belong to the provided list; passing a cell from a different list will fail the assertion
- Useful for debugging, logging, and situations where cell position information is needed
- Commonly used in list iteration macros and debugging utilities
- Returns values in the range [0, list->length-1] for valid cells
- Part of the supporting infrastructure for PostgreSQL's list manipulation API