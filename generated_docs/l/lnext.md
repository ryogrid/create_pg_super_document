# lnext

## Location
[src/include/nodes/pg_list.h:343-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L343-L372)

## Overview
Returns the next list cell after a given cell within a PostgreSQL List structure, or NULL if there are no more cells.

## Definition

```c
static inline ListCell *
lnext(const List *l, const ListCell *c)
```
## Detailed Description
The  function is a core utility for iterating through PostgreSQL's List data structure. It provides safe navigation to the next cell in a list by performing bounds checking and returning the appropriate result. The function is implemented as an inline function for performance, as it's heavily used throughout the PostgreSQL codebase for list traversal operations.

The function increments the cell pointer and validates that the result is still within the valid range of the list's elements array. This design ensures memory safety while providing efficient list iteration.

## Parameters / Member Variables
- : A pointer to the List structure containing the cell
- : A pointer to the current ListCell for which we want to find the next cell

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for bounds checking)
  - [List](../L/List.md) structure (implicitly referenced through pointer arithmetic)
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [ConstructTupleDescriptor](../C/ConstructTupleDescriptor.md)
  - [ExecLockRows](../E/ExecLockRows.md)
  - [transformUpdateTargetList](../t/transformUpdateTargetList.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)

## Notes and Other Information
- This is an inline function defined in  for optimal performance
- Performs assertion-based bounds checking to ensure the current cell is within the valid range
- Used extensively throughout PostgreSQL for list iteration, with over 100 call sites
- Part of PostgreSQL's fundamental list manipulation API
- Returns NULL when reaching the end of the list, making it suitable for while-loop based iteration
- The function assumes that the input cell pointer is valid and within the list bounds