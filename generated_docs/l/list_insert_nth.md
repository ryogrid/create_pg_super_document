# list_insert_nth

## Location
[src/backend/nodes/list.c:439-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L439-L452)

## Overview
Inserts a given datum (void pointer) at a specified position in a PostgreSQL List, maintaining the list's structure and updating pointers accordingly.

## Definition

```c
List *
list_insert_nth(List *list, int pos, void *datum)
```
## Detailed Description
The  function provides positional insertion capability for PostgreSQL's generic List data structure. It inserts a new element at the specified position (0-based indexing) and shifts all following elements to make room. The function handles both empty lists (NIL) and existing lists, ensuring proper list invariants are maintained after the insertion.

The function has O(n) time complexity proportional to the distance to the end of the list, as subsequent entries must be moved to accommodate the new element. Position validation ensures that  is within valid bounds (0 <= pos <= list length).

## Parameters / Member Variables
- `*list`: The target List to insert into (can be NIL for empty list)
- `pos`: Zero-based position index where the new element should be inserted
- `*datum`: The void pointer data to be inserted into the list
## Dependencies
- Functions called/Symbols referenced:
  - : Validates that the list contains pointer elements
  - : Internal helper function to create and position a new list cell
  - : Debug function to verify list structural integrity
  - : Creates a new single-element list (used for NIL case)
  - : Macro to access the first element of a list cell

- Called from (representative examples):
  - : Used in genetic query optimizer for path merging
  - : Optimizer function for adding execution paths
  - : Optimizer function for partial path addition
  - : Macro for five-way list iteration

## Notes and Other Information
- Asserts that position is valid (pos == 0 for NIL lists)
- Only works with pointer-based lists, verified by  assertion
- Maintains list invariants through  in debug builds
- Time complexity is O(k) where k is the number of elements after insertion point
- Returns the modified list (same list object, not a new copy)

## Simplified Source

```c
List *
list_insert_nth(List *list, int pos, void *datum)
{
    // Handle empty list case - create new single-element list
    if (list == NIL) {
        Assert(pos == 0);
        return list_make1(datum);
    }

    // Ensure we're working with a pointer list
    Assert(IsPointerList(list));

    // Insert new cell at specified position and set its data
    lfirst(insert_new_cell(list, pos)) = datum;

    // Verify list integrity in debug builds
    check_list_invariants(list);

    return list;
}
```