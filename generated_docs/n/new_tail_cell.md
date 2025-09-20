# new_tail_cell

## Location
[src/backend/nodes/list.c:323-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L323-L338)

## Overview
Creates space for a new tail cell at the end of an existing PostgreSQL list by extending the list length and capacity if necessary.

## Definition

```c
structively
 * modify the list;
```
## Detailed Description
The new_tail_cell function is a static utility function that prepares an existing list for insertion of a new element at the tail (end) of the list. Unlike new_head_cell which must shift existing elements, new_tail_cell simply extends the list's capacity if needed and increments the length counter. This creates an empty slot at the end of the list where the new tail element can be inserted.

The function only creates the space - it does not initialize the data in the new tail cell. The caller is responsible for filling in the actual data at the new position after calling this function. This design allows for efficient batch operations and maintains consistency with the head insertion pattern.

## Parameters / Member Variables
- : Pointer to the List structure that will have a new tail cell added. Must not be NIL/NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [enlarge_list](../e/enlarge_list.md) (expands the list's capacity when needed)
- Called from (representative examples):
  - lappend (appends a generic pointer value to the list)
  - lappend_int (appends an integer value to the list)
  - lappend_oid (appends an OID value to the list)
  - lappend_xid (appends a transaction ID value to the list)

## Notes and Other Information
- This is a static function, only accessible within src/backend/nodes/list.c
- The function assumes the input list is non-NIL (non-NULL) as documented in the comment
- Much more efficient than new_head_cell since no element shifting is required - O(1) complexity vs O(n)
- The caller must initialize the data in list->elements[list->length-1] after calling this function
- This function automatically increments the list's length counter
- Tail insertion is the preferred method for building lists incrementally due to its superior performance characteristics