# new_head_cell

## Location
src/backend/nodes/list.c: 305 - 322

## Overview
Creates space for a new head cell at the beginning of an existing PostgreSQL list by shifting all current elements one position to the right.

## Definition


## Detailed Description
The new_head_cell function is a static utility function that prepares an existing list for insertion of a new element at the head (beginning) of the list. It handles the necessary memory management by first enlarging the list's capacity if needed, then shifting all existing elements one position to the right using memmove. This creates an empty slot at position 0 where the new head element can be inserted.

The function only creates the space - it does not initialize the data in the new head cell. The caller is responsible for filling in the actual data after calling this function. This design allows for efficient batch operations where multiple head insertions might be performed.

## Parameters / Member Variables
- : Pointer to the List structure that will have a new head cell added. Must not be NIL/NULL.

## Dependencies
- Functions called/Symbols referenced:
  - enlarge_list (expands the list's capacity when needed)
- Called from (representative examples):
  - lcons (prepends a generic pointer value to the list)
  - lcons_int (prepends an integer value to the list)
  - lcons_oid (prepends an OID value to the list)

## Notes and Other Information
- This is a static function, only accessible within src/backend/nodes/list.c
- The function assumes the input list is non-NIL (non-NULL) as documented in the comment
- Uses memmove for safe memory copying that handles overlapping memory regions correctly
- The caller must initialize the data in list->elements[0] after calling this function
- This function automatically increments the list's length counter
- Performance consideration: shifting all elements makes head insertion O(n) in complexity