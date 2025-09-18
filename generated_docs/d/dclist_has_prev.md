# dclist_has_prev

## Location
[src/include/lib/ilist.h:854-866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L854-L866)

## Overview
Checks whether a given node in a doubly-linked counted list has a preceding node, providing a safe way to determine if traversal can continue backwards.

## Definition


## Detailed Description
The `dclist_has_prev` function determines whether a specific node in a doubly-linked counted list has a predecessor. This function is part of PostgreSQL's counted list infrastructure, which maintains both doubly-linked list functionality and a count of elements. The function performs validation checks to ensure the node belongs to the specified list and that the list is not empty before delegating to the underlying dlist implementation.

## Parameters / Member Variables
- `head`: Pointer to the dclist_head structure representing the counted list
- `node`: Pointer to the dlist_node for which to check for a preceding node

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_member_check](dlist_member_check.md)
  - [dlist_has_prev](dlist_has_prev.md)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- The function includes a safety check using `dlist_member_check` to verify the node belongs to the list
- Contains an assertion to ensure the list count is greater than zero
- This is an inline function for performance optimization
- Caution: The node parameter must be a valid member of the specified list head
- Located in src/include/lib/ilist.h:854-866