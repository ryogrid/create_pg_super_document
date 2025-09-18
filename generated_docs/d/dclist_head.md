# dclist_head

## Location
[src/include/lib/ilist.h:212-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L212-L216)

## Overview
The  structure is a counted doubly-linked list that extends  functionality by automatically maintaining a count of the number of items in the list.

## Definition


## Detailed Description
The  structure provides an enhanced version of the standard doubly-linked list by automatically tracking the number of elements in the list. This counted list implementation internally uses a  for the actual list management while maintaining an additional  field that is automatically updated whenever items are added to or removed from the list.

This design provides O(1) access to the list size without requiring a traversal of the entire list, which is particularly useful for algorithms that need to know the list size frequently or for implementing size-based optimizations. The count is automatically maintained by all dclist manipulation functions, ensuring consistency between the actual list contents and the stored count.

The dclist implementation shares the same iteration semantics and safety properties as the underlying dlist, including support for both read-only iteration with  and safe modification during iteration with .

## Parameters / Member Variables
- : A  structure that manages the actual doubly-linked list implementation
- : A 32-bit unsigned integer automatically maintained to reflect the current number of items in the list

## Dependencies
- Functions called/Symbols referenced:
  -  (embedded as the core list management structure)
- Called from (representative examples):
  -  (initializes the counted list)
  -  (checks if list is empty)
  - / (insertion operations that update count)
  -  (returns the current item count)
  - Various dclist manipulation functions that maintain count consistency
  - Used in PostgreSQL subsystems including deadlock detection, process management, and memory management

## Notes and Other Information
- Provides O(1) list size access through the automatically maintained count field
- All dclist manipulation functions automatically update the count to maintain consistency
- Shares the same underlying implementation and safety properties as dlist_head
- Compatible with the same iterator types (dlist_iter and dlist_mutable_iter) used by regular dlists
- Particularly useful for algorithms that frequently need to know the list size or implement size-based logic
- The count field eliminates the need for manual list traversal to determine size