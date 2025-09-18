# slist_iter

## Location
[src/include/lib/ilist.h:257-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L257-L260)

## Overview
An iterator structure for safely traversing singly linked lists, maintaining state during iteration operations.

## Definition


## Detailed Description
The  structure provides a standardized way to iterate through singly linked lists in PostgreSQL. It serves as state storage for the  macro and related iteration operations. The iterator maintains a pointer to the current node being examined during traversal.

The design allows for safe modification of the list during iteration, with important restrictions. While it's generally safe to modify the list structure during iteration, deleting the iterator's current node requires special care to avoid memory safety issues. The iterator provides a consistent interface for forward-only traversal of singly linked lists.

Although the functionality could technically be achieved with a simple  pointer, PostgreSQL uses a separate iterator type for consistency with other list implementations and to provide a clear semantic distinction between node pointers and iteration state.

## Parameters / Member Variables
- : Pointer to the current slist_node being examined during iteration; points to the node that represents the current position in the traversal

## Dependencies
- Functions called/Symbols referenced:
  - [slist_node](slist_node.md) (as the type for the current position pointer)
- Called from (representative examples):
  - slist_foreach (macro that uses this iterator for list traversal)
  - [pg_event_trigger_dropped_objects](../p/pg_event_trigger_dropped_objects.md) (for iterating through dropped objects)
  - [BackgroundWorkerShmemInit](../B/BackgroundWorkerShmemInit.md) (for iterating through background worker registrations)
  - [CatCachePrintStats](../C/CatCachePrintStats.md) (for iterating through catalog cache entries)

## Notes and Other Information
- Designed for forward-only iteration through singly linked lists
- Safe to modify the list during iteration, except for deleting the current node
- Deleting the current node during iteration requires careful handling to avoid memory corruption
- The separate type (rather than using slist_node* directly) provides consistency with other PostgreSQL list iterator designs
- Commonly used with the slist_foreach() macro for clean iteration syntax
- Used extensively in PostgreSQL for traversing collections in background worker management, catalog caching, and event trigger systems
- Part of PostgreSQL's intrusive list infrastructure optimized for performance-critical code paths