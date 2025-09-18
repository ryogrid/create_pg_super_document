# slist_mutable_iter

## Location
[src/include/lib/ilist.h:272-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L272-L277)

## Overview
An enhanced iterator structure for singly linked lists that supports safe deletion of the current node during iteration.

## Definition


## Detailed Description
The  structure provides a specialized iterator for singly linked lists that allows safe deletion of the current node during iteration. Unlike the basic , this iterator maintains additional state to support modification operations while traversing the list.

The key capability of this iterator is enabling safe removal of the current node via  during iteration. This is achieved by maintaining pointers to the current, next, and previous nodes, allowing the deletion operation to properly update the list linkage without corrupting the iteration state.

The iterator is specifically designed for use with  and has strict limitations on the types of modifications allowed. Only deletion of the current node is supported; insertion or deletion of adjacent nodes would cause undefined behavior and should be avoided.

## Parameters / Member Variables
- : Pointer to the current slist_node being examined during iteration
- : Pointer to the next node that will be visited in the iteration sequence; cached to maintain iteration state during deletions
- : Pointer to the previous node in the list; required to properly relink the list when the current node is deleted

## Dependencies
- Functions called/Symbols referenced:
  - [slist_node](slist_node.md) (used for all three pointer members)
- Called from (representative examples):
  - slist_foreach_modify (macro that uses this iterator for modifiable traversal)
  - [slist_delete_current](slist_delete_current.md) (function that safely deletes the current node)
  - [AtEOSubXact_SPI](../A/AtEOSubXact_SPI.md) (for cleaning up SPI resources)
  - [ForgetBackgroundWorker](../F/ForgetBackgroundWorker.md) (for removing background worker registrations)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md) (for transaction-end GUC cleanup)

## Notes and Other Information
- Designed specifically for iterations where deletion of the current node is required
- Only supports deletion of the current node via slist_delete_current() - direct use of slist_delete() is not safe
- Insertion or deletion of nodes adjacent to the current position is not supported and will cause undefined behavior
- The three-pointer design enables safe list modification by maintaining complete linkage information
- More memory overhead than basic slist_iter due to additional state tracking
- Used extensively in PostgreSQL for cleanup operations where resources need to be removed during traversal
- Common use cases include transaction cleanup, background worker management, and resource deallocation
- Part of PostgreSQL's intrusive list infrastructure designed for performance-critical modification scenarios