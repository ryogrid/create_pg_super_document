# dlist_iter

## Location
[src/include/lib/ilist.h:177-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L177-L181)

## Overview
The  structure provides iterator state for safely traversing doubly-linked lists without modification during iteration.

## Definition

```c
typedef struct dlist_iter
{
	dlist_node *cur;			/* current element */
	dlist_node *end;			/* last node we'll iterate to */
} dlist_iter;
```
## Detailed Description
The  structure is designed to provide safe iteration over doubly-linked lists managed by  and  types. It serves as the state container for the  and  macros (and their dclist variants).

This iterator is specifically designed for read-only traversal - modifications to the list during iteration are not permitted and could lead to undefined behavior. The iterator maintains both a current position and an end marker to ensure proper termination of iteration loops.

The  field is included as an optimization to avoid multiple evaluations of arguments in the foreach macros, improving both performance and safety by ensuring the iteration boundaries are established once at the beginning of the traversal.

## Parameters / Member Variables
- : Pointer to the current  being processed during iteration
- : Pointer to the last  that will be processed in this iteration, used to determine when to stop

## Dependencies
- Functions called/Symbols referenced:
  -  (used for both cur and end members)
- Called from (representative examples):
  -  (macro for forward iteration)
  -  (macro for reverse iteration)
  -  (dclist variant)
  - Used extensively in PostgreSQL subsystems including GIN indexing, transaction management, autovacuum, replication, and memory management

## Notes and Other Information
- This iterator is intended for read-only traversal - list modification during iteration is prohibited
- The  field optimization prevents multiple macro argument evaluations, improving performance and safety
- Used as the foundation for PostgreSQL's safe list traversal patterns
- Provides consistent iteration semantics across both dlist and dclist implementations
- Essential for maintaining list integrity during complex operations that require examining all list elements