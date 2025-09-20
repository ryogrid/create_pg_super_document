# RWConflictPoolHeaderData

## Location
[src/include/storage/predicate_internals.h:206-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L206-L210)

## Overview
A structure that represents the header of a pool for managing read-write conflicts in PostgreSQL's serializable isolation implementation, containing a list of available conflicts and an element pointer.

## Definition

```c
typedef struct RWConflictPoolHeaderData
{
	dlist_head	availableList;
	RWConflict	element;
}			RWConflictPoolHeaderData;
```
## Detailed Description
RWConflictPoolHeaderData serves as the header structure for a memory pool that manages RWConflict objects in PostgreSQL's serializable snapshot isolation system. This structure is part of the predicate locking mechanism that detects and prevents serialization anomalies. The pool header maintains a doubly-linked list of available conflict objects that can be allocated when new read-write conflicts need to be tracked between transactions. This pooling approach improves performance by reusing conflict objects rather than constantly allocating and deallocating them.

## Parameters / Member Variables
- : A doubly-linked list head that tracks available RWConflict objects in the pool that can be allocated for new conflicts
- : A pointer to the first RWConflict object in this pool, serving as the base element for pool operations

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](../d/dlist_head.md) (from src/include/lib/ilist.h)
  - [RWConflict](RWConflict.md) (typedef for struct RWConflictData*)
- Called from (representative examples):
  - [RWConflictPoolHeader](RWConflictPoolHeader.md) (typedef alias)
  - RWConflictPoolHeaderDataSize (size calculation macro)

## Notes and Other Information
- This structure is part of PostgreSQL's serializable snapshot isolation implementation
- Located in src/include/storage/predicate_internals.h, indicating it's an internal implementation detail
- The pool design pattern is used to efficiently manage memory allocation for conflict tracking objects
- Works in conjunction with the predicate locking system to detect dangerous structures that could lead to serialization anomalies