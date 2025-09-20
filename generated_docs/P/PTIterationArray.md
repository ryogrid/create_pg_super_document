# PTIterationArray

## Location
[src/backend/nodes/tidbitmap.c:209-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L209-L213)

## Overview
PTIterationArray is a data structure used for managing shared iteration over pagetable entries in PostgreSQL's TID (tuple identifier) bitmap implementation, providing reference counting and index array storage for concurrent iterator access.

## Definition

```c
typedef struct PTIterationArray
{
	pg_atomic_uint32 refcount;	/* no. of iterator attached */
	int			index[FLEXIBLE_ARRAY_MEMBER];	/* index array */
} PTIterationArray;
```
## Detailed Description
PTIterationArray serves as a shared data structure that facilitates concurrent iteration over pagetable entries in the TID bitmap system. It maintains an atomic reference counter to track the number of iterators currently attached to the structure, ensuring proper resource management in multi-process environments. The flexible array member 'index' stores the actual indices that are being iterated over, allowing for variable-length arrays based on the number of pagetable entries.

This structure is primarily used in the context of shared TID bitmap operations where multiple processes may need to iterate over the same set of page table entries concurrently. The atomic reference counting mechanism ensures thread-safe access and proper cleanup when all iterators have finished.

## Parameters / Member Variables
- : An atomic 32-bit unsigned integer that tracks the number of iterators currently attached to this PTIterationArray. Uses PostgreSQL's atomic operations for thread-safe manipulation.
- : A flexible array member containing integer indices that represent the order in which pagetable entries should be iterated. The actual size is determined at allocation time based on the number of entries.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (atomic operations support)
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for flexible arrays)
- Called from (representative examples):
  - [TBMSharedIterator](../T/TBMSharedIterator.md) (as a member variable)
  - [tbm_free_shared_area](../t/tbm_free_shared_area.md)
  - [tbm_prepare_shared_iterate](../t/tbm_prepare_shared_iterate.md)

## Notes and Other Information
- This structure is part of PostgreSQL's TID bitmap implementation located in src/backend/nodes/tidbitmap.c
- The use of pg_atomic_uint32 for the refcount ensures thread-safe operations in parallel query execution scenarios
- The flexible array member pattern allows for memory-efficient storage of variable-length index arrays
- This structure is closely tied to the shared memory management of TID bitmaps and is used primarily in parallel bitmap heap scans
- The reference counting mechanism is crucial for proper cleanup and avoiding memory leaks in shared iterator scenarios