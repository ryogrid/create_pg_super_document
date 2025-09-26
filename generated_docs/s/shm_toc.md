# shm_toc

## Location
[src/backend/storage/ipc/shm_toc.c:26-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_toc.c#L26-L39)

## Overview
The shm_toc structure is PostgreSQL's shared memory table of contents (TOC), providing a mechanism for processes to register and discover data structures within shared memory segments using key-based lookups.

## Definition

```c
struct shm_toc
{
	uint64		toc_magic;		/* Magic number identifying this TOC */
	slock_t		toc_mutex;		/* Spinlock for mutual exclusion */
	Size		toc_total_bytes;	/* Bytes managed by this TOC */
	Size		toc_allocated_bytes;	/* Bytes allocated of those managed */
	uint32		toc_nentry;		/* Number of entries in TOC */
	shm_toc_entry toc_entry[FLEXIBLE_ARRAY_MEMBER];
};
```
## Detailed Description
The shm_toc structure serves as the central management structure for PostgreSQL's shared memory TOC system. It maintains metadata about a shared memory segment and provides a registry of data structures within that segment through key-value mappings stored in toc_entry array.

The TOC system is designed to solve the problem of process-independent access to shared memory structures. Since different processes may map the same shared memory segment at different virtual addresses, absolute pointers are not portable. Instead, the TOC stores relative offsets from the TOC base, allowing any process to calculate the correct address regardless of where the segment is mapped.

The structure includes synchronization primitives (spinlock) for thread-safe concurrent access, memory management fields to track allocation within the segment, and a flexible array of entries that grows as needed. The magic number provides basic validation to ensure processes are accessing compatible TOC structures.

## Parameters / Member Variables
- `toc_magic`: A 64-bit magic number used to validate TOC compatibility and detect corruption or version mismatches
- `toc_mutex`: A spinlock providing mutual exclusion for concurrent access to TOC metadata and entries
- `toc_total_bytes`: Total size in bytes of the shared memory segment managed by this TOC
- `toc_allocated_bytes`: Number of bytes currently allocated within the managed segment (excludes TOC overhead)
- `toc_nentry`: Current number of entries in the toc_entry array
- `toc_entry[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing the actual key-offset mappings for registered data structures

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](slock_t.md) (spinlock type)
  - [shm_toc_entry](shm_toc_entry.md) (entry structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array syntax)
- Used by (key functions):
  - [shm_toc_create](shm_toc_create.md) (initialize new TOC)
  - [shm_toc_attach](shm_toc_attach.md) (attach to existing TOC)
  - [shm_toc_allocate](shm_toc_allocate.md) (allocate memory within segment)
  - [shm_toc_insert](shm_toc_insert.md) (register new data structure)
  - [shm_toc_lookup](shm_toc_lookup.md) (find registered data structure)
  - [shm_toc_freespace](shm_toc_freespace.md) (query available space)
  - [shm_toc_estimate](shm_toc_estimate.md) (estimate required TOC size)

## Notes and Other Information
- The TOC system is intentionally simple and not designed to scale to large numbers of entries - it's meant for bootstrap pointers only
- Memory allocation within the TOC-managed segment uses buffer alignment (BUFFERALIGN_DOWN) for performance
- Lookup operations are lock-free for better concurrency, using only memory barriers to ensure consistency
- The structure uses relative offsets instead of absolute pointers to maintain portability across different process address spaces
- Magic number validation helps detect incompatible TOC versions and memory corruption issues
- Widely used throughout PostgreSQL's parallel processing infrastructure, including parallel queries, vacuum, index builds, and logical replication workers
- The flexible array member allows the TOC to dynamically size based on the number of registered entries while maintaining efficient memory layout