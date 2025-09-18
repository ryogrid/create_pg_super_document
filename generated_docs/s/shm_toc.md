# shm_toc

## Location
src/backend/storage/ipc/shm_toc.c: 26 - 39

## Overview
The shm_toc structure is PostgreSQL's shared memory table of contents (TOC), providing a mechanism for processes to register and discover data structures within shared memory segments using key-based lookups.

## Definition


## Detailed Description
The shm_toc structure serves as the central management structure for PostgreSQL's shared memory TOC system. It maintains metadata about a shared memory segment and provides a registry of data structures within that segment through key-value mappings stored in toc_entry array.

The TOC system is designed to solve the problem of process-independent access to shared memory structures. Since different processes may map the same shared memory segment at different virtual addresses, absolute pointers are not portable. Instead, the TOC stores relative offsets from the TOC base, allowing any process to calculate the correct address regardless of where the segment is mapped.

The structure includes synchronization primitives (spinlock) for thread-safe concurrent access, memory management fields to track allocation within the segment, and a flexible array of entries that grows as needed. The magic number provides basic validation to ensure processes are accessing compatible TOC structures.

## Parameters / Member Variables
- : A 64-bit magic number used to validate TOC compatibility and detect corruption or version mismatches
- : A spinlock providing mutual exclusion for concurrent access to TOC metadata and entries
- : Total size in bytes of the shared memory segment managed by this TOC
- : Number of bytes currently allocated within the managed segment (excludes TOC overhead)
- : Current number of entries in the toc_entry array
- : Flexible array member containing the actual key-offset mappings for registered data structures

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](slock_t.md) (spinlock type)
  - [shm_toc_entry](shm_toc_entry.md) (entry structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array syntax)
- Used by (key functions):
  - shm_toc_create (initialize new TOC)
  - shm_toc_attach (attach to existing TOC)
  - shm_toc_allocate (allocate memory within segment)
  - shm_toc_insert (register new data structure)
  - [shm_toc_lookup](shm_toc_lookup.md) (find registered data structure)
  - shm_toc_freespace (query available space)
  - [shm_toc_estimate](shm_toc_estimate.md) (estimate required TOC size)

## Notes and Other Information
- The TOC system is intentionally simple and not designed to scale to large numbers of entries - it's meant for bootstrap pointers only
- Memory allocation within the TOC-managed segment uses buffer alignment (BUFFERALIGN_DOWN) for performance
- Lookup operations are lock-free for better concurrency, using only memory barriers to ensure consistency
- The structure uses relative offsets instead of absolute pointers to maintain portability across different process address spaces
- Magic number validation helps detect incompatible TOC versions and memory corruption issues
- Widely used throughout PostgreSQL's parallel processing infrastructure, including parallel queries, vacuum, index builds, and logical replication workers
- The flexible array member allows the TOC to dynamically size based on the number of registered entries while maintaining efficient memory layout