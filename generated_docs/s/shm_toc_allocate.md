# shm_toc_allocate

## Location
[src/backend/storage/ipc/shm_toc.c:88-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_toc.c#L88-L130)

## Overview
Allocates shared memory chunks from a segment managed by a table of contents, using a backwards allocation strategy without providing deallocation capability.

## Definition
```c
void *shm_toc_allocate(shm_toc *toc, Size nbytes)
```

## Detailed Description
The `shm_toc_allocate` function provides a specialized memory allocation mechanism for shared memory segments managed by a table of contents. Unlike traditional allocators, this function has several unique characteristics:

**Key Design Features:**
- **Backwards Allocation**: Memory is allocated from the end of the segment towards the beginning, allowing TOC entries to grow forward from the start
- **No Deallocation**: This is a append-only allocator with no free() equivalent, designed for scenarios where memory is allocated during initialization and used throughout the process lifetime
- **Buffer Alignment**: All allocations are aligned to BUFFERALIGN boundaries to ensure atomic operations can be performed safely on allocated memory
- **Thread Safety**: Uses spinlocks to ensure thread-safe allocation in multi-process environments

**Memory Layout Strategy:**
The function maintains a clear separation between metadata and data by placing the TOC structure and entries at the beginning of the segment, while allocating user data from the end. This prevents conflicts and allows both sections to grow towards each other.

**Error Handling:**
The function performs comprehensive checking for memory exhaustion and integer overflow conditions, raising an ERROR if the requested allocation cannot be satisfied.

## Parameters / Member Variables
- `toc`: Pointer to the shared memory table of contents structure managing the segment
- `nbytes`: Number of bytes to allocate (will be aligned to BUFFERALIGN boundary)

## Dependencies
- Functions called/Symbols referenced:
  - BUFFERALIGN (for memory alignment)
  - SpinLockAcquire/SpinLockRelease (for thread synchronization)
  - shm_toc_entry (for TOC entry size calculations)
  - ereport/ERROR (for error handling)

- Called from (representative examples):
  - _brin_begin_parallel (src/backend/access/brin/brin.c:2446-2497)
  - _bt_begin_parallel (src/backend/access/nbtree/nbtsort.c:1500-1565)
  - InitializeParallelDSM (src/backend/access/transam/parallel.c:338-485)
  - ExecParallelSetupTupleQueues (src/backend/executor/execParallel.c:555)
  - parallel_vacuum_init (src/backend/commands/vacuumparallel.c:332-413)

## Notes and Other Information
- This allocator is specifically designed for parallel processing scenarios where multiple processes need to share data structures
- The backwards allocation strategy ensures that TOC entries and allocated data don't interfere with each other's growth
- Buffer alignment is crucial for atomic operations and performance, especially on architectures with strict alignment requirements
- The lack of deallocation capability makes this suitable for initialization-time allocation patterns common in PostgreSQL's parallel processing
- Memory exhaustion results in an ERROR being raised, which will abort the current operation - this is appropriate for shared memory scenarios where allocation failure indicates a serious system resource issue
- The volatile qualifier on vtoc ensures proper memory access semantics in multi-process environments