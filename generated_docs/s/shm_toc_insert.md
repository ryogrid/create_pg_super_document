# shm_toc_insert

## Location
[src/backend/storage/ipc/shm_toc.c:171-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_toc.c#L171-L231)

## Overview
Inserts a TOC entry that maps a 64-bit key to a memory address within the shared memory segment, enabling other processes to discover data structure locations.

## Definition
```c
void shm_toc_insert(shm_toc *toc, uint64 key, void *address)
```

## Detailed Description
The `shm_toc_insert` function creates entries in the shared memory table of contents that allow processes to register and later discover the locations of data structures within a shared memory segment. This function is fundamental to PostgreSQL's shared memory management, enabling a bootstrap mechanism for inter-process data sharing.

**Key Design Principles:**
- **Key-Value Mapping**: Uses 64-bit keys to identify data structures, allowing processes to use well-known identifiers to locate shared objects
- **Relative Addressing**: Stores relative offsets rather than absolute pointers, making the shared memory segment relocatable across different process address spaces
- **Bootstrap Mechanism**: Designed to store a minimal set of critical pointers needed for processes to initialize and discover other shared structures
- **Write Ordering**: Uses memory barriers to ensure safe lock-free reading of TOC entries

**Address Relativization:**
The function converts absolute addresses to relative offsets by subtracting the TOC base address. This is crucial because shared memory segments may be mapped at different virtual addresses in different processes.

**Memory Management:**
The function includes comprehensive checks for:
- Memory exhaustion (insufficient space for new TOC entries)
- Integer overflow conditions  
- Entry count limits (maximum PG_UINT32_MAX entries)

**Concurrency Safety:**
- Uses spinlocks for mutual exclusion during entry insertion
- Employs write barriers to ensure proper ordering for lock-free readers
- Updates entry count last to make partially-written entries invisible to readers

## Parameters / Member Variables
- `toc`: Pointer to the shared memory table of contents structure
- `key`: 64-bit identifier for the data structure being registered
- `address`: Pointer to the data structure location within the shared memory segment

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease (for thread-safe TOC modification)
  - pg_write_barrier (for memory ordering guarantees)
  - [shm_toc_entry](shm_toc_entry.md) (for TOC entry structure)
  - PG_UINT32_MAX (for entry count limits)
  - ereport/ERROR (for error handling)

- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md) (src/backend/access/brin/brin.c:2477-2499)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md) (src/backend/access/nbtree/nbtsort.c:1528-1567)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (src/backend/access/transam/parallel.c:356-489)
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (src/backend/executor/execParallel.c:747-827)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md) (src/backend/commands/vacuumparallel.c:364-416)

## Notes and Other Information
- This function is typically called during shared memory segment initialization by a master process
- The 64-bit key space allows for flexible identification schemes - keys can be enum values, hash values, or structured identifiers
- The function is not designed to scale to large numbers of entries; it's intended for registering a small number of bootstrap pointers
- Write barriers ensure that even without locking, readers will see consistent TOC state
- The address validation (address > toc) ensures that registered addresses point within the shared memory segment
- Entry insertion failure results in an ERROR, which is appropriate since TOC setup typically occurs during critical initialization phases
- The relative addressing scheme makes shared memory segments portable across process restarts and different system configurations

## Simplified Source

```c
void shm_toc_insert(shm_toc *toc, uint64 key, void *address)
{
    volatile shm_toc *vtoc = toc;
    Size total_bytes;
    Size allocated_bytes;
    Size nentry;
    Size toc_bytes;
    Size offset;

    // Convert absolute address to relative offset
    Assert(address > (void *) toc);
    offset = ((char *) address) - (char *) toc;

    SpinLockAcquire(&toc->toc_mutex);

    // Get current TOC state
    total_bytes = vtoc->toc_total_bytes;
    allocated_bytes = vtoc->toc_allocated_bytes;
    nentry = vtoc->toc_nentry;

    // Calculate space needed for current TOC + new entry
    toc_bytes = offsetof(shm_toc, toc_entry) + nentry * sizeof(shm_toc_entry) + allocated_bytes;

    // Check for space exhaustion and limits
    if (toc_bytes + sizeof(shm_toc_entry) > total_bytes ||
        toc_bytes + sizeof(shm_toc_entry) < toc_bytes ||
        nentry >= PG_UINT32_MAX)
    {
        SpinLockRelease(&toc->toc_mutex);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of shared memory")));
    }

    // Fill in the new TOC entry
    Assert(offset < total_bytes);
    vtoc->toc_entry[nentry].key = key;
    vtoc->toc_entry[nentry].offset = offset;

    // Memory barrier ensures entry is complete before making it visible
    pg_write_barrier();

    // Make new entry visible to readers
    vtoc->toc_nentry++;

    SpinLockRelease(&toc->toc_mutex);
}
```