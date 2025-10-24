# shm_toc_freespace

## Location
[src/backend/storage/ipc/shm_toc.c:131-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_toc.c#L131-L170)

## Overview
Returns the number of bytes that can still be allocated from a shared memory segment managed by a table of contents.

## Definition
```c
Size shm_toc_freespace(shm_toc *toc)
```

## Detailed Description
The `shm_toc_freespace` function calculates and returns the amount of free space remaining in a shared memory segment managed by a table of contents. This function is essential for capacity planning and determining whether future allocations can be satisfied before attempting them.

**Calculation Method:**
The function performs a precise calculation that accounts for both the current TOC structure size and the allocated data space:

1. **TOC Structure Space**: Calculates the space occupied by the TOC header plus all current TOC entries
2. **Allocated Data Space**: Tracks the total bytes allocated for user data
3. **Alignment Considerations**: Accounts for buffer alignment requirements in the TOC structure size
4. **Remaining Space**: Subtracts both components from the total available bytes

**Thread Safety:**
The function uses spinlocks to ensure atomic access to the TOC metadata, preventing race conditions when multiple processes are concurrently reading the allocation state.

**Space Accounting:**
The calculation considers that space is consumed from both ends of the segment - TOC entries grow forward from the beginning, while user allocations grow backward from the end. The free space represents the gap between these two growing regions.

## Parameters / Member Variables
- `toc`: Pointer to the shared memory table of contents structure to query

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease (for thread-safe access to TOC metadata)
  - BUFFERALIGN (for calculating aligned TOC structure size)
  - [shm_toc_entry](shm_toc_entry.md) (for TOC entry size calculations)
  - [shm_toc](shm_toc.md) (structure type for offset calculations)

- Called from (representative examples):
  - No references to this symbol (based on current codebase analysis)

## Notes and Other Information
- This function is primarily useful for diagnostic purposes and capacity planning
- The calculation includes alignment overhead to provide an accurate representation of truly available space
- The function provides a point-in-time snapshot of free space; the actual available space may change immediately after the function returns in multi-process environments
- The assertion check ensures internal consistency between allocated bytes and total bytes, helping detect corruption
- Since TOC entries cannot be freed, the free space can only decrease over time as more entries are added and more memory is allocated
- The function is read-only and does not modify the TOC state, making it safe to call for monitoring purposes

## Simplified Source

```c
Size shm_toc_freespace(shm_toc *toc) {
    // Calculate remaining free space in shared memory segment
    volatile shm_toc *vtoc = toc;
    Size total_bytes, allocated_bytes, nentry, toc_bytes;

    // Thread-safe read of TOC metadata
    SpinLockAcquire(&toc->toc_mutex);
    total_bytes = vtoc->toc_total_bytes;
    allocated_bytes = vtoc->toc_allocated_bytes;
    nentry = vtoc->toc_nentry;
    SpinLockRelease(&toc->toc_mutex);

    // Calculate space used by TOC structure and entries
    toc_bytes = offsetof(shm_toc, toc_entry) + nentry * sizeof(shm_toc_entry);

    Assert(allocated_bytes + BUFFERALIGN(toc_bytes) <= total_bytes);

    // Return remaining free space
    return total_bytes - (allocated_bytes + BUFFERALIGN(toc_bytes));
}
```