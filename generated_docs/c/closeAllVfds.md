# closeAllVfds

## Location
[src/backend/storage/file/fd.c:3017-3045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3017-L3045)

## Overview
Forces all Virtual File Descriptors (VFDs) into the physically-closed state to minimize the number of kernel file descriptors in use, while preserving their logical state.

## Definition

```c
void
closeAllVfds(void)
```
## Detailed Description
The  function is a resource management utility in PostgreSQL's file descriptor management system. It iterates through all VFDs in the cache and forces them into a physically-closed state by removing them from the LRU (Least Recently Used) list. This operation is typically performed when the system needs to free up kernel file descriptors without losing the logical file state information.

The function ensures that the VFD ring structure remains intact by asserting that the first entry (index 0) is not open, which serves as a sentinel in the ring structure. It then processes all other VFDs (starting from index 1) and calls  on any that are currently open, effectively closing their underlying file descriptors while maintaining their metadata.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if a VFD is in the closed state
  -  - Removes a VFD from the LRU list and closes its file descriptor
  -  (global variable) - Size of the VFD cache array

- Called from (representative examples):
  -  (src/backend/tcop/utility.c:844)
  - Referenced in  context (src/include/storage/fd.h:168)

## Notes and Other Information
- This function is critical for resource management, particularly during operations that require many file descriptors
- The logical state of VFDs is preserved, meaning files can be reopened later without losing their position or other metadata
- The function assumes the VFD cache is properly initialized and maintains ring structure integrity
- Performance impact is proportional to the number of open VFDs in the cache

## Simplified Source

```c
void closeAllVfds(void) {
    Index i;

    if (SizeVfdCache > 0) {
        // Verify ring structure integrity
        Assert(FileIsNotOpen(0));

        // Close all VFDs except the sentinel (index 0)
        for (i = 1; i < SizeVfdCache; i++) {
            if (!FileIsNotOpen(i))
                LruDelete(i);
        }
    }
}
```