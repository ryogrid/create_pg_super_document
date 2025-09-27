# ReleaseLruFiles

## Location
[src/backend/storage/file/fd.c:1401-1410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1401-L1410)

## Overview
Releases multiple kernel file descriptors by closing least-recently-used files until the system is under the safe file descriptor limit.

## Definition

```c
static void
ReleaseLruFiles(void)
```
## Detailed Description
ReleaseLruFiles implements a bulk file descriptor release mechanism that ensures PostgreSQL stays within safe file descriptor limits before attempting to open new files. The function continuously releases LRU files until the total count of file descriptors (including PostgreSQL VFDs, allocated descriptors, and external FDs) falls below the max_safe_fds threshold.

The function operates in a loop that:
1. Checks if the total FD usage (nfile + numAllocatedDescs + numExternalFDs) exceeds or equals max_safe_fds
2. If over the limit, calls ReleaseLruFile() to close one least-recently-used file
3. Continues until either under the safe limit or no more files can be released
4. Breaks the loop if ReleaseLruFile() returns false (no more files available to close)

This proactive approach prevents file descriptor exhaustion and ensures that subsequent file open operations have a higher probability of success.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - nfile (global counter of currently open VFD files)
  - numAllocatedDescs (count of allocated file descriptors)
  - numExternalFDs (count of external file descriptors)
  - max_safe_fds (system-determined safe limit for file descriptors)
  - [ReleaseLruFile](ReleaseLruFile.md) (function to release one LRU file)

- Called from (representative examples):
  - AllocateDesc (when allocating new descriptors)
  - [ReserveExternalFD](ReserveExternalFD.md) (when reserving external file descriptors)
  - [LruInsert](../L/LruInsert.md) (before attempting to reopen files)
  - [PathNameOpenFilePerm](../P/PathNameOpenFilePerm.md) (before opening files with permissions)
  - [AllocateFile](../A/AllocateFile.md) (when allocating FILE* structures)
  - [OpenTransientFilePerm](../O/OpenTransientFilePerm.md) (when opening transient files)
  - [OpenPipeStream](../O/OpenPipeStream.md) (when opening pipe streams)
  - [AllocateDir](../A/AllocateDir.md) (when allocating directory handles)

## Notes and Other Information
- This is a static function internal to the file descriptor management module
- The function is proactive - it's called before operations that might need file descriptors, not after failures
- Critical for preventing PostgreSQL from hitting OS file descriptor limits
- The loop will terminate either when under the safe limit or when no more files can be released
- Works in conjunction with PostgreSQL's virtual file descriptor system to manage resource constraints
- The safe limit accounts for descriptors used by different subsystems (VFDs, allocated descriptors, external FDs)

## Simplified Source

```c
// Simplified version of ReleaseLruFiles
static void ReleaseLruFiles(void) {
    // Keep releasing LRU files until we're under the safe limit
    while (total_file_descriptors >= max_safe_fds) {
        // Try to release one least-recently-used file
        if (!ReleaseLruFile()) {
            // No more files available to release, stop trying
            break;
        }
    }

    // Note: total_file_descriptors = nfile + numAllocatedDescs + numExternalFDs
}
```

Key simplifications made:
- Replaced complex condition with descriptive variable name comment
- Added explanatory comments for each step
- Clarified the break condition with a comment
- Made the purpose of the loop more explicit