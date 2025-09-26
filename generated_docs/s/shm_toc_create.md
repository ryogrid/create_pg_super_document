# shm_toc_create

## Location
src/backend/storage/ipc/shm_toc.c: 40 - 63

## Overview
Initializes a region of shared memory with a table of contents structure, setting up the fundamental metadata for shared memory object tracking and allocation.

## Definition

```c
shm_toc *
shm_toc_create(uint64 magic, void *address, Size nbytes)
```
## Detailed Description
The  function creates and initializes a shared memory table of contents (TOC) structure at the beginning of a designated shared memory region. This TOC serves as a directory for tracking allocated objects within the shared memory segment, enabling multiple processes to locate and access shared data structures efficiently.

The function performs several key initialization tasks:
- Sets up the magic number for validation purposes
- Initializes a spinlock for thread-safe access to the TOC
- Calculates and stores the total usable bytes (buffer-aligned)
- Resets allocation counters and entry counts

The implementation ensures that the starting allocation address is buffer-aligned, which is critical for the alignment logic used in subsequent allocation operations.

## Parameters / Member Variables
- : A 64-bit magic number used to validate the TOC structure and detect corruption
- : Pointer to the start of the shared memory region where the TOC will be created
- : Total size of the shared memory region in bytes

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockInit (for thread-safe TOC access)
  - BUFFERALIGN_DOWN (for memory alignment calculations)
  - shm_toc (return type and internal structure)

- Called from (representative examples):
  - GetSessionDsmHandle (src/backend/access/common/session.c:112)
  - InitializeParallelDSM (src/backend/access/transam/parallel.c:325, 332)
  - pa_setup_dsm (src/backend/replication/logical/applyparallelworker.c:361)
  - setup_dynamic_shared_memory (src/test/modules/test_shm_mq/setup.c:132)

## Notes and Other Information
- The function assumes that the provided nbytes is larger than the minimum size required for the shm_toc structure header
- Buffer alignment is critical for performance and correctness of subsequent memory allocations
- The magic number serves as both a validation mechanism and a way to identify different types of shared memory regions
- This is typically the first function called when setting up a new shared memory segment for inter-process communication