# AnonymousShmemDetach

## Location
[src/backend/port/sysv_shmem.c:675-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L675-L699)

## Overview
Detaches from an anonymous memory-mapped shared memory block, serving as a cleanup callback for process exit handling.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function serves as a cleanup callback registered with the on_shmem_exit system to properly detach from anonymous shared memory segments when a PostgreSQL process terminates. It checks if an anonymous shared memory block is currently mapped (AnonymousShmem != NULL) and calls munmap() to release it. If the munmap() call fails, it logs an error message but continues execution since this is cleanup code that shouldn't cause process termination.

The function follows PostgreSQL's exit callback pattern, accepting status and argument parameters even though they are not used in this implementation. After successfully unmapping the memory, it sets the global AnonymousShmem pointer to NULL to prevent double-free scenarios.

## Parameters / Member Variables
- : Exit status code (unused in this implementation)
- : Datum argument passed to callback (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - munmap (system call)
  - elog
- Global variables accessed:
  - AnonymousShmem
  - AnonymousShmemSize
- Called from (representative examples):
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md) (registers as callback)

## Notes and Other Information
- Static function only used within sysv_shmem.c
- Registered as an on_shmem_exit callback for process cleanup
- Uses LOG level for munmap() failure messages rather than FATAL to avoid termination during cleanup
- Sets AnonymousShmem to NULL after successful munmap() to prevent double-free
- Part of PostgreSQL's resource cleanup framework for proper shared memory management
- Handles only anonymous (mmap-based) shared memory, not System V shared memory segments