# PGSharedMemoryDetach

## Location
[src/backend/port/sysv_shmem.c:970-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L970-L991)

## Overview
Detaches the current process from the shared memory segment, designed primarily for subprocesses that have inherited an attachment and need to clean up their connection.

## Definition

```c
void
PGSharedMemoryDetach(void)
```
## Detailed Description
This function safely detaches the current process from any attached shared memory segments. It is specifically designed for subprocess cleanup rather than being called by the process that originally created the shared memory segment (which uses on_shmem_exit callbacks for cleanup).

The function handles two types of shared memory:
1. **System V shared memory**: Detaches using shmdt() system call from the segment identified by UsedShmemSegAddr
2. **Anonymous shared memory**: Unmaps using munmap() system call for memory mapped via mmap()

Special handling is included for Cygwin systems in EXEC_BACKEND configurations, where a workaround is applied for a cygipc exec bug by attempting shmdt(NULL) if the normal detachment fails.

The function is designed to be safe to call multiple times - it checks if attachments exist before attempting to detach and sets pointers to NULL after successful detachment.

## Parameters / Member Variables
This function takes no parameters but operates on global variables:
- : Address of the attached System V shared memory segment
- : System V shared memory segment identifier (implicit)
- : Address of anonymous shared memory mapping
- : Size of the anonymous shared memory mapping

## Dependencies
- Functions called/Symbols referenced:
  - shmdt() (system call for System V shared memory detachment)
  - munmap() (system call for anonymous memory unmapping)
  - elog() (error logging)
- Called from:
  - [PGSharedMemoryReAttach](PGSharedMemoryReAttach.md) (for Cygwin cleanup)
  - [PGSharedMemoryNoReAttach](PGSharedMemoryNoReAttach.md) (for Cygwin cleanup)
  - [pgwin32_SharedMemoryDelete](../p/pgwin32_SharedMemoryDelete.md) (Windows version)
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (process cleanup)

## Notes and Other Information
- Not intended for use by the process that created the shared memory segment originally
- Safe to call multiple times due to NULL pointer checks
- Includes Cygwin-specific workaround for cygipc exec bug in EXEC_BACKEND mode
- Handles both System V and anonymous shared memory types
- Errors during detachment are logged but not fatal
- Sets global pointers to NULL after successful detachment to prevent double-free issues
- Critical for proper cleanup in multi-process PostgreSQL architectures