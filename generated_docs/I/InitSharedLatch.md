# InitSharedLatch

## Location
[src/backend/storage/ipc/latch.c:430-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L430-L462)

## Overview
Initializes a shared latch that can be accessed and set from multiple processes, designed for inter-process communication and synchronization.

## Definition
```c
void InitSharedLatch(Latch *latch)
```

## Detailed Description
InitSharedLatch initializes a latch structure for shared memory usage across multiple processes. Unlike process-local latches, shared latches start with no owner and must be explicitly associated with a process using OwnLatch. On Windows, it creates an inheritable event object that child processes can access. The function must be called in the postmaster process before forking children, typically right after allocating the shared memory block. The latch is marked as shared and initially unset with no sleeping processes.

## Parameters / Member Variables
- `latch`: Pointer to the Latch structure in shared memory to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md) (structure type)
  - SECURITY_ATTRIBUTES (Windows-specific)
- Called from (representative examples):
  - [XLogRecoveryShmemInit](../X/XLogRecoveryShmemInit.md)
  - InitProcGlobal

## Notes and Other Information
- Must be called in postmaster before forking child processes
- On Windows, creates inheritable event objects for cross-process access
- Unix implementations don't require special initialization but Windows does
- The latch starts with owner_pid = 0, indicating no current owner
- Designed for use in shared memory structures allocated with ShmemInitStruct
- Other handles in the latch module are never marked as inheritable for security