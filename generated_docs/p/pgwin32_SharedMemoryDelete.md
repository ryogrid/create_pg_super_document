# pgwin32_SharedMemoryDelete

## Location
[src/backend/port/win32_shmem.c:549-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32_shmem.c#L549-L572)

## Overview
A Windows-specific callback function that detaches from and cleans up the shared memory segment during process shutdown.

## Definition
```c
static void pgwin32_SharedMemoryDelete(int status, Datum shmId)
```

## Detailed Description
This function serves as an on_shmem_exit callback specifically for Windows platforms in PostgreSQL's shared memory management system. It provides a clean shutdown mechanism by ensuring proper detachment from shared memory segments when a process terminates. The function follows the standard callback signature required by PostgreSQL's exit callback system.

The function performs a simple validation check to ensure the provided shared memory ID matches the currently used segment, then delegates the actual detachment work to the cross-platform PGSharedMemoryDetach function. This design maintains consistency with the broader shared memory management architecture while providing Windows-specific integration.

## Parameters / Member Variables
- `status`: Exit status code (standard for exit callbacks, not used in this implementation)
- `shmId`: Datum containing the shared memory segment ID to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [PGSharedMemoryDetach](../P/PGSharedMemoryDetach.md) (performs the actual detachment)
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro for extracting pointer from Datum)
  - Assert (for validation checking)
- Called from (representative examples):
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md) (registered as exit callback)

## Notes and Other Information
- Windows-specific implementation (part of win32_shmem.c)
- Declared as static, indicating internal use within the win32_shmem module
- Uses the standard PostgreSQL exit callback signature (int status, Datum arg)
- Includes an assertion to verify the shared memory ID matches the expected value
- Does not directly handle the Windows-specific cleanup but delegates to the cross-platform detach function
- Part of PostgreSQL's platform abstraction layer for shared memory management

## Simplified Source

```c
static void pgwin32_SharedMemoryDelete(int status, Datum shmId) {
    // Verify we're cleaning up the correct shared memory segment
    Assert(DatumGetPointer(shmId) == UsedShmemSegID);

    // Detach from shared memory segment
    PGSharedMemoryDetach();
}
```