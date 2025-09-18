# IpcMemoryDelete

## Location
src/backend/port/sysv_shmem.c: 298 - 316

## Overview
IpcMemoryDelete is a static callback function that permanently removes a System V shared memory segment from the system.

## Definition
```c
static void IpcMemoryDelete(int status, Datum shmId)
```

## Detailed Description
This function serves as an on_shmem_exit callback that removes a shared memory segment from the system using the System V IPC_RMID operation. Unlike IpcMemoryDetach which only detaches the segment from the current process, this function completely deletes the segment, making it unavailable to all processes. The function is designed to be called automatically during process cleanup to prevent shared memory leaks.

The deletion is performed using the shmctl() system call with the IPC_RMID command, which marks the segment for destruction. The segment will be destroyed once all processes have detached from it.

## Parameters / Member Variables
- `status`: Exit status (unused, required by on_shmem_exit callback signature)
- `shmId`: Datum containing the System V shared memory identifier to delete

## Dependencies
- Functions called/Symbols referenced:
  - shmctl (System V IPC function)
  - [DatumGetInt32](../D/DatumGetInt32.md) (PostgreSQL datum conversion)
  - elog (PostgreSQL logging)
  - IPC_RMID (System V IPC constant)
- Called from (representative examples):
  - [InternalIpcMemoryCreate](InternalIpcMemoryCreate.md) (via on_shmem_exit registration)

## Notes and Other Information
- This is a static function, only accessible within the sysv_shmem.c file
- Designed specifically as an on_shmem_exit callback with the required signature
- Uses LOG level for error reporting rather than FATAL, allowing graceful shutdown even if deletion fails
- The function converts the Datum parameter to an integer using DatumGetInt32()
- Automatically registered by InternalIpcMemoryCreate to ensure segments are cleaned up during process exit
- The IPC_RMID operation marks the segment for deletion but actual removal occurs when all processes detach