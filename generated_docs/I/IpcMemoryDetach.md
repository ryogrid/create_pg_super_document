# IpcMemoryDetach

## Location
[src/backend/port/sysv_shmem.c:286-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L286-L297)

## Overview
IpcMemoryDetach is a static callback function that detaches a System V shared memory segment from the current process's address space.

## Definition
```c
static void IpcMemoryDetach(int status, Datum shmaddr)
```

## Detailed Description
This function serves as an on_shmem_exit callback that safely detaches a shared memory segment from the process. It is designed to be called automatically during process cleanup to ensure that shared memory segments are properly detached before the process terminates. The function uses the System V shmdt() system call to perform the detachment operation.

The function logs any errors that occur during detachment but does not treat them as fatal, since the process may already be in an error state during shutdown.

## Parameters / Member Variables
- `status`: Exit status (unused, required by on_shmem_exit callback signature)
- `shmaddr`: Datum containing the address of the shared memory segment to detach

## Dependencies
- Functions called/Symbols referenced:
  - shmdt (System V IPC function)
  - [DatumGetPointer](../D/DatumGetPointer.md) (PostgreSQL datum conversion)
  - elog (PostgreSQL logging)
- Called from (representative examples):
  - [InternalIpcMemoryCreate](InternalIpcMemoryCreate.md) (via on_shmem_exit registration)

## Notes and Other Information
- This is a static function, only accessible within the sysv_shmem.c file
- Designed specifically as an on_shmem_exit callback with the required signature
- Uses LOG level for error reporting rather than FATAL, allowing graceful shutdown even if detachment fails
- The function converts the Datum parameter back to a pointer using DatumGetPointer()
- Automatically registered by InternalIpcMemoryCreate to ensure cleanup occurs during process exit