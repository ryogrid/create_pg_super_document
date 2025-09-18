# InternalIpcMemoryCreate

## Location
[src/backend/port/sysv_shmem.c:121-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L121-L285)

## Overview
InternalIpcMemoryCreate is a static function that creates a new System V shared memory segment with a specified key and attaches it to the current process.

## Definition
```c
static void *InternalIpcMemoryCreate(IpcMemoryKey memKey, Size size)
```

## Detailed Description
This function attempts to create a new shared memory segment using the System V IPC mechanism. It performs the following key operations:

1. **Memory Key Creation**: Uses shmget() with IPC_CREAT | IPC_EXCL flags to create a new segment
2. **Collision Handling**: Returns NULL if a segment with the same key already exists
3. **Error Recovery**: Implements special logic for BSD-derived kernels that may return EINVAL instead of EEXIST
4. **Memory Attachment**: Attaches the created segment to the current process using shmat()
5. **Cleanup Registration**: Registers exit callbacks to properly detach and delete the segment
6. **Lock File Updates**: Records the shared memory key and ID in the data directory lock file

The function includes platform-specific handling for EXEC_BACKEND builds, allowing users to specify the memory address via the PG_SHMEM_ADDR environment variable, with special default handling for macOS to work around ASLR issues.

## Parameters / Member Variables
- `memKey`: The IPC key to use for creating the shared memory segment
- `size`: The size in bytes of the shared memory segment to create

## Dependencies
- Functions called/Symbols referenced:
  - shmget
  - shmat
  - shmctl
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [IpcMemoryDelete](IpcMemoryDelete.md)
  - [IpcMemoryDetach](IpcMemoryDetach.md)
  - [AddToDataDirLockFile](../A/AddToDataDirLockFile.md)
  - ereport/elog (error reporting)
- Called from (representative examples):
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md)

## Notes and Other Information
- This is a static function, only accessible within the sysv_shmem.c file
- Implements robust error handling with detailed error messages for common shared memory configuration issues
- Handles platform-specific differences between BSD and Linux systems
- Automatically registers cleanup callbacks to ensure proper resource management
- Fails fatally on unrecoverable errors (except for expected collisions with existing segments)
- The function is designed to work around various kernel quirks, particularly on BSD-derived systems