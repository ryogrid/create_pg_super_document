# AttachSharedMemoryStructs

## Location
[src/backend/storage/ipc/ipci.c:178-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipci.c#L178-L198)

## Overview
Initializes a child process's access to existing shared memory structures in EXEC_BACKEND mode.

## Definition


## Detailed Description
AttachSharedMemoryStructs is responsible for enabling child processes to access and utilize shared memory structures that were previously created by the postmaster. This function is specifically designed for EXEC_BACKEND mode, where child processes are started via exec() rather than fork(), requiring explicit attachment to shared memory segments.

The function performs critical validation checks to ensure the process is properly initialized (MyProc is set) and is indeed running under a postmaster. It then calls CreateOrAttachShmemStructs() to establish connections to all core PostgreSQL shared memory structures. Finally, it provides an opportunity for loadable modules to initialize their shared memory allocations through the shmem_startup_hook.

In non-EXEC_BACKEND builds, this functionality is not needed since shared memory access is inherited through fork().

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (debugging assertions)
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (core shared memory structure attachment)
  - shmem_startup_hook (extension hook for shared memory initialization)
- Called from (representative examples):
  - InitProcess (regular backend process initialization)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (auxiliary process initialization)

## Notes and Other Information
- Only relevant in EXEC_BACKEND mode (Windows and some other platforms)
- Requires InitProcess to have been called previously (MyProc must be set)
- Must be called by child processes running under the postmaster
- Provides extension hook mechanism for modules to initialize shared memory access
- In fork-based systems, shared memory access is inherited and this function is not needed
- Critical for proper child process initialization in exec-based architectures