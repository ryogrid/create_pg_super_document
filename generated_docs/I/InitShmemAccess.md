# InitShmemAccess

## Location
[src/backend/storage/ipc/shmem.c:100-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L100-L114)

## Overview
InitShmemAccess initializes basic pointers to shared memory by setting up global variables that track the shared memory segment header, base address, and end address.

## Definition
```c
void InitShmemAccess(void *seghdr)
```

## Detailed Description
InitShmemAccess is a fundamental initialization function in PostgreSQL's shared memory management system. It takes a pointer to a shared memory segment header and sets up three critical global variables that are used throughout the system to manage shared memory:

- **ShmemSegHdr**: Points to the shared memory segment header structure
- **ShmemBase**: Points to the start address of the shared memory segment  
- **ShmemEnd**: Points to the end+1 address of the shared memory segment

The function performs a simple but essential role in establishing the memory boundaries and header reference needed by other shared memory allocation and management functions. The parameter is declared as void* to avoid including ipc.h in shmem.h, but internally it's cast to PGShmemHeader*.

## Parameters / Member Variables
- `seghdr`: A void pointer to the shared memory segment header (internally cast to PGShmemHeader*). This contains metadata about the shared memory segment including its total size.

## Dependencies
- Functions called/Symbols referenced:
  - [PGShmemHeader](../P/PGShmemHeader.md) (type cast)
- Called from (representative examples):
  - [SubPostmasterMain](../S/SubPostmasterMain.md)
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)

## Notes and Other Information
- This function must be called early in the PostgreSQL startup process before any shared memory allocation operations can take place
- The void* parameter type is used as a design choice to minimize header dependencies between shmem.h and ipc.h
- The function sets up the fundamental memory boundaries that are used by subsequent shared memory allocation functions like ShmemAlloc
- Located in src/backend/storage/ipc/shmem.c:100-114