# ShmemBackendArrayAllocation

## Location
[src/backend/postmaster/postmaster.c:4556-4565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4556-L4565)

## Overview
Allocates shared memory for the backend process array and initializes all slots as empty during PostgreSQL startup.

## Definition

```c
void
ShmemBackendArrayAllocation(void)
```
## Detailed Description
This function is responsible for allocating shared memory space for the ShmemBackendArray, which tracks active backend processes in PostgreSQL. It calculates the required size using ShmemBackendArraySize(), allocates the memory using ShmemAlloc(), and initializes all slots to zero to mark them as empty. This initialization is crucial for the postmaster's process management system.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemBackendArraySize](ShmemBackendArraySize.md) (calculates required array size)
  - [ShmemAlloc](ShmemAlloc.md) (allocates shared memory)
  - [Backend](../B/Backend.md) (data structure type)
- Called from:
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md) (src/backend/storage/ipc/ipci.c:252)

## Notes and Other Information
- This function is part of the shared memory initialization process during PostgreSQL startup
- The allocated array is used by the postmaster to track and manage backend processes
- All slots are zeroed out initially to indicate they are available for new backend processes
- Located in src/backend/postmaster/postmaster.c:4556-4565