# IpcSemaphoreCreate

## Location
[src/backend/port/sysv_sema.c:229-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_sema.c#L229-L312)

## Overview
Creates a System V IPC semaphore set with the specified number of semaphores, detecting and recycling dead PostgreSQL semaphore sets while avoiding conflicts with non-PostgreSQL applications.

## Definition


## Detailed Description
IpcSemaphoreCreate is a static function that creates a System V IPC semaphore set with intelligent resource management. It allocates one additional semaphore beyond the requested count to serve as an identifier semaphore. The function implements a sophisticated collision detection and recovery mechanism that can identify and reuse semaphore sets left behind by crashed PostgreSQL processes while avoiding conflicts with semaphore sets belonging to other applications.

The function uses a sequential key search algorithm, trying up to 1000 different IPC keys before giving up. For each key, it first attempts to create a new semaphore set. If that fails due to an existing set, it examines the existing set to determine if it belongs to a dead PostgreSQL process by checking the magic number and creator process ID. If the set appears to be abandoned, it removes the old set and creates a new one with the same key.

Once a semaphore set is successfully created, the function marks it as belonging to the current PostgreSQL process by setting the identifier semaphore to the magic value and recording the current process ID.

## Parameters / Member Variables
- : The number of useful semaphores to create (an additional identifier semaphore is allocated automatically)

## Dependencies
- Functions called/Symbols referenced:
  - [InternalIpcSemaphoreCreate](InternalIpcSemaphoreCreate.md)
  - [IpcSemaphoreGetValue](IpcSemaphoreGetValue.md)
  - [IpcSemaphoreGetLastPID](IpcSemaphoreGetLastPID.md)
  - [IpcSemaphoreInitialize](IpcSemaphoreInitialize.md)
  - [PGSemaphoreUnlock](../P/PGSemaphoreUnlock.md)
  - kill
  - semget
  - semctl
- Types referenced:
  - IpcSemaphoreId
  - [PGSemaphoreData](../P/PGSemaphoreData.md)
  - union semun
  - pid_t
- Constants referenced:
  - PGSemaMagic
  - IPC_RMID
- Called from (representative examples):
  - [PGSemaphoreCreate](../P/PGSemaphoreCreate.md)

## Notes and Other Information
- The function is static and only used internally within the sysv_sema.c module
- Uses nextSemaKey as a global counter to generate sequential IPC keys
- Implements a retry mechanism with a maximum of 1000 attempts to find an available key
- The identifier semaphore (at index numSems) is used for PostgreSQL-specific magic number storage
- Process ownership is established through the sempid field of the identifier semaphore
- The function handles race conditions where multiple processes might try to create the same semaphore key simultaneously