# PGSemaphoreCreate

## Location
[src/backend/port/posix_sema.c:262-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L262-L294)

## Overview
Allocates and initializes a new PostgreSQL semaphore structure with an initial count of 1.

## Definition

```c
PGSemaphore
PGSemaphoreCreate(void)
```
## Detailed Description
PGSemaphoreCreate is responsible for creating a new counting semaphore within PostgreSQL's semaphore management system. The function allocates a PGSemaphore structure and initializes it with a count of 1, making it immediately available for use. The implementation varies depending on the platform's semaphore support - it can use either named POSIX semaphores or unnamed POSIX semaphores stored in shared memory.

The function enforces that it can only be called from the postmaster process (not from backend processes) since semaphore allocation involves managing global state. It also maintains a count of allocated semaphores and will panic if the maximum number of semaphores is exceeded.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsUnderPostmaster (assertion check)
  - elog (error reporting with PANIC level)
  - [PosixSemaphoreCreate](PosixSemaphoreCreate.md) (platform-specific semaphore creation)
  - PG_SEM_REF (macro for getting semaphore reference)
- Called from (representative examples):
  - InitProcGlobal (process management initialization)
  - SpinlockSemaInit (spinlock semaphore initialization)

## Notes and Other Information
- Must be called only from the postmaster process, not from backend processes
- The created semaphore starts with count 1, meaning it's immediately available for locking
- The function will panic if the maximum number of semaphores (maxSems) is exceeded
- On systems with USE_NAMED_POSIX_SEMAPHORES, the semaphore pointer is stored in mySemPointers array for cleanup
- The global variable numSems is incremented to track the total number of allocated semaphores
- This is part of PostgreSQL's platform-independent semaphore API defined in pg_sema.h