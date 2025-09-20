# PGSemaphoreData

## Location
[src/backend/port/sysv_sema.c:30-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_sema.c#L30-L34)

## Overview
PGSemaphoreData is a structure that serves as the concrete implementation of PostgreSQL's platform-independent semaphore abstraction on POSIX systems using POSIX semaphores.

## Definition

```c
typedef struct PGSemaphoreData
{
	int			semId;			/* semaphore set identifier */
	int			semNum;			/* semaphore number within set */
} PGSemaphoreData;
```
## Detailed Description
PGSemaphoreData is the actual data structure that implements PostgreSQL semaphores on POSIX platforms. It wraps a padded semaphore structure to ensure proper cache line alignment and avoid false sharing in multi-processor environments. This structure is part of PostgreSQL's platform abstraction layer for semaphores, providing a uniform interface across different operating systems while optimizing for performance on each platform.

The structure is designed to be opaque to platform-independent code - only the platform-specific semaphore implementation should access its contents directly. The padding ensures that each semaphore occupies a full cache line, preventing performance degradation due to cache line contention between different semaphores.

The typedef  is defined as a pointer to this structure (), providing the public interface that the rest of PostgreSQL uses for semaphore operations.

## Parameters / Member Variables
- `semId`: Semaphore set identifier for System V semaphores
- `semNum`: Semaphore number within the semaphore set

## Dependencies
- Types referenced:
  - SemTPadded (union containing sem_t and padding)
  - sem_t (POSIX semaphore type)
  - PG_CACHE_LINE_SIZE (PostgreSQL cache line size constant)

- Referenced by:
  - [PGSemaphoreShmemSize](PGSemaphoreShmemSize.md) (calculates shared memory needed for semaphores)
  - [IpcSemaphoreCreate](../I/IpcSemaphoreCreate.md) (creates semaphores in System V implementation)
  - [PGSemaphore](PGSemaphore.md) (typedef pointer to this structure)

## Notes and Other Information
- This implementation is specific to POSIX semaphore-based platforms (when USE_WIN32_SEMAPHORES is not defined)
- The structure is designed to live in shared memory, with each instance cache-aligned to prevent false sharing
- A helper macro  is provided to access the underlying sem_t from a PGSemaphore pointer
- The structure supports both named and unnamed POSIX semaphores, though unnamed semaphores are preferred
- On Windows platforms, PGSemaphore is simply a HANDLE rather than a pointer to this structure
- The implementation cannot be used with named POSIX semaphores when EXEC_BACKEND is enabled, as the semaphore structures must be accessible across exec'd processes
- File location: src/backend/port/posix_sema.c:52-55