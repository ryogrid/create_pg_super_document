# RequestNamedLWLockTranche

## Location
src/backend/storage/lmgr/lwlock.c: 672 - 708

## Overview
Requests allocation of additional lightweight locks during postmaster startup for extensions loaded via shared_preload_libraries.

## Definition
```c
void RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
```

## Detailed Description
This function allows shared libraries loaded during postmaster startup to request additional lightweight locks that will be allocated in shared memory. It is a critical component of PostgreSQL's extension infrastructure, enabling modules to define their own synchronization primitives.

The function maintains an array of requests that will be processed during shared memory initialization. It enforces strict timing constraints - requests can only be made during the shmem_request_hook phase of postmaster startup, ensuring that all lock requirements are known before shared memory is allocated.

Key behaviors include:
- Validates that the function is called only during the appropriate startup phase
- Dynamically manages a request array using power-of-2 allocation strategy
- Stores both the tranche name and the number of locks needed
- Enforces PostgreSQL's naming length limits (NAMEDATALEN)

## Parameters / Member Variables
- `tranche_name`: The name for the tranche (will be visible in wait events), must be <= NAMEDATALEN characters
- `num_lwlocks`: The number of lightweight locks to allocate for this tranche

## Dependencies
- Functions called/Symbols referenced:
  - NamedLWLockTrancheRequest (struct type)
  - MemoryContextAlloc (memory allocation)
  - pg_nextpower2_32 (memory allocation utility)
  - repalloc (memory reallocation)
  - NAMEDATALEN (constant for name length limit)
  - strlcpy (string copy function)
- Called from (representative examples):
  - Extensions loaded via shared_preload_libraries during shmem_request_hook

## Notes and Other Information
- FATAL error if called outside of shmem_request_hook phase - this timing restriction is crucial for proper shared memory management
- The tranche name becomes user-visible in wait event monitoring, so should follow PostgreSQL naming conventions
- Uses power-of-2 allocation strategy to minimize memory fragmentation and reallocation overhead
- Requests are stored in a global array that gets processed during CreateLWLocks()
- Part of PostgreSQL's extension mechanism for requesting shared resources during startup