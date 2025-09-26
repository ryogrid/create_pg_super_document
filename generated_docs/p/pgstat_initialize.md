# pgstat_initialize

## Location
[src/backend/utils/activity/pgstat.c:537-578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L537-L578)

## Overview
Initializes the PostgreSQL statistics system for a backend process by attaching to shared memory and setting up cleanup hooks. This function establishes the foundation for statistics collection in each backend.

## Definition

```c
void
pgstat_initialize(void)
```
## Detailed Description
This function performs the essential initialization of the PostgreSQL statistics system for an individual backend process. It is called early in backend startup from  to establish the necessary infrastructure for statistics collection and reporting throughout the backend's lifetime.

The initialization process consists of several key steps:

1. **Shared memory attachment**: Calls  to connect the backend to the statistics shared memory area. This includes attaching to the DSA (Dynamic Shared Area) and the shared hash table used for storing statistics entries.

2. **WAL statistics initialization**: Calls  to initialize WAL usage tracking by setting up baseline counters for measuring WAL activity.

3. **Exit hook registration**: Registers  as a cleanup function to be called before shared memory exit. This ensures proper cleanup of statistics resources when the backend terminates.

4. **State marking**: In debug builds, marks the statistics system as initialized to enable assertion checking for proper usage.

The function includes a safety assertion to prevent double initialization and operates before the database ID is set, which is why the shutdown hook must handle cases where  may not be valid.

The initialization is crucial for enabling all subsequent statistics operations in the backend, including database activity tracking, performance monitoring, and resource usage statistics.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_attach_shmem](pgstat_attach_shmem.md)
  - [pgstat_init_wal](pgstat_init_wal.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
  - [pgstat_shutdown_hook](pgstat_shutdown_hook.md)
- Called from (representative examples):
  - [BaseInit](../B/BaseInit.md) (src/backend/utils/init/postinit.c:668)

## Notes and Other Information
- Called early in backend initialization from  
- Must be called before  is set, affecting shutdown hook behavior
- Establishes DSA and shared hash table connections for statistics storage
- Registers cleanup hooks to ensure proper resource management on exit
- Required for all subsequent statistics operations in the backend
- In debug builds, enables assertion checking for proper statistics system usage
- The shared memory attachment persists for the entire backend lifetime
- WAL usage tracking is initialized to enable delta calculations
- The function is located in src/backend/utils/activity/pgstat.c:537-578