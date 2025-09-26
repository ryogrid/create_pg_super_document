# PgStat_LocalState

## Location
[src/include/utils/pgstat_internal.h:491-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L491-L499)

## Overview
PgStat_LocalState represents the backend-local state for PostgreSQL's statistics system, providing each backend process with access to shared memory statistics and maintaining a local statistics snapshot.

## Definition
```c
typedef struct PgStat_LocalState
{
    PgStat_ShmemControl *shmem;
    dsa_area   *dsa;
    dshash_table *shared_hash;
    
    /* the current statistics snapshot */
    PgStat_Snapshot snapshot;
} PgStat_LocalState;
```

## Detailed Description
This structure maintains the per-backend local state for accessing PostgreSQL's statistics system. Each backend process has its own instance of this structure that provides access to the shared memory statistics infrastructure while maintaining a local cached snapshot for efficient repeated access. The structure bridges the gap between the shared memory statistics system and individual backend processes.

The local state includes pointers to the shared memory control structure, the dynamic shared area for variable statistics, and the shared hash table containing statistics for database objects. It also maintains a current statistics snapshot that can be used for consistent reads of related statistics without repeatedly accessing shared memory.

## Parameters / Member Variables
- `shmem`: Pointer to the shared memory control structure (PgStat_ShmemControl) that manages the global statistics system
- `dsa`: Pointer to the dynamic shared area used for managing variable-sized statistics storage
- `shared_hash`: Pointer to the shared hash table containing statistics for variable-numbered objects (databases, tables, functions, etc.)
- `snapshot`: Current statistics snapshot (PgStat_Snapshot) cached locally for efficient access to consistent statistics data

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_ShmemControl](PgStat_ShmemControl.md)
  - [dsa_area](../d/dsa_area.md)
  - [dshash_table](../d/dshash_table.md)
  - [PgStat_Snapshot](PgStat_Snapshot.md)
- Called from (representative examples):
  - SH_DECLARE (hash table declarations in pgstat.c)

## Notes and Other Information
- Located in src/include/utils/pgstat_internal.h:491-499
- Provides per-backend access to the global statistics system in shared memory
- Each backend process maintains its own instance of this structure
- Essential for bridging shared memory statistics with local backend operations
- The cached snapshot enables efficient repeated access to statistics without continuous shared memory access
- Critical component for PostgreSQL's statistics infrastructure, ensuring each backend can efficiently access and work with system statistics
- The structure design separates shared memory access concerns from local caching and usage patterns
- Supports PostgreSQL's multi-process architecture by providing clean per-backend interfaces to shared statistics