# StatsShmemInit

## Location
[src/backend/utils/activity/pgstat_shmem.c:141-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L141-L217)

## Overview
Function that initializes the PostgreSQL cumulative statistics system during startup, creating shared memory structures and dynamic shared hash tables for statistics collection.

## Definition

```c
void
StatsShmemInit(void)
```
## Detailed Description
This function initializes the shared memory statistics system during PostgreSQL startup. It creates and initializes the main statistics control structure in shared memory, sets up a dynamic shared area (DSA) for the statistics hash table, and initializes various locks for different statistics components. The function behaves differently in postmaster vs. backend processes - the postmaster creates all structures while backends just attach to existing ones.

## Parameters / Member Variables
- No parameters (void function)
- No return value

## Dependencies
- Functions called/Symbols referenced:
  - StatsShmemSize: Gets total shared memory size needed
  - ShmemInitStruct: Creates or attaches to shared memory structure
  - dsa_create_in_place: Creates DSA in predetermined memory location
  - dsa_pin/dsa_detach: DSA lifecycle management
  - dsa_set_size_limit: Controls DSA growth limits
  - dshash_create: Creates dynamic shared hash table
  - dshash_get_hash_table_handle/dshash_detach: Hash table management
  - pg_atomic_init_u64: Initializes atomic counter for garbage collection
  - LWLockInitialize: Initializes lightweight locks for various stats components
  - pgstat_dsa_init_size: Gets DSA initialization size
- Called from (representative examples):
  - CreateOrAttachShmemStructs: Called during shared memory initialization

## Notes and Other Information
- Only the postmaster process (IsUnderPostmaster == false) creates the structures
- Backend processes just attach to existing shared memory structures
- Creates DSA in "plain" shared memory to avoid DSM dependency in postmaster
- Temporarily limits DSA size during hash table creation to ensure placement in plain shared memory
- Initializes locks for: archiver, bgwriter, checkpointer, SLRU, WAL, and I/O statistics
- Uses LWTRANCHE_PGSTATS_DSA for DSA locks and LWTRANCHE_PGSTATS_DATA for statistics data locks
- Postmaster detaches from DSA/hash table references after creation since it won't access them again
- Initializes garbage collection request counter to 1