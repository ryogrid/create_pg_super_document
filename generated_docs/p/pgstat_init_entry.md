# pgstat_init_entry

## Location
[src/backend/utils/activity/pgstat_shmem.c:267-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L267-L300)

## Overview
Initializes a new shared memory statistics entry with proper reference counting, generation tracking, and DSA memory allocation for PostgreSQL's statistics collection system.

## Definition

```c
PgStatShared_Common *
pgstat_init_entry(PgStat_Kind kind,
				  PgStatShared_HashEntry *shhashent)
```
## Detailed Description
The  function creates and initializes a new shared memory statistics entry for PostgreSQL's statistics collection framework. It performs several critical initialization tasks:

1. **Reference counting**: Initializes the reference count to 1, marking the entry as valid and preventing it from being freed during initialization
2. **Generation tracking**: Sets the generation counter to 0 for freshly created entries 
3. **Memory allocation**: Allocates DSA (Dynamic Shared Area) memory based on the statistics kind's shared size requirements
4. **Lock initialization**: Sets up an LWLock for concurrent access protection
5. **Entry linking**: Links the newly allocated memory chunk to the hash entry

The function ensures atomicity by holding the dshash partition lock during initialization, preventing the entry from being found or accessed until fully initialized.

## Parameters / Member Variables
- : The type of statistics entry being created (PgStat_Kind), which determines the size and structure of the allocated memory
- : Pointer to the shared hash entry that will contain the reference to the newly initialized statistics entry

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_init_u32
  - pgstat_get_kind_info
  - dsa_allocate0
  - dsa_get_address
  - LWLockInitialize
- Called from (representative examples):
  - pgstat_read_statsfile
  - pgstat_get_entry_ref

## Notes and Other Information
- The function uses a magic number (0xdeadbeef) to mark the header for debugging purposes
- The entry cannot be freed before initialization completes due to the dshash partition lock protection
- Callers must increase the reference count if they need a longer-lived reference to the entry
- The dropped flag is explicitly set to false to indicate the entry is active
- Memory is allocated using dsa_allocate0 which ensures zero-initialization