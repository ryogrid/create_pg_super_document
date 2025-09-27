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
  - [StatsShmemSize](StatsShmemSize.md): Gets total shared memory size needed
  - [ShmemInitStruct](ShmemInitStruct.md): Creates or attaches to shared memory structure
  - dsa_create_in_place: Creates DSA in predetermined memory location
  - [dsa_pin](../d/dsa_pin.md)/dsa_detach: DSA lifecycle management
  - [dsa_set_size_limit](../d/dsa_set_size_limit.md): Controls DSA growth limits
  - [dshash_create](../d/dshash_create.md): Creates dynamic shared hash table
  - [dshash_get_hash_table_handle](../d/dshash_get_hash_table_handle.md)/dshash_detach: Hash table management
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md): Initializes atomic counter for garbage collection
  - [LWLockInitialize](../L/LWLockInitialize.md): Initializes lightweight locks for various stats components
  - [pgstat_dsa_init_size](../p/pgstat_dsa_init_size.md): Gets DSA initialization size
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md): Called during shared memory initialization

## Notes and Other Information
- Only the postmaster process (IsUnderPostmaster == false) creates the structures
- [Backend](../B/Backend.md) processes just attach to existing shared memory structures
- Creates DSA in "plain" shared memory to avoid DSM dependency in postmaster
- Temporarily limits DSA size during hash table creation to ensure placement in plain shared memory
- Initializes locks for: archiver, bgwriter, checkpointer, SLRU, WAL, and I/O statistics
- Uses LWTRANCHE_PGSTATS_DSA for DSA locks and LWTRANCHE_PGSTATS_DATA for statistics data locks
- Postmaster detaches from DSA/hash table references after creation since it won't access them again
- Initializes garbage collection request counter to 1

## Simplified Source

```c
// Simplified version of StatsShmemInit
void StatsShmemInit(void) {
    bool found;
    Size sz;

    // Calculate shared memory size and create/attach to shared memory structure
    sz = StatsShmemSize();
    pgStatLocal.shmem = (PgStat_ShmemControl *)
        ShmemInitStruct("Shared Memory Stats", sz, &found);

    if (!IsUnderPostmaster) {
        // Postmaster process: create all statistics structures
        PgStat_ShmemControl *ctl = pgStatLocal.shmem;
        char *p = (char *) ctl;

        // Skip past the main control structure
        p += MAXALIGN(sizeof(PgStat_ShmemControl));

        // Create DSA (Dynamic Shared Area) in plain shared memory
        ctl->raw_dsa_area = p;
        p += MAXALIGN(pgstat_dsa_init_size());

        dsa_area *dsa = dsa_create_in_place(ctl->raw_dsa_area,
                                          pgstat_dsa_init_size(),
                                          LWTRANCHE_PGSTATS_DSA, 0);
        dsa_pin(dsa);

        // Create hash table with size limit to ensure plain shared memory placement
        dsa_set_size_limit(dsa, pgstat_dsa_init_size());
        dshash_table *dsh = dshash_create(dsa, &dsh_params, NULL);
        ctl->hash_handle = dshash_get_hash_table_handle(dsh);
        dsa_set_size_limit(dsa, -1); // Remove size limit

        // Cleanup local references - postmaster won't use these again
        dshash_detach(dsh);
        dsa_detach(dsa);

        // Initialize garbage collection counter
        pg_atomic_init_u64(&ctl->gc_request_count, 1);

        // Initialize locks for all statistics components
        LWLockInitialize(&ctl->archiver.lock, LWTRANCHE_PGSTATS_DATA);
        LWLockInitialize(&ctl->bgwriter.lock, LWTRANCHE_PGSTATS_DATA);
        LWLockInitialize(&ctl->checkpointer.lock, LWTRANCHE_PGSTATS_DATA);
        LWLockInitialize(&ctl->slru.lock, LWTRANCHE_PGSTATS_DATA);
        LWLockInitialize(&ctl->wal.lock, LWTRANCHE_PGSTATS_DATA);

        // Initialize I/O statistics locks for all backend types
        for (int i = 0; i < BACKEND_NUM_TYPES; i++) {
            LWLockInitialize(&ctl->io.locks[i], LWTRANCHE_PGSTATS_DATA);
        }
    } else {
        // Backend process: just verify shared memory was found
        Assert(found);
    }
}
```

Key simplifications made:
- Removed detailed comments and combined related operations
- Simplified variable declarations and initialization
- Consolidated memory pointer arithmetic explanations
- Abstracted DSA and hash table creation details
- Streamlined lock initialization into logical groups
- Preserved the essential two-phase logic (postmaster vs backend)