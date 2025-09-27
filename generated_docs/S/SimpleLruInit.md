# SimpleLruInit

## Location
[src/backend/access/transam/slru.c:252-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L252-L354)

## Overview
SimpleLruInit initializes or attaches to a Simple LRU cache in shared memory, setting up all necessary data structures, locks, and buffers for SLRU operation.

## Definition

```c
struct, including directory path. We
	 * assume caller set PagePrecedes.
	 */
	ctl->shared = shared;
```
## Detailed Description
This function performs comprehensive initialization of an SLRU control structure, handling both the shared memory initialization (when running as postmaster) and attachment (for backend processes). The initialization process includes:

**Shared Memory Initialization (postmaster only):**
1. **Memory layout setup**: Calculates and assigns memory offsets for all data structures including page buffers, status arrays, dirty flags, page numbers, LRU counters, and optional LSN groups
2. **Lock initialization**: Sets up both per-buffer locks and bank locks using the specified tranche IDs for lock wait event reporting
3. **Buffer allocation**: Allocates BLCKSZ-sized page buffers for each slot, properly aligned
4. **Bank organization**: Organizes buffers into banks (groups of SLRU_BANK_SIZE) to reduce lock contention
5. **Statistics integration**: Registers the SLRU with PostgreSQL's statistics system
6. **Atomic counter setup**: Initializes the latest page number atomic counter

**Control Structure Setup (all processes):**
- Links the local control structure to shared memory
- Sets up the sync handler for coordinating with checkpointer
- Configures segment naming strategy (standard vs long names)
- Copies the subdirectory path for file operations

The function uses ShmemInitStruct() to either create new shared memory (postmaster) or attach to existing shared memory (backends).

## Parameters / Member Variables
- : Local (unshared) control structure to initialize
- : User-visible name of the SLRU (used for shared memory segment naming and statistics)
- : Number of page buffer slots to allocate (must be ≤ SLRU_MAX_ALLOWED_BUFFERS)
- : Number of LSN groups per page for WAL consistency (0 if not needed)
- : PGDATA-relative subdirectory where SLRU files will be stored
- : Tranche ID for per-buffer LWLocks (for lock wait event reporting)
- : Tranche ID for bank LWLocks (for lock wait event reporting)
- : Function set for handling sync requests from checkpointer
- : Whether to use long (15-char) or standard (4-6 char) segment filenames

## Dependencies
- Functions called/Symbols referenced:
  - SlruCtl, SlruShared (control structure types)
  - SyncRequestHandler (function pointer type)
  - SLRU_BANK_SIZE, SLRU_MAX_ALLOWED_BUFFERS (constants)
  - [ShmemInitStruct](ShmemInitStruct.md) (shared memory allocation)
  - [SimpleLruShmemSize](SimpleLruShmemSize.md) (memory size calculation)
  - [SlruSharedData](SlruSharedData.md), SlruPageStatus (data structure types)
  - LWLockPadded, LWLockInitialize (locking infrastructure)
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md) (atomic operations)
  - [pgstat_get_slru_index](../p/pgstat_get_slru_index.md) (statistics system)
  - SLRU_PAGE_EMPTY (page status constant)
  - [strlcpy](../s/strlcpy.md) (string operations)

- Called from (representative examples):
  - [CLOGShmemInit](../C/CLOGShmemInit.md)
  - [CommitTsShmemInit](../C/CommitTsShmemInit.md)
  - [MultiXactShmemInit](../M/MultiXactShmemInit.md)
  - [SUBTRANSShmemInit](SUBTRANSShmemInit.md)
  - [AsyncShmemInit](../A/AsyncShmemInit.md)
  - [SerialInit](SerialInit.md)

## Notes and Other Information
- This function must be called during shared memory initialization for each SLRU subsystem
- The banking system (nbanks = nslots / SLRU_BANK_SIZE) improves concurrency by partitioning locks
- Memory layout is carefully calculated to ensure proper alignment and efficient access patterns
- The function validates shared memory size matches expectations with an assertion
- [Backend](../B/Backend.md) processes skip initialization and only attach to existing shared memory
- The sync_handler parameter enables integration with PostgreSQL's checkpoint and sync mechanisms
- Long segment names are used for high-volume SLRUs to avoid filename space exhaustion
- Each SLRU gets its own statistics tracking via pgstat_get_slru_index()
- The function assumes the caller has already set up the PagePrecedes callback function

## Simplified Source

```c
// Simplified version of SimpleLruInit
void SimpleLruInit(SlruCtl ctl, const char *name, int nslots, int nlsns,
                   const char *subdir, int buffer_tranche_id, int bank_tranche_id,
                   SyncRequestHandler sync_handler, bool long_segment_names) {
    SlruShared shared;
    bool found;
    int nbanks = nslots / SLRU_BANK_SIZE;

    // Core logic step 1: Get or create shared memory structure
    shared = (SlruShared) ShmemInitStruct(name, SimpleLruShmemSize(nslots, nlsns), &found);

    if (!IsUnderPostmaster) {
        // Core logic step 2: Initialize shared memory (postmaster only)
        memset(shared, 0, sizeof(SlruSharedData));
        shared->num_slots = nslots;
        shared->lsn_groups_per_page = nlsns;

        // Initialize atomic counter and statistics
        pg_atomic_init_u64(&shared->latest_page_number, 0);
        shared->slru_stats_idx = pgstat_get_slru_index(name);

        // Core logic step 3: Set up memory layout for buffers and arrays
        char *ptr = (char *) shared;
        Size offset = MAXALIGN(sizeof(SlruSharedData));

        // Assign memory regions for data structures
        shared->page_buffer = (char **) (ptr + offset);
        shared->page_status = (SlruPageStatus *) (ptr + advance_offset(&offset, nslots));
        shared->page_dirty = (bool *) (ptr + advance_offset(&offset, nslots));
        shared->page_number = (int64 *) (ptr + advance_offset(&offset, nslots));
        shared->page_lru_count = (int *) (ptr + advance_offset(&offset, nslots));

        // Set up locks and bank management
        shared->buffer_locks = (LWLockPadded *) (ptr + advance_offset(&offset, nslots));
        shared->bank_locks = (LWLockPadded *) (ptr + advance_offset(&offset, nbanks));
        shared->bank_cur_lru_count = (int *) (ptr + advance_offset(&offset, nbanks));

        // Core logic step 4: Initialize all buffer slots
        ptr += BUFFERALIGN(offset);
        for (int slotno = 0; slotno < nslots; slotno++) {
            LWLockInitialize(&shared->buffer_locks[slotno].lock, buffer_tranche_id);
            shared->page_buffer[slotno] = ptr;
            shared->page_status[slotno] = SLRU_PAGE_EMPTY;
            shared->page_dirty[slotno] = false;
            shared->page_lru_count[slotno] = 0;
            ptr += BLCKSZ;
        }

        // Core logic step 5: Initialize bank locks for concurrency
        for (int bankno = 0; bankno < nbanks; bankno++) {
            LWLockInitialize(&shared->bank_locks[bankno].lock, bank_tranche_id);
            shared->bank_cur_lru_count[bankno] = 0;
        }
    }

    // Core logic step 6: Set up local control structure
    ctl->shared = shared;
    ctl->sync_handler = sync_handler;
    ctl->long_segment_names = long_segment_names;
    ctl->nbanks = nbanks;
    strlcpy(ctl->Dir, subdir, sizeof(ctl->Dir));
}
```

Key simplifications made:
- Removed detailed offset calculations and consolidated into advance_offset() helper concept
- Abstracted LSN group initialization for clarity
- Focused on the main initialization flow
- Simplified memory layout setup while preserving essential structure
- Removed verbose error checking assertions for readability
- Consolidated similar initialization loops