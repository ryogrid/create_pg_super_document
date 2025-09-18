# SimpleLruInit

## Location
src/backend/access/transam/slru.c: 252 - 354

## Overview
SimpleLruInit initializes or attaches to a Simple LRU cache in shared memory, setting up all necessary data structures, locks, and buffers for SLRU operation.

## Definition


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
  - strlcpy (string operations)

- Called from (representative examples):
  - [CLOGShmemInit](../C/CLOGShmemInit.md)
  - CommitTsShmemInit
  - [MultiXactShmemInit](../M/MultiXactShmemInit.md)
  - SUBTRANSShmemInit
  - [AsyncShmemInit](../A/AsyncShmemInit.md)
  - SerialInit

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