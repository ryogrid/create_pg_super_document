# XLogPrefetchShmemInit

## Location
[src/backend/access/transam/xlogprefetcher.c:315-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L315-L339)

## Overview
Initializes the shared memory structure for XLog prefetch statistics, creating and setting up the statistics counters during PostgreSQL startup.

## Definition

```c
void
XLogPrefetchShmemInit(void)
```
## Detailed Description
This function is responsible for setting up the XLog prefetch statistics in shared memory during PostgreSQL server initialization. It uses the PostgreSQL shared memory infrastructure to either create or attach to an existing shared memory segment named "XLogPrefetchStats".

The function performs two main operations:
1. Obtains a pointer to the shared memory segment using ShmemInitStruct
2. If this is a fresh initialization (not found), initializes all atomic counters to their default values

When initializing a new segment, it sets up:
- Reset timestamp (current time)
- All statistical counters (prefetch, hit, various skip counters) to zero

The use of atomic operations ensures thread-safe access in PostgreSQL's multi-process architecture.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [XLogPrefetchStats](XLogPrefetchStats.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- This function is called during PostgreSQL server startup as part of shared memory initialization
- The 'found' parameter indicates whether the shared memory segment already existed
- Only initializes counters when creating a new segment (found == false)
- Uses atomic initialization functions to ensure proper memory ordering
- The shared memory segment name "XLogPrefetchStats" is used for identification
- Located in src/backend/access/transam/xlogprefetcher.c:315-339

## Simplified Source

```c
// Simplified version of XLogPrefetchShmemInit
void XLogPrefetchShmemInit(void) {
    bool found;

    // Step 1: Get or create shared memory segment for prefetch stats
    SharedStats = (XLogPrefetchStats *)
        ShmemInitStruct("XLogPrefetchStats",
                        sizeof(XLogPrefetchStats),
                        &found);

    // Step 2: Initialize counters only if this is a new segment
    if (!found) {
        // Initialize reset timestamp to current time
        pg_atomic_init_u64(&SharedStats->reset_time, GetCurrentTimestamp());

        // Initialize all statistical counters to zero
        pg_atomic_init_u64(&SharedStats->prefetch, 0);
        pg_atomic_init_u64(&SharedStats->hit, 0);
        pg_atomic_init_u64(&SharedStats->skip_init, 0);
        pg_atomic_init_u64(&SharedStats->skip_new, 0);
        pg_atomic_init_u64(&SharedStats->skip_fpw, 0);
        pg_atomic_init_u64(&SharedStats->skip_rep, 0);
    }
}
```

Key simplifications made:
- Added step-by-step comments explaining the core logic
- Preserved the essential shared memory initialization pattern
- Maintained all atomic counter initializations as they represent the core functionality
- Simplified variable declarations for clarity
- Focused on the main execution path (new segment initialization)