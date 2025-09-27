# InitSync

## Location
[src/backend/storage/sync/sync.c:124-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L124-L176)

## Overview
Initializes data structures for file synchronization tracking, creating necessary hash tables and memory contexts when running as a standalone process or checkpointer.

## Definition

```c
void
InitSync(void)
```
## Detailed Description
InitSync is responsible for setting up the infrastructure needed for tracking pending file synchronization operations in PostgreSQL. It creates a hash table () and associated memory context () that will be used to track files that need to be fsync'd. This initialization only occurs when the process is either running standalone (not under a postmaster) or when it's the checkpointer auxiliary process, as these are the processes that need to manage file synchronization operations.

The function creates a specialized memory context that is allowed to allocate memory within critical sections, which is normally forbidden but necessary for the checkpointer's operation when absorbing fsync requests.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsUnderPostmaster (macro to check if running under postmaster)
  - AmCheckpointerProcess (check if current process is checkpointer)
  - AllocSetContextCreate (create memory allocation context)
  - [MemoryContextAllowInCriticalSection](../M/MemoryContextAllowInCriticalSection.md) (allow memory allocation in critical sections)
  - [hash_create](../h/hash_create.md) (create hash table)
  - [HASHCTL](../H/HASHCTL.md) (hash table control structure)
  - [FileTag](../F/FileTag.md) (key type for hash table)
  - PendingFsyncEntry (value type for hash table)
- Called from (representative examples):
  - [BaseInit](../B/BaseInit.md) (initialization function in postinit.c:671)

## Notes and Other Information
- The function includes a detailed comment about the theoretical risk of running out of memory while absorbing fsync requests within a critical section, which would lead to a PANIC
- The hash table is created with 100 initial slots and uses HASH_ELEM, HASH_BLOBS, and HASH_CONTEXT flags
- The pendingUnlinks list is also initialized to NIL for tracking files to be unlinked
- Only processes that actually need to track sync operations (standalone processes and checkpointer) create these data structures

## Simplified Source

```c
// Simplified version of InitSync
void InitSync(void) {
    // Only initialize sync structures for standalone processes or checkpointer
    if (!IsUnderPostmaster || AmCheckpointerProcess()) {
        HASHCTL hash_ctl;

        // Create memory context for pending operations
        // Allow allocation in critical sections for checkpointer
        pendingOpsCxt = AllocSetContextCreate(TopMemoryContext,
                                              "Pending ops context",
                                              ALLOCSET_DEFAULT_SIZES);
        MemoryContextAllowInCriticalSection(pendingOpsCxt, true);

        // Set up hash table configuration
        hash_ctl.keysize = sizeof(FileTag);
        hash_ctl.entrysize = sizeof(PendingFsyncEntry);
        hash_ctl.hcxt = pendingOpsCxt;

        // Create hash table for tracking pending fsync operations
        pendingOps = hash_create("Pending Ops Table",
                                 100L,
                                 &hash_ctl,
                                 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

        // Initialize list for pending unlinks
        pendingUnlinks = NIL;
    }
}
```

Key simplifications made:
- Removed detailed comment about memory allocation risks in critical sections
- Condensed variable declarations and assignments
- Added concise comments explaining each major step
- Focused on the main execution path
- Preserved all essential functionality and logic flow