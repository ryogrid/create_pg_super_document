# InitBufferPoolAccess

## Location
[src/backend/storage/buffer/bufmgr.c:3565-3589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3565-L3589)

## Overview
InitBufferPoolAccess initializes a backend's access to the shared buffer pool by setting up private reference counting structures and registering cleanup handlers.

## Definition
void InitBufferPoolAccess(void)

## Detailed Description
This function is called during backend startup (whether standalone or under the postmaster) to set up the backend's access to the already-existing shared buffer pool. It initializes the private reference counting mechanism used to track buffer pins held by this specific backend process. The function creates a hash table for tracking private reference counts that exceed what can be stored in the static PrivateRefCountArray, and registers the AtProcExit_Buffers function to be called during backend shutdown to ensure proper cleanup.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [HASHCTL](../H/HASHCTL.md) (hash table control structure)
  - [PrivateRefCountEntry](../P/PrivateRefCountEntry.md) (hash table entry type)
  - [hash_create](../h/hash_create.md) (creates the private reference count hash table)
  - HASH_ELEM (hash table creation flag)
  - HASH_BLOBS (hash table creation flag)
  - [AtProcExit_Buffers](../A/AtProcExit_Buffers.md) (cleanup function)
  - [on_shmem_exit](../o/on_shmem_exit.md) (registers shutdown callback)
- Called from (representative examples):
  - [BaseInit](../B/BaseInit.md)

## Notes and Other Information
- Part of the buffer management initialization sequence during backend startup
- Sets up private reference counting structures (PrivateRefCountArray and PrivateRefCountHash) used to track buffer pins
- The hash table is created with an initial size of 100 entries for tracking reference counts that overflow the static array
- Registers AtProcExit_Buffers as a shutdown callback to ensure proper cleanup when the backend exits
- Includes an assertion to verify that MyProc is properly initialized before registering the exit callback
- The function is essential for proper buffer pin tracking and prevents buffer leaks

## Simplified Source

```c
// Simplified version of InitBufferPoolAccess
void InitBufferPoolAccess(void) {
    HASHCTL hash_ctl;

    // Step 1: Initialize private reference count array to zero
    memset(&PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));

    // Step 2: Set up hash table configuration
    hash_ctl.keysize = sizeof(int32);
    hash_ctl.entrysize = sizeof(PrivateRefCountEntry);

    // Step 3: Create hash table for overflow reference counts
    PrivateRefCountHash = hash_create("PrivateRefCount", 100, &hash_ctl,
                                      HASH_ELEM | HASH_BLOBS);

    // Step 4: Register cleanup function for backend shutdown
    Assert(MyProc != NULL);
    on_shmem_exit(AtProcExit_Buffers, 0);
}
```

Key simplifications made:
- Added step-by-step comments explaining the main operations
- Preserved all essential initialization logic
- Maintained the exact function signature and core functionality
- Focused on the four main initialization steps: array clearing, hash config, hash creation, and cleanup registration