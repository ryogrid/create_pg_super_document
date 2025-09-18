# InitBufferPoolAccess

## Location
src/backend/storage/buffer/bufmgr.c: 3565 - 3589

## Overview
InitBufferPoolAccess initializes a backend's access to the shared buffer pool by setting up private reference counting structures and registering cleanup handlers.

## Definition
void InitBufferPoolAccess(void)

## Detailed Description
This function is called during backend startup (whether standalone or under the postmaster) to set up the backend's access to the already-existing shared buffer pool. It initializes the private reference counting mechanism used to track buffer pins held by this specific backend process. The function creates a hash table for tracking private reference counts that exceed what can be stored in the static PrivateRefCountArray, and registers the AtProcExit_Buffers function to be called during backend shutdown to ensure proper cleanup.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - HASHCTL (hash table control structure)
  - PrivateRefCountEntry (hash table entry type)
  - hash_create (creates the private reference count hash table)
  - HASH_ELEM (hash table creation flag)
  - HASH_BLOBS (hash table creation flag)
  - AtProcExit_Buffers (cleanup function)
  - on_shmem_exit (registers shutdown callback)
- Called from (representative examples):
  - BaseInit

## Notes and Other Information
- Part of the buffer management initialization sequence during backend startup
- Sets up private reference counting structures (PrivateRefCountArray and PrivateRefCountHash) used to track buffer pins
- The hash table is created with an initial size of 100 entries for tracking reference counts that overflow the static array
- Registers AtProcExit_Buffers as a shutdown callback to ensure proper cleanup when the backend exits
- Includes an assertion to verify that MyProc is properly initialized before registering the exit callback
- The function is essential for proper buffer pin tracking and prevents buffer leaks