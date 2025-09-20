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
  - MemoryContextAllowInCriticalSection (allow memory allocation in critical sections)
  - [hash_create](../h/hash_create.md) (create hash table)
  - HASHCTL (hash table control structure)
  - FileTag (key type for hash table)
  - PendingFsyncEntry (value type for hash table)
- Called from (representative examples):
  - [BaseInit](../B/BaseInit.md) (initialization function in postinit.c:671)

## Notes and Other Information
- The function includes a detailed comment about the theoretical risk of running out of memory while absorbing fsync requests within a critical section, which would lead to a PANIC
- The hash table is created with 100 initial slots and uses HASH_ELEM, HASH_BLOBS, and HASH_CONTEXT flags
- The pendingUnlinks list is also initialized to NIL for tracking files to be unlinked
- Only processes that actually need to track sync operations (standalone processes and checkpointer) create these data structures