# WaitEventCustomShmemInit

## Location
[src/backend/utils/activity/wait_event.c:120-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L120-L163)

## Overview
Initializes shared memory structures for custom wait events, including the allocation counter and two hash tables for efficient lookup by event information and name.

## Definition


## Detailed Description
This function performs the initialization of shared memory data structures required for the custom wait event subsystem. It creates or attaches to:

1. **WaitEventCustomCounter**: A shared counter structure that tracks the next available custom wait event ID and includes a spinlock for thread-safe access
2. **WaitEventCustomHashByInfo**: A hash table that stores wait events indexed by their event information (uint32 key)
3. **WaitEventCustomHashByName**: A hash table that stores wait events indexed by their name (string key up to NAMEDATALEN length)

The function handles both first-time initialization (when structures don't exist) and attachment to existing structures in shared memory. For new installations, it initializes the counter with WAIT_EVENT_CUSTOM_INITIAL_ID and sets up the spinlock.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory structure initialization)
  - ShmemInitHash (shared memory hash table initialization)
  - SpinLockInit (spinlock initialization)
  - WaitEventCustomCounterData (counter data structure type)
  - WaitEventCustomEntryByInfo (hash entry type for info-based lookup)
  - WaitEventCustomEntryByName (hash entry type for name-based lookup)
  - WAIT_EVENT_CUSTOM_INITIAL_ID (initial ID constant)
  - WAIT_EVENT_CUSTOM_HASH_INIT_SIZE (initial hash table size)
  - WAIT_EVENT_CUSTOM_HASH_MAX_SIZE (maximum hash table size)
  - HASH_ELEM, HASH_BLOBS, HASH_STRINGS (hash table configuration flags)
  - NAMEDATALEN (maximum name length constant)

- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (in src/backend/storage/ipc/ipci.c:358)
  - PG_WAIT_INJECTIONPOINT (in src/include/utils/wait_event.h:61)

## Notes and Other Information
- This function is called during PostgreSQL startup as part of shared memory initialization
- The function is idempotent - it can safely handle both first-time initialization and re-attachment to existing structures
- Uses two different hash table configurations: HASH_BLOBS for binary keys (event info) and HASH_STRINGS for string keys (event names)
- The spinlock protects concurrent access to the ID counter from multiple processes
- Must be called after WaitEventCustomShmemSize() has been used to allocate the required shared memory space