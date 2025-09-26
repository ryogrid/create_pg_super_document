# ResourceOwnerEnlarge

## Location
[src/backend/utils/resowner/resowner.c:442-513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L442-L513)

## Overview
Ensures there is sufficient space in a ResourceOwner's internal storage structures to accommodate at least one more resource entry.

## Definition
```c
void ResourceOwnerEnlarge(ResourceOwner owner)
```

## Detailed Description
ResourceOwnerEnlarge implements a two-tier storage strategy for resource tracking: a small fixed-size array for initial resources, and a dynamically-sized hash table for larger collections. The function performs capacity management by migrating resources from the array to the hash table when the array fills up, and by expanding the hash table when it approaches capacity.

The function implements a critical safety mechanism by requiring space allocation before resource acquisition. This prevents memory allocation failures during resource cleanup operations, which could lead to resource leaks. The hash table uses power-of-2 sizing for efficient modulo operations and maintains a load factor threshold to ensure good performance.

## Parameters / Member Variables
- `owner`: The ResourceOwner whose capacity should be enlarged

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAllocZero (for allocating expanded hash table)
  - ResourceOwnerAddToHash (for transferring items to hash table)
  - pfree (for releasing old hash table)
  - RESOWNER_ARRAY_SIZE, RESOWNER_HASH_INIT_SIZE, RESOWNER_HASH_MAX_ITEMS (sizing constants)
- Called from (representative examples):
  - IncrTupleDescRefCount (tuple descriptor reference management)
  - BufferAlloc, GetVictimBuffer (buffer management)
  - OpenTemporaryFile (file descriptor management)
  - SearchCatCacheInternal (catalog cache management)
  - CreateWaitEventSet (wait event management)
  - dsm_create_descriptor (dynamic shared memory)

## Notes and Other Information
- Must be called before acquiring a resource, never after ResourceOwner release has started
- Uses a two-tier storage strategy: array (fast, limited) and hash table (larger capacity)
- Hash table capacity always remains a power of 2 for efficient hash operations
- When expanding, all existing items are rehashed into the new table
- The function is designed to be allocation-failure safe after the initial memory allocation
- Critical for preventing resource leaks during error conditions
- Callers must ensure no unrelated ResourceOwnerRemember() calls occur between ResourceOwnerEnlarge() and the intended ResourceOwnerRemember() call