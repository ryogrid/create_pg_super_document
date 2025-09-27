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
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (for allocating expanded hash table)
  - [ResourceOwnerAddToHash](ResourceOwnerAddToHash.md) (for transferring items to hash table)
  - [pfree](../p/pfree.md) (for releasing old hash table)
  - RESOWNER_ARRAY_SIZE, RESOWNER_HASH_INIT_SIZE, RESOWNER_HASH_MAX_ITEMS (sizing constants)
- Called from (representative examples):
  - [IncrTupleDescRefCount](../I/IncrTupleDescRefCount.md) (tuple descriptor reference management)
  - [BufferAlloc](../B/BufferAlloc.md), GetVictimBuffer (buffer management)
  - [OpenTemporaryFile](../O/OpenTemporaryFile.md) (file descriptor management)
  - [SearchCatCacheInternal](../S/SearchCatCacheInternal.md) (catalog cache management)
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md) (wait event management)
  - [dsm_create_descriptor](../d/dsm_create_descriptor.md) (dynamic shared memory)

## Notes and Other Information
- Must be called before acquiring a resource, never after ResourceOwner release has started
- Uses a two-tier storage strategy: array (fast, limited) and hash table (larger capacity)
- [Hash](../H/Hash.md) table capacity always remains a power of 2 for efficient hash operations
- When expanding, all existing items are rehashed into the new table
- The function is designed to be allocation-failure safe after the initial memory allocation
- Critical for preventing resource leaks during error conditions
- Callers must ensure no unrelated ResourceOwnerRemember() calls occur between ResourceOwnerEnlarge() and the intended ResourceOwnerRemember() call

## Simplified Source

```c
// Simplified version of ResourceOwnerEnlarge
void ResourceOwnerEnlarge(ResourceOwner owner) {
    // Safety check: cannot enlarge after release started
    if (owner->releasing)
        elog(ERROR, "ResourceOwnerEnlarge called after release started");

    // Quick return if array still has space
    if (owner->narr < RESOWNER_ARRAY_SIZE)
        return;

    // Check if hash table needs expansion
    if (owner->narr + owner->nhash >= owner->grow_at) {
        // Calculate new capacity (double the size, power of 2)
        uint32 oldcap = owner->capacity;
        uint32 newcap = (oldcap > 0) ? oldcap * 2 : RESOWNER_HASH_INIT_SIZE;

        // Allocate new hash table
        ResourceElem *oldhash = owner->hash;
        ResourceElem *newhash = MemoryContextAllocZero(TopMemoryContext,
                                                       newcap * sizeof(ResourceElem));

        // Update owner with new hash table properties
        owner->hash = newhash;
        owner->capacity = newcap;
        owner->grow_at = RESOWNER_HASH_MAX_ITEMS(newcap);
        owner->nhash = 0;

        // Transfer existing entries from old hash to new hash
        if (oldhash != NULL) {
            for (uint32 i = 0; i < oldcap; i++) {
                if (oldhash[i].kind != NULL)
                    ResourceOwnerAddToHash(owner, oldhash[i].item, oldhash[i].kind);
            }
            pfree(oldhash);
        }
    }

    // Move all array items to hash table to free up array space
    for (int i = 0; i < owner->narr; i++) {
        ResourceOwnerAddToHash(owner, owner->arr[i].item, owner->arr[i].kind);
    }
    owner->narr = 0;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential ones
- Simplified variable declarations and initialization
- Consolidated the hash expansion logic flow
- Focused on the main algorithm: array to hash migration and hash expansion
- Maintained all critical safety checks and logic paths