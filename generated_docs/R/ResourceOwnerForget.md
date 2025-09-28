# ResourceOwnerForget

## Location
[src/backend/utils/resowner/resowner.c:554-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L554-L647)

## Overview
Removes a resource from a ResourceOwner's tracking system, preventing automatic cleanup of that resource.

## Definition
```c
void ResourceOwnerForget(ResourceOwner owner, Datum value, const ResourceOwnerDesc *kind)
```

## Detailed Description
ResourceOwnerForget removes a previously registered resource from the ResourceOwner's tracking system by searching both the array and hash table storage structures. The function uses a two-phase search strategy: first checking the array (searched in reverse order for efficiency), then searching the hash table using linear probing if the resource isn't found in the array.

When a resource is found in the array, it's removed by replacing it with the last array element and decrementing the count. When found in the hash table, the slot is marked as empty by clearing the item and kind fields. If the resource is not found in either storage structure, the function raises an error.

## Parameters / Member Variables
- `owner`: The ResourceOwner that currently tracks the resource
- `value`: The resource value to remove (must match exactly)
- `kind`: Pointer to ResourceOwnerDesc that must match the resource type exactly

## Dependencies
- Functions called/Symbols referenced:
  - [hash_resource_elem](../h/hash_resource_elem.md) (for computing hash table position)
  - [ResourceOwnerDesc](ResourceOwnerDesc.md) (resource type descriptor structure)
  - [DatumGetPointer](../D/DatumGetPointer.md) (for error message formatting)
- Called from (representative examples):
  - [ResourceOwnerForgetTupleDesc](ResourceOwnerForgetTupleDesc.md) (tuple descriptor tracking)
  - [ResourceOwnerForgetBuffer](ResourceOwnerForgetBuffer.md) (buffer tracking)  
  - [ResourceOwnerForgetFile](ResourceOwnerForgetFile.md) (file descriptor tracking)
  - [ResourceOwnerForgetCatCacheRef](ResourceOwnerForgetCatCacheRef.md) (catalog cache reference tracking)
  - [ResourceOwnerForgetSnapshot](ResourceOwnerForgetSnapshot.md) (snapshot tracking)
  - [ResourceOwnerForgetDSM](ResourceOwnerForgetDSM.md) (dynamic shared memory tracking)

## Notes and Other Information
- Cannot be called after ResourceOwner cleanup has started (owner->releasing is true)
- Searches array in reverse order since recent resources are more likely to be forgotten
- If the same resource is registered multiple times, only one instance is removed
- Forgetting a resource doesn't guarantee space for a new resource, except when forgetting the most recently remembered resource
- Uses linear probing for hash table collision resolution
- Raises an ERROR if the specified resource is not found in the ResourceOwner
- Critical for manual resource cleanup when resources are released before ResourceOwner cleanup
- Each resource type has its own wrapper function that calls this with the appropriate ResourceOwnerDesc

## Simplified Source

```c
// Simplified version of ResourceOwnerForget
void ResourceOwnerForget(ResourceOwner owner, Datum value, const ResourceOwnerDesc *kind)
{
    // Safety check: Cannot forget resources after cleanup has started
    if (owner->releasing)
        elog(ERROR, "ResourceOwnerForget called for %s after release started", kind->name);

    // Phase 1: Search through array storage (reverse order for efficiency)
    for (int i = owner->narr - 1; i >= 0; i--) {
        if (owner->arr[i].item == value && owner->arr[i].kind == kind) {
            // Remove by replacing with last element
            owner->arr[i] = owner->arr[owner->narr - 1];
            owner->narr--;
            return;
        }
    }

    // Phase 2: Search hash table if resource not found in array
    if (owner->nhash > 0) {
        uint32 mask = owner->capacity - 1;
        uint32 idx = hash_resource_elem(value, kind) & mask;

        // Linear probing to handle hash collisions
        for (uint32 i = 0; i < owner->capacity; i++) {
            if (owner->hash[idx].item == value && owner->hash[idx].kind == kind) {
                // Mark slot as empty
                owner->hash[idx].item = (Datum) 0;
                owner->hash[idx].kind = NULL;
                owner->nhash--;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    // Resource not found - this is a programming error
    elog(ERROR, "%s %p is not owned by resource owner %s",
         kind->name, DatumGetPointer(value), owner->name);
}
```

Key simplifications made:
- Removed debug statistics tracking (#ifdef RESOWNER_STATS blocks)
- Simplified comments to focus on core logic flow
- Consolidated error handling into essential safety checks
- Removed detailed code comments about implementation specifics
- Preserved the two-phase search algorithm (array then hash table)
- Maintained critical error conditions and resource cleanup logic