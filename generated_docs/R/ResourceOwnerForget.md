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