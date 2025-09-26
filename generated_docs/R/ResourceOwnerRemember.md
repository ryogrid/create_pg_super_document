# ResourceOwnerRemember

## Location
[src/backend/utils/resowner/resowner.c:514-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L514-L553)

## Overview
Registers a resource with a ResourceOwner so it will be automatically cleaned up when the ResourceOwner is released.

## Definition
```c
void ResourceOwnerRemember(ResourceOwner owner, Datum value, const ResourceOwnerDesc *kind)
```

## Detailed Description
ResourceOwnerRemember adds a resource to a ResourceOwner's tracking system by appending it to the internal array. The function requires that ResourceOwnerEnlarge() has been called beforehand to ensure sufficient space is available. Each resource is associated with a ResourceOwnerDesc that defines how the resource should be cleaned up, including its release phase and priority.

The function performs several safety checks to ensure the ResourceOwner is in a valid state for adding new resources. It verifies that the ResourceOwnerDesc has valid release phase and priority values, and ensures that resource registration isn't attempted after cleanup has already begun.

## Parameters / Member Variables
- `owner`: The ResourceOwner that should track this resource
- `value`: The resource value (typically a pointer, file descriptor, or other identifier)
- `kind`: Pointer to ResourceOwnerDesc defining how this resource type should be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerDesc (resource type descriptor structure)
  - RESOWNER_ARRAY_SIZE (array size limit constant)
- Called from (representative examples):
  - ResourceOwnerRememberTupleDesc (tuple descriptor tracking)
  - ResourceOwnerRememberBuffer (buffer tracking)
  - ResourceOwnerRememberFile (file descriptor tracking)
  - ResourceOwnerRememberCatCacheRef (catalog cache reference tracking)
  - ResourceOwnerRememberSnapshot (snapshot tracking)
  - ResourceOwnerRememberDSM (dynamic shared memory tracking)

## Notes and Other Information
- Must be preceded by a call to ResourceOwnerEnlarge() to ensure space availability
- Cannot be called after ResourceOwner cleanup has started (owner->releasing is true)
- The ResourceOwnerDesc must have valid non-zero release_phase and release_priority values
- Resources are stored in a simple array up to RESOWNER_ARRAY_SIZE limit
- The function assumes the caller has properly allocated the resource before registering it
- Critical for PostgreSQL's automatic resource cleanup during error recovery and transaction abort
- Each resource type has its own wrapper function that calls this with the appropriate ResourceOwnerDesc