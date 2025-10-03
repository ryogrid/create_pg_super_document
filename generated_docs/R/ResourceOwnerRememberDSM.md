# ResourceOwnerRememberDSM

## Location
[src/backend/storage/ipc/dsm.c:160-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L160-L164)

## Overview
A convenience wrapper function that registers a DSM (Dynamic Shared Memory) segment with a resource owner for automatic cleanup on transaction abort or process exit.

## Definition

```c
static inline void
ResourceOwnerRememberDSM(ResourceOwner owner, dsm_segment *seg)
```
## Detailed Description
This function is a simple wrapper around the generic ResourceOwnerRemember() function, specifically designed for DSM segments. It registers a DSM segment with the PostgreSQL resource management system, ensuring that the segment will be automatically cleaned up if the owning transaction aborts or the process exits unexpectedly. The function uses the dsm_resowner_desc descriptor to provide DSM-specific cleanup behavior through the resource owner framework.

## Parameters / Member Variables
- `owner`: The ResourceOwner that will track this DSM segment for cleanup purposes
- `*seg`: Pointer to the dsm_segment structure to be registered with the resource owner
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - dsm_resowner_desc (static resource owner descriptor)
- Called from (representative examples):
  - [dsm_unpin_mapping](../d/dsm_unpin_mapping.md)
  - [dsm_create_descriptor](../d/dsm_create_descriptor.md)

## Notes and Other Information
- This is a static inline function, so it's only visible within the dsm.c compilation unit
- The function is part of PostgreSQL's resource management system that ensures proper cleanup of resources
- Uses the generic resource owner infrastructure with DSM-specific cleanup callbacks
- The corresponding cleanup function is ResourceOwnerForgetDSM

## Simplified Source

```c
static inline void
ResourceOwnerRememberDSM(ResourceOwner owner, dsm_segment *seg)
{
    // Register DSM segment with resource owner for automatic cleanup
    ResourceOwnerRemember(owner, PointerGetDatum(seg), &dsm_resowner_desc);
}
```