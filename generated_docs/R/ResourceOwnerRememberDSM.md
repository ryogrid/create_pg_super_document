# ResourceOwnerRememberDSM

## Location
src/backend/storage/ipc/dsm.c: 160 - 164

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
- : The ResourceOwner that will track this DSM segment for cleanup purposes
- : Pointer to the dsm_segment structure to be registered with the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRemember
  - PointerGetDatum
  - dsm_resowner_desc (static resource owner descriptor)
- Called from (representative examples):
  - dsm_unpin_mapping
  - dsm_create_descriptor

## Notes and Other Information
- This is a static inline function, so it's only visible within the dsm.c compilation unit
- The function is part of PostgreSQL's resource management system that ensures proper cleanup of resources
- Uses the generic resource owner infrastructure with DSM-specific cleanup callbacks
- The corresponding cleanup function is ResourceOwnerForgetDSM