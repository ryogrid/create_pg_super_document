# ResourceOwnerForgetDSM

## Location
[src/backend/storage/ipc/dsm.c:165-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L165-L176)

## Overview
A convenience wrapper function that unregisters a DSM (Dynamic Shared Memory) segment from a resource owner, removing it from automatic cleanup tracking.

## Definition
```c
static inline void ResourceOwnerForgetDSM(ResourceOwner owner, dsm_segment *seg)
```

## Detailed Description
This function is the counterpart to ResourceOwnerRememberDSM(), providing a simple wrapper around the generic ResourceOwnerForget() function for DSM segments. It removes a previously registered DSM segment from the resource owner's tracking list, indicating that the segment no longer needs automatic cleanup by the resource management system. This is typically called when the DSM segment is being explicitly detached or when ownership is being transferred.

## Parameters / Member Variables
- `owner`: The ResourceOwner that is currently tracking this DSM segment
- `seg`: Pointer to the dsm_segment structure to be unregistered from the resource owner

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - PointerGetDatum
  - dsm_resowner_desc (static resource owner descriptor)
- Called from (representative examples):
  - dsm_create
  - dsm_detach
  - dsm_pin_mapping

## Notes and Other Information
- This is a static inline function, so it's only visible within the dsm.c compilation unit
- Must be called for any DSM segment that was previously registered with ResourceOwnerRememberDSM
- Part of PostgreSQL's resource management system for proper cleanup coordination
- Used when explicitly managing DSM segment lifecycle rather than relying on automatic cleanup
- The function pairs with ResourceOwnerRememberDSM to provide complete resource tracking control