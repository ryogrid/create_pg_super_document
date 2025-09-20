# dsa_attach

## Location
[src/backend/utils/mmgr/dsa.c:510-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L510-L544)

## Overview
Attaches to an existing Dynamic Shared Area (DSA) using a handle obtained from another process, enabling inter-process shared memory access.

## Definition

```c
dsa_area *
dsa_attach(dsa_handle handle)
```
## Detailed Description
This function enables processes to attach to DSAs created by other processes using handles obtained via dsa_get_handle(). The handle is essentially a DSM segment handle for the first segment containing the DSA control object. The function first attempts to attach to the underlying DSM segment, reporting an error if the attachment fails (which could happen if the segment no longer exists or is inaccessible).

Upon successful DSM attachment, it calls attach_internal() to set up the DSA structures and establish access to the shared memory area. The function also registers a cleanup callback that will be invoked when the control segment detaches, ensuring proper resource cleanup. This function is restricted to areas created with dsa_create() (not dsa_create_in_place()) since only those areas have valid DSM handles.

## Parameters / Member Variables
- : DSA handle obtained from dsa_get_handle() in the same or different process

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_attach](dsm_attach.md)
  - attach_internal
  - dsm_segment_address
  - on_dsm_detach
  - [dsa_on_dsm_detach_release_in_place](dsa_on_dsm_detach_release_in_place.md)
- Called from (representative examples):
  - [TidStoreAttach](../T/TidStoreAttach.md)
  - logicalrep_launcher_attach_dshmem
  - init_dsm_registry

## Notes and Other Information
- Only works with areas created using dsa_create(), not dsa_create_in_place()
- Reports ERROR if DSM segment attachment fails with specific error code
- Automatically registers cleanup callback for resource management
- Essential for parallel processing and inter-process coordination
- Used in tuple ID stores, logical replication, and DSM registry systems
- Handle represents the DSM segment handle of the first segment containing control object
- Located in src/backend/utils/mmgr/dsa.c:510-544