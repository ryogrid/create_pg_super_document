# shared_record_typmod_registry_detach

## Location
[src/backend/utils/cache/typcache.c:2868-2882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2868-L2882)

## Overview
A cleanup function that detaches from shared record typmod infrastructure when a dynamic shared memory segment is being destroyed.

## Definition

```c
static void
shared_record_typmod_registry_detach(dsm_segment *segment, Datum datum)
```
## Detailed Description
This function serves as a callback hook that is invoked when a dynamic shared memory (DSM) segment is being detached or destroyed. It safely cleans up the current session's references to shared record typmod infrastructure by detaching from the shared hash tables and clearing the registry pointer. The function includes defensive programming by checking for NULL pointers before attempting to detach, ensuring it can handle cases where initialization might not have completed successfully. This cleanup is essential for both parallel query leaders and workers to properly release shared resources.

## Parameters / Member Variables
- `*segment`: Pointer to the DSM segment being detached (parameter required by DSM callback interface)
- `datum`: Additional data passed to the callback (parameter required by DSM callback interface, not used in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [dshash_detach](../d/dshash_detach.md) (detaches from dynamic shared hash tables)
- Data structures used:
  - [dsm_segment](../d/dsm_segment.md)
  - CurrentSession (global session state)
- Called from (representative examples):
  - [SharedRecordTypmodRegistryInit](../S/SharedRecordTypmodRegistryInit.md) (registers as detach callback)
  - [SharedRecordTypmodRegistryAttach](../S/SharedRecordTypmodRegistryAttach.md) (registers as detach callback)

## Notes and Other Information
- Function is static and only used within the typcache.c module
- Used by both parallel query leaders and workers
- Includes defensive NULL pointer checks for robustness
- Part of PostgreSQL's dynamic shared memory cleanup infrastructure
- Ensures proper resource cleanup when shared memory segments are destroyed
- Critical for preventing resource leaks in parallel query execution
- The function signature matches the DSM callback interface requirements