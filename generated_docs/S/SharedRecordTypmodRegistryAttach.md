# SharedRecordTypmodRegistryAttach

## Location
[src/backend/utils/cache/typcache.c:2207-2289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2207-L2289)

## Overview
Attaches a parallel worker process to an existing SharedRecordTypmodRegistry that was previously initialized by the leader process, enabling the worker to participate in shared record type management.

## Definition

```c
void
SharedRecordTypmodRegistryAttach(SharedRecordTypmodRegistry *registry)
```
## Detailed Description
This function allows parallel worker processes to attach to a shared record typmod registry that was already initialized by the leader process via SharedRecordTypmodRegistryInit(). It establishes connection to the shared hash tables for record type management and sets up the session state needed for coordinated typmod assignment and tuple descriptor lookup across parallel backends.

The function performs several important validation checks to ensure the worker is in a clean state - it must be a parallel worker process with no existing shared registry attachment and no local record types in its cache. It then attaches to both hash tables (record_table and typmod_table) using their handles stored in the registry, and configures the CurrentSession state to redirect future record type operations to the shared registry.

A cleanup hook is registered to ensure proper detachment when the worker process exits or the DSM segment is detached.

## Parameters / Member Variables
- : Pointer to the SharedRecordTypmodRegistry structure in shared memory to attach to

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - dshash_attach
  - on_dsm_detach
  - [shared_record_typmod_registry_detach](../s/shared_record_typmod_registry_detach.md)
- Called from (representative examples):
  - [AttachSession](../A/AttachSession.md)

## Notes and Other Information
- Must be called by parallel worker processes only (asserts IsParallelWorker())
- Requires that CurrentSession is properly initialized with segment and area
- Cannot be called if already attached to a shared registry
- Worker must have no local record types (NextRecordTypmod == 0) to avoid typmod conflicts
- Currently incompatible with worker recycling due to potential record-typmod state persistence
- Uses TopMemoryContext for hash table attachment to ensure proper memory management
- Registers the same detach hook as the leader, though future versions may differentiate
- Once attached, the worker will use shared registry until process exit or detachment