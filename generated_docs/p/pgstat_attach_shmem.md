# pgstat_attach_shmem

## Location
src/backend/utils/activity/pgstat_shmem.c: 218 - 237

## Overview
Function that attaches a backend process to the shared memory statistics system by establishing connections to the DSA and shared hash table.

## Definition

```c
void
pgstat_attach_shmem(void)
```
## Detailed Description
This function connects a backend process to the statistics shared memory structures that were previously initialized by the postmaster. It attaches to the dynamic shared area (DSA) and the shared hash table, establishing the local references needed for the backend to participate in the statistics system. The function ensures these structures persist for the lifetime of the backend by allocating them in TopMemoryContext.

## Parameters / Member Variables
- No parameters (void function)
- No return value

## Dependencies
- Functions called/Symbols referenced:
  - dsa_attach_in_place: Attaches to existing DSA in shared memory
  - dsa_pin_mapping: Pins the DSA mapping to prevent automatic cleanup
  - dshash_attach: Attaches to existing shared hash table
  - MemoryContextSwitchTo: Switches to TopMemoryContext for persistent allocation
- Called from (representative examples):
  - pgstat_initialize: Called during backend statistics initialization

## Notes and Other Information
- Must be called after pgStatLocal.shmem has been set up (contains assertion check)
- Uses TopMemoryContext to ensure DSA and hash table references persist for backend lifetime
- Pins the DSA mapping to prevent it from being automatically cleaned up
- Part of the backend initialization sequence for statistics collection
- The DSA and hash table were previously created by StatsShmemInit in the postmaster