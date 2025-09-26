# dsa_detach

## Location
src/backend/utils/mmgr/dsa.c: 1952 - 1977

## Overview
Detaches from a dynamic shared area (DSA) that was either created or attached to by the current process, cleaning up all associated dynamic shared memory segments.

## Definition

```c
void
dsa_detach(dsa_area *area)
```
## Detailed Description
The  function performs cleanup operations when a process no longer needs access to a dynamic shared area. It iterates through all segments in the area and detaches from each associated dynamic shared memory (DSM) segment using . The function then frees the backend-local area object.

Importantly, this function only handles "detaching" (disconnecting from DSM segments) and does not handle "releasing" (adjusting reference counts). This separation exists because client code might not always call  due to error paths, and using detach hooks on individual segments would be too late to detach other segments without risking leak warnings in non-error scenarios.

## Parameters / Member Variables
- : Pointer to the  structure representing the dynamic shared area to detach from

## Dependencies
- Functions called/Symbols referenced:
  - dsm_detach
  - pfree
- Called from (representative examples):
  - DetachSession
  - TidStoreDetach
  - TidStoreDestroy
  - ExecParallelCleanup
  - ParallelQueryMain
  - pgstat_detach_shmem

## Notes and Other Information
- The function detaches from all segments by iterating through the  array up to 
- Only segments that are not NULL are detached from
- The separation between detaching and releasing reference counts is a deliberate design choice to handle error scenarios gracefully
- After detaching from all segments, the function frees the area object itself using 
- This function is commonly used in parallel query cleanup and various shared memory management scenarios