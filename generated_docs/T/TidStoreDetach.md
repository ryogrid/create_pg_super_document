# TidStoreDetach

## Location
src/backend/access/common/tidstore.c: 280 - 297

## Overview
Detaches from a shared TidStore, cleaning up backend-local resources and disconnecting from the shared Dynamic Shared Area (DSA) while leaving the shared data intact for other processes.

## Definition
```c
void TidStoreDetach(TidStore *ts)
```

## Detailed Description
TidStoreDetach is used to cleanly disconnect a backend process from a shared TidStore that was previously attached using TidStoreAttach. The function performs proper cleanup by detaching from the shared radix tree structure, disconnecting from the DSA area, and freeing the backend-local TidStore object. This function only works with shared TidStores and includes an assertion to verify this precondition.

The function ensures that the shared TID data remains available to other processes that may still be attached to the same TidStore, while properly cleaning up all resources specific to the detaching backend process.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object to detach from (must be a shared TidStore)

## Dependencies
- Functions called/Symbols referenced:
  - `Assert`
  - `TidStoreIsShared`
  - `shared_ts_detach`
  - `dsa_detach`
  - `[pfree](../p/pfree.md)`
- Called from (representative examples):
  - `[parallel_vacuum_main](../p/parallel_vacuum_main.md)` (src/backend/commands/vacuumparallel.c:1088)

## Notes and Other Information
- Can only be used with shared TidStores (verified by assertion on TidStoreIsShared)
- The function only cleans up backend-local resources; the shared TID data remains available to other attached processes
- This is the cleanup counterpart to TidStoreAttach
- Primarily used in parallel vacuum operations when worker processes finish their work and need to disconnect
- Does not destroy the shared TidStore itself - other processes may still be using it