# tbm_free_shared_area

## Location
src/backend/nodes/tidbitmap.c: 341 - 376

## Overview
Frees shared iterator state and associated shared memory structures for TID bitmaps in parallel query execution contexts, using reference counting to ensure safe cleanup.

## Definition


## Detailed Description
The `tbm_free_shared_area` function manages the cleanup of shared TID bitmap iterator state in parallel query execution scenarios. It uses atomic reference counting to safely deallocate shared memory structures when they are no longer needed by any parallel worker. The function handles three types of shared structures: the main page table (pagetable), shared pages arrays (spages), and shared chunks arrays (schunks).

Each shared structure maintains a reference count that is atomically decremented. When the count reaches zero, indicating no remaining references, the shared memory is freed from the DSA. This ensures that shared resources are properly cleaned up without interfering with other parallel workers that might still be using them.

## Parameters / Member Variables
- `dsa`: Pointer to the Dynamic Shared Area containing the shared memory structures
- `dp`: DSA pointer to the TBMSharedIteratorState structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - dsa_get_address
  - DsaPointerIsValid
  - pg_atomic_sub_fetch_u32
  - dsa_free
  - TBMSharedIteratorState (struct type)
  - PTEntryArray (struct type)
  - PTIterationArray (struct type)
  - dsa_area (struct type)
  - dsa_pointer (type)
- Called from (representative examples):
  - ExecBitmapHeapReInitializeDSM

## Notes and Other Information
- Uses atomic operations for thread-safe reference count management in parallel contexts
- Only frees shared structures when reference count reaches zero to prevent use-after-free
- Handles three distinct shared memory areas: pagetable, spages, and schunks
- Always frees the iterator state itself (dp) regardless of reference counts on sub-structures
- Critical for preventing memory leaks in parallel bitmap heap scans
- Works exclusively with DSA (Dynamic Shared Area) allocated memory, not regular memory contexts
- Reference counting ensures safe cleanup even when multiple parallel workers are involved