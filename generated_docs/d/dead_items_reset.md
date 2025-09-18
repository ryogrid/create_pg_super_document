# dead_items_reset

## Location
src/backend/access/heap/vacuumlazy.c: 2910 - 2929

## Overview
`dead_items_reset` is a static utility function that clears all collected dead tuple identifiers from the vacuum state, preparing the dead items collection for reuse during VACUUM operations.

## Definition
```c
static void dead_items_reset(LVRelState *vacrel)
```

## Detailed Description
This function provides a mechanism to reset the dead items collection during VACUUM operations, typically called between different phases of vacuum processing. It handles both parallel and non-parallel vacuum scenarios differently. In parallel vacuum mode, it delegates to the parallel vacuum subsystem to coordinate the reset across all worker processes. In non-parallel mode, it destroys the existing TidStore and creates a new one with the same memory limitations, then resets the item counter to zero.

The function is essential for vacuum operations that need to process dead items in batches, allowing the same dead items collection to be reused multiple times within a single vacuum run.

## Parameters / Member Variables
- `vacrel`: Pointer to LVRelState structure containing vacuum operation state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - ParallelVacuumIsActive
  - [parallel_vacuum_reset_dead_items](../p/parallel_vacuum_reset_dead_items.md)
  - [TidStoreDestroy](../T/TidStoreDestroy.md)
  - [TidStoreCreateLocal](../T/TidStoreCreateLocal.md)
- Called from (representative examples):
  - [lazy_vacuum](../l/lazy_vacuum.md)

## Notes and Other Information
- This is a static function, only accessible within vacuumlazy.c
- The function preserves the original memory limit (`max_bytes`) when recreating the TidStore
- In parallel vacuum mode, coordination is handled by the parallel vacuum subsystem
- The `true` parameter to TidStoreCreateLocal indicates the store should track memory usage
- Resetting allows vacuum to reuse the same dead items collection for multiple cleanup cycles