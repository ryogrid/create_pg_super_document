# parallel_vacuum_reset_dead_items

## Location
[src/backend/commands/vacuumparallel.c:473-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L473-L497)

## Overview
Clears all dead tuple identifiers from the shared TidStore and recreates it with fresh memory, effectively resetting the dead items storage for continued parallel vacuum operations.

## Definition

```c
void
parallel_vacuum_reset_dead_items(ParallelVacuumState *pvs)
```
## Detailed Description
This function performs a complete reset of the shared dead items storage by:

1. **Memory Cleanup**: Destroys the current TidStore, which frees all allocated DSA (Dynamic Shared Area) segments and returns memory to the operating system
2. **Storage Recreation**: Creates a new shared TidStore with the same memory limitations as the previous one
3. **Handle Updates**: Updates the DSA handle and TidStore handle in the shared state to point to the newly created storage
4. **Counter Reset**: Resets the item count to zero in the dead items information structure

This operation is typically performed between vacuum phases when the dead items storage needs to be cleared but parallel vacuum operations will continue.

## Parameters / Member Variables
- `*pvs`: Pointer to the parallel vacuum state structure containing the shared TidStore and metadata
## Dependencies
- Functions called/Symbols referenced:
  -  - Destroys the existing shared TidStore
  -  - Creates a new shared TidStore with specified memory limit
  -  - Gets the DSA from the new TidStore
  -  - Gets the handle for the new TidStore
  -  - Gets the DSA handle for sharing across processes
  -  - Lightweight lock tranche identifier
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2914)

## Notes and Other Information
- Function is located at src/backend/commands/vacuumparallel.c:473-497
- Essential for memory management during multi-phase vacuum operations
- Maintains the same memory limit () as the original TidStore
- Updates both the DSA handle and TidStore handle to ensure all parallel workers can access the new storage
- The reset operation is atomic from the perspective of the parallel vacuum coordination system
- Critical for preventing memory exhaustion during large vacuum operations that process tables in multiple passes