# parallel_vacuum_get_dead_items

## Location
[src/backend/commands/vacuumparallel.c:465-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L465-L472)

## Overview
Provides access to the shared dead items storage and metadata information used for coordinating dead tuple tracking across parallel vacuum workers.

## Definition

```c
TidStore *
parallel_vacuum_get_dead_items(ParallelVacuumState *pvs, VacDeadItemsInfo **dead_items_info_p)
```
## Detailed Description
This function serves as an accessor to retrieve both the shared TidStore containing dead tuple identifiers and the associated metadata about the dead items storage. It provides a simple interface for vacuum operations to access the centralized dead tuple tracking system that was set up during parallel vacuum initialization.

The function returns the TidStore directly and populates the provided pointer with the address of the dead items information structure, which contains metadata such as memory limits and usage statistics.

## Parameters / Member Variables
- `*pvs`: Pointer to the parallel vacuum state structure containing shared resources
- `**dead_items_info_p`: Output parameter that will point to the dead items information structure containing metadata about the TidStore (memory limits, usage, etc.)
## Dependencies
- Functions called/Symbols referenced:
  -  - Structure type for dead items metadata
  -  - Main parallel vacuum coordination structure
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2865)

## Notes and Other Information
- Function is located at src/backend/commands/vacuumparallel.c:465-472
- Simple accessor function with no complex logic or error handling
- Provides unified access to both the TidStore storage and its associated metadata
- Used by vacuum operations to coordinate dead tuple tracking across multiple parallel workers
- The returned TidStore is shared across all parallel workers and must be accessed with appropriate synchronization