# ginInitBA

## Location
[src/backend/access/gin/ginbulk.c:109-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbulk.c#L109-L127)

## Overview
Initializes a BuildAccumulator structure for GIN index bulk loading operations by setting up the red-black tree and memory allocation state.

## Definition
```c
void ginInitBA(BuildAccumulator *accum)
```

## Detailed Description
This function initializes a BuildAccumulator structure used during GIN index bulk loading. It resets the memory allocation counters and creates a red-black tree configured with appropriate callback functions for GIN entry accumulation. The tree is set up with cmpEntryAccumulator for comparison, ginCombineData for merging duplicate entries, and ginAllocEntryAccumulator for memory allocation. The ginstate field is intentionally left unset and must be initialized separately by the caller.

## Parameters / Member Variables
- `accum`: Pointer to BuildAccumulator structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - [rbt_create](../r/rbt_create.md)
  - [cmpEntryAccumulator](../c/cmpEntryAccumulator.md)
  - ginCombineData
  - [ginAllocEntryAccumulator](ginAllocEntryAccumulator.md)
  - [BuildAccumulator](../B/BuildAccumulator.md) (struct)
  - [GinEntryAccumulator](../G/GinEntryAccumulator.md) (struct)
- Called from (representative examples):
  - [ginInsertCleanup](ginInsertCleanup.md)
  - [ginBuildCallback](ginBuildCallback.md)
  - [ginbuild](ginbuild.md)

## Notes and Other Information
- The ginstate field is intentionally not set and must be initialized by the caller
- Sets up a red-black tree optimized for GIN entry accumulation during bulk loading
- No free function is provided to the red-black tree as memory is managed in bulk
- Part of the GIN access method's bulk loading infrastructure