# _SPI_save_plan

## Location
[src/backend/executor/spi.c:3209-3279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3209-L3279)

## Overview
_SPI_save_plan creates a permanent "saved" copy of a SPI plan by duplicating it into CacheMemoryContext where it persists beyond the current transaction and connection.

## Definition

```c
static SPIPlanPtr
_SPI_save_plan(SPIPlanPtr plan)
```
## Detailed Description
This function creates a fully independent, persistent copy of a SPI plan that will survive beyond the current transaction and SPI connection. Unlike _SPI_make_plan_non_temp which moves a plan to procedure context, this function creates a completely new copy in the global CacheMemoryContext.

The process involves several phases: creating a temporary memory context for the copying process, duplicating the SPIPlan structure and all its metadata, making deep copies of all CachedPlanSource entries using CopyCachedPlan, and finally marking the plan as saved and reparenting it to CacheMemoryContext for permanent storage.

The function ensures atomicity by performing all potentially failing operations (memory allocations and copies) before marking the plan as saved and reparenting to the cache context. This prevents partial states that could lead to memory leaks or corruption.

## Parameters / Member Variables
- : The SPIPlan to be saved. Must not be a one-shot plan, as those cannot be saved for reuse.

## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](SPIPlanPtr.md): Type definition for SPI plan pointers
  - AllocSetContextCreate: Creates temporary memory context for the copying process
  - ALLOCSET_SMALL_SIZES: Memory context size parameters for small allocations
  - [_SPI_plan](_SPI_plan.md): The actual SPIPlan structure type
  - _SPI_PLAN_MAGIC: Magic number for plan validation
  - [CachedPlanSource](../C/CachedPlanSource.md): Structure representing cached execution plans
  - [CopyCachedPlan](../C/CopyCachedPlan.md): Creates deep copies of cached plan sources
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md): Reparents the plan context to CacheMemoryContext
  - [SaveCachedPlan](SaveCachedPlan.md): Marks cached plan sources as saved
  - EphemeralNamedRelation: Referenced in the same source region (context reference)
- Called from (representative examples):
  - [SPI_saveplan](SPI_saveplan.md): Public API function for saving SPI plans

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Creates complete deep copies rather than transferring ownership like _SPI_make_plan_non_temp
- Saved plans persist in CacheMemoryContext across transactions and connections
- The function includes an assertion to prevent saving one-shot plans
- Uses a two-phase approach: copy everything first, then atomically mark as saved and reparent
- The atomic final phase prevents memory leaks if the operation fails partway through
- Saved plans can be reused multiple times and shared across different SPI connections
- Essential for implementing persistent prepared statements in PostgreSQL