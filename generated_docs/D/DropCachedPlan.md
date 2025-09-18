# DropCachedPlan

## Location
src/backend/utils/cache/plancache.c: 526 - 554

## Overview
DropCachedPlan destroys a cached plan by cleaning up the CachedPlanSource and removing it from global tracking structures while safely handling reference counting for any active plans.

## Definition


## Detailed Description
DropCachedPlan safely destroys a CachedPlanSource by removing it from the global saved plan list (if saved), releasing any associated generic plans, and freeing the memory context containing all subsidiary data. The function uses reference counting to ensure that any CachedPlan objects still in use are not immediately destroyed but are marked for cleanup when their reference count reaches zero. This design handles the case where DropCachedPlan is called while plans derived from this source are still actively being executed.

For oneshot plans, the function only performs cleanup operations that don't involve freeing memory contexts, since the caller retains responsibility for memory management in that case.

## Parameters / Member Variables
- : The CachedPlanSource to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - CACHEDPLANSOURCE_MAGIC
  - [dlist_delete](../d/dlist_delete.md)
  - [ReleaseGenericPlan](../R/ReleaseGenericPlan.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)

- Called from (representative examples):
  - [DropPreparedStatement](DropPreparedStatement.md) (src/backend/commands/prepare.c:526)
  - [DropAllPreparedStatements](DropAllPreparedStatements.md) (src/backend/commands/prepare.c:551)
  - [SPI_freeplan](../S/SPI_freeplan.md) (src/backend/executor/spi.c:1037)
  - [drop_unnamed_stmt](../d/drop_unnamed_stmt.md) (src/backend/tcop/postgres.c:2885)

## Notes and Other Information
- The function only destroys the CachedPlanSource, not necessarily the associated CachedPlan objects
- Reference counting ensures that active plans remain valid until no longer in use
- Saved plans are removed from the global tracking list before destruction
- The magic number is cleared to prevent reuse of the destroyed structure
- Memory context deletion is skipped for oneshot plans since callers manage that memory
- The function safely handles concurrent access to plans during destruction