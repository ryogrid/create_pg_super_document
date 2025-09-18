# PortalCreateHoldStore

## Location
src/backend/utils/mmgr/portalmem.c: 331 - 370

## Overview
Creates the tuple store infrastructure for a portal that needs to hold cursor results across transactions, establishing both the memory context and tuple storage mechanism for cursor data persistence.

## Definition
```c
void PortalCreateHoldStore(Portal portal)
```

## Detailed Description
PortalCreateHoldStore initializes the storage infrastructure required for a portal to hold cursor results beyond the current transaction. This function is essential for implementing HOLD cursors in PostgreSQL, which allow cursor results to persist even after the transaction that created them commits.

The function creates two key components:
1. A dedicated memory context (holdContext) that is a child of TopPortalContext rather than the portal's own context, ensuring it survives transaction boundaries
2. A tuple store (holdStore) configured for cross-transaction temporary files with optional scrolling support

The tuple store is configured based on the cursor options - enabling random access only if the cursor requires scrolling functionality. This optimization saves memory and improves performance for forward-only cursors.

## Parameters / Member Variables
- `portal`: Target portal that must not already have hold store infrastructure (holdContext, holdStore, and holdSnapshot must all be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - tuplestore_begin_heap
  - Portal (type)
  - ALLOCSET_DEFAULT_SIZES
  - CURSOR_OPT_SCROLL
- Called from (representative examples):
  - FillPortalStore
  - HoldPortal

## Notes and Other Information
- The holdContext is intentionally NOT a child of the portal's portalContext to ensure survival across transaction boundaries
- Tuple store is configured for cross-transaction temporary files, allowing data to persist beyond transaction commit
- Random access (scrolling) is only enabled if CURSOR_OPT_SCROLL is set in the portal's cursor options
- Uses work_mem for tuple store size management
- The function includes a TODO comment questioning whether maintenance_work_mem should be used instead of work_mem
- Critical for HOLD cursor functionality where cursor results must survive transaction commits
- All three hold-related fields (holdContext, holdStore, holdSnapshot) must be NULL before calling this function